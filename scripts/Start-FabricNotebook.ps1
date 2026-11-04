[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json'),
    [string] $PackagePath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\streaming_pipeline.ipynb'),
    [string] $RunStatePath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\notebook-run.json'),
    [ValidateRange(1, 30)] [int] $StartTimeoutMinutes = 10
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
$notebookPackage = Get-Content -LiteralPath $PackagePath -Raw | ConvertFrom-Json
$source = [string]::Join('', $notebookPackage.cells[0].source)
$runMatch = [regex]::Match($source, 'RUN_ID\s*=\s*["'']([0-9a-fA-F-]{36})["'']')
if (-not $runMatch.Success) { throw 'Packaged notebook does not contain a valid injected RUN_ID.' }
$packageRunId = ([guid]::Parse($runMatch.Groups[1].Value)).ToString()

$account = & az account show --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or
    $account.id -ne '0a0cf761-9d5d-4862-9165-7b69c188a0ee' -or
    $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527') {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}
$token = & az account get-access-token --resource 'https://api.fabric.microsoft.com' `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'spark-cli'
}

function Invoke-FabricRequest {
    param(
        [Parameter(Mandatory)][ValidateSet('Get', 'Post')][string] $Method,
        [Parameter(Mandatory)][string] $Uri
    )
    try {
        return Invoke-WebRequest -Method $Method -Uri $Uri -Headers $headers `
            -ContentType 'application/json' -ErrorAction Stop
    }
    catch {
        throw "Fabric request failed ($Method $Uri): $($_.Exception.Message)"
    }
}

function Get-FabricPages {
    param([Parameter(Mandatory)][string] $Uri)
    $results = [System.Collections.Generic.List[object]]::new()
    $next = $Uri
    while ($next) {
        $page = (Invoke-FabricRequest -Method Get -Uri $next).Content | ConvertFrom-Json
        foreach ($item in @($page.value)) { $results.Add($item) }
        if ($page.continuationUri) {
            $next = [string]$page.continuationUri
        }
        elseif ($page.continuationToken) {
            $separator = if ($Uri.Contains('?')) { '&' } else { '?' }
            $next = "$Uri$separator" + 'continuationToken=' +
                [Uri]::EscapeDataString([string]$page.continuationToken)
        }
        else {
            $next = $null
        }
    }
    return @($results)
}

$workspaces = Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/workspaces'
$workspaceMatches = @($workspaces | Where-Object displayName -eq $inventory.workspaceName)
if ($workspaceMatches.Count -ne 1 -or $workspaceMatches[0].id -ne $inventory.workspaceId) {
    throw 'The fixed-name Fabric workspace does not match the saved inventory.'
}
$workspace = (Invoke-FabricRequest -Method Get `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)").Content | ConvertFrom-Json
if ($workspace.capacityId -ne $inventory.capacityId -or
    $workspace.capacityAssignmentProgress -ne 'Completed') {
    throw 'Workspace capacity assignment is not complete for the approved Fabric capacity.'
}
$capacities = Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/capacities'
$capacity = @($capacities | Where-Object {
    $_.id -eq $inventory.capacityId -and $_.displayName -eq $inventory.capacityName
})
if ($capacity.Count -ne 1 -or $capacity[0].sku -ne 'F8' -or $capacity[0].state -ne 'Active') {
    throw 'The approved Fabric F8 capacity is not active; refusing to start Spark.'
}
$notebooks = Get-FabricPages -Uri `
    "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/items?type=Notebook"
$notebookMatches = @($notebooks | Where-Object {
    $_.displayName -eq $inventory.notebookName -and $_.id -eq $inventory.notebookId
})
if ($notebookMatches.Count -ne 1) { throw 'The fixed-name notebook does not match the saved inventory.' }

$base = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/items/$($inventory.notebookId)"
$jobsUri = "$base/jobs/instances"
$jobsResponse = Invoke-FabricRequest -Method Get -Uri $jobsUri
$jobs = @((($jobsResponse.Content | ConvertFrom-Json).value))
$now = [DateTime]::UtcNow
$recent = @($jobs | Where-Object {
    $startValue = if ($_.startTimeUtc) { $_.startTimeUtc } else { $_.createdTimeUtc }
    if (-not $startValue) { return $_.status -in @('NotStarted', 'Running', 'InProgress') }
    try { return ([DateTime]::Parse([string]$startValue).ToUniversalTime() -ge $now.AddMinutes(-5)) }
    catch { return $false }
} | Sort-Object {
    $startValue = if ($_.startTimeUtc) { $_.startTimeUtc } else { $_.createdTimeUtc }
    if ($startValue) { [DateTime]::Parse([string]$startValue).ToUniversalTime() }
    else { [DateTime]::MinValue }
} -Descending)

$reused = $false
$job = $null
if ($recent.Count -gt 0) {
    $job = $recent[0]
    $reused = $true
    $jobId = if ($job.jobInstanceId) { [string]$job.jobInstanceId } else { [string]$job.id }
    if ([string]::IsNullOrWhiteSpace($jobId)) {
        throw 'A recent notebook job exists but its job ID is missing; refusing a duplicate submission.'
    }
}
else {
    $startUri = "$jobsUri`?jobType=RunNotebook"
    try {
        $response = Invoke-FabricRequest -Method Post -Uri $startUri
    }
    catch {
        $originalError = $_.Exception.Message
        $afterFailure = Invoke-FabricRequest -Method Get -Uri $jobsUri
        $possibleJob = @((($afterFailure.Content | ConvertFrom-Json).value) | Where-Object {
            $startValue = if ($_.startTimeUtc) { $_.startTimeUtc } else { $_.createdTimeUtc }
            if (-not $startValue) { return $false }
            try { return ([DateTime]::Parse([string]$startValue).ToUniversalTime() -ge $now.AddMinutes(-1)) }
            catch { return $false }
        } | Sort-Object startTimeUtc -Descending)
        if ($possibleJob.Count -eq 0) { throw $originalError }
        $job = $possibleJob[0]
        $jobId = if ($job.jobInstanceId) { [string]$job.jobInstanceId } else { [string]$job.id }
        $reused = $true
    }
    if (-not $job) {
        $location = [string]$response.Headers['Location']
        $jobId = $null
        if ($location) {
            $jobId = [Uri]::UnescapeDataString(([Uri]$location).Segments[-1].TrimEnd('/'))
        }
        if (-not $jobId -and $response.Content) {
            $createdBody = $response.Content | ConvertFrom-Json
            $jobId = if ($createdBody.jobInstanceId) { [string]$createdBody.jobInstanceId } else { [string]$createdBody.id }
        }
        if (-not $jobId) {
            throw 'Fabric accepted the notebook run without a discoverable job ID; no retry was attempted.'
        }
    }
}

$state = [ordered]@{
    workspaceId = [string]$inventory.workspaceId
    notebookId = [string]$inventory.notebookId
    notebookName = [string]$inventory.notebookName
    jobInstanceId = [string]$jobId
    packageRunId = $packageRunId
    reusedRecentJob = $reused
    submittedAtUtc = [DateTime]::UtcNow.ToString('o')
}
$stateParent = Split-Path -Parent $RunStatePath
New-Item -ItemType Directory -Force -Path $stateParent | Out-Null
[IO.File]::WriteAllText(
    $RunStatePath,
    ($state | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false)
)

$jobUri = "$base/jobs/instances/$jobId"
$deadline = [DateTime]::UtcNow.AddMinutes($StartTimeoutMinutes)
do {
    $jobResponse = Invoke-FabricRequest -Method Get -Uri $jobUri
    $job = $jobResponse.Content | ConvertFrom-Json
    if ($job.status -in @('Running', 'InProgress')) { break }
    if ($job.status -in @('Completed', 'Failed', 'Cancelled')) {
        throw "Notebook job reached terminal state '$($job.status)' before Spark was ready."
    }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw "Notebook job did not enter Running before the $StartTimeoutMinutes-minute startup deadline."
    }
    Start-Sleep -Seconds 10
} while ($true)

Write-Output "Notebook job is Running. jobInstanceId=$jobId packageRunId=$packageRunId reusedRecentJob=$reused"
Write-Output "Job state saved to $RunStatePath. Independently verify the notebook READY audit record before starting the generator."
