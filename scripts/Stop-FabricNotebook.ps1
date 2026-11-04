[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json'),
    [string] $RunStatePath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\notebook-run.json'),
    [ValidateRange(1, 60)] [int] $TimeoutMinutes = 30
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
$runState = Get-Content -LiteralPath $RunStatePath -Raw | ConvertFrom-Json
if ($runState.workspaceId -ne $inventory.workspaceId -or
    $runState.notebookId -ne $inventory.notebookId -or
    [string]::IsNullOrWhiteSpace([string]$runState.jobInstanceId)) {
    throw 'Saved notebook run state does not match the approved Fabric inventory.'
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
$base = "https://api.fabric.microsoft.com/v1/workspaces/$($runState.workspaceId)/items/$($runState.notebookId)/jobs/instances/$($runState.jobInstanceId)"

function Get-Job {
    try {
        return Invoke-RestMethod -Method Get -Uri $base -Headers $headers -ErrorAction Stop
    }
    catch {
        throw "Unable to read notebook job $($runState.jobInstanceId): $($_.Exception.Message)"
    }
}

$job = Get-Job
if ($job.status -notin @('Completed', 'Failed', 'Cancelled')) {
    try {
        Invoke-WebRequest -Method Post -Uri "$base/cancel" -Headers $headers `
            -ContentType 'application/json' -ErrorAction Stop | Out-Null
    }
    catch {
        throw "Notebook cancellation request failed for job $($runState.jobInstanceId): $($_.Exception.Message)"
    }
}

$deadline = [DateTime]::UtcNow.AddMinutes($TimeoutMinutes)
do {
    $job = Get-Job
    if ($job.status -in @('Completed', 'Failed', 'Cancelled')) { break }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw "Notebook job did not reach a terminal state within $TimeoutMinutes minutes; retain the capacity and investigate."
    }
    Start-Sleep -Seconds 10
} while ($true)

Write-Output "Notebook job reached terminal state '$($job.status)' (jobInstanceId=$($runState.jobInstanceId))."
