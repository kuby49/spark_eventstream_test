[CmdletBinding()]
param([switch] $VerifyOutboundProtection)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$root = Split-Path -Parent $PSScriptRoot
$directory = Join-Path $root '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
$runId = [guid]::NewGuid().ToString()
$deadline = [DateTime]::UtcNow.AddMinutes(12)
$account = & az account show --subscription $subscription --output json --only-show-errors | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscription -or
    $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$token = & az account get-access-token --subscription $subscription `
    --resource https://api.fabric.microsoft.com --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Fabric token acquisition failed.' }
$headers = @{ Authorization = "Bearer $token"; 'x-ms-fabric-skill' = 'spark-cli' }
function Request {
    param([string] $Method, [string] $Uri, [object] $Body)
    $args = @{ Method=$Method; Uri=$Uri; Headers=$headers; TimeoutSec=60 }
    if ($null -ne $Body) { $args.ContentType='application/json'; $args.Body=($Body | ConvertTo-Json -Depth 40 -Compress) }
    return Invoke-WebRequest @args
}
function Await-Operation {
    param($Response)
    if ($Response.StatusCode -ne 202) { return }
    $url = [string]@($Response.Headers.Location)[0]
    if (-not $url) { throw 'Missing LRO Location.' }
    do {
        $wait = 5
        $retry = [string]@($Response.Headers['Retry-After'])[0]
        if ($retry) { [int]::TryParse($retry, [ref]$wait) | Out-Null }
        Start-Sleep -Seconds ([Math]::Max(1, [Math]::Min(30, $wait)))
        $Response = Request Get $url
        $status = $Response.Content | ConvertFrom-Json
        if ($status.status -in @('Failed','Cancelled')) { throw "Fabric operation $($status.status)." }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Diagnostic operation exceeded time budget.' }
    } while ($status.status -ne 'Succeeded')
}
$base = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)"
$workspace = (Request Get $base).Content | ConvertFrom-Json
if ($workspace.capacityId -ne $inventory.capacityId) { throw 'Workspace capacity changed.' }
if ($VerifyOutboundProtection) {
    $diagnosticPolicy = (Request Get "$base/networking/communicationPolicy").Content | ConvertFrom-Json
    if ($diagnosticPolicy.outbound.publicAccessRules.defaultAction -ne 'Deny' -or
        $diagnosticPolicy.inbound.publicAccessRules.defaultAction -ne 'Allow') {
        throw 'Outbound diagnostic requires observed Deny and preserved inbound Allow.'
    }
}
$capacityId = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
$capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $capacity.sku.name -ne 'F8') { throw 'Unable to verify exact F8.' }
$jobUri = $null
$id = $null
try {
if ($capacity.properties.state -eq 'Paused') {
    & az rest --method post --url "https://management.azure.com$capacityId/resume?api-version=2023-11-01" `
        --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'Capacity resume failed.' }
}
do {
    Start-Sleep -Seconds 10
    $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Unable to verify capacity readiness.' }
    if ([DateTime]::UtcNow -gt $deadline) { throw 'Capacity readiness timed out.' }
} while ($capacity.properties.state -ne 'Active')
$source = Get-Content (Join-Path $root 'fabric\notebooks\prerequisite_diagnostic.py') -Raw
$source = $source.Replace('<injected-run-id>', $runId).
    Replace('<injected-workspace-id>', $inventory.workspaceId).
    Replace('<injected-lakehouse-id>', $inventory.lakehouseId).
    Replace('<injected-outbound-deny-probe>', $VerifyOutboundProtection.IsPresent.ToString().ToLowerInvariant())
$package = Get-Content (Join-Path $directory 'streaming_pipeline.ipynb') -Raw | ConvertFrom-Json
$package.cells = @(@{cell_type='code';execution_count=$null;metadata=@{};outputs=@();
    source=@($source -split "`n" | ForEach-Object {"$_`n"})})
$definition = @{format='ipynb';parts=@(@{path='notebook-content.ipynb';payloadType='InlineBase64';
    payload=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($package | ConvertTo-Json -Depth 40)))})}
$name = 'esehspark-prerequisite-diagnostic'
$items = (Request Get "$base/items?type=Notebook").Content | ConvertFrom-Json
$matches = @($items.value | Where-Object displayName -eq $name)
if ($matches.Count -gt 1) { throw 'Ambiguous diagnostic notebook name.' }
if ($matches.Count -eq 0) {
    Await-Operation (Request Post "$base/items" @{displayName=$name;type='Notebook';definition=$definition})
    $items = (Request Get "$base/items?type=Notebook").Content | ConvertFrom-Json
    $matches = @($items.value | Where-Object displayName -eq $name)
}
if ($matches.Count -ne 1) { throw 'Diagnostic notebook creation was not discovered.' }
$id = $matches[0].id
$jobsBase = "$base/items/$id/jobs/instances"
$jobs = (Request Get $jobsBase).Content | ConvertFrom-Json
if (@($jobs.value | Where-Object status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')).Count) {
    throw 'An existing diagnostic job is active; refusing duplicate.'
}
Await-Operation (Request Post "$base/notebooks/$id/updateDefinition" @{definition=$definition})
$state = [ordered]@{runId=$runId;notebookId=$id;workspaceId=$inventory.workspaceId;
    startedAtUtc=[DateTime]::UtcNow.ToString('o');resultRelativePath="Files/diagnostics/$runId.json";
    productionNotebookChanged=$false;vmResumed=$false}
$state | ConvertTo-Json | Set-Content (Join-Path $directory 'staged-diagnostic-run.json') -Encoding utf8
$run = Request Post "$jobsBase`?jobType=RunNotebook"
$jobUri = [string]@($run.Headers.Location)[0]
if (-not $jobUri) { throw 'Run accepted without job Location; inspect jobs before any retry.' }
$state.jobUri=$jobUri
$state | ConvertTo-Json | Set-Content (Join-Path $directory 'staged-diagnostic-run.json') -Encoding utf8
do {
    Start-Sleep -Seconds 15
    $job = (Request Get $jobUri).Content | ConvertFrom-Json
    $job | ConvertTo-Json -Depth 30 | Set-Content (Join-Path $directory 'staged-diagnostic-job.json') -Encoding utf8
    if ([DateTime]::UtcNow -gt $deadline) {
        Request Post "$jobUri/cancel" | Out-Null
        throw 'Diagnostic timed out; cancellation requested, verify terminal status before suspension.'
    }
} while ($job.status -notin @('Completed','Failed','Cancelled','Canceled'))
$storageToken = & az account get-access-token --subscription $subscription `
    --resource https://storage.azure.com --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $storageToken) { throw 'Diagnostic storage token unavailable.' }
$resultUri = "https://onelake.dfs.fabric.microsoft.com/$($inventory.workspaceId)/$($inventory.lakehouseId)/$($state.resultRelativePath)"
Invoke-WebRequest -Uri $resultUri -Headers @{Authorization="Bearer $storageToken";'x-ms-version'='2021-06-08'} `
    -OutFile (Join-Path $directory 'staged-diagnostic-results.json') -TimeoutSec 60
Write-Output "Staged diagnostic $($job.status). Result: $($state.resultRelativePath)"
}
finally {
    if ($jobUri) {
        $finalJob = (Request Get $jobUri).Content | ConvertFrom-Json
        if ($finalJob.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')) {
            Request Post "$jobUri/cancel" | Out-Null
            $cancelDeadline = [DateTime]::UtcNow.AddMinutes(2)
            do {
                Start-Sleep -Seconds 10
                $finalJob = (Request Get $jobUri).Content | ConvertFrom-Json
                if ([DateTime]::UtcNow -gt $cancelDeadline) { throw 'Cannot verify diagnostic cancellation; capacity suspension withheld.' }
            } while ($finalJob.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped'))
        }
        $finalJob | ConvertTo-Json -Depth 30 |
            Set-Content (Join-Path $directory 'staged-diagnostic-job.json') -Encoding utf8
    }
    $notebooks = (Request Get "$base/items?type=Notebook").Content | ConvertFrom-Json
    foreach ($notebook in $notebooks.value) {
        $remaining = (Request Get "$base/items/$($notebook.id)/jobs/instances").Content | ConvertFrom-Json
        if (@($remaining.value | Where-Object status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')).Count) {
            throw "Notebook $($notebook.id) still has an active job; capacity suspension withheld."
        }
    }
    & az rest --method post --url "https://management.azure.com$capacityId/suspend?api-version=2023-11-01" `
        --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'Diagnostic cleanup: capacity suspension failed.' }
    $shutdownDeadline = [DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $finalCapacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0) { throw 'Diagnostic cleanup: capacity state unreadable.' }
        if ([DateTime]::UtcNow -gt $shutdownDeadline) { throw 'Diagnostic cleanup: suspension verification timed out.' }
    } while ($finalCapacity.properties.state -ne 'Paused')
    $vm = & az vm get-instance-view -g rg_esehspark -n vmesehspark --subscription $subscription `
        --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $vm -ne 'PowerState/deallocated') { throw 'Diagnostic cleanup: VM is not deallocated.' }
    [ordered]@{verifiedAtUtc=[DateTime]::UtcNow.ToString('o');capacityState=$finalCapacity.properties.state;
        vmPowerState=$vm;acceptancePassed=$false;resourcesRetained=$true} | ConvertTo-Json |
        Set-Content (Join-Path $directory 'staged-diagnostic-shutdown.json') -Encoding utf8
}
