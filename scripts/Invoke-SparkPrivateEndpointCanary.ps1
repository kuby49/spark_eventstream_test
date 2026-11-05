[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json'),
    [string] $ManagedEndpointStatePath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-managed-private-endpoints.json'),
    [string] $RunStatePath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\spark-private-route-canary-run.json'),
    [string] $ResultPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\spark-private-route-canary.json')
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$root = Split-Path -Parent $PSScriptRoot
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
$endpointState = Get-Content -LiteralPath $ManagedEndpointStatePath -Raw | ConvertFrom-Json
$account = & az account show --subscription $subscriptionId --only-show-errors --output json |
    ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}
if ($endpointState.workspaceId -ne $inventory.workspaceId) {
    throw 'Managed endpoint evidence belongs to a different Fabric workspace.'
}
$endpointCapturedAt = [DateTimeOffset]::Parse(
    [string]$endpointState.capturedAtUtc,
    [Globalization.CultureInfo]::InvariantCulture,
    [Globalization.DateTimeStyles]::AssumeUniversal -bor [Globalization.DateTimeStyles]::AdjustToUniversal
)
if ($endpointCapturedAt -lt [DateTimeOffset]::UtcNow.AddHours(-1) -or
    $endpointCapturedAt -gt [DateTimeOffset]::UtcNow.AddMinutes(5)) {
    throw 'Managed endpoint status evidence is stale; refresh it before running the Spark canary.'
}

$requiredEndpoints = @(
    @{ name = 'mpe-esehspark-score-api'; subresource = 'sites'; host = 'esehspark-score-api.azurewebsites.net' },
    @{ name = 'mpe-esehsparkkv'; subresource = 'vault'; host = 'esehsparkkv.vault.azure.net' }
)
$verifiedEndpoints = [System.Collections.Generic.List[object]]::new()
foreach ($required in $requiredEndpoints) {
    $matches = @($endpointState.endpoints | Where-Object name -eq $required.name)
    if ($matches.Count -ne 1 -or
        $matches[0].targetSubresourceType -ne $required.subresource -or
        $matches[0].targetFqdn -ne $required.host -or
        $matches[0].provisioningState -ne 'Succeeded' -or
        $matches[0].connectionStatus -ne 'Approved') {
        throw "Required approved Spark managed private endpoint '$($required.name)' is missing or mismatched."
    }
    $verifiedEndpoints.Add([pscustomobject]@{
        name = $matches[0].name
        id = $matches[0].id
        targetPrivateLinkResourceId = $matches[0].targetPrivateLinkResourceId
        targetSubresourceType = $matches[0].targetSubresourceType
        targetFqdn = $required.host
        provisioningState = $matches[0].provisioningState
        connectionStatus = $matches[0].connectionStatus
    })
}

$runId = [guid]::NewGuid()
$restoreRunId = [guid]::NewGuid()
$packageUpdated = $false
$canaryCompleted = $false
$failureType = $null
$restoreFailureType = $null
$jobId = $null
$jobStatus = $null
$packagePath = Join-Path $root '.deployment\streaming_pipeline.ipynb'
$runStateParent = Split-Path -Parent $RunStatePath
New-Item -ItemType Directory -Force -Path $runStateParent | Out-Null

$packageArguments = @{
    WorkspaceId = [guid]$inventory.workspaceId
    LakehouseId = [guid]$inventory.lakehouseId
    KeyVaultUrl = 'https://esehsparkkv.vault.azure.net/'
    KustoEndpoint = [string]$inventory.kustoQueryEndpoint
    FunctionUrl = 'https://esehspark-score-api.azurewebsites.net'
}
try {
    $packageUpdated = $true
    & (Join-Path $PSScriptRoot 'Set-FabricNotebookPackage.ps1') `
        @packageArguments -RunId $runId -RunMode 'private-canary'
    $packagedNotebook = Get-Content -LiteralPath $packagePath -Raw | ConvertFrom-Json
    $packagedSource = [string]::Join('', $packagedNotebook.cells[0].source)
    if ($packagedSource -notmatch 'RUN_MODE = "private-canary"' -or
        $packagedSource -notmatch [regex]::Escape($runId.ToString())) {
        throw 'Packaged Spark notebook does not contain the expected private-canary mode and run ID.'
    }

    & (Join-Path $PSScriptRoot 'Start-FabricNotebook.ps1') `
        -InventoryPath $InventoryPath `
        -PackagePath $packagePath `
        -RunStatePath $RunStatePath `
        -StartTimeoutMinutes 10
    $runState = Get-Content -LiteralPath $RunStatePath -Raw | ConvertFrom-Json
    if ($runState.packageRunId -ne $runId.ToString()) {
        throw 'Notebook start reused a job for a different package run ID; refusing to treat it as a canary.'
    }
    $jobId = [string]$runState.jobInstanceId
    if ([string]::IsNullOrWhiteSpace($jobId)) {
        throw 'Spark private-route canary job has no recorded instance ID.'
    }

    $fabricToken = & az account get-access-token --resource 'https://api.fabric.microsoft.com' `
        --query accessToken -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($fabricToken)) {
        throw 'Unable to acquire the Fabric API token for notebook job monitoring.'
    }
    $jobHeaders = @{
        Authorization = "******"
        'x-ms-fabric-skill' = 'spark-cli'
    }
    $jobUri = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/items/$($inventory.notebookId)/jobs/instances/$jobId"
    $deadline = [DateTime]::UtcNow.AddMinutes(25)
    do {
        $jobResponse = Invoke-WebRequest -Method Get -Uri $jobUri -Headers $jobHeaders -ErrorAction Stop
        $job = $jobResponse.Content | ConvertFrom-Json
        $jobStatus = [string]$job.status
        if ($jobStatus -in @('Completed', 'Succeeded')) {
            $canaryCompleted = $true
            break
        }
        if ($jobStatus -in @('Failed', 'Cancelled')) {
            throw "Spark private-route canary job reached terminal status '$jobStatus'."
        }
        if ([DateTime]::UtcNow -ge $deadline) {
            throw "Spark private-route canary did not complete before the 25-minute deadline; last status '$jobStatus'."
        }
        Start-Sleep -Seconds 10
    } while ($true)
}
catch {
    $failureType = $_.Exception.GetType().Name
}
finally {
    if ($packageUpdated) {
        try {
            & (Join-Path $PSScriptRoot 'Set-FabricNotebookPackage.ps1') `
                @packageArguments -RunId $restoreRunId -RunMode 'streaming'
            $restoredNotebook = Get-Content -LiteralPath $packagePath -Raw | ConvertFrom-Json
            $restoredSource = [string]::Join('', $restoredNotebook.cells[0].source)
            if ($restoredSource -notmatch 'RUN_MODE = "streaming"' -or
                $restoredSource -match 'RUN_MODE = "private-canary"') {
                throw 'Restored notebook package is not in streaming mode.'
            }
        }
        catch {
            $restoreFailureType = $_.Exception.GetType().Name
        }
    }
}

$state = [ordered]@{
    workspaceId = [string]$inventory.workspaceId
    notebookId = [string]$inventory.notebookId
    runId = $runId.ToString()
    jobInstanceId = $jobId
    jobStatus = $jobStatus
    canaryCompleted = $canaryCompleted
    functionKeySecretRead = $canaryCompleted
    functionMpeTlsScoringVerified = $canaryCompleted
    privateDnsAssertionsPassed = $canaryCompleted
    auditDeltaWriteAndReadbackPassed = $canaryCompleted
    endpoints = @($verifiedEndpoints)
    streamingDefinitionRestored = ($packageUpdated -and -not $restoreFailureType)
    restoreRunId = if ($packageUpdated -and -not $restoreFailureType) { $restoreRunId.ToString() } else { $null }
    failureType = $failureType
    restoreFailureType = $restoreFailureType
    completedAtUtc = [DateTime]::UtcNow.ToString('o')
}
[IO.File]::WriteAllText(
    $ResultPath,
    ($state | ConvertTo-Json -Depth 20),
    [Text.UTF8Encoding]::new($false)
)
if ($failureType -or $restoreFailureType -or -not $canaryCompleted) {
    throw "Spark private endpoint canary failed or did not restore the streaming notebook. Redacted state: $ResultPath"
}
Write-Output "Spark private endpoint canary completed and the streaming notebook definition was restored. Evidence: $ResultPath"
