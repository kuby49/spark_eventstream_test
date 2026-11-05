[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory, ParameterSetName = 'Acceptance')]
    [ValidateScript({ Test-Path -LiteralPath $_ })] [string] $ShutdownEvidencePath,
    [Parameter(Mandatory, ParameterSetName = 'Idle')]
    [ValidateScript({ Test-Path -LiteralPath $_ })] [string] $IdleShutdownEvidencePath
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$evidencePath = if ($PSCmdlet.ParameterSetName -eq 'Idle') {
    $IdleShutdownEvidencePath
}
else {
    $ShutdownEvidencePath
}
$evidence = Get-Content -LiteralPath $evidencePath -Raw | ConvertFrom-Json
if ($PSCmdlet.ParameterSetName -eq 'Idle') {
    if ($evidence.mode -ne 'approved-idle-shutdown') {
        throw "Idle shutdown evidence must declare mode 'approved-idle-shutdown'."
    }
    $requiredEvidence = @(
        'generatorStopped',
        'notebookStopped',
        'processingDrained',
        'evidenceExported',
        'inventoryCaptured',
        'fullPoliciesCaptured',
        'canaryDiagnosticsCaptured'
    )
    foreach ($field in $requiredEvidence) {
        if ($evidence.$field -isnot [bool] -or -not $evidence.$field) {
            throw "Idle shutdown evidence is incomplete; required proof '$field' is not true."
        }
    }
    foreach ($field in @('activeGeneratorTaskCount', 'activeNotebookJobCount', 'unresolvedCount')) {
        $count = 0L
        if (-not [long]::TryParse([string]$evidence.$field, [ref]$count) -or $count -ne 0) {
            throw "Idle shutdown requires '$field' to be a verified zero."
        }
    }
    $verifiedAt = [DateTimeOffset]::MinValue
    if (-not [DateTimeOffset]::TryParse(
            [string]$evidence.verifiedAtUtc,
            [Globalization.CultureInfo]::InvariantCulture,
            [Globalization.DateTimeStyles]::AssumeUniversal,
            [ref]$verifiedAt
        ) -or $verifiedAt -gt [DateTimeOffset]::UtcNow.AddMinutes(2) -or
        $verifiedAt -lt [DateTimeOffset]::UtcNow.AddMinutes(-15)) {
        throw 'Idle shutdown requires evidence verified within the last 15 minutes.'
    }
}
else {
    $requiredEvidence = @(
        'acceptancePassed',
        'generatorStopped',
        'notebookStopped',
        'processingDrained',
        'rawMirrorPublished',
        'evidenceExported'
    )
    foreach ($field in $requiredEvidence) {
        if ($evidence.$field -isnot [bool] -or -not $evidence.$field) {
            throw "Shutdown evidence is incomplete; required proof '$field' is not true."
        }
    }
    $unresolvedCount = 0L
    if (-not [long]::TryParse([string]$evidence.unresolvedCount, [ref]$unresolvedCount) -or
        $unresolvedCount -ne 0) {
        throw 'Shutdown is blocked until every acceptance message has a terminal outcome.'
    }
}

$accountJson = & az account show --only-show-errors --output json
if ($LASTEXITCODE -ne 0) { throw 'Unable to read the Azure CLI account context.' }
$account = $accountJson | ConvertFrom-Json
if ($account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

$capacityId = & az resource list `
    --subscription $subscriptionId `
    --resource-group $resourceGroup `
    --name 'fabricessparktest8' `
    --resource-type 'Microsoft.Fabric/capacities' `
    --query '[0].id' -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($capacityId)) {
    throw 'Could not discover the approved Fabric capacity by its fixed name; no shutdown action was taken.'
}
$vmId = & az vm show `
    --subscription $subscriptionId `
    --resource-group $resourceGroup `
    --name 'vmesehspark' `
    --query id -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($vmId)) {
    throw 'Could not discover the approved generator VM by its fixed name; no shutdown action was taken.'
}
if (-not $capacityId.EndsWith('/providers/Microsoft.Fabric/capacities/fabricessparktest8', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Discovered capacity ID does not match the approved fixed name.'
}
if (-not $vmId.EndsWith('/providers/Microsoft.Compute/virtualMachines/vmesehspark', [StringComparison]::OrdinalIgnoreCase)) {
    throw 'Discovered VM ID does not match the approved fixed name.'
}

$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
$capacityResourceJson = & az rest --method get --url $capacityUri --subscription $subscriptionId `
    --only-show-errors --output json
if ($LASTEXITCODE -ne 0) { throw 'Unable to read the approved Fabric capacity state.' }
$capacityResource = $capacityResourceJson | ConvertFrom-Json
if ($capacityResource.sku.name -ne 'F8') {
    throw "Refusing to suspend a capacity whose current SKU is '$($capacityResource.sku.name)', not F8."
}
if ($capacityResource.properties.state -notin @('Paused', 'Suspended')) {
    if ($PSCmdlet.ShouldProcess($capacityId, 'Suspend exact F8 capacity')) {
        & az rest --method post --url "https://management.azure.com$capacityId/suspend?api-version=2023-11-01" `
            --subscription $subscriptionId --only-show-errors --output none
        if ($LASTEXITCODE -ne 0) { throw 'F8 suspend failed; the VM was not deallocated.' }
        $deadline = [DateTime]::UtcNow.AddMinutes(10)
        do {
            Start-Sleep -Seconds 15
            $capacityResourceJson = & az rest --method get --url $capacityUri `
                --subscription $subscriptionId --only-show-errors --output json
            if ($LASTEXITCODE -ne 0) { throw 'Unable to verify Fabric capacity suspension.' }
            $capacityResource = $capacityResourceJson | ConvertFrom-Json
            if ($capacityResource.properties.state -in @('Paused', 'Suspended')) { break }
            if ([DateTime]::UtcNow -ge $deadline) {
                throw 'Fabric capacity did not reach a suspended state within 10 minutes; VM deallocation was not attempted.'
            }
        } while ($true)
    }
}

$vmState = & az vm get-instance-view `
    --subscription $subscriptionId `
    --resource-group $resourceGroup `
    --name 'vmesehspark' `
    --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" `
    -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0) { throw 'Unable to read the approved VM power state.' }
if ($vmState -ne 'PowerState/deallocated') {
    if ($PSCmdlet.ShouldProcess($vmId, 'Deallocate exact generator VM')) {
        & az vm deallocate --ids $vmId --subscription $subscriptionId --only-show-errors --output none
        if ($LASTEXITCODE -ne 0) { throw 'VM deallocation failed; all resources have been retained.' }
    }
}

if (-not $WhatIfPreference) {
    $vmState = & az vm get-instance-view --subscription $subscriptionId `
        --resource-group $resourceGroup --name 'vmesehspark' `
        --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" `
        -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $vmState -ne 'PowerState/deallocated') {
        throw 'VM deallocation could not be verified; do not report shutdown complete.'
    }
    [ordered]@{
        verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
        mode = $PSCmdlet.ParameterSetName
        capacityResourceId = $capacityId
        capacityState = $capacityResource.properties.state
        vmResourceId = $vmId
        vmPowerState = $vmState
        acceptancePassed = ($PSCmdlet.ParameterSetName -eq 'Acceptance')
        resourcesRetained = $true
    } | ConvertTo-Json -Depth 10 |
        Set-Content -LiteralPath (Join-Path (Split-Path -Parent $evidencePath) 'shutdown-result.json') -Encoding utf8
}

if ($WhatIfPreference) {
    Write-Output 'Shutdown evidence passed; WhatIf prevented changes to the exact approved capacity and VM.'
}
else {
    if ($PSCmdlet.ParameterSetName -eq 'Idle') {
        Write-Output 'Approved idle-shutdown evidence passed. The exact F8 capacity is suspended and the exact generator VM is deallocated; acceptance remains incomplete and resources/data are retained.'
    }
    else {
        Write-Output 'Approved acceptance shutdown evidence passed. The exact F8 capacity is suspended and the exact generator VM is deallocated; resources and data are retained.'
    }
}
