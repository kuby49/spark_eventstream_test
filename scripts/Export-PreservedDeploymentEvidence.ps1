[CmdletBinding()]
param(
    [ValidateSet('Deny','Allow')][string] $ExpectedOutbound = 'Deny',
    [switch] $DiagnosticPreflight
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json

function Invoke-AzJson {
    param([string[]] $Arguments)
    $value = & az @Arguments --subscription $subscription --only-show-errors -o json
    if ($LASTEXITCODE -ne 0) { throw "Preservation read failed: $($Arguments[0])." }
    $value | ConvertFrom-Json
}

function Save-Evidence {
    param([string] $Name, [object] $Value)
    $Value | ConvertTo-Json -Depth 60 | Set-Content (Join-Path $directory $Name) -Encoding utf8
}

$account = Invoke-AzJson @('account', 'show')
if ($account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') {
    throw 'Preservation reads require the approved MCAP identity.'
}
$capacity = Invoke-AzJson @('rest', '--method', 'get', '--url',
    "https://management.azure.com/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8?api-version=2023-11-01")
$vm = Invoke-AzJson @('vm', 'get-instance-view', '-g', 'rg_esehspark', '-n', 'vmesehspark',
    '--query', '{id:id,statuses:instanceView.statuses}')
$function = Invoke-AzJson @('functionapp', 'show', '-g', 'rg_esehspark', '-n', 'esehspark-score-api',
    '--query', '{id:id,state:state,publicNetworkAccess:publicNetworkAccess,serverFarmId:serverFarmId}')
$resources = Invoke-AzJson @('resource', 'list', '-g', 'rg_esehspark',
    '--query', '[].{id:id,name:name,type:type,location:location}')
$endpoints = Invoke-AzJson @('network', 'private-endpoint', 'list', '-g', 'rg_esehspark',
    '--query', '[].{id:id,name:name,networkInterfaces:networkInterfaces,connections:privateLinkServiceConnections}')

$token = Invoke-AzJson @('account', 'get-access-token', '--resource', 'https://api.fabric.microsoft.com')
try {
    $headers = @{Authorization = "Bearer $($token.accessToken)"; 'x-ms-fabric-skill' = 'spark-cli'}
    $policy = Invoke-RestMethod -Headers $headers -Uri (
        "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/networking/communicationPolicy")
}
finally {
    $token = $null
    $headers = $null
}

Save-Evidence 'final-resource-inventory.json' $resources
Save-Evidence 'final-private-endpoints.json' $endpoints
Save-Evidence 'final-workspace-policy.json' $policy
Save-Evidence 'final-capacity.json' $capacity
Save-Evidence 'final-vm.json' $vm
Save-Evidence 'final-function.json' $function
$vmPower = @($vm.statuses | Where-Object code -like 'PowerState/*').code
$capacityAllowed=if($DiagnosticPreflight){@('Paused','Active')}else{@('Paused')}
$vmAllowed=if($DiagnosticPreflight){@('PowerState/deallocated','PowerState/running')}else{@('PowerState/deallocated')}
if ($capacity.properties.state -notin $capacityAllowed -or $capacity.properties.provisioningState -ne 'Succeeded' -or
    $vmPower -notin $vmAllowed -or $function.publicNetworkAccess -ne 'Disabled' -or
    $policy.inbound.publicAccessRules.defaultAction -ne 'Deny' -or
    $policy.outbound.publicAccessRules.defaultAction -ne $ExpectedOutbound) {
    throw "Final preservation/security state did not match the required idle Deny/$ExpectedOutbound state; readbacks were retained."
}
$summaryName=if($DiagnosticPreflight){'diagnostic-preflight-summary.json'}else{'final-preservation-summary.json'}
Save-Evidence $summaryName ([ordered]@{
    verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
    workspaceId = $inventory.workspaceId
    capacityState = $capacity.properties.state
    capacityProvisioningState = $capacity.properties.provisioningState
    vmPowerState = $vmPower
    workspaceInbound = $policy.inbound.publicAccessRules.defaultAction
    workspaceOutbound = $policy.outbound.publicAccessRules.defaultAction
    functionPublicNetworkAccess = $function.publicNetworkAccess
    diagnosticPreflight = $DiagnosticPreflight.IsPresent
    resourcesRetained = $true
    acceptancePassed = $false
    baselineStatus = 'NotRun'
    latencyAndReconciliation = 'NotMeasured'
    workloadEvidenceBasis = 'Saved terminal diagnostic job and canary results; no production notebook or baseline generator was started. This exporter makes read-only control-plane calls and does not claim a fresh private job-list or VM process inspection.'
    remainingGate = $(if ($ExpectedOutbound -eq 'Allow') {
        'OAP disabled by explicit user direction. See kafka-oap-off-results.json for bounded connectivity outcome; original OAP-protected acceptance is not established.'
    } else {
        'Actual Spark custom Kafka hostname resolved public under Allow and failed DNS under OAP; no supported same-workspace private bootstrap/advertised-broker route demonstrated.'
    })
})
if($DiagnosticPreflight){
    Write-Output "Diagnostic preflight verified current $($capacity.properties.state)/$vmPower and Deny/$ExpectedOutbound protection; this is not shutdown evidence."
}else{
    Write-Output "Fresh read-only inventory, full Deny/$ExpectedOutbound policy, private-only Function and Paused/deallocated states exported. Acceptance remains unrun."
}
