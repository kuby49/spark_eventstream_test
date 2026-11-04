[CmdletBinding()]
param(
    [string] $ExpectedSubscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee',
    [string] $ExpectedTenantId = '4526cee1-4399-4e4b-9134-46fabd583527',
    [string] $Location = 'westus3'
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'

function Invoke-AzJson {
    param([Parameter(Mandatory)][string[]] $Arguments)
    $result = & az @Arguments --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw "Azure CLI failed: az $($Arguments -join ' ')" }
    try { return $result | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "Azure CLI returned invalid JSON for: az $($Arguments -join ' ')" }
}

$account = Invoke-AzJson -Arguments @('account', 'show')
if ($account.id -ne $ExpectedSubscriptionId -or $account.tenantId -ne $ExpectedTenantId) {
    throw "Active Azure context does not match the approved MCAP tenant/subscription."
}

$armTokenExpiry = & az account get-access-token --resource 'https://management.azure.com/' `
    --query expiresOn -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($armTokenExpiry)) {
    throw 'Azure CLI did not return an ARM access token.'
}

$providers = @(
    'Microsoft.Compute', 'Microsoft.Network', 'Microsoft.Storage',
    'Microsoft.KeyVault', 'Microsoft.Web', 'Microsoft.Insights',
    'Microsoft.OperationalInsights', 'Microsoft.Fabric'
)
foreach ($provider in $providers) {
    $state = (& az provider show --namespace $provider --query registrationState -o tsv --only-show-errors)
    if ($LASTEXITCODE -ne 0 -or $state -ne 'Registered') {
        throw "Required provider is not registered: $provider ($state)"
    }
}

$groupExists = (& az group exists --name rg_esehspark -o tsv --only-show-errors)
if ($LASTEXITCODE -ne 0) { throw 'Unable to determine target resource group state.' }

$vmSkus = Invoke-AzJson -Arguments @(
    'vm', 'list-skus', '--location', $Location, '--size', 'Standard_D2s_v5', '--all'
)
if (-not $vmSkus -or ($vmSkus | Where-Object { $_.restrictions.Count -gt 0 })) {
    throw 'Standard_D2s_v5 is unavailable or restricted in the target region.'
}

$vmUsage = Invoke-AzJson -Arguments @('vm', 'list-usage', '--location', $Location)
$regionalCores = $vmUsage | Where-Object { $_.name.localizedValue -eq 'Total Regional vCPUs' } |
    Select-Object -First 1
$dsV5Cores = $vmUsage | Where-Object { $_.name.localizedValue -eq 'Standard DSv5 Family vCPUs' } |
    Select-Object -First 1
$regionalCurrent = if ($regionalCores) { [int]$regionalCores.currentValue } else { -1 }
$regionalLimit = if ($regionalCores) { [int]$regionalCores.limit } else { -1 }
$dsV5Current = if ($dsV5Cores) { [int]$dsV5Cores.currentValue } else { -1 }
$dsV5Limit = if ($dsV5Cores) { [int]$dsV5Cores.limit } else { -1 }
if ($regionalCurrent -lt 0 -or $dsV5Current -lt 0 -or
    ($regionalCurrent + 2 -gt $regionalLimit) -or
    ($dsV5Current + 2 -gt $dsV5Limit)) {
    throw 'Insufficient or unreadable regional/DSv5 vCPU quota for the fixed 2-vCPU VM.'
}

$storageName = Invoke-AzJson -Arguments @('storage', 'account', 'check-name', '--name', 'esehsparkstore8')
$vaultName = Invoke-AzJson -Arguments @('keyvault', 'check-name', '--name', 'esehsparkkv')
$functionNameBodyPath = [IO.Path]::GetTempFileName()
try {
    [IO.File]::WriteAllText(
        $functionNameBodyPath,
        (@{ name = 'esehspark-score-api'; type = 'Microsoft.Web/sites' } | ConvertTo-Json -Compress),
        [Text.UTF8Encoding]::new($false)
    )
    $functionName = Invoke-AzJson -Arguments @(
        'rest',
        '--method', 'post',
        '--url', "https://management.azure.com/subscriptions/$ExpectedSubscriptionId/providers/Microsoft.Web/checknameavailability?api-version=2022-03-01",
        '--headers', 'Content-Type=application/json',
        '--body', "@$functionNameBodyPath"
    )
}
finally {
    Remove-Item -LiteralPath $functionNameBodyPath -Force -ErrorAction SilentlyContinue
}
if (-not $storageName.nameAvailable -or -not $vaultName.nameAvailable -or -not $functionName.nameAvailable) {
    throw "A fixed global resource name is unavailable: storage=$($storageName.nameAvailable), vault=$($vaultName.nameAvailable), function=$($functionName.nameAvailable)."
}

[pscustomobject]@{
    TenantId = $account.tenantId
    SubscriptionId = $account.id
    SubscriptionName = $account.name
    Location = $Location
    ResourceGroupExists = ($groupExists -eq 'true')
    ArmTokenExpiresOn = $armTokenExpiry
    RequiredProvidersRegistered = $true
    VmSku = 'Standard_D2s_v5'
    RegionalVcpuQuota = "$regionalCurrent/$regionalLimit"
    DsV5VcpuQuota = "$dsV5Current/$dsV5Limit"
    FixedNamesAvailable = $true
    Result = 'Read-only CLI preflight passed; ARM write authorization and Fabric/private-route gates remain unproven.'
} | Format-List
