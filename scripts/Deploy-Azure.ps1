[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $CapacityAdminUpn,
    [Parameter(Mandatory)] [string] $VmAdminUsername,
    [Parameter(Mandatory)] [securestring] $VmAdminPassword
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$root = Split-Path -Parent $PSScriptRoot
$template = Join-Path $root 'infra\entrypoint.bicep'
$tempParameters = [IO.Path]::GetTempFileName()
$plainPassword = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($VmAdminPassword)

try {
    $account = & az account show --only-show-errors --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
        throw 'Azure CLI context is not the approved MCAP subscription.'
    }
    & az bicep build --file $template --stdout --only-show-errors | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Bicep compilation failed.' }
    $groupExists = & az group exists --subscription $subscriptionId `
        --name 'rg_esehspark' --only-show-errors --output tsv
    if ($LASTEXITCODE -ne 0) { throw 'Unable to determine whether the approved resource group exists.' }
    $existingApps = @()
    if ($groupExists -eq 'true') {
        $existingAppsJson = & az resource list --subscription $subscriptionId `
            --resource-group 'rg_esehspark' --resource-type 'Microsoft.Web/sites' `
            --name 'esehspark-score-api' --only-show-errors --output json
        if ($LASTEXITCODE -ne 0) { throw 'Unable to discover the current Function network state; refusing to risk reopening public access.' }
        $existingApps = @($existingAppsJson | ConvertFrom-Json)
    }
    if ($existingApps.Count -gt 1) { throw 'Multiple matching Function resources were returned.' }
    $functionPublicAccess = 'Enabled'
    if ($existingApps.Count -eq 1) {
        $existingAppJson = & az functionapp show --subscription $subscriptionId `
            --resource-group 'rg_esehspark' --name 'esehspark-score-api' `
            --only-show-errors --output json
        if ($LASTEXITCODE -ne 0) { throw 'Unable to read the existing Function public-network setting.' }
        $existingApp = $existingAppJson | ConvertFrom-Json
        if ($existingApp.publicNetworkAccess -notin @('Enabled', 'Disabled')) {
            throw 'The existing Function public-network setting is unknown; refusing to overwrite it.'
        }
        $functionPublicAccess = [string]$existingApp.publicNetworkAccess
    }
    $parameters = @{
        '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters = @{
            capacityAdminUpn = @{ value = $CapacityAdminUpn }
            vmAdminUsername = @{ value = $VmAdminUsername }
            vmAdminPassword = @{ value = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($plainPassword) }
            functionPublicAccess = @{ value = $functionPublicAccess }
        }
    }
    New-Item -ItemType File -Path $tempParameters -Force | Out-Null
    $acl = Get-Acl -LiteralPath $tempParameters
    $acl.SetAccessRuleProtection($true, $false)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $identity, 'FullControl', 'Allow'
    )
    $acl.SetAccessRule($rule)
    Set-Acl -LiteralPath $tempParameters -AclObject $acl
    $json = $parameters | ConvertTo-Json -Depth 10
    [IO.File]::WriteAllText($tempParameters, $json, [Text.UTF8Encoding]::new($false))

    & az deployment sub create `
        --subscription $subscriptionId `
        --location westus3 `
        --name 'esehspark-infrastructure' `
        --template-file $template `
        --parameters "@$tempParameters" `
        --only-show-errors `
        --output json
    if ($LASTEXITCODE -ne 0) { throw 'Infrastructure deployment failed. Inspect the ARM deployment operation details; do not delete/recreate resources.' }
}
finally {
    if ($plainPassword -ne [IntPtr]::Zero) {
        [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($plainPassword)
    }
    $json = $null
    $parameters = $null
    Remove-Item -LiteralPath $tempParameters -Force -ErrorAction SilentlyContinue
}
