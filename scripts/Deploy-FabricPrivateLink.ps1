[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [Parameter(Mandatory)] [string] $VirtualNetworkId
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$root = Split-Path -Parent $PSScriptRoot
$template = Join-Path $root 'infra\fabric-link.bicep'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$account = & az account show --subscription $subscriptionId --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}
& az bicep build --file $template --stdout --only-show-errors | Out-Null
if ($LASTEXITCODE -ne 0) { throw 'Fabric private-link Bicep compilation failed.' }
& az deployment group create `
    --subscription $subscriptionId `
    --resource-group 'rg_esehspark' `
    --name 'esehspark-fabric-private-link' `
    --template-file $template `
    --parameters workspaceId=$WorkspaceId virtualNetworkId=$VirtualNetworkId tenantId=$tenantId `
    --only-show-errors `
    --output json
if ($LASTEXITCODE -ne 0) {
    throw 'Fabric private-link deployment failed; preserve existing resources and inspect the deployment operation details.'
}
