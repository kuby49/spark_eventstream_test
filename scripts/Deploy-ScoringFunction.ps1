[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$functionName = 'esehspark-score-api'
$planName = 'asp-esehspark-score'
$pythonRuntime = 'Python|3.13'
$root = Split-Path -Parent $PSScriptRoot
$source = Join-Path $root 'src\scoring_api'
$stage = Join-Path $root '.deployment\score-api-stage'
$zipPath = Join-Path $root '.deployment\score-api.zip'
$deploymentStatePath = Join-Path $root '.deployment\function-deployment.json'

$account = & az account show --subscription $subscriptionId --only-show-errors --output json |
    ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

$app = & az functionapp show --resource-group $resourceGroup --name $functionName `
    --subscription $subscriptionId --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $app.name -ne $functionName -or
    $app.state -ne 'Running' -or $app.publicNetworkAccess -ne 'Enabled') {
    throw 'The existing Function App is not in the approved pre-cutover deployment state.'
}
$plan = & az appservice plan show --resource-group $resourceGroup --name $planName `
    --subscription $subscriptionId --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $plan.sku.name -ne 'B1' -or $plan.kind -ne 'linux') {
    throw 'The existing Function App plan is not the approved Linux B1; no SKU change was attempted.'
}
$config = & az functionapp config show --resource-group $resourceGroup --name $functionName `
    --subscription $subscriptionId --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $config.linuxFxVersion -ne $pythonRuntime -or
    $config.alwaysOn -ne $true) {
    throw 'The existing Function App runtime or Always On configuration does not match the approved plan.'
}

$routeAllOutput = & az functionapp config set `
    --resource-group $resourceGroup `
    --name $functionName `
    --subscription $subscriptionId `
    --vnet-route-all-enabled true `
    --only-show-errors --output none 2>&1
if ($LASTEXITCODE -ne 0) {
    throw "Function VNet route-all update failed: $($routeAllOutput -join "`n")"
}
$routeAll = & az functionapp config show `
    --resource-group $resourceGroup `
    --name $functionName `
    --subscription $subscriptionId `
    --query vnetRouteAllEnabled `
    --only-show-errors --output tsv
if ($LASTEXITCODE -ne 0 -or $routeAll -ne 'true') {
    throw 'Function VNet route-all could not be enabled for the approved private storage and Key Vault paths.'
}

foreach ($file in @('host.json', 'requirements.txt', 'function_app.py', 'logic.py', 'storage.py')) {
    if (-not (Test-Path -LiteralPath (Join-Path $source $file) -PathType Leaf)) {
        throw "Required Function package file is missing: $file"
    }
}

New-Item -ItemType Directory -Force -Path $stage | Out-Null
foreach ($file in @('host.json', 'requirements.txt', 'function_app.py', 'logic.py', 'storage.py')) {
    Copy-Item -LiteralPath (Join-Path $source $file) -Destination (Join-Path $stage $file) -Force
}
Compress-Archive -Path (Join-Path $stage '*') -DestinationPath $zipPath -CompressionLevel Optimal -Force
$packageHash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()

$packageSetting = & az functionapp config appsettings delete `
    --resource-group $resourceGroup `
    --name $functionName `
    --subscription $subscriptionId `
    --setting-names 'WEBSITE_RUN_FROM_PACKAGE' `
    --only-show-errors --output none 2>&1
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to remove the run-from-package setting that disabled the required Linux Oryx build.'
}

$settings = & az functionapp config appsettings set `
    --resource-group $resourceGroup `
    --name $functionName `
    --subscription $subscriptionId `
    --settings 'SCM_DO_BUILD_DURING_DEPLOYMENT=true' 'ENABLE_ORYX_BUILD=true' `
    --only-show-errors --output none 2>&1
if ($LASTEXITCODE -ne 0) {
    throw 'Unable to enable the Linux Oryx remote build for the Function package.'
}

$deployOutput = & az functionapp deployment source config-zip `
    --resource-group $resourceGroup `
    --name $functionName `
    --subscription $subscriptionId `
    --src $zipPath `
    --build-remote true `
    --timeout 1200 `
    --only-show-errors --output json 2>&1
$deployExitCode = $LASTEXITCODE
if ($deployExitCode -ne 0) {
    throw 'Function package deployment failed; the approved B1 plan and network configuration were left unchanged.'
}
$deployment = ($deployOutput -join "`n") | ConvertFrom-Json
if ($deployment.complete -ne $true -or $deployment.status -ne 4) {
    throw "Function ZIP deployment did not finish successfully (status=$($deployment.status), complete=$($deployment.complete))."
}

$state = [ordered]@{
    appName = $functionName
    hostName = [string]$app.defaultHostName
    planName = $planName
    planSku = [string]$plan.sku.name
    runtime = $pythonRuntime
    packageSha256 = $packageHash
    deploymentId = [string]$deployment.id
    deploymentStatus = [int]$deployment.status
    deployedAtUtc = [DateTime]::UtcNow.ToString('o')
    deploymentMethod = 'az functionapp deployment source config-zip --build-remote true'
}
[IO.File]::WriteAllText(
    $deploymentStatePath,
    ($state | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false)
)
Write-Output "Function package submitted for $($state.hostName); plan=$($state.planSku); runtime=$($state.runtime); sha256=$packageHash"
