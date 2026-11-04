[CmdletBinding()]
param(
    [switch] $Rotate
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$functionApp = 'esehspark-score-api'
$vault = 'esehsparkkv'
$secretId = "/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.KeyVault/vaults/$vault/secrets/score-api-function-key"
$account = & az account show --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

$existing = & az resource show --ids $secretId --api-version '2024-11-01' --query id -o tsv --only-show-errors 2>&1
$existingExitCode = $LASTEXITCODE
$existingText = ($existing | ForEach-Object { [string]$_ }) -join "`n"
if ($existingExitCode -eq 0 -and -not $Rotate) {
    Write-Output 'A Function key secret already exists; no key rotation was performed.'
    return
}
if ($existingExitCode -ne 0 -and $existingText -notmatch '(?i)ResourceNotFound|NotFound|was not found') {
    throw "Unable to determine existing Key Vault secret state: $existingText"
}

$keyOutput = & az functionapp function keys set `
    --resource-group $resourceGroup `
    --name $functionApp `
    --function-name 'score' `
    --key-name 'score' `
    --subscription $subscriptionId `
    --only-show-errors `
    --output none 2>&1
if ($LASTEXITCODE -ne 0) { throw 'Azure Functions did not accept the Function-specific key creation request.' }

$functionKeys = & az functionapp function keys list `
    --resource-group $resourceGroup `
    --name $functionApp `
    --function-name 'score' `
    --subscription $subscriptionId `
    --only-show-errors `
    --output json 2>&1
if ($LASTEXITCODE -ne 0) { throw 'Unable to retrieve the Function-specific key into process memory.' }
$keyObject = $functionKeys | ConvertFrom-Json
$functionKey = [string]$keyObject.score
if ([string]::IsNullOrWhiteSpace($functionKey) -or $functionKey.Length -lt 32) {
    throw 'Function-specific key read-back was missing or shorter than the required minimum.'
}

$root = Split-Path -Parent $PSScriptRoot
$template = Join-Path $root 'infra\function-key-secret.bicep'
$parameterPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-function-key-$([guid]::NewGuid()).json"
$parameters = @{
    '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
    contentVersion = '1.0.0.0'
    parameters = @{
        keyVaultName = @{ value = $vault }
        functionKey = @{ value = $functionKey }
    }
}
try {
    New-Item -ItemType File -Path $parameterPath -Force | Out-Null
    $acl = Get-Acl -LiteralPath $parameterPath
    $acl.SetAccessRuleProtection($true, $false)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $rule = [Security.AccessControl.FileSystemAccessRule]::new($identity, 'FullControl', 'Allow')
    $acl.SetAccessRule($rule)
    Set-Acl -LiteralPath $parameterPath -AclObject $acl
    [IO.File]::WriteAllText(
        $parameterPath,
        ($parameters | ConvertTo-Json -Depth 10),
        [Text.UTF8Encoding]::new($false)
    )

    & az deployment group create `
        --subscription $subscriptionId `
        --resource-group $resourceGroup `
        --name 'esehspark-function-key' `
        --template-file $template `
        --parameters "@$parameterPath" `
        --only-show-errors `
        --output none
    if ($LASTEXITCODE -ne 0) {
        throw 'Secure Key Vault secret deployment failed; the key value was not emitted.'
    }
}
finally {
    $functionKey = $null
    $keyObject = $null
    $functionKeys = $null
    $parameters = $null
    Remove-Item -LiteralPath $parameterPath -Force -ErrorAction SilentlyContinue
}

$verify = & az resource show --ids $secretId --api-version '2024-11-01' --query id -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or $verify -ne $secretId) {
    throw 'Function key secret resource was not found on ARM read-back.'
}
Write-Output 'Function-specific score key stored in Key Vault; no secret value was displayed.'
