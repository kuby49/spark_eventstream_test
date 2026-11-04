[CmdletBinding(SupportsShouldProcess)]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$principalId = 'c2934d7c-4724-4cef-a50e-2a5d89afb4a4'
$role = '4633458b-17de-408a-b874-0445c86b69e6'
$scope = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.KeyVault/vaults/esehsparkkv"
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$userId = & az ad signed-in-user show --query id --only-show-errors -o tsv
if ($LASTEXITCODE -ne 0 -or $userId -ne $principalId) { throw 'Notebook submitter object ID could not be verified.' }
if ($PSCmdlet.ShouldProcess($scope, 'Grant Key Vault Secrets User to the verified MCAP notebook submitter')) {
    $assignment = & az role assignment create --subscription $subscription `
        --assignee-object-id $principalId --assignee-principal-type User `
        --role $role --scope $scope --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $assignment.principalId -ne $principalId -or
        $assignment.scope -ine $scope) { throw 'Vault-scoped notebook secret grant failed verification.' }
    [ordered]@{
        verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
        principalId = $principalId
        role = 'Key Vault Secrets User'
        scope = $scope
        roleAssignmentId = $assignment.id
        reason = 'Staged notebook secret reads returned ForbiddenByRbac; Owner is control-plane only.'
        dataPlaneRetestRequired = $true
    } | ConvertTo-Json | Set-Content (Join-Path (Split-Path -Parent $PSScriptRoot) `
        '.deployment\notebook-secret-access.json') -Encoding utf8
}
