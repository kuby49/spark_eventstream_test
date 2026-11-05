[CmdletBinding()]
param(
    [string] $ExpectedSubscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee',
    [string] $ExpectedTenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = $ExpectedSubscriptionId
$root = Split-Path -Parent $PSScriptRoot
$template = Join-Path $root 'infra\entrypoint.bicep'
$parameterPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-validation-$([guid]::NewGuid()).json"

function Invoke-AzValidation {
    param([Parameter(Mandatory)][string[]] $Arguments)
    $result = & az @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Validation command failed: az $($Arguments -join ' ')`n$($result -join "`n")"
    }
    return $result
}

try {
    $account = Invoke-AzValidation -Arguments @(
        'account', 'show', '--subscription', $subscriptionId, '--only-show-errors', '--output', 'json'
    ) | ConvertFrom-Json
    if ($account.id -ne $ExpectedSubscriptionId -or $account.tenantId -ne $ExpectedTenantId) {
        throw 'Azure CLI context does not match the approved tenant and subscription.'
    }

    $adminUpn = & az ad signed-in-user show --query userPrincipalName -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($adminUpn)) {
        throw 'Could not resolve the signed-in user principal name required for the Fabric capacity administrator.'
    }
    $password = "Validation-Only-$([guid]::NewGuid().ToString('N'))-A9!"
    $parameters = @{
        '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters = @{
            capacityAdminUpn = @{ value = $adminUpn }
            vmAdminUsername = @{ value = 'validation-only-user' }
            vmAdminPassword = @{ value = $password }
        }
    }
    New-Item -ItemType File -Path $parameterPath -Force | Out-Null
    $acl = Get-Acl -LiteralPath $parameterPath
    $acl.SetAccessRuleProtection($true, $false)
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
    $rule = [Security.AccessControl.FileSystemAccessRule]::new(
        $identity, 'FullControl', 'Allow'
    )
    $acl.SetAccessRule($rule)
    Set-Acl -LiteralPath $parameterPath -AclObject $acl
    $json = $parameters | ConvertTo-Json -Depth 10
    [IO.File]::WriteAllText($parameterPath, $json, [Text.UTF8Encoding]::new($false))

    Invoke-AzValidation -Arguments @(
        'bicep', 'build', '--file', $template, '--only-show-errors'
    ) | Out-Null
    Invoke-AzValidation -Arguments @(
        'deployment', 'sub', 'validate',
        '--subscription', $subscriptionId,
        '--location', 'westus3',
        '--template-file', $template,
        '--parameters', "@$parameterPath",
        '--only-show-errors',
        '--output', 'json'
    ) | Out-Null
    $whatIf = Invoke-AzValidation -Arguments @(
        'deployment', 'sub', 'what-if',
        '--subscription', $subscriptionId,
        '--location', 'westus3',
        '--template-file', $template,
        '--parameters', "@$parameterPath",
        '--only-show-errors',
        '--no-pretty-print',
        '--output', 'json'
    ) | ConvertFrom-Json

    $changes = @($whatIf.changes)
    $deletes = @($changes | Where-Object changeType -eq 'Delete')
    if ($deletes.Count -gt 0) {
        throw 'What-if reported resource deletions. Deployment is blocked; do not proceed.'
    }
    [pscustomobject]@{
        SubscriptionId = $account.id
        TenantId = $account.tenantId
        Template = 'infra\entrypoint.bicep'
        Build = 'PASS'
        AzureValidate = 'PASS'
        WhatIf = 'PASS'
        PlannedChanges = $changes.Count
        PlannedDeletes = $deletes.Count
        Result = 'No resource write was executed by this validation script.'
    } | Format-List
}
finally {
    $password = $null
    $json = $null
    $parameters = $null
    Remove-Item -LiteralPath $parameterPath -Force -ErrorAction SilentlyContinue
}
