[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$root = Split-Path -Parent $PSScriptRoot
$directory = Join-Path $root '.deployment'
$proof = Get-Content (Join-Path $directory 'private-api-canary.json') -Raw | ConvertFrom-Json
if ($proof.functionHost -ne 'esehspark-score-api.azurewebsites.net' -or
    $proof.httpsStatusCode -notin @(200, 201)) {
    throw 'A successful private canary for the exact Function host is required.'
}
foreach ($field in @('functionKeyAuthenticated', 'keyVaultManagedIdentityRead',
    'tlsValidated', 'approvedPrivateDns', 'functionTableWriteSucceeded')) {
    if ($proof.proof.$field -isnot [bool] -or -not $proof.proof.$field) {
        throw "Private API proof '$field' is missing."
    }
}
function Invoke-AzJson {
    param([string[]] $Arguments)
    $value = & az @Arguments --subscription $subscription --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw "Azure operation failed: $($Arguments[0])." }
    return ($value | ConvertFrom-Json)
}
$account = Invoke-AzJson @('account', 'show')
if ($account.id -ne $subscription -or
    $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') {
    throw 'The approved MCAP CLI identity is required.'
}
$endpoint = Invoke-AzJson @('network', 'private-endpoint', 'show',
    '--resource-group', 'rg_esehspark', '--name', 'pe-esehspark-api')
if ($endpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved') {
    throw 'The retained Function private endpoint is not approved.'
}
$app = Invoke-AzJson @('functionapp', 'show', '--resource-group', 'rg_esehspark',
    '--name', 'esehspark-score-api')
$updated = Invoke-AzJson @('resource', 'update', '--ids', $app.id,
    '--api-version', '2023-12-01', '--set', 'properties.publicNetworkAccess=Disabled')
$deadline = [DateTime]::UtcNow.AddMinutes(3)
do {
    $app = Invoke-AzJson @('functionapp', 'show', '--resource-group', 'rg_esehspark',
        '--name', 'esehspark-score-api')
    if ($app.publicNetworkAccess -ne 'Disabled') {
        throw 'Function publicNetworkAccess did not read back Disabled.'
    }
    $response = Invoke-WebRequest -Uri 'https://esehspark-score-api.azurewebsites.net/api/score' `
        -Method Post -ContentType 'application/json' -Body '{}' `
        -SkipHttpErrorCheck -TimeoutSec 20
    $content = if ($response.Content -is [byte[]]) {
        [Text.Encoding]::UTF8.GetString($response.Content)
    }
    else { [string]$response.Content }
    $networkDenied = ([int]$response.StatusCode -eq 403 -and
        ($response.StatusDescription -match 'IP Forbidden' -or
         $content -match 'blocked your access|IP Forbidden'))
    if ($networkDenied) { break }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw "Public probe did not establish a network denial (HTTP $([int]$response.StatusCode)); public access remains Disabled."
    }
    Start-Sleep -Seconds 10
} while ($true)
$evidence = [ordered]@{
    verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
    functionResourceId = $app.id
    publicNetworkAccess = $app.publicNetworkAccess
    privateEndpointApproval = $endpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status
    priorPrivateCanaryEvidence = 'private-api-canary.json'
    privateCanaryAtUtc = $proof.completedAtUtc
    postCutoverPrivateCanaryRun = $false
    postCutoverPrivateCanaryNotRunReason = 'The VM and F8 remain idle; neither was resumed.'
    publicProbe = @{
        uri = 'https://esehspark-score-api.azurewebsites.net/api/score'
        credentialsSent = $false
        statusCode = [int]$response.StatusCode
        statusDescription = [string]$response.StatusDescription
        networkRestrictionVerified = $networkDenied
        body = $content
    }
    computeResumed = $false
}
$evidence | ConvertTo-Json -Depth 20 |
    Set-Content (Join-Path $directory 'function-private-only-cutover.json') -Encoding utf8
Write-Output 'Function publicNetworkAccess=Disabled; public HTTPS request rejected by network restriction (403), not Function authentication. No compute was resumed.'
