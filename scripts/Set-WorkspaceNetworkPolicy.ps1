[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [Parameter(Mandatory)] [ValidateScript({ Test-Path -LiteralPath $_ })] [string] $PrivatePathProofPath,
    [switch] $EnableOutboundOap,
    [switch] $DisableOutboundOap,
    [switch] $BlockInboundPublic,
    [switch] $WorkspacePrivateCutover
)

$ErrorActionPreference = 'Stop'
if ($EnableOutboundOap -and $DisableOutboundOap) { throw 'Outbound Enable/Disable cannot be combined.' }
if (-not $EnableOutboundOap -and -not $DisableOutboundOap -and -not $BlockInboundPublic) {
    throw 'Specify at least one approved policy change.'
}
if ($WorkspacePrivateCutover -and -not $BlockInboundPublic) {
    throw 'Workspace private cutover requires an inbound-deny change.'
}
$proof = Get-Content -LiteralPath $PrivatePathProofPath -Raw | ConvertFrom-Json
$required = if ($BlockInboundPublic -and $WorkspacePrivateCutover) {
    @('workspacePrivateApi', 'workspacePrivateOneLake')
}
elseif ($BlockInboundPublic) {
    @('vmSourceCanary', 'eventhouseIngestion', 'sparkKafka', 'eventhouseKql', 'functionApi', 'keyVaultSecret', 'lakehouseDelta')
}
elseif ($EnableOutboundOap) {
    @('managedPrivateEndpointsApproved', 'functionMpe', 'keyVaultMpe')
}
else { @() }
foreach ($field in $required) {
    if ($proof.$field -isnot [bool] -or -not $proof.$field) {
        throw "Private-path evidence is incomplete; required proof '$field' is not true."
    }
}

$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$token = & az account get-access-token --subscription $subscription --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'eventstream-cli'
}
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId/networking/communicationPolicy"
$response = Invoke-WebRequest -Method Get -Uri $uri -Headers $headers -ErrorAction Stop
$policy = $response.Content | ConvertFrom-Json
if (-not $policy.inbound.publicAccessRules -or -not $policy.outbound.publicAccessRules -or
    -not $policy.inbound.publicAccessRules.defaultAction -or
    -not $policy.outbound.publicAccessRules.defaultAction) {
    throw 'GET returned an incomplete workspace policy; refusing a full-replacement PUT.'
}
$etag = $response.Headers.ETag
if (-not $etag) { $etag = $policy.etag }
if ($etag) {
    $etag = [string]@($etag)[0]
    if ($etag -notmatch '^".*"$') { $etag = '"' + $etag.Trim('"') + '"' }
}
else {
    Write-Warning 'The GA service supplied no ETag. Preserving the freshly read full policy and verifying immediately; atomic concurrency protection is unavailable. Ensure no other policy writer is active.'
}

if ($EnableOutboundOap) { $policy.outbound.publicAccessRules.defaultAction = 'Deny' }
if ($DisableOutboundOap) { $policy.outbound.publicAccessRules.defaultAction = 'Allow' }
if ($BlockInboundPublic) { $policy.inbound.publicAccessRules.defaultAction = 'Deny' }
$body = @{
    inbound = $policy.inbound
    outbound = $policy.outbound
} | ConvertTo-Json -Depth 30 -Compress
if ($etag) { $headers['If-Match'] = $etag }
$put = Invoke-WebRequest -Method Put -Uri $uri -Headers $headers -ContentType 'application/json' `
    -Body $body -ErrorAction Stop
if ($put.StatusCode -ne 200) { throw "Workspace network policy PUT returned HTTP $($put.StatusCode)." }
$verifyHeaders = @{}
foreach ($key in $headers.Keys) {
    if ($key -ne 'If-Match') { $verifyHeaders[$key] = $headers[$key] }
}
$readback = Invoke-RestMethod -Method Get -Uri $uri -Headers $verifyHeaders -ErrorAction Stop
if ($EnableOutboundOap -and $readback.outbound.publicAccessRules.defaultAction -ne 'Deny') {
    throw 'Workspace outbound OAP did not read back as Deny.'
}
if ($DisableOutboundOap -and $readback.outbound.publicAccessRules.defaultAction -ne 'Allow') {
    throw 'Workspace outbound OAP removal did not read back as Allow.'
}
if ($BlockInboundPublic -and $readback.inbound.publicAccessRules.defaultAction -ne 'Deny') {
    throw 'Workspace inbound public access did not read back as Deny.'
}
foreach ($section in @('inbound','outbound')) {
    $expected = $policy.$section | ConvertTo-Json -Depth 30 -Compress
    $actual = $readback.$section | ConvertTo-Json -Depth 30 -Compress
    if ($actual -ne $expected) {
        throw "Workspace $section policy readback differs from the full policy submitted."
    }
}
$proof | Add-Member -NotePropertyName policyConcurrencyProtection -NotePropertyValue $(
    if ($etag) { 'ETag' } else { 'Unavailable: service supplied no ETag' }
) -Force
$proof | Add-Member -NotePropertyName fullPolicyReadback -NotePropertyValue $readback -Force
if ($EnableOutboundOap) { $proof | Add-Member -NotePropertyName workspaceOapEnabled -NotePropertyValue $true -Force }
if ($DisableOutboundOap) { $proof | Add-Member -NotePropertyName workspaceOapEnabled -NotePropertyValue $false -Force }
if ($BlockInboundPublic) {
    $proof | Add-Member -NotePropertyName workspacePublicAccessDenied -NotePropertyValue $true -Force
}
$proofTempPath = "$PrivatePathProofPath.$([guid]::NewGuid().ToString('N')).tmp"
try {
    [IO.File]::WriteAllText(
        $proofTempPath,
        ($proof | ConvertTo-Json -Depth 20),
        [Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $proofTempPath -Destination $PrivatePathProofPath -Force
}
finally {
    Remove-Item -LiteralPath $proofTempPath -Force -ErrorAction SilentlyContinue
}
Write-Output "Workspace policy read-back passed. Inbound=$($readback.inbound.publicAccessRules.defaultAction); Outbound=$($readback.outbound.publicAccessRules.defaultAction)."
