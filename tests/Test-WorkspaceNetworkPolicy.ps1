$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$proofPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-policy-$([guid]::NewGuid()).json"
$global:policyTest = @{currentPolicy=@{
    inbound = @{publicAccessRules=@{defaultAction='Allow';extraRules=@('preserve-inbound')}}
    outbound = @{publicAccessRules=@{defaultAction='Allow';extraRules=@('preserve-outbound')}}
}}
function az {
    $global:LASTEXITCODE = 0
    if ($args -contains 'show') {
        return '{"tenantId":"4526cee1-4399-4e4b-9134-46fabd583527","user":{"name":"admin@MngEnvMCAP885977.onmicrosoft.com"}}'
    }
    return 'test-token'
}
function Invoke-WebRequest {
    param($Method, $Uri, $Headers, $ContentType, $Body, $ErrorAction)
    if ($Method -eq 'Get') {
        $responseHeaders = @{}
        if ($global:policyTest.etag) { $responseHeaders.ETag = $global:policyTest.etag }
        return [pscustomobject]@{Content=($global:policyTest.currentPolicy | ConvertTo-Json -Depth 20);Headers=$responseHeaders}
    }
    $global:policyTest.submitted = $Body | ConvertFrom-Json
    $global:policyTest.matchHeader = $Headers['If-Match']
    if ($global:policyTest.stale) { throw 'HTTP 412 Precondition Failed' }
    return [pscustomobject]@{StatusCode=200}
}
function Invoke-RestMethod {
    param($Method, $Uri, $Headers, $ErrorAction)
    return $global:policyTest.submitted
}
try {
    foreach ($case in @('missing-etag','etag','stale-etag')) {
        @{managedPrivateEndpointsApproved=$true;functionMpe=$true;keyVaultMpe=$true} |
            ConvertTo-Json | Set-Content $proofPath
        $global:policyTest.etag = if ($case -eq 'missing-etag') { $null } else { '"version-1"' }
        $global:policyTest.stale = $case -eq 'stale-etag'
        $failed = $false
        try {
            & (Join-Path $root 'scripts\Set-WorkspaceNetworkPolicy.ps1') `
                -WorkspaceId 'ddd993fe-8d65-46f6-90f5-7002e6aa5990' `
                -PrivatePathProofPath $proofPath -EnableOutboundOap | Out-Null
        }
        catch {
            if (-not $global:policyTest.stale -or $_.Exception.Message -notlike '*412*') { throw }
            $failed = $true
        }
        if ($failed -ne $global:policyTest.stale) { throw "$case failed its stale-write assertion." }
        if ($global:policyTest.submitted.inbound.publicAccessRules.defaultAction -ne 'Allow' -or
            $global:policyTest.submitted.outbound.publicAccessRules.defaultAction -ne 'Deny' -or
            $global:policyTest.submitted.inbound.publicAccessRules.extraRules[0] -ne 'preserve-inbound' -or
            $global:policyTest.submitted.outbound.publicAccessRules.extraRules[0] -ne 'preserve-outbound') {
            throw "$case did not preserve the complete policy."
        }
        if (($case -eq 'missing-etag' -and $global:policyTest.matchHeader) -or
            ($case -ne 'missing-etag' -and $global:policyTest.matchHeader -ne '"version-1"')) {
            throw "$case submitted an incorrect concurrency header."
        }
        $proof = Get-Content $proofPath -Raw | ConvertFrom-Json
        if ($global:policyTest.stale -and $proof.PSObject.Properties.Name -contains 'workspaceOapEnabled') {
            throw 'Rejected stale write must not record a successful policy change.'
        }
    }
    $global:policyTest.currentPolicy.inbound.publicAccessRules.defaultAction = 'Deny'
    $global:policyTest.currentPolicy.outbound.publicAccessRules.defaultAction = 'Deny'
    $global:policyTest.etag = $null
    $global:policyTest.stale = $false
    @{} | ConvertTo-Json | Set-Content $proofPath
    & (Join-Path $root 'scripts\Set-WorkspaceNetworkPolicy.ps1') `
        -WorkspaceId 'ddd993fe-8d65-46f6-90f5-7002e6aa5990' `
        -PrivatePathProofPath $proofPath -DisableOutboundOap | Out-Null
    if ($global:policyTest.submitted.inbound.publicAccessRules.defaultAction -ne 'Deny' -or
        $global:policyTest.submitted.outbound.publicAccessRules.defaultAction -ne 'Allow' -or
        $global:policyTest.submitted.inbound.publicAccessRules.extraRules[0] -ne 'preserve-inbound' -or
        $global:policyTest.submitted.outbound.publicAccessRules.extraRules[0] -ne 'preserve-outbound') {
        throw 'User-authorized OAP removal must preserve inbound Deny and all rules.'
    }
    if ((Get-Content $proofPath -Raw | ConvertFrom-Json).workspaceOapEnabled -ne $false) {
        throw 'OAP removal evidence must record disabled protection.'
    }
    Write-Output 'Full-policy preservation, optional/stale ETag and outbound-only OAP removal checks passed.'
}
finally {
    Remove-Item -LiteralPath $proofPath -Force -ErrorAction SilentlyContinue
    Remove-Variable -Name policyTest -Scope Global
}
