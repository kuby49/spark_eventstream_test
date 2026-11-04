[CmdletBinding()]
param([ValidateRange(1,30)] [int] $TimeoutMinutes = 30)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$workspace = 'ddd993fe-8d65-46f6-90f5-7002e6aa5990'
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$token = & az account get-access-token --subscription $subscription --resource https://api.fabric.microsoft.com `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Fabric token unavailable.' }
$headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='spark-cli'}
$base = "https://api.fabric.microsoft.com/v1/workspaces/$workspace"
$history = [Collections.Generic.List[object]]::new()
$historyPath = Join-Path $directory 'workspace-public-convergence.json'
if (Test-Path $historyPath) {
    foreach ($entry in @(Get-Content $historyPath -Raw | ConvertFrom-Json)) { $history.Add($entry) }
}
$started = if ($history.Count) { [DateTime]::Parse($history[0].atUtc).ToUniversalTime() } else { [DateTime]::UtcNow }
$deadline = $started.AddMinutes($TimeoutMinutes)
do {
    $policy = Invoke-RestMethod -Uri "$base/networking/communicationPolicy" -Headers $headers -TimeoutSec 30
    if ($policy.inbound.publicAccessRules.defaultAction -ne 'Deny' -or
        $policy.outbound.publicAccessRules.defaultAction -ne 'Deny') { throw 'Final workspace protection is not Deny/Deny.' }
    $response = $null
    $transportError = $null
    try {
        $response = Invoke-WebRequest -Uri $base -Headers $headers -SkipHttpErrorCheck -TimeoutSec 30
    }
    catch [System.Net.Http.HttpRequestException] {
        $transportError = $_.Exception.GetType().Name
    }
    catch [System.OperationCanceledException] {
        $transportError = $_.Exception.GetType().Name
    }
    $code = $null
    if ($response -and $response.StatusCode -ne 200) {
        $body = $response.Content
        if ($body -is [byte[]]) { $body = [Text.Encoding]::UTF8.GetString($body) }
        $errorBody = $body | ConvertFrom-Json
        $code = $errorBody.errorCode
    }
    $status = if ($response) { [int]$response.StatusCode } else { $null }
    $observation = @{atUtc=[DateTime]::UtcNow.ToString('o');httpStatus=$status;transportError=$transportError;
        errorCode=$code;otherwiseAuthorized=$true;requestUri=$base}
    $history.Add($observation)
    $history.ToArray() | ConvertTo-Json -Depth 10 | Set-Content $historyPath
    if ($status -eq 403 -and $code -eq 'RequestDeniedByInboundPolicy') {
        $observation | ConvertTo-Json | Set-Content (Join-Path $directory 'workspace-public-negative.json')
        Write-Output 'Authorized outside workspace request rejected by inbound network policy (403).'
        return
    }
    if ($response -and $status -ne 200) { throw "Unexpected public response: HTTP $status, code '$code'." }
    if ([DateTime]::UtcNow -ge $deadline) { throw 'Public access denial did not converge within the bounded documented window; policies were not reopened.' }
    Start-Sleep -Seconds 15
} while ($true)
