[CmdletBinding()]
param(
    [string] $RunId,
    [string] $MessageId
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$vmName = 'vmesehspark'
$functionHost = 'esehspark-score-api.azurewebsites.net'
$vaultHost = 'esehsparkkv.vault.azure.net'
$root = Split-Path -Parent $PSScriptRoot
$requestPath = Join-Path $root '.deployment\private-api-canary-request.json'
$resultPath = Join-Path $root '.deployment\private-api-canary.json'
$requestTimestamp = $null

if ([string]::IsNullOrWhiteSpace($RunId) -and (Test-Path -LiteralPath $requestPath)) {
    $savedRequest = Get-Content -LiteralPath $requestPath -Raw | ConvertFrom-Json
    if ($savedRequest.timestamp) {
        $RunId = [string]$savedRequest.runId
        $MessageId = [string]$savedRequest.messageId
        $requestTimestamp = ([DateTimeOffset]$savedRequest.timestamp).ToUniversalTime().ToString('o')
    }
}
if ([string]::IsNullOrWhiteSpace($RunId)) {
    $RunId = [guid]::NewGuid().ToString()
}
if ([string]::IsNullOrWhiteSpace($MessageId)) {
    $MessageId = [guid]::NewGuid().ToString()
}
try {
    $parsedRunId = [guid]::Parse($RunId).ToString()
    $messageId = [guid]::Parse($MessageId).ToString()
}
catch {
    throw 'RunId and MessageId must be UUIDs.'
}
if (-not $requestTimestamp -and (Test-Path -LiteralPath $requestPath)) {
    $savedRequest = Get-Content -LiteralPath $requestPath -Raw | ConvertFrom-Json
    if ($savedRequest.runId -eq $parsedRunId -and $savedRequest.messageId -eq $messageId) {
        throw 'The saved canary request lacks an immutable timestamp; use new UUIDs instead of reusing its idempotency key.'
    }
}
if (-not $requestTimestamp) {
    $requestTimestamp = [DateTime]::UtcNow.ToString('o')
}
$parsedTimestamp = [DateTimeOffset]::Parse($requestTimestamp)
if ($parsedTimestamp.Offset -ne [TimeSpan]::Zero) {
    throw 'The saved canary timestamp must be UTC.'
}
$requestTimestamp = $parsedTimestamp.ToUniversalTime().ToString('o')
$requestState = [ordered]@{
    runId = $parsedRunId
    messageId = $messageId
    timestamp = $requestTimestamp
    equipmentId = 17
    calculatedTemperature = 82.5
}
[IO.File]::WriteAllText(
    $requestPath,
    ($requestState | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false)
)

$account = & az account show --subscription $subscriptionId --only-show-errors --output json |
    ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

function Get-PrivateEndpointAddress {
    param([Parameter(Mandatory)][string] $EndpointName)

    $endpoint = & az network private-endpoint show `
        --resource-group $resourceGroup `
        --name $EndpointName `
        --subscription $subscriptionId `
        --only-show-errors --output json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or
        $endpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved') {
        throw "Private endpoint '$EndpointName' is not approved."
    }

    $nicId = [string]$endpoint.networkInterfaces[0].id
    if ([string]::IsNullOrWhiteSpace($nicId)) {
        throw "Private endpoint '$EndpointName' has no network interface."
    }
    $addresses = & az network nic show --ids $nicId --subscription $subscriptionId `
        --query 'ipConfigurations[].privateIPAddress' --only-show-errors --output json |
        ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or @($addresses).Count -ne 1) {
        throw "Private endpoint '$EndpointName' does not have exactly one discovered private address."
    }
    $addressList = @($addresses)
    if ($addressList.Count -ne 1) {
        throw "Private endpoint '$EndpointName' did not return exactly one private address."
    }
    return [string]$addressList[0]
}

$expectedApiIp = Get-PrivateEndpointAddress -EndpointName 'pe-esehspark-api'
$expectedVaultIp = Get-PrivateEndpointAddress -EndpointName 'pe-esehspark-kv'

$remoteScript = @'
$ErrorActionPreference='Stop'
$h='esehspark-score-api.azurewebsites.net';$v='esehsparkkv.vault.azure.net'
$ai='__EXPECTED_API_IP__';$vi='__EXPECTED_VAULT_IP__';$r='__RUN_ID__';$m='__MESSAGE_ID__';$s='api-dns'
try{
$a=@([Net.Dns]::GetHostAddresses($h)|Where-Object AddressFamily -eq InterNetwork|ForEach-Object IPAddressToString)
if($a -notcontains $ai){throw 'private DNS mismatch'}
$s='key-vault-dns'
$b=@([Net.Dns]::GetHostAddresses($v)|Where-Object AddressFamily -eq InterNetwork|ForEach-Object IPAddressToString)
if($b -notcontains $vi){throw 'private DNS mismatch'}
$s='managed-identity'
$t=(Invoke-RestMethod 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fvault.azure.net' -Headers @{Metadata='true'} -TimeoutSec 15).access_token
$s='key-vault-read'
$k=(Invoke-RestMethod "https://$v/secrets/score-api-function-key?api-version=7.4" -Headers @{Authorization="Bearer $t"} -TimeoutSec 15).value
$s='function-https'
$p=@{message_id=$m;run_id=$r;equipment_id=17;timestamp='__TIMESTAMP__';calculated_temperature=82.5}
$w=Invoke-WebRequest -UseBasicParsing "https://$h/api/score" -Method Post -Headers @{'x-functions-key'=$k;'Idempotency-Key'="$r`:$m"} -ContentType 'application/json' -Body ($p|ConvertTo-Json -Compress) -TimeoutSec 20
$j=$w.Content|ConvertFrom-Json
if($j.message_id -ne $m -or $j.run_id -ne $r -or $j.score -isnot [int] -or $j.score -lt 1 -or $j.score -gt 100 -or $w.StatusCode -notin @(200,201)){throw 'response contract mismatch'}
$s='function-idempotent-retry'
$retry=Invoke-WebRequest -UseBasicParsing "https://$h/api/score" -Method Post -Headers @{'x-functions-key'=$k;'Idempotency-Key'="$r`:$m"} -ContentType 'application/json' -Body ($p|ConvertTo-Json -Compress) -TimeoutSec 20
$retryBody=$retry.Content|ConvertFrom-Json
if($retry.StatusCode -ne 200 -or $retryBody.score -ne $j.score -or $retryBody.message_id -ne $m -or $retryBody.run_id -ne $r){throw 'durable retry mismatch'}
$s='function-payload-conflict'
$p.calculated_temperature=83.5
$conflictStatus=$null
try{
    $conflict=Invoke-WebRequest -UseBasicParsing "https://$h/api/score" -Method Post -Headers @{'x-functions-key'=$k;'Idempotency-Key'="$r`:$m"} -ContentType 'application/json' -Body ($p|ConvertTo-Json -Compress) -TimeoutSec 20
    $conflictStatus=[int]$conflict.StatusCode
}catch{
    if(-not $_.Exception.Response){throw}
    $conflictStatus=[int]$_.Exception.Response.StatusCode
}
if($conflictStatus -ne 409){throw 'payload conflict was not rejected'}
$o=@{success=$true;runId=$r;messageId=$m;functionHost=$h;keyVaultHost=$v;expectedFunctionPrivateIp=$ai;functionResolvedIps=$a;expectedKeyVaultPrivateIp=$vi;keyVaultResolvedIps=$b;httpsStatusCode=[int]$w.StatusCode;score=[int]$j.score;retryStatus=[int]$retry.StatusCode;retryScore=[int]$retryBody.score;conflictStatus=$conflictStatus}
Write-Output ("PRIVATE_API_CANARY:"+($o|ConvertTo-Json -Compress))
}catch{
$c=$null;if($_.Exception.Response -and $_.Exception.Response.StatusCode){$c=[int]$_.Exception.Response.StatusCode}
$o=@{stage=$s;errorType=$_.Exception.GetType().Name;httpStatusCode=$c}
Write-Output ("PRIVATE_API_CANARY_ERROR:"+($o|ConvertTo-Json -Compress));exit 1
}
'@
$remoteScript = $remoteScript.Replace('__EXPECTED_API_IP__', $expectedApiIp)
$remoteScript = $remoteScript.Replace('__EXPECTED_VAULT_IP__', $expectedVaultIp)
$remoteScript = $remoteScript.Replace('__RUN_ID__', $parsedRunId)
$remoteScript = $remoteScript.Replace('__MESSAGE_ID__', $messageId)
$remoteScript = $remoteScript.Replace('__TIMESTAMP__', $requestTimestamp)
$runtimePath = Join-Path $root '.deployment\private-api-canary-runtime.ps1'
[IO.File]::WriteAllText($runtimePath, $remoteScript, [Text.UTF8Encoding]::new($false))

$runCommandOutput = & az vm run-command invoke `
    --resource-group $resourceGroup `
    --name $vmName `
    --subscription $subscriptionId `
    --command-id RunPowerShellScript `
    --scripts "@$runtimePath" `
    --only-show-errors --output json 2>&1
$runCommandExitCode = $LASTEXITCODE
try {
    $runCommand = ($runCommandOutput -join "`n") | ConvertFrom-Json
}
catch {
    if ($runCommandExitCode -ne 0) {
        $firstOutputLine = [string]($runCommandOutput | Select-Object -First 1)
        $firstOutputLine = $firstOutputLine -replace '(?i)(x-functions-key|authorization|access_token|functionKey|password|secret)\s*[:=]\s*\S+', '$1=[REDACTED]'
        if ($firstOutputLine.Length -gt 300) {
            $firstOutputLine = $firstOutputLine.Substring(0, 300)
        }
        throw "Private API canary Run Command failed with Azure CLI exit $runCommandExitCode; no network route was inferred. First output line: $firstOutputLine"
    }
    throw 'Private API canary returned an unreadable Run Command response; data-plane success is unproven.'
}
$messages = @($runCommand.value | ForEach-Object { [string]$_.message })
$outputText = $messages -join "`n"
$errorLines = @($outputText -split "`r?`n" | Where-Object { $_ -like 'PRIVATE_API_CANARY_ERROR:*' })
if ($errorLines.Count -eq 1) {
    $failure = $errorLines[0].Substring('PRIVATE_API_CANARY_ERROR:'.Length) | ConvertFrom-Json
    throw "VM private API canary failed at stage '$($failure.stage)' (error=$($failure.errorType), HTTP=$($failure.httpStatusCode))."
}
if ($runCommandExitCode -ne 0) {
    throw 'Private API canary Run Command failed without a structured error; no route success was inferred.'
}
$resultLines = @($outputText -split "`r?`n" | Where-Object { $_ -like 'PRIVATE_API_CANARY:*' })
if ($resultLines.Count -ne 1) {
    $diagnostic = @($runCommand.value | ForEach-Object {
        $message = [string]$_.message
        if ($message.Length -gt 300) {
            $message = $message.Substring(0, 300)
        }
        [ordered]@{
            code = [string]$_.code
            displayStatus = [string]$_.displayStatus
            message = $message
        }
    })
    $diagnosticPath = Join-Path $root '.deployment\private-api-canary-diagnostic.json'
    [IO.File]::WriteAllText(
        $diagnosticPath,
        ($diagnostic | ConvertTo-Json -Depth 10),
        [Text.UTF8Encoding]::new($false)
    )
    throw "VM canary did not return exactly one structured result; private data-plane success is unproven. Redacted Run Command diagnostics: $diagnosticPath"
}
$proof = $resultLines[0].Substring('PRIVATE_API_CANARY:'.Length) | ConvertFrom-Json
[IO.File]::WriteAllText(
    (Join-Path $root '.deployment\private-api-canary-response.json'),
    ($proof | ConvertTo-Json -Depth 10), [Text.UTF8Encoding]::new($false))
if ($proof.success -ne $true -or $proof.expectedFunctionPrivateIp -notin $proof.functionResolvedIps -or
    $proof.expectedKeyVaultPrivateIp -notin $proof.keyVaultResolvedIps -or
    $proof.httpsStatusCode -notin @(200, 201) -or
    ($proof.score -isnot [int] -and $proof.score -isnot [long]) -or
    $proof.score -lt 1 -or $proof.score -gt 100 -or
    $proof.retryStatus -ne 200 -or $proof.retryScore -ne $proof.score -or $proof.conflictStatus -ne 409) {
    throw 'VM canary result did not satisfy the private DNS, TLS, authentication, and scoring assertions.'
}

$state = [ordered]@{
    runId = [string]$proof.runId
    messageId = [string]$proof.messageId
    timestamp = $requestTimestamp
    functionHost = [string]$proof.functionHost
    expectedFunctionPrivateIp = [string]$proof.expectedFunctionPrivateIp
    functionResolvedIps = @($proof.functionResolvedIps)
    keyVaultHost = [string]$proof.keyVaultHost
    expectedKeyVaultPrivateIp = [string]$proof.expectedKeyVaultPrivateIp
    keyVaultResolvedIps = @($proof.keyVaultResolvedIps)
    httpsStatusCode = [int]$proof.httpsStatusCode
    score = [int]$proof.score
    retryStatus = [int]$proof.retryStatus
    retryScore = [int]$proof.retryScore
    conflictStatus = [int]$proof.conflictStatus
    proof = @{
        approvedPrivateDns = $true
        keyVaultManagedIdentityRead = $true
        tlsValidated = $true
        functionKeyAuthenticated = $true
        functionTableWriteSucceeded = $true
        durableRetryReusedScore = ($proof.retryStatus -eq 200 -and $proof.retryScore -eq $proof.score)
        changedPayloadRejected = ($proof.conflictStatus -eq 409)
    }
    completedAtUtc = [DateTime]::UtcNow.ToString('o')
}
[IO.File]::WriteAllText(
    $resultPath,
    ($state | ConvertTo-Json -Depth 10),
    [Text.UTF8Encoding]::new($false)
)
Write-Output "Private API canary passed from $vmName; Function IP=$($state.expectedFunctionPrivateIp), Key Vault IP=$($state.expectedKeyVaultPrivateIp), status=$($state.httpsStatusCode), score=$($state.score)."
