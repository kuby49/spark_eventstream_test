$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
$source = Get-Content (Join-Path $directory 'private-source-canary.json') -Raw | ConvertFrom-Json
if (-not $source.acknowledged -or -not $source.tlsVerified -or $source.baselineRun) {
    throw 'Eventhouse readback requires the authenticated private low-temperature source fixture.'
}

function Invoke-AzJson {
    param([string[]] $Arguments)
    $result = & az @Arguments --subscription $subscription --only-show-errors -o json
    if ($LASTEXITCODE -ne 0) { throw "Eventhouse diagnostic command failed: $($Arguments[0])." }
    $result | ConvertFrom-Json
}

function Invoke-AzAction {
    param([string[]] $Arguments)
    & az @Arguments --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw "Eventhouse diagnostic action failed: $($Arguments[0])." }
}

$account = Invoke-AzJson @('account', 'show')
if ($account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Wrong MCAP identity.' }
$endpoint = Invoke-AzJson @('network', 'private-endpoint', 'show', '-g', 'rg_esehspark',
    '-n', 'pe-esehspark-fabric-workspace')
if ($endpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved') {
    throw 'Actual Fabric workspace PE is not approved.'
}
$nic = Invoke-AzJson @('network', 'nic', 'show', '--ids', $endpoint.networkInterfaces[0].id)
$addresses = @($nic.ipConfigurations.privateIPAddress)
if (-not $addresses.Count) { throw 'Actual Fabric private NIC addresses unavailable.' }
$encoded = [Convert]::ToBase64String([IO.File]::ReadAllBytes(
    (Join-Path $PSScriptRoot 'Read-PrivateEventhouseCanary.ps1')))
$addressArgument = (@($addresses | ForEach-Object { "'$_'" }) -join ',')
$remote = @"
`$body=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$encoded'))
& ([scriptblock]::Create(`$body)) -WorkspaceId '$($inventory.workspaceId)' -DatabaseId '$($inventory.kqlDatabaseId)' -RunId '$($source.run_id)' -MessageId '$($source.message_id)' -ExpectedPrivateAddresses @($addressArgument)
"@
$runtimePath = Join-Path $directory 'private-eventhouse-runtime.ps1'
[IO.File]::WriteAllText($runtimePath, $remote, [Text.UTF8Encoding]::new($false))
$capacityId = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
try {
    $capacity = Invoke-AzJson @('rest', '--method', 'get', '--url', $capacityUri)
    if ($capacity.sku.name -ne 'F8') { throw 'Exact approved F8 is unavailable.' }
    if ($capacity.properties.state -eq 'Paused') {
        Invoke-AzAction @('rest', '--method', 'post', '--url',
            "https://management.azure.com$capacityId/resume?api-version=2023-11-01")
    }
    $deadline = [DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $capacity = Invoke-AzJson @('rest', '--method', 'get', '--url', $capacityUri)
        if ([DateTime]::UtcNow -gt $deadline) { throw 'F8 readiness unverified within budget.' }
    } while ($capacity.properties.state -ne 'Active')
    Invoke-AzAction @('vm', 'start', '-g', 'rg_esehspark', '-n', 'vmesehspark')
    $command = Invoke-AzJson @('vm', 'run-command', 'invoke', '-g', 'rg_esehspark', '-n', 'vmesehspark',
        '--command-id', 'RunPowerShellScript', '--scripts', "@$runtimePath")
    $command | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $directory 'private-eventhouse-command.json')
    $stdout = @($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if ($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'PRIVATE_EVENTHOUSE_CANARY:(\{[^\r\n]+\})') {
        throw 'Private Eventhouse diagnostic did not return structured evidence.'
    }
    $result = $Matches[1] | ConvertFrom-Json
    $result | ConvertTo-Json -Depth 30 | Set-Content (Join-Path $directory 'private-eventhouse-canary.json')
    if (-not $result.eventhouseIngestionProven) {
        throw 'Final-policy Eventhouse ingestion remains unproven; redacted stage evidence is retained.'
    }
}
finally {
    try {
        Invoke-AzAction @('vm', 'deallocate', '-g', 'rg_esehspark', '-n', 'vmesehspark')
    }
    finally {
        Invoke-AzAction @('rest', '--method', 'post', '--url',
            "https://management.azure.com$capacityId/suspend?api-version=2023-11-01")
        $deadline = [DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 10
            $capacity = Invoke-AzJson @('rest', '--method', 'get', '--url', $capacityUri)
            if ([DateTime]::UtcNow -gt $deadline) { throw 'F8 idle suspension unverified within budget.' }
        } while ($capacity.properties.state -ne 'Paused')
        & (Join-Path $PSScriptRoot 'Export-PreservedDeploymentEvidence.ps1')
    }
}
