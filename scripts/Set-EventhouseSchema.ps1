[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json')
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
$evidenceDirectory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
function Save-SchemaReadback {
    param([string] $Name, [string] $Command, [object] $Response)
    Save-FabricEvidence -Path (Join-Path $evidenceDirectory $Name) -Value @{
        capturedAtUtc=[DateTime]::UtcNow.ToString('o');workspaceId=$inventory.workspaceId;
        databaseId=$inventory.kqlDatabaseId;databaseName=$database;tableName='RawEvents';
        queryEndpoint=$endpoint;command=$Command;response=$Response;historicalFixtureStateReconstructed=$false
    }
}
$database = [string]$inventory.kqlDatabaseName
$endpoint = ([string]$inventory.kustoQueryEndpoint).TrimEnd('/')
$commands = @(
    '.create-merge table RawEvents (message_id: string, run_id: string, equipment_id: int, temperature: real, timestamp: datetime, payload: dynamic)',
    '.alter table RawEvents policy streamingingestion enable',
    '.alter-merge table RawEvents policy mirroring dataformat=parquet with (IsEnabled=true, TargetLatencyInMinutes=5)'
)

foreach ($command in $commands) {
    $requestPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-kql-$([guid]::NewGuid()).json"
    try {
        $body = @{ db = $database; csl = $command } | ConvertTo-Json -Compress
        [IO.File]::WriteAllText($requestPath, $body, [Text.UTF8Encoding]::new($false))
        $resultText = & az rest --method post `
            --url "$endpoint/v1/rest/mgmt" `
            --resource 'https://kusto.kusto.windows.net' `
            --headers 'Content-Type=application/json' `
            --body "@$requestPath" `
            --only-show-errors --output json
        if ($LASTEXITCODE -ne 0) {
            throw "Eventhouse management command failed; command index $([array]::IndexOf($commands, $command) + 1)."
        }
        $result = $resultText | ConvertFrom-Json
        if ($result.error) {
            throw "Eventhouse management command returned an error; command index $([array]::IndexOf($commands, $command) + 1)."
        }
    }
    finally {
        Remove-Item -LiteralPath $requestPath -Force -ErrorAction SilentlyContinue
    }
}

$verifyPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-kql-verify-$([guid]::NewGuid()).json"
try {
    $body = @{
        db = $database
        csl = '.show table RawEvents schema as json'
    } | ConvertTo-Json -Compress
    [IO.File]::WriteAllText($verifyPath, $body, [Text.UTF8Encoding]::new($false))
    $resultText = & az rest --method post `
        --url "$endpoint/v1/rest/mgmt" `
        --resource 'https://kusto.kusto.windows.net' `
        --headers 'Content-Type=application/json' `
        --body "@$verifyPath" `
        --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw 'Eventhouse RawEvents schema read-back failed.' }
    $result = $resultText | ConvertFrom-Json
    Save-SchemaReadback 'eventhouse-deployed-schema.json' '.show table RawEvents schema as json' $result
    if ($result.error -or -not $result.Tables) { throw 'Eventhouse schema read-back returned no result table.' }
}
finally {
    Remove-Item -LiteralPath $verifyPath -Force -ErrorAction SilentlyContinue
}
$mirrorVerifyPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-kql-mirroring-$([guid]::NewGuid()).json"
try {
    $body = @{ db = $database; csl = '.show table RawEvents policy mirroring' } | ConvertTo-Json -Compress
    [IO.File]::WriteAllText($mirrorVerifyPath, $body, [Text.UTF8Encoding]::new($false))
    $resultText = & az rest --method post `
        --url "$endpoint/v1/rest/mgmt" `
        --resource 'https://kusto.kusto.windows.net' `
        --headers 'Content-Type=application/json' `
        --body "@$mirrorVerifyPath" `
        --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw 'Eventhouse OneLake mirroring policy read-back failed.' }
    $mirrorResult = $resultText | ConvertFrom-Json
    Save-SchemaReadback 'eventhouse-deployed-mirroring.json' '.show table RawEvents policy mirroring' $mirrorResult
    if ($mirrorResult.error -or -not $mirrorResult.Tables) {
        throw 'Eventhouse OneLake mirroring policy read-back returned no result table.'
    }
}
finally {
    Remove-Item -LiteralPath $mirrorVerifyPath -Force -ErrorAction SilentlyContinue
}
foreach ($mappingScope in @('table RawEvents', "database $database")) {
    $mappingPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-mapping-$([guid]::NewGuid()).json"
    try {
        $command = ".show $mappingScope ingestion json mappings"
        $body = @{db=$database;csl=$command} | ConvertTo-Json -Compress
        [IO.File]::WriteAllText($mappingPath, $body, [Text.UTF8Encoding]::new($false))
        $resultText = & az rest --method post --url "$endpoint/v1/rest/mgmt" `
            --resource 'https://kusto.kusto.windows.net' --headers 'Content-Type=application/json' `
            --body "@$mappingPath" --only-show-errors --output json
        if ($LASTEXITCODE -ne 0) { throw 'Documented JSON ingestion mapping read-back failed.' }
        $mapping = $resultText | ConvertFrom-Json
        $scope = if ($mappingScope.StartsWith('table')) { 'table' } else { 'database' }
        Save-SchemaReadback "eventhouse-deployed-$scope-mappings.json" $command $mapping
        if ($mapping.error -or -not $mapping.Tables) { throw 'Mapping read-back returned no valid result table.' }
    }
    finally { Remove-Item -LiteralPath $mappingPath -Force -ErrorAction SilentlyContinue }
}
Write-Output 'RawEvents schema, streaming ingestion, and 5-minute OneLake mirroring policy applied and read back.'
