$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '..\scripts\FabricEvidence.ps1')
$scalarRows='{"Tables":[{"Rows":[["message","run",17,95.0,"2026-11-03T21:10:07.918712Z"]]}]}'|ConvertFrom-Json
$scalarEvidence=Protect-FabricEvidence $scalarRows
$roundtrip=ConvertTo-Json -InputObject $scalarEvidence -Depth 20|ConvertFrom-Json
if($roundtrip.Tables[0].Rows[0][2] -ne 17 -or $roundtrip.Tables[0].Rows[0][3] -ne 95.0 -or
    ([DateTimeOffset]$roundtrip.Tables[0].Rows[0][4]).UtcDateTime -ne ([DateTimeOffset]'2026-11-03T21:10:07.918712Z').UtcDateTime){
    throw 'Adapted KQL scalar numbers/timestamps must retain their exact values, not serialize as empty/expanded objects.'
}
$templatePath = Join-Path $PSScriptRoot '..\fabric\eventstream\eventstream-definition.template.json'
$text = (Get-Content $templatePath -Raw).Replace('{{WORKSPACE_ID}}','workspace').Replace('{{KQL_DATABASE_ID}}','database')
$expected = $text | ConvertFrom-Json
$actual = $text | ConvertFrom-Json
Assert-FabricEventstreamGraph $actual $expected
foreach ($property in @('itemId','tableName','databaseName','workspaceId')) {
    $changed = $text | ConvertFrom-Json
    $changed.destinations[0].properties.$property = 'wrong'
    $rejected = $false
    try { Assert-FabricEventstreamGraph $changed $expected } catch { $rejected = $true }
    if (-not $rejected) { throw "Destination mismatch not rejected: $property" }
}
$changed = $text | ConvertFrom-Json
$changed.destinations[0].inputNodes[0].name = 'HighTemperatureEvents'
$rejected = $false
try { Assert-FabricEventstreamGraph $changed $expected } catch { $rejected = $true }
if (-not $rejected) { throw 'Filtered raw edge was not rejected.' }
$changed = $text | ConvertFrom-Json
$changed.operators[0].properties.conditions[0].value.value = '90.0'
$rejected = $false
try { Assert-FabricEventstreamGraph $changed $expected } catch { $rejected = $true }
if (-not $rejected) { throw 'Changed high-temperature filter was not rejected.' }
$actual.sources[0].properties | Add-Member password 'FAKE_PASSWORD_SENTINEL'
$actual.sources[0].properties | Add-Member connectionString 'Endpoint=sb://safe;SharedAccessKey=FAKE_KEY_SENTINEL'
$actual.sources[0].properties | Add-Member metadata '{"accessToken":"FAKE_TOKEN_SENTINEL","itemId":"preserved-id"}'
$actual.sources[0].properties | Add-Member note 'https://safe.example/path?sig=FAKE_SIG_SENTINEL'
$parts = @(
    @{path='eventstream.json';payloadType='InlineBase64';payload=[Convert]::ToBase64String(
        [Text.Encoding]::UTF8.GetBytes(($actual | ConvertTo-Json -Depth 30)))},
    @{path='eventstreamProperties.json';payloadType='InlineBase64';payload=[Convert]::ToBase64String(
        [Text.Encoding]::UTF8.GetBytes('{"retentionTimeInDays":1,"credentials":{"value":"FAKE_NESTED_SENTINEL"}}'))},
    @{path='.platform';payloadType='InlineBase64';payload=[Convert]::ToBase64String(
        [Text.Encoding]::UTF8.GetBytes('{"metadata":{"displayName":"esspark"},"config":{"logicalId":"retained"}}'))}
)
$path = New-TemporaryFile
try {
    Save-FabricEvidence $path.FullName (Convert-FabricDefinitionEvidence @{definition=@{parts=$parts}})
    $saved = Get-Content $path.FullName -Raw
    if ($saved -match 'FAKE_|SharedAccessKey=|InlineBase64' -or $saved -notmatch '\[REDACTED\]') {
        throw 'Encoded, nested or free-text credentials leaked into definition evidence.'
    }
    $decoded = $saved | ConvertFrom-Json
    Save-FabricEvidence $path.FullName $decoded
    if ((Get-Content $path.FullName -Raw) -match 'FAKE_') { throw 'Redaction must remain safe and idempotent.' }
    $graph = @($decoded.parts | Where-Object path -eq 'eventstream.json')[0].content
    Assert-FabricEventstreamGraph $graph $expected
    if(@($decoded.parts|Where-Object path -eq '.platform').Count -ne 1){throw 'Documented platform JSON must be retained.'}
    $normalized=$text | ConvertFrom-Json
    $normalized.operators[0].properties.conditions[0].column.PSObject.Properties.Remove('columnPath')
    $normalized.operators[0].properties.conditions[0].column | Add-Member columnPathSegments $null
    Assert-FabricEventstreamGraph $normalized $expected
    $normalized.operators[0].properties.conditions[0].column.columnPathSegments=@('unexpected')
    try {Assert-FabricEventstreamGraph $normalized $expected;throw 'Unexpected nested path was accepted.'}
    catch {if($_.Exception.Message -notlike '*column path mismatch*'){throw}}
    if ($graph.sources[0].properties.metadata -notmatch 'preserved-id') { throw 'Nonsecret metadata lost.' }
    $opaque = @{definition=@{parts=@(@{path='opaque.bin';payloadType='InlineBase64';payload='AAAA'})}}
    $rejected = $false
    try { $null = Convert-FabricDefinitionEvidence $opaque } catch { $rejected = $true }
    if (-not $rejected) { throw 'Opaque definition payload must fail closed.' }
    $state = Get-FabricComponentStateEvidence $expected
    if ($state.sourceDestinationStatusComplete) { throw 'Absent state was treated as running.' }
    $rejected = $false
    try { $null = Get-FabricComponentStateEvidence $expected -RequireState } catch { $rejected = $true }
    if (-not $rejected) { throw 'Unavailable required component state must fail explicitly.' }
    foreach ($node in @($actual.sources)+@($actual.destinations)) { $node | Add-Member status 'Running' }
    if (-not (Get-FabricComponentStateEvidence $actual -RequireState).sourceDestinationStatusComplete) {
        throw 'Returned documented state was not retained.'
    }
    Save-FabricEvidence $path.FullName @{
        Tables=@(@{Rows=@(@('{"password":"FAKE_MAPPING_SENTINEL","Path":"$.message_id"}'))})
    }
    $saved = Get-Content $path.FullName -Raw
    if ($saved -match 'FAKE_MAPPING' -or $saved -notmatch '\$\.message_id') { throw 'Mapping redaction/preservation failed.' }
    $row=@('RawEvents','{"Name":"RawEvents","OrderedColumns":[]}','database-id',$null,$null)
    Add-Member -InputObject $row -NotePropertyName ExtraAdaptedProperty -NotePropertyValue 'not-a-row-column'
    $arrayResponse=Protect-FabricEvidence @{Tables=@(@{Rows=@(,$row)})}
    $arrayJson=$arrayResponse|ConvertTo-Json -Depth 30
    if($arrayJson -match '"SyncRoot"|"ExtraAdaptedProperty"' -or
        $arrayResponse.Tables[0].Rows[0].Count -ne 5 -or $arrayResponse.Tables[0].Rows[0][0] -ne 'RawEvents'){
        throw 'Adapted Kusto row arrays must remain arrays, not collection-property objects.'
    }
    $array=Protect-FabricEvidence '[{"column":"message_id","path":"$.message_id","secret":"FAKE_MAPPING"}]'
    if(-not $array.StartsWith('[') -or $array -match 'FAKE_MAPPING'){throw 'Single-entry embedded mapping array shape/redaction lost.'}
    $empty=Protect-FabricEvidence '[]'
    if($empty -ne '[]'){throw 'Empty embedded mapping array shape lost.'}
    $mirroring=@{Tables=@(@{
        Columns=@(@{ColumnName='PolicyName'},@{ColumnName='EntityName'},@{ColumnName='Policy'})
        Rows=@(,@('MirroringPolicy','[database-id].[RawEvents]',
            '{"ConnectionStrings":["https://example.microsoft.com/table/;AccountKey=FAKE_MIRRORING"],"Format":"parquet","IsEnabled":true}'))
    })}
    Save-FabricEvidence $path.FullName $mirroring
    $saved=Get-Content $path.FullName -Raw
    $policyEvidence=$saved|ConvertFrom-Json
    if($saved -match 'FAKE_MIRRORING' -or
        $policyEvidence.Tables[0].Rows[0][1] -ne '[database-id].[RawEvents]' -or
        ($policyEvidence.Tables[0].Rows[0][2]|ConvertFrom-Json).IsEnabled -ne $true){
        throw 'Documented mirroring EntityName shape must survive while nested connection credentials are removed.'
    }
    try{$null=Protect-FabricEvidence @{errorId='{"error":"FAKE_CREDENTIAL"},Cmdlet'};throw 'Malformed JSON must fail.'}
    catch{if($_.Exception.Message -notlike '*at $.errorId;*'){throw}}
    $schemaSource = Get-Content (Join-Path $PSScriptRoot '..\scripts\Set-EventhouseSchema.ps1') -Raw
    $schemaReadback = '$result = $resultText | ConvertFrom-Json' + "`n" +
        "    Save-SchemaReadback 'eventhouse-deployed-schema.json' '.show table RawEvents schema as json' " + '$result'
    if (-not $schemaSource.Replace("`r`n","`n").Contains($schemaReadback)) {
        throw 'Full schema evidence must be saved from the actual schema read response.'
    }
    if ([regex]::Matches($schemaSource,"Save-SchemaReadback 'eventhouse-deployed-schema.json'").Count -ne 1) {
        throw 'Schema evidence must not be overwritten by a mutation response.'
    }
    $captureSource = Get-Content (Join-Path $PSScriptRoot '..\scripts\Read-CurrentFabricIngestionEvidence.ps1') -Raw
    if ($captureSource -match 'updateDefinition|\.create|\.alter|Set-FabricEventstream|Set-EventhouseSchema|az vm start|/resume') {
        throw 'Current capture must contain no deployment setter, schema mutation or compute resume.'
    }
    if ($captureSource -notmatch 'MaximumRedirection 0' -or
        $captureSource -notmatch 'stoppedWithoutReopeningOrResuming' -or
        $captureSource -notmatch "10\.74\.2\.11") {
        throw 'Current capture must retain bounded failure and private KQL guards.'
    }
    Write-Output 'Definition decoding/redaction, graph identity/edges, mapping preservation and explicit missing-state checks passed.'
}
finally { Remove-Item -LiteralPath $path.FullName -Force }
