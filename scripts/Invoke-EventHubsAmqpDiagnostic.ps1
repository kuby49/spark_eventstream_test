param(
    [ValidateSet('eventhubs-amqp','kafka-oap-off','functional-processing')][string] $DiagnosticKind = 'eventhubs-amqp',
    [switch] $KeepRunning,
    [switch] $LiveHighFixture,
    [switch] $ReuseLastHighFixture,
    [switch] $Baseline,
    [switch] $LoadTest,
    [switch] $FaultMatrix,
    [switch] $WriteProfile,
    [switch] $OptimizeWriteProfile,
    [switch] $SustainedDiagnostic,
    [string] $ReplayQuarantineRunId,
    [string] $ReplayQuarantineMessageId,
    [ValidateScript({$_ -gt 0 -and $_ % 30 -eq 0})][int] $TestRate = 30
)
$ErrorActionPreference = 'Stop'
if($OptimizeWriteProfile -and -not $WriteProfile){
    throw 'OptimizeWriteProfile requires the explicit unchanged-row WriteProfile mode.'
}
if($SustainedDiagnostic){
    if($DiagnosticKind -ne 'functional-processing' -or -not $KeepRunning -or $Baseline -or
        $LoadTest -or $FaultMatrix -or $WriteProfile -or $LiveHighFixture -or
        $ReuseLastHighFixture -or $ReplayQuarantineRunId -or $ReplayQuarantineMessageId){
        throw 'SustainedDiagnostic requires functional-processing and KeepRunning without other modes.'
    }
    $Baseline=$true
}
if($WriteProfile -and ($DiagnosticKind -ne 'functional-processing' -or -not $KeepRunning -or
    $FaultMatrix -or $Baseline -or $LiveHighFixture -or $ReuseLastHighFixture -or
    $ReplayQuarantineRunId -or $ReplayQuarantineMessageId)){
    throw 'WriteProfile requires functional-processing and KeepRunning without sends or replay.'
}
$replayRequested=($ReplayQuarantineRunId -or $ReplayQuarantineMessageId)
if($replayRequested){
    if($DiagnosticKind -ne 'functional-processing' -or -not $KeepRunning -or
        $FaultMatrix -or $Baseline -or $LiveHighFixture -or $ReuseLastHighFixture -or
        -not $ReplayQuarantineRunId -or -not $ReplayQuarantineMessageId){
        throw 'Quarantine replay requires both target UUIDs, functional-processing and KeepRunning without other test modes.'
    }
    $ReplayQuarantineRunId=[guid]::Parse($ReplayQuarantineRunId).ToString()
    $ReplayQuarantineMessageId=[guid]::Parse($ReplayQuarantineMessageId).ToString()
}
if($FaultMatrix -and ($DiagnosticKind -ne 'functional-processing' -or -not $KeepRunning -or
    $Baseline -or $LiveHighFixture -or $ReuseLastHighFixture)){
    throw 'FaultMatrix requires functional-processing and KeepRunning without live/baseline sends.'
}
if($Baseline){
    if($DiagnosticKind -ne 'functional-processing' -or -not $KeepRunning -or $ReuseLastHighFixture){
        throw 'Baseline requires functional-processing, KeepRunning and a new isolated run.'
    }
    $LiveHighFixture=$true
}
if($LoadTest -and -not $Baseline){throw 'LoadTest requires Baseline execution mode.'}
if(-not $LoadTest -and $TestRate -ne 30){throw 'Initial baseline rate must remain 30/min.'}
$testMinutes=if($SustainedDiagnostic){5}elseif($LoadTest){15}else{30}
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$env:AZURE_CORE_CONNECTION_TIMEOUT = '10'
$env:AZURE_CORE_READ_TIMEOUT = '20'
$env:AZURE_CORE_RETRY_TOTAL = '0'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$root = Split-Path -Parent $PSScriptRoot
$directory = Join-Path $root '.deployment'
if($Baseline){
    if($LoadTest){
        $accepted=Get-Content (Join-Path $directory 'baseline-acceptance.json') -Raw|ConvertFrom-Json
        if(-not $accepted.pass -or $accepted.rate -ne 30 -or $accepted.minutes -ne 30 -or
            $accepted.latencyMeasurementBasis -ne 'spark_pre_write'){
            throw 'Initial 30-minute baseline must pass before load tests.'
        }
    }
    $clean=Get-Content (Join-Path $directory 'functional-processing-fresh-clean-latency.json') -Raw|ConvertFrom-Json
    if(-not $clean.result.allDiagnosticStagesSucceeded -or
        $clean.latencyMeasurementBasis -ne 'spark_pre_write'){
        throw 'Clean fresh functional Spark pre-write measurement gate required before baseline.'
    }
    $sent=[DateTimeOffset]$clean.newFixture.events[0].payload.timestamp
    foreach($table in @('Events','Scores')){
        $visible=@($clean.result.outputRecords."lhspark.$table")
        $limit=if($table -eq 'Events'){20}else{30}
        if($visible.Count -ne 1 -or
            -not $visible[0].spark_processed_at -or
            ([DateTimeOffset]$visible[0].spark_processed_at-$sent).TotalSeconds -lt 0 -or
            ([DateTimeOffset]$visible[0].spark_processed_at-$sent).TotalSeconds -gt $limit){
            throw 'Fresh output latency gate missing or exceeded; no baseline.'
        }
    }
}
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
function Az-Json {
    param([string[]] $Arguments)
    $text = & az @Arguments --subscription $subscription --only-show-errors -o json
    if ($LASTEXITCODE -ne 0) { throw "AMQP diagnostic Azure read/action failed: $($Arguments[0])." }
    if ($text) { $text | ConvertFrom-Json }
}
$account = Az-Json @('account','show')
if ($account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.id -ne $subscription) { throw 'Incorrect MCAP account context.' }
$expectedOutbound = if ($DiagnosticKind -eq 'eventhubs-amqp') { 'Deny' } else { 'Allow' }
& (Join-Path $PSScriptRoot 'Export-PreservedDeploymentEvidence.ps1') -ExpectedOutbound $expectedOutbound -DiagnosticPreflight:$KeepRunning
$runId = [guid]::NewGuid().ToString()
$productionPayload=''
$fixtureMessageId=''
$fixtureProducer=''
$fixturePrivateHelper=''
$lifecycleHelper=''
$observerSource=''
$workloadInventorySource=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $PSScriptRoot 'Read-PrivateWorkloadInventory.ps1')))
if($DiagnosticKind -eq 'functional-processing'){
    $gate=Get-Content (Join-Path $directory 'oap-off-ingestion-gate-results.json') -Raw | ConvertFrom-Json
    if(-not $gate.ingestionProven -or -not $gate.fixtures.allAcknowledged){throw 'Raw fixture ingestion must be proven first.'}
    $high=@($gate.fixtures.events | Where-Object {$_.payload.temperature -gt 80})
    if($high.Count -ne 1){throw 'Exactly one high fixture required.'}
    $average=(@($gate.fixtures.events | ForEach-Object {$_.payload.temperature})|Measure-Object -Average).Average
    $fixtureRunId=$gate.runId
    $fixtureMessageId=$high[0].payload.message_id
    if($ReuseLastHighFixture){
        if($LiveHighFixture){throw 'Choose retained fixture or new live fixture, not both.'}
        $previous=Get-Content (Join-Path $directory 'functional-processing-results.json') -Raw|ConvertFrom-Json
        $sent=@($previous.newFixture.events)
        if(-not $previous.newFixture.allAcknowledged -or $sent.Count -ne 1 -or $sent[0].payload.temperature -ne 95){
            throw 'Last live high fixture acknowledgement unavailable.'
        }
        $fixtureRunId=$previous.newFixture.run_id
        $fixtureMessageId=$sent[0].payload.message_id
        $average=95.0
    }
    if($LiveHighFixture){
        if(-not $KeepRunning){throw 'Live fixture continuation requires KeepRunning.'}
        $fixtureRunId=$runId
        $fixtureMessageId=[guid]::NewGuid().ToString()
        $average=95.0
        $producerFile=if($Baseline){'src\generator\producer.py'}else{'src\generator\functional_fixtures.py'}
        $fixtureProducer=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $root $producerFile)))
        $fixturePrivateHelper=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $root 'src\generator\private_source_canary.py')))
        $lifecycleHelper=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $PSScriptRoot 'EventstreamLifecycle.ps1')))
        $observerSource=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $root 'src\reconciliation\private_delta_observer.py')))
    }
    & (Join-Path $PSScriptRoot 'Build-FabricNotebook.ps1') -WorkspaceId $inventory.workspaceId `
        -LakehouseId $inventory.lakehouseId -KeyVaultUrl 'https://esehsparkkv.vault.azure.net' `
        -KustoEndpoint $inventory.kustoQueryEndpoint -FunctionUrl 'https://esehspark-score-api.azurewebsites.net' `
        -RunId $runId -OutputPath (Join-Path $directory 'streaming_pipeline.ipynb')
    $productionPayload=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $directory 'streaming_pipeline.ipynb')))
    $source=(Get-Content (Join-Path $root 'fabric\notebooks\streaming_pipeline.py') -Raw).
        Split(@('if RUN_MODE == "private-canary":'),[StringSplitOptions]::None)[0]
    $tail=Get-Content (Join-Path $root 'fabric\notebooks\functional_processing_diagnostic_tail.py') -Raw
    if($Baseline){
        $source+=$tail.Split(@('gate_output_records = {}'),[StringSplitOptions]::None)[0]
        $source+=Get-Content (Join-Path $root 'fabric\notebooks\baseline_diagnostic_tail.py') -Raw
    }elseif($WriteProfile){
        $source+=$tail.Split(@('gate_output_records = {}'),[StringSplitOptions]::None)[0]
        $source+=Get-Content (Join-Path $root 'fabric\notebooks\write_profile_diagnostic_tail.py') -Raw
    }elseif($replayRequested){
        $source+=$tail.Split(@('gate_output_records = {}'),[StringSplitOptions]::None)[0]
        $source+=Get-Content (Join-Path $root 'fabric\notebooks\quarantine_replay_diagnostic_tail.py') -Raw
    }elseif($FaultMatrix){
        $source+=$tail.Split(@('gate_output_records = {}'),[StringSplitOptions]::None)[0]
        $source+=Get-Content (Join-Path $root 'fabric\notebooks\fault_processing_diagnostic_tail.py') -Raw
    }else{$source+=$tail}
    $replacements=@{
        '<injected-run-id>'=$runId;'<injected-workspace-id>'=$inventory.workspaceId;
        '<injected-lakehouse-id>'=$inventory.lakehouseId;'<injected-run-mode>'='functional-processing';
        '<injected-private-key-vault-url>'='https://esehsparkkv.vault.azure.net';
        '<injected-kql-query-endpoint>'=$inventory.kustoQueryEndpoint;
        '<injected-private-function-url>'='https://esehspark-score-api.azurewebsites.net';
        '<injected-fixture-run-id>'=$fixtureRunId;'<injected-fixture-message-id>'=$fixtureMessageId;
        '<injected-live-fixture>'=$LiveHighFixture.IsPresent.ToString().ToLowerInvariant();
        '<injected-fixture-average>'=$average.ToString([Globalization.CultureInfo]::InvariantCulture)
        '<injected-load-test>'=$LoadTest.IsPresent.ToString().ToLowerInvariant();
        '<injected-sustained-diagnostic>'=$SustainedDiagnostic.IsPresent.ToString().ToLowerInvariant();
        '<injected-test-rate>'=$TestRate;'<injected-test-minutes>'=$testMinutes
        '<injected-replay-run-id>'=$ReplayQuarantineRunId
        '<injected-replay-message-id>'=$ReplayQuarantineMessageId
        '<injected-optimize-write-profile>'=$OptimizeWriteProfile.IsPresent.ToString().ToLowerInvariant()
    }
    foreach($key in $replacements.Keys){$source=$source.Replace($key,[string]$replacements[$key])}
}
else{
    $sourceFile = if ($DiagnosticKind -eq 'kafka-oap-off') { 'kafka_oap_off_diagnostic.py' } else { 'eventhubs_amqp_diagnostic.py' }
    $source = (Get-Content (Join-Path $root "fabric\notebooks\$sourceFile") -Raw).
        Replace('<injected-run-id>', $runId)
}
if($source -match '<injected-[^>]+>'){throw 'Diagnostic contains unresolved deployment values.'}
$package = Get-Content (Join-Path $directory 'streaming_pipeline.ipynb') -Raw | ConvertFrom-Json
if ($package.metadata.trident.lakehouse.default_lakehouse -ne $inventory.lakehouseId) {
    throw 'Existing notebook package lacks the expected Lakehouse binding.'
}
$configurationCells=@($package.cells|Where-Object {$_.source[0].Trim() -eq '%%configure'})
if($DiagnosticKind -eq 'functional-processing' -and $configurationCells.Count -ne 1){
    throw 'Functional diagnostic requires the shared first-cell Spark session configuration.'
}
$package.cells = $configurationCells + @(@{cell_type='code';execution_count=$null;metadata=@{};outputs=@();
    source=@($source -split "`n" | ForEach-Object {"$_`n"})})
$payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($package | ConvertTo-Json -Depth 50)))
$runtime = [Convert]::ToBase64String([IO.File]::ReadAllBytes(
    (Join-Path $PSScriptRoot 'Invoke-EventHubsAmqpPrivateRuntime.ps1')))
$measureRawPublication=$Baseline -and -not $LoadTest -and -not $SustainedDiagnostic
$remote = @"
`$source=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$runtime'))
& ([scriptblock]::Create(`$source)) -WorkspaceId '$($inventory.workspaceId)' -LakehouseId '$($inventory.lakehouseId)' -ExpectedCapacityId '$($inventory.capacityId)' -NotebookPayload '$payload' -RunId '$runId' -DiagnosticKind '$DiagnosticKind' -ProductionNotebookPayload '$productionPayload' -FixtureMessageId '$fixtureMessageId' -FixtureProducer '$fixtureProducer' -FixturePrivateHelper '$fixturePrivateHelper' -LifecycleHelper '$lifecycleHelper' -ObserverSource '$observerSource' -WorkloadInventorySource '$workloadInventorySource' -Baseline `$$($Baseline.IsPresent.ToString().ToLowerInvariant()) -SustainedDiagnostic `$$($SustainedDiagnostic.IsPresent.ToString().ToLowerInvariant()) -MeasureRawPublication `$$($measureRawPublication.ToString().ToLowerInvariant()) -FaultMatrix `$$($FaultMatrix.IsPresent.ToString().ToLowerInvariant()) -TestRate $TestRate -TestMinutes $testMinutes
"@
$runtimePath = Join-Path $directory "$DiagnosticKind-runtime.ps1"
[IO.File]::WriteAllText($runtimePath, $remote, [Text.UTF8Encoding]::new($false))
$capacityId = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
Save-FabricEvidence (Join-Path $directory "$DiagnosticKind-run.json") @{
    runId=$runId;startedAtUtc=[DateTime]::UtcNow.ToString('o');
    diagnosticKind=$DiagnosticKind;productionNotebookChanged=$false;newEventHubsResource=$false
}
try {
    $capacity = Az-Json @('rest','--method','get','--url',$capacityUri)
    if ($capacity.sku.name -ne 'F8') { throw 'Exact approved F8 not verified.' }
    if ($capacity.properties.state -eq 'Paused') {
        $null = Az-Json @('rest','--method','post','--url',
            "https://management.azure.com$capacityId/resume?api-version=2023-11-01")
    }
    $deadline = [DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $capacity = Az-Json @('rest','--method','get','--url',$capacityUri)
        if ([DateTime]::UtcNow -gt $deadline) { throw 'F8 readiness timed out.' }
    } while ($capacity.properties.state -ne 'Active')
    $null = Az-Json @('vm','start','-g','rg_esehspark','-n','vmesehspark')
    $command = Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
        '--command-id','RunPowerShellScript','--scripts',"@$runtimePath")
    $stdout = @($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if ($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'EVENTHUBS_AMQP_DIAGNOSTIC:(\{[^\r\n]+\})') {
        Save-FabricEvidence (Join-Path $directory "$DiagnosticKind-command-failure.json") @{
            runId=$runId;statusCodes=@($command.value.code);structuredResultAvailable=$false
        }
        throw 'AMQP diagnostic returned no structured evidence; raw logs were not exported.'
    }
    $result = $Matches[1] | ConvertFrom-Json
    if($result.encoding -eq 'gzip-base64-file'){
        $expected="C:\ProgramData\esehspark\diagnostics\$DiagnosticKind-$runId.b64"
        if($result.runId -ne $runId -or $result.archivePath -ne $expected -or
            $result.archiveLength -le 0 -or $result.archiveLength -gt $(if($Baseline){2000000}else{100000})){throw 'Diagnostic archive identity/size invalid.'}
        $encoded=''
        for($offset=0;$offset -lt $result.archiveLength;$offset+=2800){
            $read="`$data=[IO.File]::ReadAllText('$expected');Write-Output ('DIAG_CHUNK:'+`$data.Substring($offset,[Math]::Min(2800,`$data.Length-$offset)))"
            $chunk=Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
                '--command-id','RunPowerShellScript','--scripts',$read)
            $out=@($chunk.value|Where-Object code -eq 'ComponentStatus/StdOut/succeeded')[0].message
            if($out -notmatch 'DIAG_CHUNK:([A-Za-z0-9+/=]+)'){throw 'Diagnostic archive chunk missing.'}
            $encoded+=$Matches[1]
        }
        if($encoded.Length -ne $result.archiveLength){throw 'Diagnostic archive length mismatch.'}
        $result=@{encoding='gzip-base64';payload=$encoded}
    }
    if($result.encoding -eq 'gzip-base64'){
        $memory=[IO.MemoryStream]::new([Convert]::FromBase64String($result.payload))
        $gzip=[IO.Compression.GZipStream]::new($memory,[IO.Compression.CompressionMode]::Decompress)
        $reader=[IO.StreamReader]::new($gzip)
        try{$result=$reader.ReadToEnd() | ConvertFrom-Json}finally{$reader.Dispose();$gzip.Dispose();$memory.Dispose()}
    }
    Save-FabricEvidence (Join-Path $directory "$DiagnosticKind-results.json") $result
    Write-Output ($result | ConvertTo-Json -Depth 30)
}
finally {
    if($KeepRunning){
        Write-Output 'No compute shutdown performed; VM and F8 retained running as requested.'
    }else{
    try { $null = Az-Json @('vm','deallocate','-g','rg_esehspark','-n','vmesehspark') }
    finally {
        $null = Az-Json @('rest','--method','post','--url',
            "https://management.azure.com$capacityId/suspend?api-version=2023-11-01")
        $deadline = [DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 10
            $capacity = Az-Json @('rest','--method','get','--url',$capacityUri)
            if ([DateTime]::UtcNow -gt $deadline) { throw 'F8 suspension not verified within budget.' }
        } while ($capacity.properties.state -ne 'Paused')
        & (Join-Path $PSScriptRoot 'Export-PreservedDeploymentEvidence.ps1') -ExpectedOutbound $expectedOutbound
        Copy-Item (Join-Path $directory 'final-preservation-summary.json') `
            (Join-Path $directory "$DiagnosticKind-shutdown.json") -Force
    }
    }
}
