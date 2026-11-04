param(
    [Parameter(Mandatory)][string] $WorkspaceId,
    [Parameter(Mandatory)][string] $LakehouseId,
    [Parameter(Mandatory)][string] $ExpectedCapacityId,
    [Parameter(Mandatory)][string] $NotebookPayload,
    [Parameter(Mandatory)][string] $RunId,
    [ValidateSet('eventhubs-amqp','kafka-oap-off','functional-processing')][string] $DiagnosticKind = 'eventhubs-amqp',
    [string] $ProductionNotebookPayload,
    [string] $FixtureMessageId,
    [string] $FixtureProducer,
    [string] $FixturePrivateHelper,
    [string] $LifecycleHelper,
    [string] $ObserverSource,
    [string] $WorkloadInventorySource,
    [bool] $Baseline = $false,
    [bool] $SustainedDiagnostic = $false,
    [bool] $MeasureRawPublication = $false,
    [bool] $FaultMatrix = $false,
    [int] $TestRate = 30,
    [int] $TestMinutes = 30
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$deadline = [DateTime]::UtcNow.AddMinutes($(if($Baseline){$TestMinutes+18}elseif($FaultMatrix){20}else{10}))
$prefix = $WorkspaceId.Replace('-','')
$apiHost = "$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
$report = @{runId=$RunId;workspaceId=$WorkspaceId;productionNotebookChanged=$false;
    protectionChanged=$false;acceptancePassed=$false;liveAmqpTestRun=$false}
$jobUri = $null
$stage = 'workspace_private_dns'
function Assert-PrivateHost {
    param([string] $Hostname, [string] $Expected)
    $addresses = @([Net.Dns]::GetHostAddresses($Hostname) | ForEach-Object IPAddressToString)
    if (-not $addresses.Count -or @($addresses | Where-Object { $_ -ne $Expected }).Count) {
        throw 'Endpoint does not resolve exclusively to its approved private address.'
    }
}
function Token {
    param([string] $Resource)
    (Invoke-RestMethod -Uri ("http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=" +
        [uri]::EscapeDataString($Resource)) -Headers @{Metadata='true'} -TimeoutSec 15 -MaximumRedirection 0).access_token
}
function Read-PersistedDiagnostic {
    $hostName="$prefix.z$($prefix.Substring(0,2)).dfs.fabric.microsoft.com"
    Assert-PrivateHost $hostName '10.74.2.13'
    $readToken=Token 'https://storage.azure.com'
    try {
        Invoke-RestMethod -Uri "https://$hostName/$WorkspaceId/$LakehouseId/Files/diagnostics/$DiagnosticKind-$RunId.json" `
            -Headers @{Authorization="Bearer $readToken";'x-ms-version'='2021-06-08'} -TimeoutSec 30 -MaximumRedirection 0
    } finally { $readToken=$null }
}
function Request {
    param([string] $Method, [string] $Uri, [object] $Body)
    $parsed = [uri]$Uri
    if ($parsed.Scheme -ne 'https' -or $parsed.Host -ne $apiHost) { throw 'Refusing nonprivate Fabric API route.' }
    Assert-PrivateHost $apiHost '10.74.2.10'
    $arguments = @{Method=$Method;Uri=$Uri;Headers=$headers;TimeoutSec=30;MaximumRedirection=0;UseBasicParsing=$true}
    if ($null -ne $Body) { $arguments.ContentType='application/json';$arguments.Body=$Body | ConvertTo-Json -Depth 50 -Compress }
    for($attempt=1;$attempt -le 3;$attempt++){
        try { return Invoke-WebRequest @arguments }
        catch {
            $status=if($_.Exception.Response){[int]$_.Exception.Response.StatusCode}else{0}
            $transport=$_.Exception.Status.ToString()
            $transient=($status -in @(408,429,500,502,503,504) -or
                ($status -eq 0 -and $transport -in @('Timeout','ConnectFailure','ConnectionClosed','ReceiveFailure')))
            if($Method -ne 'Get' -or -not $transient -or $attempt -eq 3){throw}
            $report.requestRetries=@($report.requestRetries)+@(@{
                stage=$stage;atUtc=[DateTime]::UtcNow.ToString('o')
                attempt=$attempt;httpStatus=$status;transportStatus=$transport
            })
            Start-Sleep -Seconds $attempt
        }
    }
}
function Private-Location {
    param([object] $Response)
    $location = [uri]([string]$Response.Headers['Location'])
    if ($location.Scheme -ne 'https' -or $location.Host -notin @($apiHost,'api.fabric.microsoft.com')) {
        throw 'Unrecognized Fabric operation Location.'
    }
    return "https://$apiHost$($location.PathAndQuery)"
}
function Await-Operation {
    param([object] $Response)
    if ($Response.StatusCode -ne 202) { return }
    $location = Private-Location $Response
    do {
        Start-Sleep -Seconds 5
        $operation = (Request Get $location).Content | ConvertFrom-Json
        if ($operation.status -in @('Failed','Cancelled')) { throw 'Fabric definition operation failed.' }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Bounded diagnostic definition deadline exceeded.' }
    } while ($operation.status -ne 'Succeeded')
}
function Confirm-NotebookDefinition {
    param([string] $NotebookId, [string] $ExpectedPayload)
    $response = Request Post "$base/notebooks/$NotebookId/getDefinition?format=ipynb"
    if ($response.StatusCode -eq 202) {
        $location = Private-Location $response
        Await-Operation $response
        $response = Request Get "$location/result"
    }
    $content = $response.Content
    if ($content -is [byte[]]) { $content = [Text.Encoding]::UTF8.GetString($content) }
    $readback = $content | ConvertFrom-Json
    $parts = @($readback.definition.parts | Where-Object path -eq 'notebook-content.ipynb')
    if ($parts.Count -ne 1 -or $parts[0].payloadType -ne 'InlineBase64') {
        throw 'Notebook definition readback is missing the expected ipynb part.'
    }
    $actual = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($parts[0].payload)) | ConvertFrom-Json
    $expected = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($ExpectedPayload)) | ConvertFrom-Json
    if ($actual.cells.Count -ne $expected.cells.Count) { throw 'Notebook cell count differs on readback.' }
    for ($index = 0; $index -lt $expected.cells.Count; $index++) {
        if ($actual.cells[$index].cell_type -ne $expected.cells[$index].cell_type -or
            ($actual.cells[$index].source -join '') -cne ($expected.cells[$index].source -join '')) {
            throw 'Notebook cell source differs on readback.'
        }
    }
}
try {
    if($Baseline){
        if(-not $WorkloadInventorySource){throw 'Fresh workload inventory required before baseline.'}
        $inventoryText=& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString(
            [Convert]::FromBase64String($WorkloadInventorySource)))) -WorkspaceId $WorkspaceId
        $inventoryJson=($inventoryText|Where-Object {$_ -like 'WORKLOAD_INVENTORY:*'}) -replace '^WORKLOAD_INVENTORY:',''
        $workload=$inventoryJson|ConvertFrom-Json
        if(-not $workload.complete -or @($workload.nonterminalJobs).Count -or
            @($workload.sessions).Count -or @($workload.pythonProcesses).Count -or
            @($workload.generatorTasks|Where-Object State -eq 'Running').Count){
            throw 'Unrelated workload active or inventory unavailable; no baseline started.'
        }
        $report.workloadInventory=$workload
    }
    Assert-PrivateHost $apiHost '10.74.2.10'
    $token = Token 'https://api.fabric.microsoft.com'
    $headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='spark-cli'}
    $base = "https://$apiHost/v1/workspaces/$WorkspaceId"
    $stage = 'private_workspace_metadata'
    $workspace = (Request Get $base).Content | ConvertFrom-Json
    if ($workspace.capacityId -ne $ExpectedCapacityId) { throw 'Workspace capacity identity changed.' }
    $stage = 'diagnostic_notebook'
    $name = if($DiagnosticKind -eq 'functional-processing'){'esehspark-kafka-oap-off-diagnostic'}else{"esehspark-$DiagnosticKind-diagnostic"}
    $items = (Request Get "$base/items?type=Notebook").Content | ConvertFrom-Json
    $matches = @($items.value | Where-Object displayName -eq $name)
    if ($items.continuationUri) { throw 'Notebook list was paginated; refusing ambiguous discovery.' }
    if ($matches.Count -gt 1) { throw 'Ambiguous AMQP diagnostic notebook.' }
    $definition = @{format='ipynb';parts=@(@{path='notebook-content.ipynb';payloadType='InlineBase64';payload=$NotebookPayload})}
    if ($matches.Count -eq 0) {
        if($DiagnosticKind -eq 'functional-processing'){throw 'Existing Kafka diagnostic notebook required; no new item authorized.'}
        Await-Operation (Request Post "$base/items" @{displayName=$name;type='Notebook';definition=$definition})
        $items = (Request Get "$base/items?type=Notebook").Content | ConvertFrom-Json
        $matches = @($items.value | Where-Object displayName -eq $name)
    }
    if ($matches.Count -ne 1) { throw 'Diagnostic notebook could not be uniquely discovered.' }
    $id = $matches[0].id
    $report.notebookId = $id
    $jobs = "$base/items/$id/jobs/instances"
    $recent = (Request Get $jobs).Content | ConvertFrom-Json
    if (@($recent.value | Where-Object {
        $_.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped') -or
        [DateTime]$_.startTimeUtc -gt [DateTime]::UtcNow.AddMinutes(-5)
    }).Count) { throw 'Recent or active diagnostic job exists; refusing duplicate.' }
    if($DiagnosticKind -eq 'functional-processing'){
        $production=@($items.value|Where-Object displayName -eq 'esehnotebookspark')
        if($production.Count -ne 1 -or -not $ProductionNotebookPayload){throw 'Production notebook/package identity missing.'}
        $productionJobs=(Request Get "$base/items/$($production[0].id)/jobs/instances").Content|ConvertFrom-Json
        if(@($productionJobs.value|Where-Object {$_.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')}).Count){
            throw 'Active production job prevents safe source update.'
        }
        $productionDefinition=@{format='ipynb';parts=@(@{path='notebook-content.ipynb';payloadType='InlineBase64';payload=$ProductionNotebookPayload})}
        Await-Operation (Request Post "$base/notebooks/$($production[0].id)/updateDefinition" @{definition=$productionDefinition})
        $report.productionNotebookChanged=$true
        $report.productionNotebookId=$production[0].id
        $report.productionDefinitionUpdatedAtUtc=[DateTime]::UtcNow.ToString('o')
        Confirm-NotebookDefinition $production[0].id $ProductionNotebookPayload
        $report.productionDefinitionReadbackVerified=$true
    }
    Await-Operation (Request Post "$base/notebooks/$id/updateDefinition" @{definition=$definition})
    Confirm-NotebookDefinition $id $NotebookPayload
    $report.diagnosticDefinitionReadbackVerified=$true
    $stage = 'notebook_run'
    $run = Request Post "$jobs`?jobType=RunNotebook"
    $jobUri = Private-Location $run
    $report.jobId = ([uri]$jobUri).Segments[-1]
    if($FixtureProducer){
        $stage='wait_verified_consumer_before_new_fixture'
        $storageHost="$prefix.z$($prefix.Substring(0,2)).dfs.fabric.microsoft.com"
        Assert-PrivateHost $storageHost '10.74.2.13'
        $storageToken=Token 'https://storage.azure.com'
        $readyUri="https://$storageHost/$WorkspaceId/$LakehouseId/Files/diagnostics/functional-ready-$RunId.json"
        $ready=$null
        do{
            Start-Sleep -Seconds 10
            $job=(Request Get $jobUri).Content|ConvertFrom-Json
            if($job.status -in @('Completed','Failed','Cancelled','Canceled')){
                $report.jobStatus=$job.status
                $report.consumerStoppedBeforeSend=$true
                if($job.status -eq 'Completed'){$report.result=Read-PersistedDiagnostic}
                throw 'Consumer terminated before fixture readiness.'
            }
            try{
                $ready=Invoke-RestMethod -Uri $readyUri -Headers @{Authorization="Bearer $storageToken";'x-ms-version'='2021-06-08'} -TimeoutSec 15
            }catch{
                if([int]$_.Exception.Response.StatusCode -ne 404){throw}
            }
            if([DateTime]::UtcNow -gt $deadline){throw 'Consumer readiness deadline exceeded; no fixture sent.'}
        }while(-not $ready)
        if($ready.runId -ne $RunId -or $ready.fixtureMessageId -ne $FixtureMessageId -or
            $ready.fixtureRunId -ne $RunId -or -not $ready.consumerActive){throw 'Consumer readiness identity mismatch.'}
        if($Baseline -and ($ready.sessionAllocation.activeExecutors -ne 1 -or
            $ready.sessionAllocation.maximumDriverAndExecutorCores -ne 16 -or
            $ready.sessionAllocation.configuration.'spark.dynamicAllocation.enabled' -ne 'false' -or
            $ready.sessionAllocation.configuration.'spark.executor.instances' -ne '1')){
            throw 'Fixed single-executor configuration must be proven before any baseline/diagnostic send.'
        }
        $report.consumerReadyBeforeSend=$ready
        $report.latencyMeasurementBasis='spark_pre_write'
        $report.outputWriteTimeIncluded=$false
        $report.independentOutputObserverUsed=$false
        $observer='C:\ProgramData\esehspark\diagnostics\private_delta_observer.py'
        if($ObserverSource){
            [IO.File]::WriteAllBytes($observer,[Convert]::FromBase64String($ObserverSource))
        }
        $observeSeconds=if($Baseline){$TestMinutes*60+600}else{180}
        if($MeasureRawPublication){
            if(-not(Test-Path $observer)){throw 'Raw-publication reader must be staged before baseline.'}
            $rawOutput="C:\ProgramData\esehspark\diagnostics\raw-visibility-$RunId.jsonl"
            $rawObserverProcess=Start-Process -FilePath 'C:\ProgramData\esehspark\Python313\python.exe' `
                -ArgumentList "$observer --run-id $RunId --output $rawOutput --seconds $observeSeconds --raw --expected-count $($TestRate*$TestMinutes)" `
                -PassThru -RedirectStandardOutput "$rawOutput.stdout" -RedirectStandardError "$rawOutput.stderr"
            $null=$rawObserverProcess.Handle
            $rawReadyPath=[IO.Path]::ChangeExtension($rawOutput,'.ready.json')
            $rawReadyDeadline=[DateTime]::UtcNow.AddMinutes(5)
            while(-not(Test-Path $rawReadyPath)){
                if($rawObserverProcess.HasExited -or [DateTime]::UtcNow -gt $rawReadyDeadline){
                    throw 'Independent raw-publication reader failed to arm; no baseline sent.'
                }
                Start-Sleep -Milliseconds 500
            }
            $report.independentRawObserverArmed=Get-Content $rawReadyPath -Raw|ConvertFrom-Json
            $report.rawPublicationMeasured=$true
        }
        $stage='presend_eventstream_readiness'
        . ([scriptblock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($LifecycleHelper))))
        $streams=(Request Get "$base/eventstreams").Content|ConvertFrom-Json
        $stream=@($streams.value|Where-Object displayName -eq 'esspark')
        if($stream.Count -ne 1){throw 'Eventstream identity ambiguous before fixture.'}
        $headers['x-ms-fabric-skill']='eventstream-cli'
        $topology=(Request Get "$base/eventstreams/$($stream[0].id)/topology").Content|ConvertFrom-Json
        $components=@(Get-RequiredEventstreamComponents $topology)
        if(@($components|Where-Object status -ne 'Running').Count){throw 'All required processing must be Running before live fixture.'}
        $ownedReaders=@()
        if($rawObserverProcess){$ownedReaders+=@($rawObserverProcess.Id)}
        if(@(Get-CimInstance Win32_Process|Where-Object {$_.Name -match '^python.*\.exe$' -and $_.ProcessId -notin $ownedReaders}).Count){throw 'Other Python process active; no new fixture sent.'}
        $folder="C:\ProgramData\esehspark\diagnostics\live-$RunId"
        New-Item -ItemType Directory -Force -Path $folder|Out-Null
        [IO.File]::WriteAllBytes((Join-Path $folder 'functional_fixtures.py'),[Convert]::FromBase64String($FixtureProducer))
        [IO.File]::WriteAllBytes((Join-Path $folder 'private_source_canary.py'),[Convert]::FromBase64String($FixturePrivateHelper))
        $fixturePath=Join-Path $folder 'fixture.json'
        $stage='new_identifiable_high_fixture'
        if($Baseline){
            Assert-PrivateHost 'esehsparkkv.vault.azure.net' '10.74.2.4'
            Assert-PrivateHost 'esehusw3qzfnf3vqakins22j.servicebus.windows.net' '10.74.2.15'
            $env:KEY_VAULT_URL='https://esehsparkkv.vault.azure.net'
            & 'C:\ProgramData\esehspark\Python313\python.exe' -c 'import azure.identity, azure.keyvault.secrets, confluent_kafka'
            if($LASTEXITCODE -ne 0){throw 'Production generator dependency missing; no baseline sent.'}
            $producerArguments="$folder\functional_fixtures.py --rate $TestRate --minutes $TestMinutes --run-id $RunId --manifest $folder\manifest.jsonl"
        }else{
            $producerArguments="$folder\functional_fixtures.py --run-id $RunId --output $fixturePath --high-only --message-id $FixtureMessageId --equipment-id 91"
        }
        $process=Start-Process -FilePath 'C:\ProgramData\esehspark\Python313\python.exe' `
            -ArgumentList $producerArguments `
            -PassThru -RedirectStandardOutput (Join-Path $folder 'stdout') -RedirectStandardError (Join-Path $folder 'stderr')
        $null=$process.Handle
        $sendTimeout=if($Baseline){($TestMinutes*60+100)*1000}else{60000}
        if(-not $process.WaitForExit($sendTimeout)){Stop-Process -Id $process.Id;throw 'Generator send timeout; preserve manifest.'}
        if($Baseline){
            $report.baselineRun=-not $SustainedDiagnostic
            $report.sustainedDiagnostic=$SustainedDiagnostic
            $report.generatorExitCode=$process.ExitCode
            $report.generatorManifest=@(Get-Content "$folder\manifest.jsonl"|ForEach-Object {$_|ConvertFrom-Json})
        }else{
            $report.newFixture=Get-Content $fixturePath -Raw|ConvertFrom-Json
            if($process.ExitCode -ne 0 -or -not $report.newFixture.allAcknowledged){throw 'New fixture not acknowledged.'}
        }
        $headers['x-ms-fabric-skill']='spark-cli'
    }
    do {
        Start-Sleep -Seconds 15
        $job = (Request Get $jobUri).Content | ConvertFrom-Json
        if ([DateTime]::UtcNow -gt $deadline) { throw 'AMQP diagnostic job deadline exceeded.' }
    } while ($job.status -notin @('Completed','Failed','Cancelled','Canceled'))
    $report.jobStatus = $job.status
        if($rawObserverProcess){
            if(-not $rawObserverProcess.WaitForExit(650000)){throw 'Independent raw reader did not finish in bound.'}
            $report.independentRawObserverExitCode=$rawObserverProcess.ExitCode
            $report.independentRawVisibility=@(Get-Content $rawOutput|ForEach-Object {$_|ConvertFrom-Json})
            $report.independentRawObserverTimings=Get-Content ([IO.Path]::ChangeExtension($rawOutput,'timings-summary.json')) -Raw|ConvertFrom-Json
            if($rawObserverProcess.ExitCode -ne 0){
                $report.independentRawObserverFailed=$true
                $report.independentRawObserverErrorAvailable=(Test-Path "$rawOutput.stderr")
        }
    }
    if ($job.failureReason.errorCode) { $report.jobErrorCode = $job.failureReason.errorCode }
    $stage = 'diagnostic_results'
    if ($job.exitValue) {
        $report.result = $job.exitValue | ConvertFrom-Json
    }
    else {
        $report.result = Read-PersistedDiagnostic
    }
    $report.liveAmqpTestRun = $DiagnosticKind -eq 'eventhubs-amqp'
    $report.liveDiagnosticRun = $true
    $report.diagnosticKind = $DiagnosticKind
}
catch {
    $report.failedStage = $stage
    $report.errorType = $_.Exception.GetType().Name
    if($_.Exception -is [Net.WebException]){$report.transportStatus=$_.Exception.Status.ToString()}
    if ($_.Exception.Response) { $report.httpStatus = [int]$_.Exception.Response.StatusCode }
    if ($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"') { $report.errorCode=$Matches[1] }
}
finally {
    if($rawObserverProcess -and -not $rawObserverProcess.HasExited){
        Stop-Process -Id $rawObserverProcess.Id
        $report.ownedRawObserverStoppedAfterFailure=$true
    }
    if ($jobUri) {
        try {
            $job = (Request Get $jobUri).Content | ConvertFrom-Json
            if ($job.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')) {
                Request Post "$jobUri/cancel" | Out-Null
                $cancelDeadline = [DateTime]::UtcNow.AddMinutes(2)
                do {
                    Start-Sleep -Seconds 10
                    $job = (Request Get $jobUri).Content | ConvertFrom-Json
                    if ([DateTime]::UtcNow -gt $cancelDeadline) { throw 'Cancellation remained unverified.' }
                } while ($job.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped'))
            }
            $report.finalJobStatus = $job.status
        }
        catch { $report.cancellationErrorType=$_.Exception.GetType().Name }
    }
    $token=$null;$headers=$null;$storageToken=$null
}
$report.checkedAtUtc = [DateTime]::UtcNow.ToString('o')
$bytes=[Text.Encoding]::UTF8.GetBytes(($report | ConvertTo-Json -Depth 40 -Compress))
$memory=New-Object IO.MemoryStream
$gzip=New-Object IO.Compression.GZipStream($memory,[IO.Compression.CompressionMode]::Compress,$true)
$gzip.Write($bytes,0,$bytes.Length)
$gzip.Dispose()
$compressed=[Convert]::ToBase64String($memory.ToArray())
if($compressed.Length -gt 3500){
    $archive="C:\ProgramData\esehspark\diagnostics\$DiagnosticKind-$RunId.b64"
    [IO.File]::WriteAllText($archive,$compressed)
    Write-Output ('EVENTHUBS_AMQP_DIAGNOSTIC:'+(@{
        encoding='gzip-base64-file';archivePath=$archive;archiveLength=$compressed.Length;runId=$RunId
    }|ConvertTo-Json -Compress))
    return
}
Write-Output ('EVENTHUBS_AMQP_DIAGNOSTIC:' + (@{encoding='gzip-base64';payload=$compressed}|ConvertTo-Json -Compress))
