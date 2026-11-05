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
    [bool] $StressTest = $false,
    [bool] $MaintenanceOnly = $false,
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
    protectionChanged=$false;acceptancePassed=$false;liveAmqpTestRun=$false;sparkMonitoringSamples=@()}
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
    if($Baseline -or $MaintenanceOnly){
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
        if(-not $StressTest -and -not $MaintenanceOnly){
            $productionDefinition=@{format='ipynb';parts=@(@{path='notebook-content.ipynb';payloadType='InlineBase64';payload=$ProductionNotebookPayload})}
            Await-Operation (Request Post "$base/notebooks/$($production[0].id)/updateDefinition" @{definition=$productionDefinition})
            $report.productionNotebookChanged=$true
            $report.productionNotebookId=$production[0].id
            $report.productionDefinitionUpdatedAtUtc=[DateTime]::UtcNow.ToString('o')
            Confirm-NotebookDefinition $production[0].id $ProductionNotebookPayload
            $report.productionDefinitionReadbackVerified=$true
        }
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
        if($Baseline -and ($ready.sessionAllocation.activeExecutors -lt 1 -or
            $ready.sessionAllocation.activeExecutors -gt 5 -or
            $ready.sessionAllocation.maximumDriverAndExecutorCores -ne 48 -or
            $ready.sessionAllocation.configuration.'spark.dynamicAllocation.enabled' -ne 'true' -or
            $ready.sessionAllocation.configuration.'spark.dynamicAllocation.minExecutors' -ne '1' -or
            $ready.sessionAllocation.configuration.'spark.dynamicAllocation.maxExecutors' -ne '5')){
            throw 'Dynamic allocation within existing 1-to-5 executor pool must be proven before sends.'
        }
        $report.consumerReadyBeforeSend=$ready
        if($StressTest){
            $sessions=(Request Get "$base/notebooks/$id/livySessions").Content|ConvertFrom-Json
            $ownedSession=@($sessions.value|Where-Object jobInstanceId -eq $report.jobId)
            if($ownedSession.Count -eq 1 -and $ownedSession[0].livyId -and $ownedSession[0].sparkApplicationId){
                $report.sparkSession=@{livyId=$ownedSession[0].livyId;
                    applicationId=$ownedSession[0].sparkApplicationId}
                $monitorBase="$base/notebooks/$id/livySessions/$($ownedSession[0].livyId)/applications/$($ownedSession[0].sparkApplicationId)"
            }else{
                $report.sparkMonitoringUnavailable='Owned session identity unavailable'
            }
        }
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
            if($StressTest){$producerArguments+=" --buffered-manifest"}
        }else{
            $producerArguments="$folder\functional_fixtures.py --run-id $RunId --output $fixturePath --high-only --message-id $FixtureMessageId --equipment-id 91"
        }
        $process=Start-Process -FilePath 'C:\ProgramData\esehspark\Python313\python.exe' `
            -ArgumentList $producerArguments `
            -PassThru -RedirectStandardOutput (Join-Path $folder 'stdout') -RedirectStandardError (Join-Path $folder 'stderr')
        $null=$process.Handle
        $report.producerStartedAtUtc=[DateTime]::UtcNow.ToString('o')
        $report.ownedProducerId=$process.Id
        $sendTimeout=if($Baseline){($TestMinutes*60+100)*1000}else{60000}
        if($StressTest){
            $report.stressTest=$true
            $report.safetySamples=@()
            $report.safetyPolicy=@{pollSeconds=10;maximumHeartbeatAgeSeconds=150;
                noCommitWithBacklogSeconds=150;backlogHighEvents=2*$TestRate/10;
                backlogSustainedSeconds=120;scope='Only owned producer/query; compute retained'}
            $sendDeadline=[DateTime]::UtcNow.AddMilliseconds($sendTimeout)
            $sendingStarted=[DateTime]::UtcNow
            $backlogSince=$null
            $lastMetrics=[DateTime]::MinValue
            $healthUri="https://$storageHost/$WorkspaceId/$LakehouseId/Files/diagnostics/stress-health-$RunId.json"
            $headers['x-ms-fabric-skill']='spark-cli'
            while(-not $process.WaitForExit(10000)){
                $now=[DateTime]::UtcNow
                if($monitorBase -and ($now-$lastMetrics).TotalSeconds -ge 60){
                    $lastMetrics=$now
                    $metricSample=@{atUtc=$now.ToString('o')}
                    foreach($surface in @('resourceUsage','executors','stages')){
                        $metric=$null
                        try{
                            $metricContent=(Request Get "$monitorBase/$surface").Content
                            $metric=$metricContent|ConvertFrom-Json
                            if($surface -ne 'resourceUsage'){
                                if($metric -is [array]){
                                    $metricSample[$surface]=@($metric)
                                }elseif('value' -in @($metric.PSObject.Properties.Name)){
                                    $metricSample[$surface]=@($metric.value)
                                }elseif(([string]$metricContent).Trim() -eq '[]'){
                                    $metricSample[$surface]=@()
                                }elseif(([string]$metricContent).TrimStart().StartsWith('[') -and
                                    (('id' -in @($metric.PSObject.Properties.Name) -and $surface -eq 'executors') -or
                                     ('stageId' -in @($metric.PSObject.Properties.Name) -and $surface -eq 'stages'))){
                                    $metricSample[$surface]=@($metric)
                                }else{
                                    throw "Unexpected Spark $surface collection shape."
                                }
                            }else{
                                $metricSample[$surface]=$metric
                            }
                        }catch{
                            $metricSample["${surface}Unavailable"]=@{errorType=$_.Exception.GetType().Name;
                                collectionType=$(if($null -ne $metric){$metric.GetType().Name});
                                collectionProperties=@($metric.PSObject.Properties.Name);
                                httpStatus=$(if($_.Exception.Response){[int]$_.Exception.Response.StatusCode}else{$null})}
                        }
                    }
                    $report.sparkMonitoringSamples=@($report.sparkMonitoringSamples)+@($metricSample)
                }
                $stage='stress_health_read'
                $job=(Request Get $jobUri).Content|ConvertFrom-Json
                $health=$null
                # fs.put replaces the health file; a reader can briefly see an incomplete publication.
                for($healthAttempt=1;$healthAttempt -le 3;$healthAttempt++){
                    try{
                        $candidate=Invoke-RestMethod -Uri $healthUri -Headers @{
                            Authorization="Bearer $storageToken";'x-ms-version'='2021-06-08'} -TimeoutSec 15
                        $report.lastHealthRead=@{atUtc=[DateTime]::UtcNow.ToString('o');
                            responseType=$(if($null -ne $candidate){$candidate.GetType().Name});
                            stringLength=$(if($candidate -is [string]){$candidate.Length});attempt=$healthAttempt}
                        if($candidate -is [string]){$candidate=$candidate|ConvertFrom-Json}
                        if($candidate.runId -and $candidate.runId -ne $RunId){throw 'Stress health identity mismatch.'}
                        if($candidate.runId -eq $RunId -and $candidate.atUtc){$health=$candidate;break}
                    }catch{
                        if($_.Exception.Message -eq 'Stress health identity mismatch.'){throw}
                        $report.healthReadRetries=@($report.healthReadRetries)+@(@{
                            atUtc=[DateTime]::UtcNow.ToString('o');attempt=$healthAttempt;
                            errorType=$_.Exception.GetType().Name})
                    }
                    Start-Sleep -Seconds 2
                }
                if(-not $health){throw 'Stress health unavailable after bounded read retries.'}
                if($health.runId -ne $RunId){throw 'Stress health identity mismatch.'}
                $stage='stress_manifest_read'
                $manifest=@(Get-Content "$folder\manifest.jsonl"|ForEach-Object {$_|ConvertFrom-Json})
                $ackIds=@{}
                foreach($row in @($manifest|Where-Object status -eq 'acknowledged')){$ackIds[$row.message_id]=$true}
                $ackHigh=@($manifest|Where-Object {
                    $_.status -eq 'generated' -and $_.temperature -gt 80 -and $ackIds.ContainsKey($_.message_id)}).Count
                $backlog=[Math]::Max(0,$ackHigh-$health.completedLogicalEvents)
                $stage='stress_safety_evaluation'
                $sample=@{atUtc=$now.ToString('o');acknowledged=$ackIds.Count;acknowledgedHigh=$ackHigh;
                    completedHigh=$health.completedLogicalEvents;backlogHigh=$backlog;
                    healthAtUtc=$health.atUtc;jobStatus=$job.status;partitionLag=$health.partitionLag;
                    allocation=$health.allocation;scalingEvents=$health.scalingEvents}
                $report.safetySamples+=@($sample)
                $reason=$null
                if($job.status -in @('Completed','Failed','Cancelled','Canceled') -or -not $health.consumerActive){
                    $reason='ConsumerTerminatedWhileSending'
                }elseif(($now-([DateTimeOffset]$health.atUtc).UtcDateTime).TotalSeconds -gt 150){
                    $reason='HealthTelemetryStale'
                }elseif(@($manifest|Where-Object status -eq 'failed').Count){
                    $reason='ProducerDeliveryFailure'
                }elseif(@($report.requestRetries|Where-Object httpStatus -eq 429).Count -or
                    @($health.rejections|Where-Object {
                        $_.errorCode -match '429|Throttl|Capacity' -or $_.errorType -match 'Throttl|Capacity'}).Count){
                    $reason='ExplicitRejectionOrThrottling'
                }elseif($now -gt $sendDeadline){
                    $reason='ProducerDeadlineExceeded'
                }elseif($backlog -gt 0 -and $health.lastBatchFinishedAtUtc -and
                    ($now-([DateTimeOffset]$health.lastBatchFinishedAtUtc).UtcDateTime).TotalSeconds -gt 150){
                    $reason='NoCommittedProgressWithBacklog'
                }
                if($backlog -ge 2*$TestRate/10){
                    if(-not $backlogSince){$backlogSince=$now}
                    if(($now-$backlogSince).TotalSeconds -ge 120){$reason='SustainedUnsafeBacklog'}
                }else{$backlogSince=$null}
                if($reason){
                    $report.safetyStop=@{reason=$reason;atUtc=$now.ToString('o');sample=$sample}
                    Stop-Process -Id $process.Id
                    $process.WaitForExit()
                    break
                }
            }
        }elseif(-not $process.WaitForExit($sendTimeout)){
            Stop-Process -Id $process.Id;throw 'Generator send timeout; preserve manifest.'
        }
        if($Baseline){
            $report.baselineRun=-not $SustainedDiagnostic
            $report.sustainedDiagnostic=$SustainedDiagnostic
            $report.generatorExitCode=$process.ExitCode
            $report.generatorManifest=@(Get-Content "$folder\manifest.jsonl"|ForEach-Object {$_|ConvertFrom-Json})
            if($StressTest -and $process.ExitCode -ne 0 -and -not $report.safetyStop){
                $report.safetyStop=@{reason='ProducerExitedUnsuccessfully';atUtc=[DateTime]::UtcNow.ToString('o')}
            }
        }else{
            $report.newFixture=Get-Content $fixturePath -Raw|ConvertFrom-Json
            if($process.ExitCode -ne 0 -or -not $report.newFixture.allAcknowledged){throw 'New fixture not acknowledged.'}
        }
        $headers['x-ms-fabric-skill']='spark-cli'
    }
    if($StressTest -and $report.safetyStop){throw 'Stress safety stop; owned query cancellation follows; checkpoint retained.'}
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
        $exitResult = $job.exitValue | ConvertFrom-Json
        if($exitResult.kind -eq 'persisted_stress_evidence_v1'){
            if(-not $StressTest -or $exitResult.runId -ne $RunId -or
                $exitResult.evidencePath -cne "Files/diagnostics/functional-processing-$RunId.json"){
                throw 'Stress evidence exit pointer identity mismatch.'
            }
            $report.result = Read-PersistedDiagnostic
            if($report.result.runId -ne $RunId){throw 'Persisted stress evidence identity mismatch.'}
            $report.reportingExitContract='durable_full_evidence_compact_pointer_v1'
        }else{
            $report.result = $exitResult
        }
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
    $report.errorId=$_.FullyQualifiedErrorId
    $report.failureStack=$_.ScriptStackTrace
    if($_.Exception.Message -match '^(Stress |Consumer |Producer |Fixed |Unrelated |Incorrect |Fresh |Bounded |AMQP |All required |Other Python |Endpoint |Refusing |Unrecognized |Recent or active |Active production |Existing Kafka |Workspace capacity |Diagnostic notebook)'){
        $report.failureReason=$_.Exception.Message
    }
    if($_.Exception -is [Net.WebException]){$report.transportStatus=$_.Exception.Status.ToString()}
    if ($_.Exception.Response) { $report.httpStatus = [int]$_.Exception.Response.StatusCode }
    if ($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"') { $report.errorCode=$Matches[1] }
}
finally {
    if($process -and -not $process.HasExited){
        Stop-Process -Id $process.Id
        $process.WaitForExit()
        $report.ownedProducerStoppedAfterFailure=$true
    }
    if($StressTest -and $folder -and (Test-Path "$folder\manifest.jsonl")){
        $report.generatorExitCode=$process.ExitCode
        $report.generatorManifest=@(Get-Content "$folder\manifest.jsonl"|ForEach-Object {$_|ConvertFrom-Json})
        $stderr=Get-Content (Join-Path $folder 'stderr') -Raw
        $report.generatorDiagnosticCodes=@([regex]::Matches($stderr,
            'Producer exceeded one event interval|[A-Za-z]*Error|[A-Za-z]*Exception|[A-Za-z]*TIMED_OUT[A-Za-z]*')|
            ForEach-Object Value|Sort-Object -Unique)
    }
    if($rawObserverProcess -and -not $rawObserverProcess.HasExited){
        Stop-Process -Id $rawObserverProcess.Id
        $report.ownedRawObserverStoppedAfterFailure=$true
    }
    if ($jobUri) {
        try {
            $job = (Request Get $jobUri).Content | ConvertFrom-Json
            if ($job.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')) {
                $report.ownedNotebookCancellationRequestedAtUtc=[DateTime]::UtcNow.ToString('o')
                Request Post "$jobUri/cancel" | Out-Null
                $cancelDeadline = [DateTime]::UtcNow.AddMinutes(2)
                do {
                    Start-Sleep -Seconds 10
                    $job = (Request Get $jobUri).Content | ConvertFrom-Json
                    if ([DateTime]::UtcNow -gt $cancelDeadline) { throw 'Cancellation remained unverified.' }
                } while ($job.status -notin @('Completed','Failed','Cancelled','Canceled','Deduped'))
            }
            $report.finalJobStatus = $job.status
            if($StressTest -and -not $report.result){
                try{$report.result=Read-PersistedDiagnostic}
                catch{$report.persistedEvidenceReadErrorType=$_.Exception.GetType().Name}
            }
        }
        catch { $report.cancellationErrorType=$_.Exception.GetType().Name }
    }
    $token=$null;$headers=$null;$storageToken=$null
}
if($StressTest){
    try {
        $phaseHost="$prefix.z$($prefix.Substring(0,2)).dfs.fabric.microsoft.com"
        Assert-PrivateHost $phaseHost '10.74.2.13'
        $phaseToken=Token 'https://storage.azure.com'
        $report.reportingPhases=Invoke-RestMethod `
            -Uri "https://$phaseHost/$WorkspaceId/$LakehouseId/Files/diagnostics/reporting-phases-$RunId.json" `
            -Headers @{Authorization="Bearer $phaseToken";'x-ms-version'='2021-06-08'} -TimeoutSec 30 -MaximumRedirection 0
        if($report.reportingPhases.runId -ne $RunId){throw 'Reporting phase identity mismatch.'}
    } catch { $report.reportingPhasesReadErrorType=$_.Exception.GetType().Name }
    finally { $phaseToken=$null }
}
$report.checkedAtUtc = [DateTime]::UtcNow.ToString('o')
if($StressTest){
    $fullPath="C:\ProgramData\esehspark\diagnostics\stress-full-$RunId.json"
    [IO.File]::WriteAllText($fullPath,($report|ConvertTo-Json -Depth 60 -Compress))
    # Derive compact evidence from the same persisted JSON used by recovery.
    $persisted=Get-Content $fullPath -Raw|ConvertFrom-Json
    $report=@{}
    foreach($property in $persisted.PSObject.Properties){$report[$property.Name]=$property.Value}
    $summary=@{runId=$RunId;rate=$TestRate;minutes=$TestMinutes;jobId=$report.jobId;
        producerEnvelope='scheduled_background_delivery_buffered_diagnostic_manifest';
        jobStatus=$report.jobStatus;finalJobStatus=$report.finalJobStatus;failedStage=$report.failedStage;
        errorType=$report.errorType;httpStatus=$report.httpStatus;errorCode=$report.errorCode;
        jobErrorCode=$report.jobErrorCode;error=$report.result.error;
        reportingPhases=$report.reportingPhases;reportingPhasesReadErrorType=$report.reportingPhasesReadErrorType;
        reportingExitContract=$report.reportingExitContract;
        failureReason=$report.failureReason;failureStack=$report.failureStack;errorId=$report.errorId;
        healthReadRetries=$report.healthReadRetries;persistedEvidenceReadErrorType=$report.persistedEvidenceReadErrorType;
        lastHealthRead=$report.lastHealthRead;
        ownedNotebookCancellationRequestedAtUtc=$report.ownedNotebookCancellationRequestedAtUtc;
        safetyStop=$report.safetyStop;generatorExitCode=$report.generatorExitCode;
        generatorDiagnosticCodes=$report.generatorDiagnosticCodes;requestRetries=$report.requestRetries;
        fullEvidencePath=$fullPath;latencyMeasurementBasis='spark_pre_write';ownOutputWriteExcluded=$true;
        originalReadableOutputSlaProven=$false;computeShutdown=$false;
        productionNotebookChanged=$report.productionNotebookChanged;sessionAllocation=$report.result.sessionAllocation;
        checkpointPreserved=$true;optimizations=$report.result.optimizations;counts=@{};latency=@{};
        postStepOptimization=$report.result.postStepOptimization;
        sparkSession=$report.sparkSession;sparkMonitoringUnavailable=$report.sparkMonitoringUnavailable;
        allocationBasis=$report.result.allocationBasis;
        producerStartedAtUtc=$report.producerStartedAtUtc;ownedProducerId=$report.ownedProducerId;
        readySessionAllocation=$report.consumerReadyBeforeSend.sessionAllocation;
        scalingEvents=@($report.result.scalingEvents);
        stages=@();safetySamples=$report.safetySamples;metricsAvailability=@()}
    if(-not $summary.allocationBasis){
        $summary.allocationBasis=$summary.readySessionAllocation.allocationBasis
    }
    if(-not $report.result -and $report.safetySamples){
        $summary.scalingEvents=@($report.safetySamples[-1].scalingEvents)
    }
    foreach($status in @('generated','acknowledged','failed')){
        $summary.counts[$status]=@($report.generatorManifest|Where-Object status -eq $status).Count
    }
    $summary.producerTiming=@($report.generatorManifest|Where-Object status -eq 'producer_timing')
    $summary.producerSchedulingFailures=@($report.generatorManifest|Where-Object status -eq 'scheduling_failed')
    $summary.maximumSchedulingLagSeconds=($report.generatorManifest|Where-Object status -eq 'generated'|
        Measure-Object scheduling_lag_seconds -Maximum).Maximum
    $summary.maximumSendToAckSeconds=($report.generatorManifest|Where-Object status -eq 'acknowledged'|
        Measure-Object send_to_ack_seconds -Maximum).Maximum
    foreach($name in @('rawRecords','derivedRecords')){
        $summary.counts[$name]=@($report.result.$name).Where({$null -ne $_}).Count
    }
    $summary.counts.completedHigh=(@($report.result.batches)|Measure-Object -Property logicalEvents -Sum).Sum
    foreach($table in @('Events','Scores','ProcessingAudit','Quarantine')){
        $rows=@($report.result.outputRecords."lhspark.$table").Where({$null -ne $_})
        $summary.counts[$table]=$rows.Count
        if($table -in @('Events','Scores')){
            $limit=if($table -eq 'Events'){20}else{30}
            $latencies=@($rows|Where-Object {$_.spark_processed_at -and $_.message_timestamp}|ForEach-Object {
                $processed=[DateTimeOffset]::Parse($_.spark_processed_at,
                    [Globalization.CultureInfo]::InvariantCulture,[Globalization.DateTimeStyles]::AssumeUniversal)
                $sent=[DateTimeOffset]::Parse($_.message_timestamp,
                    [Globalization.CultureInfo]::InvariantCulture,[Globalization.DateTimeStyles]::AssumeUniversal)
                ($processed-$sent).TotalSeconds
            }|Sort-Object)
            $summary.latency[$table]=@{timed=$latencies.Count;maximumSeconds=($latencies|Measure-Object -Maximum).Maximum;
                overLimit=@($latencies|Where-Object {$_ -gt $limit}).Count;limitSeconds=$limit}
        }
    }
    $summary.counts.scoredAudit=@($report.result.outputRecords.'lhspark.ProcessingAudit'|Where-Object status -eq 'SCORED').Count
    $summary.finalNotebookEvidenceAvailable=[bool]$report.result
    if(-not $report.result){
        foreach($key in @('rawRecords','derivedRecords','completedHigh','Events','Scores','ProcessingAudit','Quarantine','scoredAudit')){
            $summary.counts[$key]=$null
        }
    }
    foreach($group in @($report.result.stageTimings|Group-Object stage,table)){
        $summary.stages+=@(@{name=$group.Name;count=$group.Count;
            totalSeconds=($group.Group|Measure-Object elapsedSeconds -Sum).Sum;
            maximumSeconds=($group.Group|Measure-Object elapsedSeconds -Maximum).Maximum;
            errors=@($group.Group|Where-Object errorType|Select-Object stage,errorType,errorCode,attempts)})
    }
    $summary.metricsAvailability=@($report.result.stressHealthErrors|Select-Object source,errorType -Unique)
    $summary.sparkMetricSamples=@($report.sparkMonitoringSamples|Where-Object atUtc|ForEach-Object {
        $executorRows=if('value' -in @($_.executors.PSObject.Properties.Name)){@($_.executors.value)}else{@($_.executors)}
        $stageRows=if('value' -in @($_.stages.PSObject.Properties.Name)){@($_.stages.value)}else{@($_.stages)}
        @{atUtc=$_.atUtc;executors=@($executorRows|Where-Object {$null -ne $_}|Select-Object id,isActive,totalCores,activeTasks,
            failedTasks,completedTasks,totalDuration,totalGCTime,memoryUsed,maxMemory,memoryMetrics,
            totalShuffleRead,totalShuffleWrite,addTime,removeTime,removeReason);
          slowStages=@($stageRows|Where-Object {$null -ne $_}|Sort-Object executorRunTime -Descending|
              Select-Object -First 3 stageId,status,submissionTime,completionTime,numTasks,numFailedTasks,
                  executorRunTime,executorCpuTime,jvmGcTime,memoryBytesSpilled,diskBytesSpilled,peakExecutionMemory,
                  shuffleFetchWaitTime,shuffleReadBytes,shuffleWriteBytes);
          failedStages=@($stageRows|Where-Object {$_.numFailedTasks -gt 0}|
              Select-Object stageId,attemptId,status,numFailedTasks,numKilledTasks,
                  @{n='failureReason';e={if($_.failureReason){"Spark failure: $($_.failureReason)"}else{$null}}},
                  killedTasksSummary);
          resourceUsage=($_.resourceUsage|Select-Object duration,idleTime,coreEfficiency,capacityExceeded);
          resourceUsageUnavailable=$_.resourceUsageUnavailable;
          executorsUnavailable=$_.executorsUnavailable;stagesUnavailable=$_.stagesUnavailable}
    })
    $generated=@($report.generatorManifest|Where-Object status -eq 'generated')
    $acked=@($report.generatorManifest|Where-Object status -eq 'acknowledged')
    $summary.offeredRateBasis='generated_timestamp_before_manifest_fsync'
    $enqueueRows=@($acked|Where-Object first_enqueue_started_at|
        Sort-Object {[DateTimeOffset]$_.first_enqueue_started_at})
    if($enqueueRows.Count -gt 1){
        $enqueueSeconds=(([DateTimeOffset]$enqueueRows[-1].first_enqueue_started_at)-
            ([DateTimeOffset]$enqueueRows[0].first_enqueue_started_at)).TotalSeconds
        if($enqueueSeconds -gt 0){
            $summary.acknowledgedEnqueueMessagesPerMinute=60*($enqueueRows.Count-1)/$enqueueSeconds
            $summary.actualEnqueueStartUtc=$enqueueRows[0].first_enqueue_started_at
            $summary.actualEnqueueEndUtc=$enqueueRows[-1].first_enqueue_started_at
        }
    }
    if($generated.Count -gt 1){
        $sendSeconds=(([DateTimeOffset]$generated[-1].timestamp)-([DateTimeOffset]$generated[0].timestamp)).TotalSeconds
        $summary.offeredMessagesPerMinute=60*($generated.Count-1)/$sendSeconds
        $summary.sendStartUtc=$generated[0].timestamp
        $summary.sendEndUtc=$generated[-1].timestamp
        if($acked.Count -gt 0){$summary.acknowledgementEndUtc=$acked[-1].acknowledged_at}
        if($report.result.batches){
            $lastCommit=$report.result.batches[-1].finishedAtUtc
            $summary.lastBatchCommitUtc=$lastCommit
            $summary.drainAfterLastSendSeconds=(([DateTimeOffset]$lastCommit)-([DateTimeOffset]$generated[-1].timestamp)).TotalSeconds
            $summary.committedHighPerMinute=60*$summary.counts.completedHigh/
                (([DateTimeOffset]$lastCommit)-([DateTimeOffset]$generated[0].timestamp)).TotalSeconds
        }
        if($enqueueRows.Count -gt 0){
            $summary.generatedMessagesPerMinute=$summary.offeredMessagesPerMinute
            $summary.offeredMessagesPerMinute=$null
            $summary.offeredRateBasis='actual_first_enqueue_for_fully_acknowledged_generated_set'
            if($enqueueRows.Count -eq $generated.Count -and $generated.Count -gt 1){
                $summary.offeredMessagesPerMinute=$summary.acknowledgedEnqueueMessagesPerMinute
            }
        }
    }
    $summary.peakHighBacklog=($report.safetySamples|Measure-Object backlogHigh -Maximum).Maximum
    $summary.peakKafkaEnvelopeLag=(@($report.safetySamples|ForEach-Object partitionLag)|
        Measure-Object lagEnvelopes -Maximum).Maximum
    $summary.drained=$summary.counts.generated -eq $TestRate*$TestMinutes -and
        $summary.counts.acknowledged -eq $TestRate*$TestMinutes -and
        $summary.counts.rawRecords -eq $TestRate*$TestMinutes -and
        $summary.counts.Events -eq $TestRate*$TestMinutes/10 -and
        $summary.counts.Scores -eq $TestRate*$TestMinutes/10 -and
        $summary.counts.scoredAudit -eq $TestRate*$TestMinutes/10 -and
        $summary.counts.Quarantine -eq 0
    $summary.sustained=$summary.drained -and -not $summary.safetyStop -and
        -not $summary.failedStage -and -not $summary.error -and
        $summary.generatorExitCode -eq 0 -and $summary.jobStatus -eq 'Completed'
    $summary.latencyBoundary=@($summary.latency.Values|Where-Object {$_.overLimit -gt 0}).Count -gt 0
    $report=$summary
}
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
