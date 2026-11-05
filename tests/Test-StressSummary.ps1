$ErrorActionPreference='Stop'
$runtime=Get-Content (Join-Path $PSScriptRoot '..\scripts\Invoke-EventHubsAmqpPrivateRuntime.ps1') -Raw
$start=$runtime.IndexOf('    $summary=@{runId=')
$end=$runtime.IndexOf('    $report=$summary',$start)
if($start -lt 0 -or $end -lt 0){throw 'Stress summary block not found.'}
$summarize=[scriptblock]::Create($runtime.Substring($start,$end-$start))
$RunId='summary-test'
$TestRate=30
$TestMinutes=15
$fullPath='preserved-on-VM'
$report=@{
    generatorExitCode=-1
    generatorManifest=@(
        @{status='generated';timestamp='2026-11-04T10:00:00Z'},
        @{status='generated';timestamp='2026-11-04T10:00:02Z'})
    safetySamples=@()
}
. $summarize
if($summary.sustained -or $summary.drained -or $null -ne $summary.counts.rawRecords -or
    $summary.counts.generated -ne 2 -or $summary.offeredMessagesPerMinute -ne 30){
    throw 'Interrupted-run summary must preserve partial counts and never claim sustained throughput.'
}
$rows=@(
    @{spark_processed_at='2026-11-04T10:00:01';message_timestamp='2026-11-04T10:00:00Z'},
    @{spark_processed_at='2026-11-04T10:00:23';message_timestamp='2026-11-04T10:00:02Z'})
$report=@{
    generatorExitCode=0;jobStatus='Completed';generatorManifest=@(
        @{status='generated';timestamp='2026-11-04T10:00:00Z'},
        @{status='generated';timestamp='2026-11-04T10:00:02Z'},
        @{status='acknowledged';acknowledged_at='2026-11-04T10:00:02Z'})
    result=@{outputRecords=@{'lhspark.Events'=$rows;'lhspark.Scores'=$rows}}
    safetySamples=@()
}
. $summarize
if($summary.latency.Events.overLimit -ne 1 -or $summary.latency.Scores.overLimit -ne 0 -or
    $summary.latency.Events.maximumSeconds -ne 21 -or -not $summary.latencyBoundary){
    throw 'Spark naive UTC timestamps or separate latency boundary computed incorrectly.'
}
$executor=@{id='2';isActive=$true;totalCores=8;totalGCTime=123;failedTasks=0;memoryUsed=456}
$stage=@{stageId=10;status='COMPLETE';executorRunTime=1000;executorCpuTime=800;
    jvmGcTime=12;numFailedTasks=1;failureReason='[TASK_WRITE_FAILED] Write failed'}
foreach($wrapped in @($true,$false)){
    $metric=@{
        atUtc='2026-11-04T10:00:00Z'
        executors=$(if($wrapped){@{value=@($executor);Count=1}}else{@($executor)})
        stages=$(if($wrapped){@{value=@($stage);Count=1}}else{@($stage)})
        resourceUsage=@{coreEfficiency=0.07;capacityExceeded=$false}
    }
    $report.sparkMonitoringSamples=@($null,($metric|ConvertTo-Json -Depth 10|ConvertFrom-Json))
    . $summarize
    if($summary.sparkMetricSamples.Count -ne 1 -or
        $summary.sparkMetricSamples[0].executors[0].id -ne '2' -or
        $summary.sparkMetricSamples[0].executors[0].totalGCTime -ne 123 -or
        $summary.sparkMetricSamples[0].slowStages[0].stageId -ne 10 -or
        $summary.sparkMetricSamples[0].slowStages[0].jvmGcTime -ne 12){
        throw 'Spark collection wrapper or null sample corrupted compact metrics.'
    }
    . (Join-Path $PSScriptRoot '..\scripts\FabricEvidence.ps1')
    $safe=Protect-FabricEvidence $summary
    if($safe.sparkMetricSamples[0].failedStages[0].failureReason -ne
        'Spark failure: [TASK_WRITE_FAILED] Write failed'){
        throw 'Bracketed Spark failure diagnostic must remain readable and safely sanitizable.'
    }
}
$report.result=$null
$report.consumerReadyBeforeSend=@{sessionAllocation=@{allocationBasis='dynamic_1_to_5_medium_existing_pool'}}
$transition=@{fromExecutors=1;toExecutors=2;observedAtUtc='2026-11-04T10:00:00Z'}
$report.safetySamples=@(@{scalingEvents=@($transition)})
. $summarize
if($summary.allocationBasis -ne 'dynamic_1_to_5_medium_existing_pool' -or
    $summary.scalingEvents[0].toExecutors -ne 2){
    throw 'Interrupted runs lost their verified allocation or observed scaling evidence.'
}
$report=@{generatorExitCode=0;generatorManifest=@(
    @{status='generated';timestamp='2026-11-04T10:00:00Z'},
    @{status='generated';timestamp='2026-11-04T10:00:02Z'},
    @{status='acknowledged';acknowledged_at='2026-11-04T10:00:03Z';
        first_enqueue_started_at='2026-11-04T10:00:00.600Z'},
    @{status='acknowledged';acknowledged_at='2026-11-04T10:00:04Z';
        first_enqueue_started_at='2026-11-04T10:00:02Z'})}
. $summarize
if([Math]::Abs($summary.offeredMessagesPerMinute-60/1.4) -gt 0.00001 -or
    $summary.generatedMessagesPerMinute -ne 30){
    throw 'Background sender must measure actual first enqueue rather than pre-fsync timestamp.'
}
$report.generatorManifest=$report.generatorManifest[0..2]
. $summarize
if($null -ne $summary.offeredMessagesPerMinute){
    throw 'Partial delivery tracking must not claim a complete actual enqueue rate.'
}
Write-Output 'Stress partial-evidence, UTC-latency, scaling and metric-summary checks passed.'
