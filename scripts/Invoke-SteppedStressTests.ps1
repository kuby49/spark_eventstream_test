[CmdletBinding()]
param(
    [ValidateScript({$_ -gt 0 -and $_ % 30 -eq 0})][int] $StartRate = 30,
    [ValidateRange(1,8)][int] $MaximumSteps = 6,
    [ValidateRange(30,360)][int] $MaximumElapsedMinutes = 240
)
$ErrorActionPreference='Stop'
$directory=Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$deadline=[DateTime]::UtcNow.AddMinutes($MaximumElapsedMinutes)
$summary=@{startedAtUtc=[DateTime]::UtcNow.ToString('o');maximumSteps=$MaximumSteps;startRate=$StartRate;
    producerEnvelope='scheduled_background_delivery_buffered_diagnostic_manifest';
    maximumElapsedMinutes=$MaximumElapsedMinutes;stepMinutes=15;incrementPerMinute=30;
    measurementBasis='spark_pre_write';ownOutputWriteExcluded=$true;
    allocationBasis='dynamic_1_to_5_medium_existing_pool';configuredMaximumTotalCores=48;
    originalReadableOutputSlaProven=$false;computeShutdown=$false;
    capacityMetricsAvailability='ARM Microsoft.Fabric/capacities platform metric namespace unsupported; no utilization or absence-of-throttling inference';
    firstLatencyBoundary=@{rate=30;source='30-minute baseline';
        eventsMaximumSeconds=34.243844;scoresMaximumSeconds=40.720362};
    peakSustainedRate=$null;firstOverloadOrExecutionBoundary=$null;runs=@();historicalOtherAllocationRuns=@();status='Running'}
$summaryPath=Join-Path $directory 'stepped-stress-summary.json'
if($StartRate -gt 30){
    foreach($file in @(Get-ChildItem $directory -Filter 'stress-*.json'|Sort-Object LastWriteTimeUtc)){
        if($file.Name -match '^stress-[0-9]+-[0-9a-f-]{36}\.json$'){
            $previous=Get-Content $file.FullName -Raw|ConvertFrom-Json
            if($previous.rate -lt $StartRate -and $previous.sustained){
                if($previous.allocationBasis -eq $summary.allocationBasis){
                    $summary.runs+=@($previous)
                    if($previous.rate -gt $summary.peakSustainedRate){$summary.peakSustainedRate=$previous.rate}
                }else{
                    $summary.historicalOtherAllocationRuns+=@(@{runId=$previous.runId;rate=$previous.rate;
                        allocationBasis=$previous.allocationBasis;sessionAllocation=$previous.sessionAllocation;
                        interpretation='Separate historical allocation; not a dynamic-series throughput result'})
                }
            }
        }
    }
}
function Save-Summary {
    $summary|ConvertTo-Json -Depth 40|Set-Content $summaryPath -Encoding utf8
}
Save-Summary
try{
    for($step=1;$step -le $MaximumSteps;$step++){
        if([DateTime]::UtcNow.AddMinutes(40) -gt $deadline){
            $summary.status='ElapsedSafetyBudget';break
        }
        $rate=$StartRate+30*($step-1)
        $summary.currentRate=$rate
        Save-Summary
        try{
            & (Join-Path $PSScriptRoot 'Invoke-EventHubsAmqpDiagnostic.ps1') `
                -DiagnosticKind functional-processing -KeepRunning -Baseline -LoadTest -StressTest -TestRate $rate |
                Out-File (Join-Path $directory "stress-$rate-execution.log") -Encoding utf8
        }catch{
            if($_.Exception.Message -notlike 'AMQP diagnostic returned no structured evidence*'){throw}
            $failedRun=Get-Content (Join-Path $directory 'functional-processing-run.json') -Raw|ConvertFrom-Json
            & (Join-Path $PSScriptRoot 'Export-StressRunEvidence.ps1') -RunId $failedRun.runId -Rate $rate |
                Out-File (Join-Path $directory "stress-$rate-recovery.log") -Encoding utf8
            Copy-Item (Join-Path $directory "stress-$rate-$($failedRun.runId).json") `
                (Join-Path $directory 'functional-processing-results.json')
        }
        $result=Get-Content (Join-Path $directory 'functional-processing-results.json') -Raw|ConvertFrom-Json
        if($result.rate -ne $rate -or -not $result.runId -or $null -eq $result.sustained){
            throw 'Stress result missing or wrong identity; refusing next step.'
        }
        if($result.allocationBasis -ne $summary.allocationBasis){
            throw 'Stress allocation envelope missing or changed; refusing next step.'
        }
        Copy-Item (Join-Path $directory 'functional-processing-results.json') `
            (Join-Path $directory "stress-$rate-$($result.runId).json")
        $summary.runs+=@($result)
        Write-Host ("Stress step {0}/min run {1}: sustained={2}, generated={3}, acknowledged={4}, outputs={5}/{6}" -f
            $rate,$result.runId,$result.sustained,$result.counts.generated,$result.counts.acknowledged,
            $result.counts.Events,$result.counts.Scores)
        foreach($transition in @($result.scalingEvents|Where-Object {$null -ne $_})){
            Write-Host ("Executor scale {0} at {1}: {2} -> {3}; observed total cores={4}" -f
                $transition.direction,([DateTimeOffset]$transition.observedAtUtc).UtcDateTime.ToString('o'),$transition.fromExecutors,
                $transition.toExecutors,$transition.observedDriverAndExecutorCores)
        }
        if($result.sustained){
            $summary.peakSustainedRate=$rate
        }else{
            $summary.firstOverloadOrExecutionBoundary=@{rate=$rate;runId=$result.runId;
                safetyStop=$result.safetyStop;error=$result.error;failedStage=$result.failedStage;
                generatorDiagnosticCodes=$result.generatorDiagnosticCodes;
                interpretation='Not automatically a crash or proof of capacity exhaustion'}
            $summary.status='StoppedAtSafetyOrExecutionBoundary'
            Save-Summary
            break
        }
        Save-Summary
        Start-Sleep -Seconds 15
    }
    if($summary.status -eq 'Running'){$summary.status='BoundedRateCeilingReached'}
}catch{
    $summary.status='OrchestrationBlocked'
    $summary.errorType=$_.Exception.GetType().Name
    $summary.errorMessage=$_.Exception.Message
    throw
}finally{
    $summary.finishedAtUtc=[DateTime]::UtcNow.ToString('o')
    Save-Summary
}
