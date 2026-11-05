[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$root=Split-Path -Parent $PSScriptRoot
$directory=Join-Path $root '.deployment'
$baseline=Get-Content (Join-Path $directory 'baseline-acceptance.json') -Raw|ConvertFrom-Json
if(-not $baseline.pass -or $baseline.rate -ne 30 -or $baseline.minutes -ne 30 -or
    $baseline.latencyMeasurementBasis -ne 'spark_pre_write'){
    throw 'Stepped latency tests require successful initial 30-minute baseline.'
}
$runs=@()
for($rate=30;;$rate+=30){
    & (Join-Path $PSScriptRoot 'Invoke-EventHubsAmqpDiagnostic.ps1') `
        -DiagnosticKind functional-processing -KeepRunning -Baseline -LoadTest -TestRate $rate |
        Out-File (Join-Path $directory "load-$rate-execution.log") -Encoding utf8
    $resultPath=Join-Path $directory 'functional-processing-results.json'
    $result=Get-Content $resultPath -Raw|ConvertFrom-Json
    $saved=Join-Path $directory "load-$rate-$($result.runId).json"
    Copy-Item $resultPath $saved
    $evaluation=Join-Path $directory "load-$rate-acceptance.json"
    & python (Join-Path $root 'src\reconciliation\evaluate_run.py') `
        --input $saved --output $evaluation --rate $rate --minutes 15 --latency-only
    $evaluated=Get-Content $evaluation -Raw|ConvertFrom-Json
    $runs+=@{rate=$rate;runId=$result.runId;pass=$evaluated.pass;failures=$evaluated.failures}
    @{stopRule='first failure';runs=$runs;computeShutdown=$false}|
        ConvertTo-Json -Depth 10|Set-Content (Join-Path $directory 'stepped-load-summary.json')
    if(-not $evaluated.pass){
        Write-Output "Stopped at first failed load rate: $rate/min; compute retained running."
        break
    }
}
