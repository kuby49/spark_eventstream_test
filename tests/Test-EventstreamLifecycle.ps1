$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot '..\scripts\EventstreamLifecycle.ps1')
function Topology {
    param([string]$Raw='Running',[string]$Derived='Running')
    [pscustomobject]@{
        sources=@(@{id='source';name='VmKafkaIngress';type='CustomEndpoint';status='Running'})
        streams=@(
            @{id='default';name='esspark-stream';type='DefaultStream';status='Running'},
            @{id='derived';name='HighTemperatureEvents';type='DerivedStream';status=$Derived})
        destinations=@(
            @{id='raw';name='RawEvents';type='Eventhouse';status=$Raw},
            @{id='custom';name='HighTemperatureKafka';type='CustomEndpoint';status='Running'})
    }
}
function Must-Fail {
    param([scriptblock]$Action,[string]$Pattern)
    try {& $Action|Out-Null}catch{if($_.Exception.Message -like $Pattern){return};throw}
    throw "Expected failure: $Pattern"
}
$script:resumes=0
$script:reads=0
if(-not(Test-EventstreamMessageProbeReadiness (Topology -Derived Paused))){
    throw 'Explicit two-event diagnostic may observe Paused derived status without claiming production readiness.'
}
if(Test-EventstreamMessageProbeReadiness (Topology -Raw Paused)){throw 'Even diagnostic sends require raw destination Running.'}
if(Test-EventstreamMessageProbeReadiness (Topology -Derived Resuming)){throw 'A resuming derived node must be polled, never treated as ready.'}
Must-Fail {Test-EventstreamMessageProbeReadiness (Topology -Derived Failed)} '*unhealthy*'
$ready=Ensure-EventstreamProcessingRunning -ReadTopology {Topology} -Resume {$script:resumes++}
if(-not $ready.running -or $script:resumes){throw 'Already running topology should not resume.'}
$resumed=Ensure-EventstreamProcessingRunning -ReadTopology {
    $script:reads++
    if($script:reads -eq 1){Topology -Raw Paused -Derived Paused}else{Topology}
} -Resume {
    param($body)
    if($body.startType -ne 'WhenLastStopped'){throw 'Resume must retain position.'}
    $script:resumes++
}
if(-not $resumed.resumed -or $script:resumes -ne 1 -or $script:reads -ne 2){throw 'Partial topology must resume and read back.'}
$script:reads=0
$script:observed=0
$targeted=Ensure-EventstreamProcessingRunning -ReadTopology {
    $script:reads++
    if($script:reads -eq 1){Topology -Raw Paused -Derived Paused}else{Topology}
} -Resume {throw 'Already running sources must not be resumed for a paused destination'} -ResumeDestination {
    param($id,$body)
    if($id -ne 'raw' -or $body.startType -ne 'WhenLastStopped'){throw 'Only the actual paused destination must resume.'}
} -OnObservation {param($components);$script:observed++}
if(-not $targeted.running -or $script:observed -ne 2){throw 'Targeted resume must emit explicit state readbacks.'}
Must-Fail {Ensure-EventstreamProcessingRunning -ReadTopology {Topology -Raw Paused} -Resume {throw 'ResumeFailed'}} '*ResumeFailed*'
Must-Fail {Ensure-EventstreamProcessingRunning -ReadTopology {Topology -Derived Paused} -Resume {} -TimeoutSeconds 0} '*did not prove*'
Must-Fail {Ensure-EventstreamProcessingRunning -ReadTopology {Topology -Raw Failed} -Resume {throw 'MustNotResume'}} '*unhealthy*'
Must-Fail {Ensure-EventstreamProcessingRunning -ReadTopology {Topology -Derived ''} -Resume {throw 'MustNotResume'}} '*identity/state missing*'
$script:reads=0
Must-Fail {Ensure-EventstreamProcessingRunning -ReadTopology {
    $script:reads++
    if($script:reads -eq 1){Topology -Raw Paused}else{throw 'ReadbackFailed'}
} -Resume {}} '*ReadbackFailed*'
$starter=Get-Content (Join-Path $PSScriptRoot '..\scripts\Start-Generator.ps1') -Raw
if($starter.IndexOf('Start-EventstreamProcessing.ps1') -gt $starter.IndexOf('Start-ScheduledTask')){
    throw 'Generator must verify processing before starting.'
}
if($starter.IndexOf('Start-EventstreamProcessing.ps1') -gt $starter.IndexOf('& $PythonPath $producer')){
    throw 'Scheduled inline execution must also verify current processing before Python starts.'
}
$installer=Get-Content (Join-Path $PSScriptRoot '..\scripts\Install-GeneratorTask.ps1') -Raw
if($installer -notmatch '\-RunInline' -or $installer -match 'New-ScheduledTaskAction -Execute \$PythonPath'){
    throw 'A direct scheduled-task start must not bypass readiness checks.'
}
Write-Output 'Partial topology, idempotent resume, missing state, resume failure and readback failure gates passed.'
