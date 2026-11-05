function Get-RequiredEventstreamComponents {
    param([Parameter(Mandatory)] [object] $Topology)
    $required=@(
        @{group='sources';name='VmKafkaIngress';type='CustomEndpoint'},
        @{group='streams';name='esspark-stream';type='DefaultStream'},
        @{group='streams';name='HighTemperatureEvents';type='DerivedStream'},
        @{group='destinations';name='RawEvents';type='Eventhouse'},
        @{group='destinations';name='HighTemperatureKafka';type='CustomEndpoint'}
    )
    foreach($component in $required){
        $nodes=@($Topology.($component.group) | Where-Object name -eq $component.name)
        if($nodes.Count -ne 1 -or $nodes[0].type -ne $component.type -or -not $nodes[0].id -or
            [string]::IsNullOrWhiteSpace([string]$nodes[0].status)){
            throw "Required Eventstream component identity/state missing: $($component.name)."
        }
        [pscustomobject]@{name=$nodes[0].name;id=$nodes[0].id;status=$nodes[0].status;kind=$component.group}
    }
}

function Test-EventstreamMessageProbeReadiness {
    param([Parameter(Mandatory)] [object] $Topology)
    $components=@(Get-RequiredEventstreamComponents $Topology)
    foreach($component in $components){
        if($component.name -eq 'HighTemperatureEvents'){
            if($component.status -eq 'Resuming'){return $false}
            if($component.status -notin @('Running','Paused')){
                throw 'Message probe requires an observed Running or Paused derived status, not unhealthy/unknown processing.'
            }
        }elseif($component.status -ne 'Running'){return $false}
    }
    return $true
}

function Ensure-EventstreamProcessingRunning {
    param(
        [Parameter(Mandatory)] [scriptblock] $ReadTopology,
        [Parameter(Mandatory)] [scriptblock] $Resume,
        [scriptblock] $ResumeDestination,
        [scriptblock] $OnObservation,
        [ValidateRange(0,300)] [int] $TimeoutSeconds=120,
        [ValidateRange(0,30)] [int] $PollSeconds=5
    )
    $topology=& $ReadTopology
    $before=@(Get-RequiredEventstreamComponents $topology)
    if($OnObservation){$null=& $OnObservation $before}
    $pending=@($before | Where-Object status -ne 'Running')
    $resumed=$false
    if($pending.Count){
        if(@($pending|Where-Object status -ne 'Paused').Count){
            throw 'Required Eventstream processing is unhealthy or transitioning; refusing generator start.'
        }
        $destinations=@($pending|Where-Object kind -eq 'destinations')
        if($ResumeDestination -and $destinations.Count -and -not @($pending|Where-Object kind -eq 'sources').Count){
            foreach($destination in $destinations){
                $null=& $ResumeDestination $destination.id @{startType='WhenLastStopped'}
            }
        }else{$null=& $Resume @{startType='WhenLastStopped'}}
        $resumed=$true
        $deadline=[DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
        do {
            $topology=& $ReadTopology
            $after=@(Get-RequiredEventstreamComponents $topology)
            if($OnObservation){$null=& $OnObservation $after}
            $pending=@($after|Where-Object status -ne 'Running')
            if(-not $pending.Count){break}
            if([DateTime]::UtcNow -ge $deadline){throw 'Eventstream resume readback did not prove all required processing Running.'}
            Start-Sleep -Seconds $PollSeconds
        }while($true)
    }else{$after=$before}
    [pscustomobject]@{
        verifiedAtUtc=[DateTime]::UtcNow.ToString('o')
        running=$true;resumed=$resumed;startType='WhenLastStopped'
        before=$before;after=$after;topology=$topology
        historicalFixtureStateReconstructed=$false
    }
}
