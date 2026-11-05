[CmdletBinding()]
param(
    [switch] $UserRequestedMessageProbe,
    [switch] $ShutdownExistingDesktopApproved,
    [switch] $KeepRunning,
    [guid] $RecoverRunId=[guid]::Empty
)
$ErrorActionPreference='Stop'
$env:AZURE_CONFIG_DIR=Join-Path $HOME '.azure-mcap'
$env:AZURE_CORE_CONNECTION_TIMEOUT='10'
$env:AZURE_CORE_READ_TIMEOUT='20'
$env:AZURE_CORE_RETRY_TOTAL='0'
$subscription='0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$root=Split-Path -Parent $PSScriptRoot
$directory=Join-Path $root '.deployment'
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
$inventory=Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
function Az-Json {
    param([string[]] $Arguments)
    $text=& az @Arguments --subscription $subscription --only-show-errors -o json
    if($LASTEXITCODE -ne 0){throw "Functional gate Azure operation failed: $($Arguments[0])."}
    if($text){$text | ConvertFrom-Json}
}
$account=Az-Json @('account','show')
if($account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or $account.id -ne $subscription){throw 'Wrong MCAP context.'}
& (Join-Path $PSScriptRoot 'Export-PreservedDeploymentEvidence.ps1') -ExpectedOutbound Allow -DiagnosticPreflight
$runId=if($RecoverRunId -ne [guid]::Empty){$RecoverRunId.ToString()}else{[guid]::NewGuid().ToString()}
function Encoded-File {param([string] $Path);[Convert]::ToBase64String([IO.File]::ReadAllBytes($Path))}
$workloadCollector=Encoded-File (Join-Path $PSScriptRoot 'Read-PrivateWorkloadInventory.ps1')
function Read-WorkloadInventory {
    $payload="& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$workloadCollector')))) -WorkspaceId '$($inventory.workspaceId)'"
    $path=Join-Path $directory 'private-workload-inventory-runtime.ps1'
    [IO.File]::WriteAllText($path,$payload,[Text.UTF8Encoding]::new($false))
    $command=Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
        '--command-id','RunPowerShellScript','--scripts',"@$path")
    $stdout=@($command.value|Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'WORKLOAD_INVENTORY:(\{[^\r\n]+\})'){
        Save-FabricEvidence (Join-Path $directory 'message-probe-workload-inventory-command.json') $command
        throw 'Private workload inventory unavailable or truncated; existing compute must be preserved.'
    }
    $work=$Matches[1]|ConvertFrom-Json
    Save-FabricEvidence (Join-Path $directory 'message-probe-workload-inventory.json') $work
    if(-not $work.complete){throw 'Private workload inventory incomplete; existing compute must be preserved.'}
    if($work.nonterminalJobs.Count -or $work.sessions.Count -or $work.pythonProcesses.Count -or
        @($work.generatorTasks|Where-Object State -eq 4).Count){
        throw 'Active workspace/generator workload found; preserving compute.'
    }
    if(-not $work.idle -and -not $ShutdownExistingDesktopApproved -and -not $KeepRunning){
        throw 'Existing desktop/task activity requires explicit shutdown approval.'
    }
    return $work
}
$runtime=Encoded-File (Join-Path $PSScriptRoot 'Read-OapOffIngestionGateRuntime.ps1')
$helper=Encoded-File (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
$lifecycle=Encoded-File (Join-Path $PSScriptRoot 'EventstreamLifecycle.ps1')
$producer=Encoded-File (Join-Path $root 'src\generator\functional_fixtures.py')
$privateHelper=Encoded-File (Join-Path $root 'src\generator\private_source_canary.py')
$remote=@"
`$code=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$runtime'))
& ([scriptblock]::Create(`$code)) -WorkspaceId '$($inventory.workspaceId)' -EventstreamId '$($inventory.eventstreamId)' -DatabaseId '$($inventory.kqlDatabaseId)' -RunId '$runId' -EvidenceHelper '$helper' -ProducerCode '$producer' -PrivateSourceHelper '$privateHelper' -LifecycleHelper '$lifecycle' -UserRequestedMessageProbe:`$$($UserRequestedMessageProbe.IsPresent.ToString().ToLowerInvariant())
"@
$runtimePath=Join-Path $directory 'oap-off-ingestion-gate-runtime.ps1'
[IO.File]::WriteAllText($runtimePath,$remote,[Text.UTF8Encoding]::new($false))
$capacityId="/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri="https://management.azure.com$capacityId`?api-version=2023-11-01"
if($RecoverRunId -eq [guid]::Empty){
Save-FabricEvidence (Join-Path $directory 'oap-off-ingestion-gate-run.json') @{
    runId=$runId;startedAtUtc=[DateTime]::UtcNow.ToString('o');oapEnabled=$false
    userRequestedMessageProbe=$UserRequestedMessageProbe.IsPresent;baselineRun=$false
    shutdownExistingDesktopApproved=$ShutdownExistingDesktopApproved.IsPresent
    keepRunning=$KeepRunning.IsPresent
}
}
$shutdownAllowed=$false
try {
    if($RecoverRunId -ne [guid]::Empty){
        if(-not $KeepRunning){throw 'Recovery requires KeepRunning; no compute shutdown is authorized by result recovery.'}
        $path="C:\ProgramData\esehspark\diagnostics\functional-gate-$runId.b64"
        $read="if(-not(Test-Path '$path')){throw 'RequestedGateArchiveNotReady'};`$archive=[IO.File]::ReadAllText('$path');Write-Output ('OAP_OFF_GATE:'+(@{runId='$runId';archivePath='$path';archiveLength=`$archive.Length}|ConvertTo-Json -Compress))"
        $command=Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
            '--command-id','RunPowerShellScript','--scripts',$read)
    }else{
    $capacity=Az-Json @('rest','--method','get','--url',$capacityUri)
    if($capacity.sku.name -ne 'F8'){throw 'Approved F8 identity not verified.'}
    if($capacity.properties.state -eq 'Paused'){
        $null=Az-Json @('rest','--method','post','--url',"https://management.azure.com$capacityId/resume?api-version=2023-11-01")
    }
    $deadline=[DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $capacity=Az-Json @('rest','--method','get','--url',$capacityUri)
        if([DateTime]::UtcNow -gt $deadline){throw 'F8 resume timed out.'}
    }while($capacity.properties.state -ne 'Active')
    $vm=Az-Json @('vm','get-instance-view','-g','rg_esehspark','-n','vmesehspark',
        '--query','{statuses:instanceView.statuses}')
    $power=@($vm.statuses|Where-Object code -like 'PowerState/*').code
    if($power -eq 'PowerState/deallocated'){
        $null=Az-Json @('vm','start','-g','rg_esehspark','-n','vmesehspark')
    }elseif($power -ne 'PowerState/running'){throw 'VM state unavailable or transitioning; preserving existing compute.'}
    $null=Read-WorkloadInventory
    $shutdownAllowed=$true
    $command=Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
        '--command-id','RunPowerShellScript','--scripts',"@$runtimePath")
    }
    $stdout=@($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'OAP_OFF_GATE:(\{[^\r\n]+\})'){
        Save-FabricEvidence (Join-Path $directory 'oap-off-ingestion-gate-command.json') $command
        throw 'Functional gate did not return its structured result.'
    }
    $summary=$Matches[1] | ConvertFrom-Json
    if($summary.runId -ne $runId -or $summary.archiveLength -le 0 -or $summary.archiveLength -gt 100000){
        throw 'Remote gate archive identity/size invalid.'
    }
    $expectedPath="C:\ProgramData\esehspark\diagnostics\functional-gate-$runId.b64"
    if($summary.archivePath -ne $expectedPath){throw 'Remote archive path mismatch.'}
    $encoded=''
    for($offset=0;$offset -lt $summary.archiveLength;$offset+=2800){
        $read="`$data=[IO.File]::ReadAllText('$expectedPath');Write-Output ('GATE_CHUNK:'+`$data.Substring($offset,[Math]::Min(2800,`$data.Length-$offset)))"
        $chunk=Az-Json @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
            '--command-id','RunPowerShellScript','--scripts',$read)
        $output=@($chunk.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')[0].message
        if($output -notmatch 'GATE_CHUNK:([A-Za-z0-9+/=]+)'){throw 'Gate archive chunk missing.'}
        $encoded+=$Matches[1]
    }
    if($encoded.Length -ne $summary.archiveLength){throw 'Gate archive length mismatch.'}
    $memory=[IO.MemoryStream]::new([Convert]::FromBase64String($encoded))
    $gzip=[IO.Compression.GZipStream]::new($memory,[IO.Compression.CompressionMode]::Decompress)
    $reader=[IO.StreamReader]::new($gzip)
    try{$result=$reader.ReadToEnd() | ConvertFrom-Json}finally{$reader.Dispose();$gzip.Dispose();$memory.Dispose()}
    if($result.runId -ne $runId){throw 'Gate result run identity mismatch.'}
    Save-FabricEvidence (Join-Path $directory 'oap-off-ingestion-gate-results.json') $result
    Write-Output ($summary | ConvertTo-Json)
}
finally {
    if($KeepRunning){
        Write-Output 'VM, F8 and Eventstream left running as explicitly requested; no shutdown performed.'
    }elseif($shutdownAllowed){
    $null=Read-WorkloadInventory
    try{$null=Az-Json @('vm','deallocate','-g','rg_esehspark','-n','vmesehspark')}
    finally{
        $null=Az-Json @('rest','--method','post','--url',"https://management.azure.com$capacityId/suspend?api-version=2023-11-01")
        $deadline=[DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 10
            $capacity=Az-Json @('rest','--method','get','--url',$capacityUri)
            if([DateTime]::UtcNow -gt $deadline){throw 'F8 shutdown not verified.'}
        }while($capacity.properties.state -ne 'Paused')
        & (Join-Path $PSScriptRoot 'Export-PreservedDeploymentEvidence.ps1') -ExpectedOutbound Allow
        Copy-Item (Join-Path $directory 'final-preservation-summary.json') `
            (Join-Path $directory 'oap-off-ingestion-gate-shutdown.json') -Force
    }
    }else{Write-Warning 'No verified workload shutdown ownership; existing compute preserved.'}
}
if(-not $result.ingestionProven){
    $state=if($KeepRunning){'compute retained running as requested'}else{'see verified shutdown evidence'}
    throw "Raw/high fixture gate failed at '$($result.failedStage)'; $state. See redacted result."
}
