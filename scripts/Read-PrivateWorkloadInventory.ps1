[CmdletBinding()]
param([Parameter(Mandatory)][string] $WorkspaceId)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$report=@{checkedAtUtc=[DateTime]::UtcNow.ToString('o');complete=$false}
try{
    $prefix=$WorkspaceId.Replace('-','')
    $apiHost="$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
    $ips=@([Net.Dns]::GetHostAddresses($apiHost)|ForEach-Object IPAddressToString)
    if(-not $ips.Count -or @($ips|Where-Object {$_ -ne '10.74.2.10'}).Count){throw 'PrivateDnsMismatch'}
    $token=(Invoke-RestMethod -Uri ('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='+[uri]::EscapeDataString('https://api.fabric.microsoft.com')) -Headers @{Metadata='true'} -TimeoutSec 15).access_token
    $headers=@{Authorization="Bearer $token";'x-ms-fabric-skill'='spark-cli'}
    function Read-List {
        param([string] $Path)
        $rows=@()
        for($page=0;$page -lt 20;$page++){
            $response=Invoke-RestMethod -Uri "https://$apiHost$Path" -Headers $headers -TimeoutSec 20
            if($null -eq $response.value){throw 'InventoryListShapeUnavailable'}
            $rows+=@($response.value)
            if(-not $response.continuationUri){return $rows}
            $next=[uri]$response.continuationUri
            if($next.Scheme -ne 'https' -or $next.Host -notin @($apiHost,'api.fabric.microsoft.com')){throw 'InventoryContinuationHostInvalid'}
            $Path=$next.PathAndQuery
        }
        throw 'InventoryPaginationLimitExceeded'
    }
    $items=@(Read-List "/v1/workspaces/$WorkspaceId/items")
    $report.items=@($items|Select-Object id,type,displayName)
    $jobs=@()
    foreach($item in $items){
        if($item.type -in @('Notebook','SparkJobDefinition','DataPipeline','Lakehouse')){
            $path=if($item.type -eq 'Lakehouse'){"/v1/workspaces/$WorkspaceId/lakehouses/$($item.id)/jobs/instances"}else{"/v1/workspaces/$WorkspaceId/items/$($item.id)/jobs/instances"}
            foreach($job in @(Read-List $path)){
                if($job.status -notin @('Completed','Failed','Cancelled','Deduped')){
                    $jobs+=@{itemId=$item.id;itemName=$item.displayName;id=$job.id;status=$job.status;startTimeUtc=$job.startTimeUtc}
                }
            }
        }
    }
    $report.nonterminalJobs=$jobs
    $report.sessions=@(Read-List "/v1/workspaces/$WorkspaceId/spark/livySessions" |
        Where-Object {$_.state -notin @('dead','killed','error','success','Succeeded','Failed','Cancelled')} |
        Select-Object id,state,applicationId,itemId)
    $processes=@(Get-CimInstance Win32_Process -ErrorAction Stop |
        Where-Object {$_.Name -match '^(python.*|explorer)\.exe$'} |
        Select-Object ProcessId,Name,CreationDate)
    $report.workloadProcesses=$processes
    $report.runningTasks=@(Get-ScheduledTask -ErrorAction Stop |
        Where-Object {$_.State -eq 'Running'} | Select-Object TaskName,TaskPath,State)
    $report.generatorTasks=@(Get-ScheduledTask -ErrorAction Stop |
        Where-Object {$_.TaskName -match 'esehspark|generator'} | Select-Object TaskName,TaskPath,State)
    $report.pythonProcesses=@($processes|Where-Object Name -ne 'explorer.exe')
    $report.desktopProcesses=@($processes|Where-Object Name -eq 'explorer.exe')
    $report.complete=$true
    $report.idle=($jobs.Count -eq 0 -and $report.sessions.Count -eq 0 -and $processes.Count -eq 0 -and $report.runningTasks.Count -eq 0)
}catch{
    $report.errorType=$_.Exception.GetType().Name
    if($_.Exception.Message -match '^[A-Za-z][A-Za-z0-9]+$'){$report.reason=$_.Exception.Message}
    if($_.Exception.Response){$report.httpStatus=[int]$_.Exception.Response.StatusCode}
    if($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]+)"'){$report.errorCode=$Matches[1]}
    $report.idle=$false
}finally{$token=$null;$headers=$null}
Write-Output ('WORKLOAD_INVENTORY:'+($report|ConvertTo-Json -Depth 8 -Compress))
