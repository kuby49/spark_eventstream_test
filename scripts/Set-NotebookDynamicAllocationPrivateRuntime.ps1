param(
    [Parameter(Mandatory)][string] $ConfigurationBase64,
    [Parameter(Mandatory)][string] $VerifierBase64
)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$workspace='ddd993fe-8d65-46f6-90f5-7002e6aa5990'
$hostName='ddd993fe8d6546f690f57002e6aa5990.zdd.w.api.fabric.microsoft.com'
$ips=@([Net.Dns]::GetHostAddresses($hostName)|ForEach-Object IPAddressToString)
if(-not $ips.Count -or @($ips|Where-Object {$_ -ne '10.74.2.10'}).Count){throw 'PrivateDnsMismatch'}
$token=(Invoke-RestMethod -Uri ('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='+[uri]::EscapeDataString('https://api.fabric.microsoft.com')) -Headers @{Metadata='true'} -TimeoutSec 15).access_token
$headers=@{Authorization="Bearer $token";'x-ms-fabric-skill'='spark-cli'}
$base="https://$hostName/v1/workspaces/$workspace"
$deadline=[DateTime]::UtcNow.AddMinutes(8)
function Request([string]$Method,[string]$Uri,$Body){
    if(([uri]$Uri).Host -ne $hostName){throw 'Nonprivate management route refused.'}
    $args=@{Method=$Method;Uri=$Uri;Headers=$headers;TimeoutSec=30;UseBasicParsing=$true;MaximumRedirection=0}
    if($null -ne $Body){$args.ContentType='application/json';$args.Body=$Body|ConvertTo-Json -Depth 80 -Compress}
    $script:lastRequest="$Method $(([uri]$Uri).AbsolutePath)"
    Invoke-WebRequest @args
}
function Location($Response){
    $uri=[uri]([string]$Response.Headers['Location'])
    if($uri.Host -notin @($hostName,'api.fabric.microsoft.com') -or $uri.Scheme -ne 'https'){
        throw 'Invalid LRO location.'
    }
    "https://$hostName$($uri.PathAndQuery)"
}
function Await($Response){
    if($Response.StatusCode -ne 202){return}
    $location=Location $Response
    $origin=$script:lastRequest
    do{
        if([DateTime]::UtcNow -gt $deadline){throw 'Notebook configuration LRO deadline exceeded.'}
        Start-Sleep -Seconds 5
        $operation=(Request Get $location).Content|ConvertFrom-Json
        if($operation.status -in @('Failed','Cancelled')){
            $failure=@{request=$origin;operation=$location;status=$operation.status;error=$operation.error}
            $safe=$failure|ConvertTo-Json -Depth 10 -Compress
            $safe=$safe -replace '(?i)(Bearer\s+)[^\s"]+','$1[redacted]'
            $safe=$safe -replace '(?i)([?&](?:sig|token|key|code)=)[^&"\s]+','$1[redacted]'
            [IO.File]::WriteAllText('C:\ProgramData\esehspark\diagnostics\dynamic-allocation-failure.json',$safe)
            throw "Notebook configuration LRO failed: $safe"
        }
    }while($operation.status -ne 'Succeeded')
}
$settings=(Request Get "$base/spark/settings").Content|ConvertFrom-Json
$pools=(Request Get "$base/spark/pools").Content|ConvertFrom-Json
$pool=@($pools.value|Where-Object id -eq $settings.pool.defaultPool.id)
if($pool.Count -ne 1 -or $pool[0].nodeSize -ne 'Medium' -or
    $pool[0].name -ne 'Starter Pool' -or -not $pool[0].autoScale.enabled -or
    $pool[0].autoScale.maxNodeCount -ne 6 -or -not $pool[0].dynamicExecutorAllocation.enabled -or
    $pool[0].dynamicExecutorAllocation.minExecutors -ne 1 -or
    $pool[0].dynamicExecutorAllocation.maxExecutors -ne 5){
    throw 'Existing pool no longer matches approved 6-node/5-executor Medium envelope.'
}
$sessions=(Request Get "$base/spark/livySessions").Content|ConvertFrom-Json
if(@($sessions.value|Where-Object {$_.state -notin @('dead','killed','error','success','Succeeded','Failed','Cancelled')}).Count){
    throw 'Active Spark session prevents configuration change.'
}
$configuration=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($ConfigurationBase64))
$verifier=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($VerifierBase64))
$config=$configuration|ConvertFrom-Json
if($config.conf.'spark.dynamicAllocation.maxExecutors' -ne '5' -or $config.driverCores -ne 8 -or
    $config.executorCores -ne 8 -or $config.driverMemory -ne '56g' -or $config.executorMemory -ne '56g'){
    throw 'Invalid allocation payload.'
}
$items=(Request Get "$base/items?type=Notebook").Content|ConvertFrom-Json
if($items.continuationUri){throw 'Notebook discovery incomplete.'}
$report=@{checkedAtUtc=[DateTime]::UtcNow.ToString('o');workspaceId=$workspace;poolUnchanged=$true;
    capacitySkuUnchanged=$true;nodeSize='Medium';minimumExecutors=1;maximumExecutors=5;
    maximumDriverAndExecutorCores=48;maximumAvailabilityGuaranteed=$false;notebooks=@()}
foreach($name in @('esehnotebookspark','esehspark-kafka-oap-off-diagnostic')){
    $notebook=@($items.value|Where-Object displayName -eq $name)
    if($notebook.Count -ne 1){throw 'Notebook identity ambiguous.'}
    $id=$notebook[0].id
    $jobs=(Request Get "$base/items/$id/jobs/instances").Content|ConvertFrom-Json
    if(@($jobs.value|Where-Object status -notin @('Completed','Failed','Cancelled','Canceled','Deduped')).Count){
        throw 'Active notebook job prevents definition update.'
    }
    $response=Request Post "$base/notebooks/$id/getDefinition?format=ipynb" @{}
    if($response.StatusCode -eq 202){
        $location=Location $response
        Await $response
        $response=Request Get "$location/result"
    }
    $definition=($response.Content|ConvertFrom-Json).definition
    $part=@($definition.parts|Where-Object path -eq 'notebook-content.ipynb')
    if($part.Count -ne 1){throw 'Expected ipynb definition missing.'}
    $package=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part[0].payload))|ConvertFrom-Json
    $cells=@($package.cells|Where-Object {$_.source[0].Trim() -eq '%%configure'})
    if($cells.Count -ne 1){throw 'Existing notebook configuration cell is ambiguous.'}
    $before=($cells[0].source -join '') -replace '^%%configure\s*',''
    $before=$before|ConvertFrom-Json
    if($before.driverCores -ne 8 -or $before.executorCores -ne 8 -or
        $before.driverMemory -ne '56g' -or $before.executorMemory -ne '56g'){
        throw 'Existing node shape differs; no node-size change authorized.'
    }
    $cells[0].source=@("%%configure`n")+@($configuration -split "`n"|ForEach-Object {"$_`n"})
    $replacements=0
    foreach($cell in $package.cells){
        $source=$cell.source -join ''
        $pattern='(?s)def _verify_session_allocation\(require_executor=False\):.*?(?=\r?\ndef _upsert\()'
        $matches=[regex]::Matches($source,$pattern)
        if($matches.Count -eq 1){
            $existing=$matches[0].Value
            if($existing -notmatch 'FixedExecutorConfigurationNotApplied|DynamicExecutorConfigurationNotApplied'){
                throw 'Allocation verifier differs from expected implementation.'
            }
            $source=$source.Substring(0,$matches[0].Index)+$verifier+"`n`n"+
                $source.Substring($matches[0].Index+$matches[0].Length)
            $cell.source=@($source -split "`n"|ForEach-Object {"$_`n"})
            $replacements++
        }elseif($matches.Count -gt 1){throw 'Allocation verifier is ambiguous.'}
    }
    if($replacements -ne 1){throw 'Allocation verifier not found; refusing incompatible configuration.'}
    $part[0].payload=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes(($package|ConvertTo-Json -Depth 80 -Compress)))
    $definition|Add-Member -NotePropertyName format -NotePropertyValue 'ipynb' -Force
    Await (Request Post "$base/notebooks/$id/updateDefinition" @{definition=$definition})
    $readback=Request Post "$base/notebooks/$id/getDefinition?format=ipynb" @{}
    if($readback.StatusCode -eq 202){
        $readLocation=Location $readback
        Await $readback
        $readback=Request Get "$readLocation/result"
    }
    $readPart=@(($readback.Content|ConvertFrom-Json).definition.parts|Where-Object path -eq 'notebook-content.ipynb')
    if($readPart.Count -ne 1){throw 'Updated definition readback missing.'}
    $actual=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($readPart[0].payload))|ConvertFrom-Json
    if($actual.cells.Count -ne $package.cells.Count){throw 'Updated definition cell count differs.'}
    for($i=0;$i -lt $package.cells.Count;$i++){
        if(($actual.cells[$i].source -join '') -cne ($package.cells[$i].source -join '')){
            throw 'Updated definition source readback differs.'
        }
    }
    $report.notebooks+=@(@{id=$id;name=$name;definitionUpdate='Succeeded';configuration=$config;
        definitionReadbackVerified=$true;processingBusinessLogicChanged=$false;verifierUpdated=$true})
    [IO.File]::WriteAllText('C:\ProgramData\esehspark\diagnostics\dynamic-allocation-enable.json',
        ($report|ConvertTo-Json -Depth 30 -Compress))
}
Write-Output ('DYNAMIC_ALLOCATION_ENABLED:'+($report|ConvertTo-Json -Depth 20 -Compress))
