param(
    [string] $WorkspaceId, [string] $EventstreamId, [string] $DatabaseId,
    [string] $RunId, [string] $EvidenceHelper, [string] $ProducerCode, [string] $PrivateSourceHelper,
    [string] $LifecycleHelper,
    [switch] $UserRequestedMessageProbe
)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
. ([scriptblock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($EvidenceHelper))))
. ([scriptblock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($LifecycleHelper))))
$folder='C:\ProgramData\esehspark\diagnostics'
New-Item -ItemType Directory -Force -Path $folder | Out-Null
$report=@{runId=$RunId;oapEnabled=$false;baselineRun=$false;historicalFixtureStateReconstructed=$false;ingestionProven=$false}
$report.userRequestedMessageProbe=$UserRequestedMessageProbe.IsPresent
$report.acceptancePassed=$false
$stage='private_api_dns'
$prefix=$WorkspaceId.Replace('-','')
$apiHost="$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
function Private-Dns {
    param([string] $HostName,[string] $Expected)
    $ips=@([Net.Dns]::GetHostAddresses($HostName) | ForEach-Object IPAddressToString)
    if(-not $ips.Count -or @($ips | Where-Object {$_ -ne $Expected}).Count){throw 'PrivateDnsMismatch'}
    return $ips
}
function Token {
    param([string] $Resource)
    (Invoke-RestMethod -Uri ('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='+[uri]::EscapeDataString($Resource)) -Headers @{Metadata='true'} -TimeoutSec 15 -MaximumRedirection 0).access_token
}
function Fabric {
    param([string] $Method,[string] $Path,[object] $Body)
    Invoke-WebRequest -Method $Method -Uri "https://$apiHost$Path" -Headers $headers -UseBasicParsing `
        -TimeoutSec 30 -MaximumRedirection 0 -ContentType 'application/json' -Body $(
            if($null -ne $Body){$Body | ConvertTo-Json -Depth 20 -Compress}elseif($Method -eq 'Post'){'{}'}else{$null})
}
try {
    $report.privateApiAddresses=@(Private-Dns $apiHost '10.74.2.10')
    $token=Token 'https://api.fabric.microsoft.com'
    $headers=@{Authorization="Bearer $token";'x-ms-fabric-skill'='eventstream-cli'}
    $base="/v1/workspaces/$WorkspaceId"
    $eventstreams=(Fabric Get "$base/eventstreams").Content|ConvertFrom-Json
    $streamMatches=@($eventstreams.value|Where-Object {$_.displayName -eq 'esspark' -and $_.id -eq $EventstreamId})
    if($streamMatches.Count -ne 1){throw 'EventstreamDiscoveryIdentityMismatch'}
    $stage='deployed_topology'
    $topology=(Fabric Get "$base/eventstreams/$EventstreamId/topology").Content | ConvertFrom-Json
    $report.topology=$topology
    $report.componentState=Get-FabricComponentStateEvidence $topology
    $stage='deployed_definition'
    $definitionResponse=Fabric Post "$base/eventstreams/$EventstreamId/getDefinition"
    if($definitionResponse.StatusCode -eq 202){
        $location=[uri]([string]$definitionResponse.Headers['Location'])
        if($location.Scheme -ne 'https' -or $location.Host -notin @($apiHost,'api.fabric.microsoft.com')){throw 'UnrecognizedLroHost'}
        $deadline=[DateTime]::UtcNow.AddMinutes(2)
        do {
            Start-Sleep -Seconds 5
            $operation=(Fabric Get $location.PathAndQuery).Content | ConvertFrom-Json
            if($operation.status -in @('Failed','Cancelled')){throw 'DefinitionReadFailed'}
            if([DateTime]::UtcNow -gt $deadline){throw 'DefinitionReadTimeout'}
        }while($operation.status -ne 'Succeeded')
        $definitionResponse=Fabric Get ($location.PathAndQuery.TrimEnd('/')+'/result')
    }
    $definition=$definitionResponse.Content | ConvertFrom-Json
    $report.definitionPartDescriptors=@($definition.definition.parts | Select-Object path,payloadType)
    $report.definition=Convert-FabricDefinitionEvidence $definition
    $stage='database_identity'
    $headers['x-ms-fabric-skill']='eventhouse-cli'
    $database=(Fabric Get "$base/kqlDatabases/$DatabaseId").Content | ConvertFrom-Json
    $report.database=$database
    if($database.id -ne $DatabaseId -or $database.displayName -ne 'ehspark'){throw 'DatabaseIdentityMismatch'}
    $kql=[uri]$database.properties.queryServiceUri
    $report.kqlPrivateAddresses=@(Private-Dns $kql.Host '10.74.2.11')
    $token=Token 'https://kusto.kusto.windows.net'
    $kqlHeaders=@{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'}
    function Kql {
        param([string] $Command)
        $result=Invoke-RestMethod -Method Post -Uri ($kql.AbsoluteUri.TrimEnd('/')+'/v1/rest/mgmt') `
            -Headers $kqlHeaders -ContentType 'application/json' -Body (@{db='ehspark';csl=$Command}|ConvertTo-Json -Compress) `
            -TimeoutSec 30 -MaximumRedirection 0
        if($result.error -or -not $result.Tables){throw 'KqlResultMissing'}
        return $result
    }
    $stage='schema_mapping_capture'
    $report.schema=Kql '.show table RawEvents schema as json'
    $report.tableMappings=Kql '.show table RawEvents ingestion json mappings'
    $report.databaseMappings=Kql '.show database ehspark ingestion json mappings'
    $report.mirroring=Kql '.show table RawEvents policy mirroring'
    $stage='resume_paused_processing'
    $headers=@{Authorization="Bearer $(Token 'https://api.fabric.microsoft.com')";'x-ms-fabric-skill'='eventstream-cli'}
    $report.processingReadbacks=@()
    $report.resumeResponses=@()
    function Resume-Processing {
        param($path,$body)
        $response=Fabric Post $path $body
        $report.resumeResponses+=@{
            path=$path;statusCode=[int]$response.StatusCode
            response=[string]$response.Content;atUtc=[DateTime]::UtcNow.ToString('o')
        }
    }
    if($UserRequestedMessageProbe){
        $report.probeReadinessBasis='Explicit user-requested two-event diagnostic; observed derived Paused does not waive the unchanged production readiness gate.'
        Resume-Processing "$base/eventstreams/$EventstreamId/resume" @{startType='WhenLastStopped'}
        $deadline=[DateTime]::UtcNow.AddSeconds(60)
        do{
            $probeTopology=(Fabric Get "$base/eventstreams/$EventstreamId/topology").Content|ConvertFrom-Json
            $components=@(Get-RequiredEventstreamComponents $probeTopology)
            $report.processingReadbacks+=@{atUtc=[DateTime]::UtcNow.ToString('o');components=$components}
            if(Test-EventstreamMessageProbeReadiness $probeTopology){break}
            if([DateTime]::UtcNow -ge $deadline){throw 'MessageProbeRequiredDestinationsNotRunning'}
            Start-Sleep -Seconds 5
        }while($true)
        $report.allRequiredProcessingRunning=@($components|Where-Object status -ne 'Running').Count -eq 0
        $report.processingResume=@{resumed=$true;startType='WhenLastStopped';diagnosticOnly=$true;topology=$probeTopology}
    }else{
        $report.processingResume=Ensure-EventstreamProcessingRunning -ReadTopology {
        (Fabric Get "$base/eventstreams/$EventstreamId/topology").Content | ConvertFrom-Json
    } -Resume {
        param($resumeBody)
        Resume-Processing "$base/eventstreams/$EventstreamId/resume" $resumeBody
    } -ResumeDestination {
        param($id,$resumeBody)
        Resume-Processing "$base/eventstreams/$EventstreamId/destinations/$id/resume" $resumeBody
    } -OnObservation {
        param($components)
        $report.processingReadbacks+=@{atUtc=[DateTime]::UtcNow.ToString('o');components=@($components)}
        }
    }
    $stage='source_fixtures'
    [IO.File]::WriteAllBytes((Join-Path $folder 'functional_fixtures.py'),[Convert]::FromBase64String($ProducerCode))
    [IO.File]::WriteAllBytes((Join-Path $folder 'private_source_canary.py'),[Convert]::FromBase64String($PrivateSourceHelper))
    $python='C:\ProgramData\esehspark\Python313\python.exe'
    $fixturePath=Join-Path $folder "functional-fixtures-$RunId.json"
    $process=Start-Process -FilePath $python -ArgumentList "$folder\functional_fixtures.py --run-id $RunId --output $fixturePath" `
        -PassThru -RedirectStandardOutput (Join-Path $folder 'functional-producer.stdout') -RedirectStandardError (Join-Path $folder 'functional-producer.stderr')
    $null=$process.Handle
    if(-not $process.WaitForExit(90000)){Stop-Process -Id $process.Id;throw 'FixtureSendTimeout'}
    if(-not(Test-Path $fixturePath)){throw 'FixtureReportMissing'}
    $report.fixtures=Get-Content $fixturePath -Raw | ConvertFrom-Json
    if($process.ExitCode -ne 0 -or -not $report.fixtures.allAcknowledged){throw 'FixtureSendFailed'}
    $stage='raw_fixture_readback'
    $deadline=[DateTime]::UtcNow.AddMinutes(3)
    $query="RawEvents | where run_id == '$RunId' | project message_id, run_id, equipment_id, temperature, timestamp | take 3"
    do {
        $raw=Invoke-RestMethod -Method Post -Uri ($kql.AbsoluteUri.TrimEnd('/')+'/v1/rest/query') `
            -Headers $kqlHeaders -ContentType 'application/json' -Body (@{db='ehspark';csl=$query}|ConvertTo-Json -Compress) -TimeoutSec 20 -MaximumRedirection 0
        $tables=@($raw.Tables | Where-Object {(@($_.Columns.ColumnName)-join ',') -eq 'message_id,run_id,equipment_id,temperature,timestamp'})
        if($tables.Count -ne 1){throw 'FixtureQueryProjectionMissing'}
        $report.rawReadback=$tables[0]
        $report.rawRowCount=@($tables[0].Rows).Count
        $report.ingestionProven=$false
        if($report.rawRowCount -eq 2){
            $matches=0
            foreach($fixture in $report.fixtures.events){
                $rows=@($tables[0].Rows | Where-Object {
                    $_[0] -eq $fixture.payload.message_id -and $_[1] -eq $RunId -and
                    $_[2] -eq $fixture.payload.equipment_id -and $_[3] -eq $fixture.payload.temperature -and
                    ([DateTimeOffset]$_[4]).UtcDateTime -eq ([DateTimeOffset]$fixture.payload.timestamp).UtcDateTime
                })
                if($rows.Count -eq 1){$matches++}
            }
            $report.ingestionProven=$matches -eq 2
        }
        if($report.ingestionProven){break}
        Start-Sleep -Seconds 10
    }while([DateTime]::UtcNow -lt $deadline)
    $report.postSendTopology=(Fabric Get "$base/eventstreams/$EventstreamId/topology").Content|ConvertFrom-Json
    $report.observationLimitSeconds=180
    if(-not $report.ingestionProven){throw 'NoTwoMatchingRawFixturesWithinThreeMinutes'}
}
catch {
    $report.failedStage=$stage
    $report.errorType=$_.Exception.GetType().Name
    if($_.FullyQualifiedErrorId -match '^[A-Za-z][A-Za-z0-9._, -]{1,200}$'){
        $report.errorId=$_.FullyQualifiedErrorId
    }else{$report.errorIdNotRetained=$true}
    if($_.Exception.Message -match '^[A-Za-z][A-Za-z0-9]{1,90}$'){$report.reason=$_.Exception.Message}
    if($_.Exception.Response){$report.httpStatus=[int]$_.Exception.Response.StatusCode}
    if($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"'){$report.errorCode=$Matches[1]}
    if($_.ErrorDetails.Message -match '^\s*\{'){
        try{$report.serviceError=$_.ErrorDetails.Message|ConvertFrom-Json -ErrorAction Stop}
        catch{$report.serviceErrorJsonUnavailable=$true}
    }
}
finally{$token=$null;$headers=$null;$kqlHeaders=$null}
$report.checkedAtUtc=[DateTime]::UtcNow.ToString('o')
$path=Join-Path $folder "functional-gate-$RunId.json"
$redactionFailures=@()
foreach($name in @($report.Keys)){
    try{$null=Protect-FabricEvidence $report[$name]}
    catch{
        $failure=@{field=$name;errorType=$_.Exception.GetType().Name;retained=$false}
        if($_.Exception.Message -match ' at (\$[^;]+);'){$failure.path=$Matches[1]}
        $redactionFailures+=$failure
        $report.Remove($name)
    }
}
if($redactionFailures.Count){
    $report.evidenceRedactionFailures=$redactionFailures
    $report.ingestionProven=$false
    $report.evidenceCollectionFailed=$true
    if(-not $report.failedStage){$report.failedStage='evidence_redaction'}
}
Save-FabricEvidence $path $report
$bytes=[IO.File]::ReadAllBytes($path)
$memory=New-Object IO.MemoryStream
$gzip=New-Object IO.Compression.GZipStream($memory,[IO.Compression.CompressionMode]::Compress,$true)
$gzip.Write($bytes,0,$bytes.Length)
$gzip.Dispose()
$encoded=[Convert]::ToBase64String($memory.ToArray())
$archive=Join-Path $folder "functional-gate-$RunId.b64"
[IO.File]::WriteAllText($archive,$encoded)
Write-Output ('OAP_OFF_GATE:'+(@{runId=$RunId;ingestionProven=$report.ingestionProven;failedStage=$report.failedStage;
    archiveLength=$encoded.Length;archivePath=$archive;rawRowCount=$report.rawRowCount}|ConvertTo-Json -Compress))
