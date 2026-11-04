param([Parameter(Mandatory)][guid] $RunId)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$hostName='trd-9rhk7ungwv5rcpaeey.z1.kusto.fabric.microsoft.com'
$ips=@([Net.Dns]::GetHostAddresses($hostName)|ForEach-Object IPAddressToString)
if(-not $ips.Count -or @($ips|Where-Object {$_ -ne '10.74.2.11'}).Count){throw 'PrivateKqlDnsMismatch'}
$token=(Invoke-RestMethod -Uri ('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='+[uri]::EscapeDataString('https://kusto.kusto.windows.net')) -Headers @{Metadata='true'} -TimeoutSec 15).access_token
try{
    $query="RawEvents | where run_id == '$RunId' | project message_id, run_id, equipment_id, temperature, timestamp | take 3"
    $result=Invoke-RestMethod -Method Post -Uri "https://$hostName/v1/rest/query" `
        -Headers @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'} `
        -ContentType 'application/json' -Body (@{db='ehspark';csl=$query}|ConvertTo-Json -Compress) -TimeoutSec 20
    if($result.error){throw 'IndependentFixtureQueryFailed'}
    $tables=@($result.Tables|Where-Object {(@($_.Columns.ColumnName)-join ',') -eq 'message_id,run_id,equipment_id,temperature,timestamp'})
    if($tables.Count -ne 1){throw 'IndependentFixtureProjectionMissing'}
    $rows=@(foreach($row in $tables[0].Rows){
        [pscustomobject]@{
            message_id=[string]$row[0];run_id=[string]$row[1]
            equipment_id=[int]$row[2];temperature=[double]$row[3]
            timestamp=([DateTimeOffset]$row[4]).UtcDateTime.ToString('o')
        }
    })
    Write-Output ('INDEPENDENT_FIXTURES:'+(@{
        checkedAtUtc=[DateTime]::UtcNow.ToString('o');runId=$RunId.ToString()
        privateAddresses=$ips;rows=$rows;readOnly=$true
    }|ConvertTo-Json -Depth 5 -Compress))
}finally{$token=$null}
