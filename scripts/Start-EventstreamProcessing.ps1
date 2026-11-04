[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [Parameter(Mandatory)] [guid] $EventstreamId
)
$ErrorActionPreference='Stop'
. (Join-Path $PSScriptRoot 'EventstreamLifecycle.ps1')
$prefix=$WorkspaceId.ToString().Replace('-','')
$apiHost="$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
$addresses=@([Net.Dns]::GetHostAddresses($apiHost)|ForEach-Object IPAddressToString)
if(-not $addresses.Count -or @($addresses|Where-Object {$_ -ne '10.74.2.10'}).Count){
    throw 'Eventstream start requires the approved VM private workspace API route.'
}
$token=(Invoke-RestMethod -Uri ('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource='+
    [uri]::EscapeDataString('https://api.fabric.microsoft.com')) -Headers @{Metadata='true'} -TimeoutSec 15).access_token
if(-not $token){throw 'VM managed identity Fabric token unavailable.'}
$headers=@{Authorization="Bearer $token";'x-ms-fabric-skill'='eventstream-cli'}
$uri="https://$apiHost/v1/workspaces/$WorkspaceId/eventstreams/$EventstreamId"
try {
    Ensure-EventstreamProcessingRunning -ReadTopology {
        Invoke-RestMethod -Method Get -Uri "$uri/topology" -Headers $headers -TimeoutSec 30 -MaximumRedirection 0
    } -Resume {
        param($body)
        Invoke-RestMethod -Method Post -Uri "$uri/resume" -Headers $headers -ContentType 'application/json' `
            -Body ($body|ConvertTo-Json -Compress) -TimeoutSec 30 -MaximumRedirection 0
    } -ResumeDestination {
        param($id,$body)
        Invoke-RestMethod -Method Post -Uri "$uri/destinations/$id/resume" -Headers $headers -ContentType 'application/json' `
            -Body ($body|ConvertTo-Json -Compress) -TimeoutSec 30 -MaximumRedirection 0
    }
}finally{$token=$null;$headers=$null}
