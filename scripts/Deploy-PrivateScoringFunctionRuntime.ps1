param(
    [Parameter(Mandatory)][string] $Package,
    [Parameter(Mandatory)][string] $EncryptedToken,
    [Parameter(Mandatory)][string] $CertificateThumbprint,
    [Parameter(Mandatory)][string] $ExpectedHash
)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$hostName='esehspark-score-api.scm.azurewebsites.net'
$report=@{succeeded=$false;publicAccessChanged=$false;packageSha256=$ExpectedHash;privateHost=$hostName}
$token=$null
try{
    $ips=@([Net.Dns]::GetHostAddresses($hostName)|ForEach-Object IPAddressToString)
    if(-not $ips.Count -or @($ips|Where-Object {$_ -ne '10.74.2.9'}).Count){throw 'PrivateScmDnsMismatch'}
    $report.privateAddresses=$ips
    $bytes=[Convert]::FromBase64String($Package)
    $sha=[Security.Cryptography.SHA256]::Create()
    try{$actual=([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-','').ToLowerInvariant()}finally{$sha.Dispose()}
    if($actual -ne $ExpectedHash){throw 'PackageHashMismatch'}
    $cipher=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($EncryptedToken))
    $token=Unprotect-CmsMessage -Content $cipher
    $headers=@{Authorization="Bearer $token"}
    $response=Invoke-WebRequest -Method Post -Uri "https://$hostName/api/zipdeploy?isAsync=true" `
        -Headers $headers -ContentType 'application/zip' -Body $bytes `
        -TimeoutSec 120 -MaximumRedirection 0 -UseBasicParsing
    if($response.StatusCode -notin @(200,202)){throw 'DeploymentNotAccepted'}
    $location=[uri]([string]$response.Headers['Location'])
    if($location.Scheme -ne 'https' -or $location.Host -ne $hostName){throw 'DeploymentPollHostMismatch'}
    $deadline=[DateTime]::UtcNow.AddMinutes(18)
    do{
        Start-Sleep -Seconds 10
        $state=Invoke-RestMethod -Uri $location.AbsoluteUri -Headers $headers -TimeoutSec 30 -MaximumRedirection 0
        if($state.status -eq 3){throw 'RemoteBuildFailed'}
        if([DateTime]::UtcNow -gt $deadline){throw 'PrivateDeploymentDeadlineExceeded'}
    }while($state.status -ne 4 -or -not $state.complete)
    $report.succeeded=$true
    $report.deploymentId=$state.id
    $report.deploymentStatus=$state.status
}catch{
    $report.errorType=$_.Exception.GetType().Name
    if($_.Exception.Message -match '^[A-Za-z]+$'){$report.errorCode=$_.Exception.Message}
    if($_.Exception.Response){$report.httpStatus=[int]$_.Exception.Response.StatusCode}
}finally{
    $token=$null;$headers=$null
    if($CertificateThumbprint -match '^[A-Fa-f0-9]{40}$'){
        $certPath="Cert:\LocalMachine\My\$CertificateThumbprint"
        if(Test-Path $certPath){Remove-Item -LiteralPath $certPath -DeleteKey -Force}
    }
}
$report.checkedAtUtc=[DateTime]::UtcNow.ToString('o')
Write-Output ('PRIVATE_DEPLOY:'+($report|ConvertTo-Json -Depth 5 -Compress))
