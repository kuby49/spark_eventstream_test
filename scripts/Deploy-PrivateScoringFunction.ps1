[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$env:AZURE_CONFIG_DIR=Join-Path $HOME '.azure-mcap'
$root=Split-Path -Parent $PSScriptRoot
$directory=Join-Path $root '.deployment'
$subscription='0a0cf761-9d5d-4862-9165-7b69c188a0ee'
function Az-Json {
    param([string[]] $Arguments)
    $text=& az @Arguments --subscription $subscription --only-show-errors -o json
    if($LASTEXITCODE -ne 0){throw 'Private Function deployment Azure operation failed.'}
    if($text){$text|ConvertFrom-Json}
}
function Run-PrivateScript {
    param([string] $Text,[string] $Name)
    $path=Join-Path $directory "$Name.ps1"
    [IO.File]::WriteAllText($path,$Text,[Text.UTF8Encoding]::new($false))
    $response=Az-Json -Arguments @('vm','run-command','invoke','-g','rg_esehspark','-n','vmesehspark',
        '--command-id','RunPowerShellScript','--scripts',"@$path")
    $stdout=@($response.value|Where-Object code -eq 'ComponentStatus/StdOut/succeeded')[0].message
    if($stdout -notmatch 'PRIVATE_DEPLOY:(\{[^\r\n]+\})'){throw 'Private deployment returned no structured evidence.'}
    $Matches[1]|ConvertFrom-Json
}
$account=Az-Json -Arguments @('account','show')
if($account.id -ne $subscription -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527'){
    throw 'Incorrect MCAP deployment context.'
}
$app=Az-Json -Arguments @('functionapp','show','-g','rg_esehspark','-n','esehspark-score-api')
$plan=Az-Json -Arguments @('appservice','plan','show','-g','rg_esehspark','-n','asp-esehspark-score')
$config=Az-Json -Arguments @('functionapp','config','show','-g','rg_esehspark','-n','esehspark-score-api')
if($app.publicNetworkAccess -ne 'Disabled' -or $app.state -ne 'Running' -or
    $plan.sku.name -ne 'B1' -or $config.alwaysOn -ne $true -or $config.linuxFxVersion -ne 'Python|3.13'){
    throw 'Private Function/B1/runtime invariants changed; no deployment attempted.'
}
$zip=Join-Path $directory 'random-score-api.zip'
$files=@('host.json','requirements.txt','function_app.py','logic.py','storage.py')|
    ForEach-Object {Join-Path $root "src\scoring_api\$_"}
Compress-Archive -LiteralPath $files -DestinationPath $zip -Force
$package=[Convert]::ToBase64String([IO.File]::ReadAllBytes($zip))
$hash=(Get-FileHash $zip -Algorithm SHA256).Hash.ToLowerInvariant()
$requestId=[guid]::NewGuid().ToString()
$certificate=Run-PrivateScript -Name 'private-deploy-certificate' -Text @"
`$ErrorActionPreference='Stop'
`$cert=New-SelfSignedCertificate -Type DocumentEncryptionCert -Subject 'CN=esehspark-deploy-$requestId' -CertStoreLocation 'Cert:\LocalMachine\My' -KeyLength 2048 -KeyExportPolicy NonExportable -NotAfter ([DateTime]::UtcNow.AddHours(1))
Write-Output ('PRIVATE_DEPLOY:'+(@{thumbprint=`$cert.Thumbprint;publicCertificate=[Convert]::ToBase64String(`$cert.RawData)}|ConvertTo-Json -Compress))
"@
# Only CMS ciphertext crosses Run Command or is persisted in its script.
$cert=[Security.Cryptography.X509Certificates.X509Certificate2]::new(
    [Convert]::FromBase64String($certificate.publicCertificate))
$token=& az account get-access-token --resource 'https://management.azure.com/' --query accessToken -o tsv --only-show-errors
if($LASTEXITCODE -ne 0 -or -not $token){throw 'Deployment token acquisition failed.'}
try{$encrypted=Protect-CmsMessage -To $cert -Content $token}finally{$token=$null}
$cipher=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($encrypted))
$runtime=Get-Content (Join-Path $PSScriptRoot 'Deploy-PrivateScoringFunctionRuntime.ps1') -Raw
$runtimePayload=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($runtime))
$result=Run-PrivateScript -Name 'private-function-deploy-runtime' -Text @"
`$code=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$runtimePayload'))
& ([scriptblock]::Create(`$code)) -Package '$package' -EncryptedToken '$cipher' -CertificateThumbprint '$($certificate.thumbprint)' -ExpectedHash '$hash'
"@
$result|ConvertTo-Json -Depth 8|Set-Content (Join-Path $directory 'private-random-function-deployment.json')
if(-not $result.succeeded){throw 'Private Function deployment failed; inspect redacted evidence.'}
$after=Az-Json -Arguments @('functionapp','show','-g','rg_esehspark','-n','esehspark-score-api')
if($after.publicNetworkAccess -ne 'Disabled'){throw 'Function private-only invariant changed after deployment.'}
Write-Output ($result|ConvertTo-Json -Depth 8)
