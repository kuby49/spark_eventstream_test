[CmdletBinding()]
param(
    [Parameter(Mandatory)][guid] $RunId,
    [Parameter(Mandatory)][int] $Rate
)
$ErrorActionPreference='Stop'
$env:AZURE_CONFIG_DIR=Join-Path $HOME '.azure-mcap'
$directory=Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$account=az account show --query '{tenant:tenantId,id:id}' -o json|ConvertFrom-Json
if($LASTEXITCODE -ne 0 -or $account.tenant -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.id -ne '0a0cf761-9d5d-4862-9165-7b69c188a0ee'){throw 'Incorrect MCAP context.'}
function Invoke-ReadCommand([string]$Source){
    $path=Join-Path $directory 'stress-evidence-export-runtime.ps1'
    [IO.File]::WriteAllText($path,$Source)
    $response=az vm run-command invoke -g rg_esehspark -n vmesehspark `
        --subscription '0a0cf761-9d5d-4862-9165-7b69c188a0ee' `
        --command-id RunPowerShellScript --scripts "@$path" --only-show-errors -o json
    if($LASTEXITCODE -ne 0){throw 'Stress evidence recovery VM command failed.'}
    $response=$response|ConvertFrom-Json
    $stdout=@($response.value|Where-Object code -eq 'ComponentStatus/StdOut/succeeded')[0].message
    $stderr=@($response.value|Where-Object code -eq 'ComponentStatus/StdErr/succeeded')[0].message
    if($stderr){throw "Stress evidence exporter failed: $stderr"}
    return $stdout
}
$runtime=Get-Content (Join-Path $PSScriptRoot 'Invoke-EventHubsAmqpPrivateRuntime.ps1') -Raw
$tail=$runtime.Substring($runtime.IndexOf('$report.checkedAtUtc'))
$encoded=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($tail))
$remote=@"
`$ErrorActionPreference='Stop'
`$RunId='$RunId';`$TestRate=$Rate;`$TestMinutes=15;`$StressTest=`$true;`$DiagnosticKind='functional-processing'
`$saved=Get-Content "C:\ProgramData\esehspark\diagnostics\stress-full-`$RunId.json" -Raw|ConvertFrom-Json
`$report=@{}
foreach(`$p in `$saved.PSObject.Properties){`$report[`$p.Name]=`$p.Value}
& ([scriptblock]::Create([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('$encoded'))))
"@
$out=Invoke-ReadCommand $remote
if($out -notmatch 'EVENTHUBS_AMQP_DIAGNOSTIC:(\{[^\r\n]+\})'){throw 'Recovered summary missing.'}
$envelope=$Matches[1]|ConvertFrom-Json
if($envelope.encoding -eq 'gzip-base64-file'){
    $expected="C:\ProgramData\esehspark\diagnostics\functional-processing-$RunId.b64"
    if($envelope.archivePath -ne $expected -or $envelope.archiveLength -le 0 -or
        $envelope.archiveLength -gt 2000000){throw 'Recovery archive identity/size invalid.'}
    $payload=''
    for($offset=0;$offset -lt $envelope.archiveLength;$offset+=2800){
        $out=Invoke-ReadCommand "`$v=[IO.File]::ReadAllText('$expected');Write-Output ('CHUNK:'+`$v.Substring($offset,[Math]::Min(2800,`$v.Length-$offset)))"
        if($out -notmatch 'CHUNK:([A-Za-z0-9+/=]+)'){throw 'Recovery chunk missing.'}
        $payload+=$Matches[1]
    }
    if($payload.Length -ne $envelope.archiveLength){throw 'Recovery archive incomplete.'}
}elseif($envelope.encoding -eq 'gzip-base64'){
    $payload=$envelope.payload
}else{throw 'Unsupported recovery encoding.'}
$memory=[IO.MemoryStream]::new([Convert]::FromBase64String($payload))
$gzip=[IO.Compression.GZipStream]::new($memory,[IO.Compression.CompressionMode]::Decompress)
$reader=[IO.StreamReader]::new($gzip)
try{$json=$reader.ReadToEnd()}finally{$reader.Dispose();$gzip.Dispose();$memory.Dispose()}
$result=$json|ConvertFrom-Json
if($result.runId -ne $RunId.ToString() -or $result.rate -ne $Rate){throw 'Recovered run identity mismatch.'}
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
Save-FabricEvidence (Join-Path $directory "stress-$Rate-$RunId.json") $result
$result|Select-Object runId,rate,sustained,counts,latency,drainAfterLastSendSeconds,offeredMessagesPerMinute
