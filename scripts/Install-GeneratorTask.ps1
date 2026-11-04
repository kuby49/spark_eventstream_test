[CmdletBinding()]
param(
    [Parameter(Mandatory)] [string] $SourceRoot,
    [Parameter(Mandatory)] [string] $PythonPath,
    [Parameter(Mandatory)] [string] $KeyVaultUrl
)

$ErrorActionPreference = 'Stop'
$producer = Join-Path $SourceRoot 'src\generator\producer.py'
$requirements = Join-Path $SourceRoot 'src\generator\requirements.txt'
if (-not (Test-Path -LiteralPath $producer) -or -not (Test-Path -LiteralPath $requirements)) {
    throw 'Generator source or requirements file is missing from SourceRoot.'
}
if (-not (Test-Path -LiteralPath $PythonPath)) { throw "Python executable not found: $PythonPath" }
$starter=Join-Path $SourceRoot 'scripts\Start-Generator.ps1'
foreach($required in @($starter,(Join-Path $SourceRoot 'scripts\Start-EventstreamProcessing.ps1'),
    (Join-Path $SourceRoot 'scripts\EventstreamLifecycle.ps1'))){
    if(-not(Test-Path -LiteralPath $required)){throw "Generator lifecycle script missing: $required"}
}

& $PythonPath -m pip install --disable-pip-version-check -r $requirements
if ($LASTEXITCODE -ne 0) { throw 'Installing the generator dependencies failed.' }

$taskName = 'esehspark-generator'
$logDirectory = Join-Path $SourceRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDirectory | Out-Null
[Environment]::SetEnvironmentVariable('KEY_VAULT_URL', $KeyVaultUrl, 'Machine')
[Environment]::SetEnvironmentVariable('EVENTSTREAM_SOURCE_SECRET', 'eventstream-source-connection', 'Machine')

$configuration=Join-Path $SourceRoot '.deployment\generator-task.json'
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $configuration)|Out-Null
@{pythonPath=$PythonPath}|ConvertTo-Json|Set-Content -LiteralPath $configuration -Encoding UTF8
$action = New-ScheduledTaskAction -Execute (Get-Process -Id $PID).Path -Argument (
    "-NoProfile -File `"$starter`" -RunInline -PythonPath `"$PythonPath`""
) -WorkingDirectory $SourceRoot
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddYears(10)
$principal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 1)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Principal $principal -Settings $settings -Force | Out-Null
Write-Output "Generator task registered as '$taskName'; it is not started automatically."
