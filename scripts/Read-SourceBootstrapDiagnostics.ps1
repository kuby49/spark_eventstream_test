$ErrorActionPreference = 'Stop'
$folder = 'C:\ProgramData\esehspark\diagnostics'
$python = 'C:\ProgramData\esehspark\Python313\python.exe'

function Read-Log {
    param([string] $Name)
    $path = Join-Path $folder $Name
    if (-not (Test-Path -LiteralPath $path)) { throw "Diagnostic log is missing: $Name" }
    return [string](Get-Content -LiteralPath $path -Raw)
}

$probe = [string](Read-Log 'dependency-probe.stderr')
$stderr = [string](Read-Log 'dependency-install.stderr')
$stdout = [string](Read-Log 'dependency-install.stdout')
$codes = @([regex]::Matches($stderr,
    'ReadTimeoutError|ConnectTimeout|SSLError|CERTIFICATE_VERIFY_FAILED|ProxyError|NewConnectionError|No matching distribution|Could not find a version|PermissionError|WinError [0-9]+|NameResolutionError|timed out') |
    ForEach-Object Value | Sort-Object -Unique)
$verification = Start-Process -FilePath $python -ArgumentList '-c "import kafka"' -Wait -PassThru `
    -RedirectStandardOutput (Join-Path $folder 'dependency-verification.stdout') `
    -RedirectStandardError (Join-Path $folder 'dependency-verification.stderr')
if ($null -eq $verification.ExitCode) { throw 'Dependency verification exit code unavailable.' }
$activeTasks = @(Get-ScheduledTask | Where-Object {
    $_.TaskName -like '*esehspark*' -and $_.State -in @('Running', 'Queued')
})
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match '^python(w)?\.exe$' -and
    $_.CommandLine -match 'producer\.py|private_source_canary\.py|pip install'
})
@{
    checkedAtUtc = [DateTime]::UtcNow.ToString('o')
    dependencyProbeModuleMissing = ($probe -match 'ModuleNotFoundError')
    dependencyImportExitCode = $verification.ExitCode
    dependencyInstallErrorCodes = $codes
    dependencyInstallStdoutBytes = $stdout.Length
    dependencyInstallStderrBytes = $stderr.Length
    successfulInstallReported = ($stdout -match 'Successfully installed kafka-python')
    activeGeneratorTaskCount = $activeTasks.Count
    activeProducerCanaryInstallerProcessCount = $processes.Count
    rawLogsNotExported = $true
} | ConvertTo-Json -Compress
