$ErrorActionPreference = 'Stop'
$stdout = New-TemporaryFile
$stderr = New-TemporaryFile
try {
    $python = (Get-Command python -ErrorAction Stop).Source
    $probe = Start-Process -FilePath $python -ArgumentList '-c "import __esehspark_missing_dependency_probe__"' `
        -Wait -PassThru -RedirectStandardOutput $stdout.FullName -RedirectStandardError $stderr.FullName
    if ($probe.ExitCode -ne 1 -or
        (Get-Content $stderr.FullName -Raw) -notmatch 'ModuleNotFoundError') {
        throw 'An expected missing dependency must yield a checked exit code rather than abort the bootstrap.'
    }
    $completed = Start-Process -FilePath $python -ArgumentList '-c "raise SystemExit(0)"' -PassThru `
        -RedirectStandardOutput $stdout.FullName -RedirectStandardError $stderr.FullName
    $null = $completed.Handle
    if (-not $completed.WaitForExit(10000) -or $null -eq $completed.ExitCode -or $completed.ExitCode -ne 0) {
        throw 'Bounded asynchronous bootstrap processes must retain their handle and a readable exit code.'
    }
    $emptyLog = [string](Get-Content $stderr.FullName -Raw)
    if ([regex]::Matches($emptyLog, 'SSLError').Count -ne 0) {
        throw 'Empty dependency stderr must remain valid redacted diagnostic input.'
    }
    $source = Get-Content (Join-Path $PSScriptRoot '..\scripts\Invoke-PrivateSourceCanary.ps1') -Raw
    if ($source -match '& \$python' -or $source -notmatch "stage='dependency_probe'" -or
        $source -notmatch 'if\(\$probe.ExitCode -ne 0\)' -or
        $source -notmatch 'WaitForExit\(180000\)' -or $source -notmatch 'WaitForExit\(120000\)') {
        throw 'Source bootstrap must check process exit codes, report stages and bound installation/canary execution.'
    }
    Write-Output 'Missing-module stderr is isolated from Stop error semantics; bootstrap process guards passed.'
}
finally {
    Remove-Item -LiteralPath $stdout.FullName, $stderr.FullName -Force
}
