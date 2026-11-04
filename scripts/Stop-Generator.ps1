[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$taskName = 'esehspark-generator'
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
if ($task.State -eq 'Running') {
    Stop-ScheduledTask -TaskName $taskName
    $deadline = [DateTime]::UtcNow.AddSeconds(30)
    do {
        Start-Sleep -Seconds 1
        $state = (Get-ScheduledTask -TaskName $taskName).State
        if ([DateTime]::UtcNow -gt $deadline) {
            throw 'Generator task did not stop within 30 seconds; retain the manifest and investigate before draining Spark.'
        }
    } while ($state -eq 'Running')
}
Write-Output 'Generator task is stopped.'
