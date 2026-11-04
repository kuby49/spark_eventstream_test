[CmdletBinding()]
param(
    [ValidateScript({ $_ -gt 0 -and $_ % 10 -eq 0 })] [int] $Rate = 30,
    [ValidateScript({ $_ -gt 0 })] [int] $Minutes = 30,
    [guid] $RunId = [guid]::NewGuid(),
    [string] $PrivatePathProofPath,
    [string] $InventoryPath,
    [switch] $OapOffApprovedTest,
    [switch] $RunInline,
    [string] $PythonPath
)

$ErrorActionPreference = 'Stop'
$taskName = 'esehspark-generator'
$task = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
if(-not $RunInline -and $task.State -eq 'Running'){throw 'Generator is already running; stop and drain it before starting a new run.'}
$workingDirectory = [string]$task.Actions[0].WorkingDirectory
$producer = Join-Path $workingDirectory 'src\generator\producer.py'
if (-not $workingDirectory -or -not (Test-Path -LiteralPath $producer)) {
    throw 'The registered generator task does not point to the expected source tree.'
}
$proofPath = if ($PrivatePathProofPath) {
    $PrivatePathProofPath
}
else {
    Join-Path $workingDirectory '.deployment\private-path-proof.json'
}
if (-not (Test-Path -LiteralPath $proofPath)) {
    throw "Private-path proof file is missing: $proofPath. The generator cannot start before final-policy canaries pass."
}
$proof = Get-Content -LiteralPath $proofPath -Raw | ConvertFrom-Json
foreach ($field in @(
    'vmSourceCanary',
    'eventhouseIngestion',
    'sparkKafka',
    'eventhouseKql',
    'functionApi',
    'keyVaultSecret',
    'lakehouseDelta',
    'workspacePublicAccessDenied'
)) {
    if ($proof.$field -isnot [bool] -or -not $proof.$field) {
        throw "Private-path proof is incomplete; required final-policy canary '$field' is not true."
    }
    if($OapOffApprovedTest){
        if($proof.workspaceOapEnabled -isnot [bool] -or $proof.workspaceOapEnabled -ne $false -or
            $proof.workspaceOutboundAction -ne 'Allow' -or $proof.outboundAllowUserApproved -isnot [bool] -or
            $proof.outboundAllowUserApproved -ne $true){
            throw 'OAP-off start requires explicitly labeled, user-approved outbound-Allow proof, not protected acceptance.'
        }
    }else{
        foreach($field in @('workspaceOapEnabled','unapprovedPublicRejected')){
            if($proof.$field -isnot [bool] -or -not $proof.$field){
                throw "Protected start requires final-policy proof '$field'."
            }
        }
    }
}
$manifest = Join-Path $workingDirectory "logs\generator-$RunId.jsonl"
$inventoryFile=if($InventoryPath){$InventoryPath}else{Join-Path $workingDirectory '.deployment\fabric-inventory.json'}
$inventory=Get-Content -LiteralPath $inventoryFile -Raw | ConvertFrom-Json
if(-not $inventory.workspaceId -or -not $inventory.eventstreamId){throw 'Generator requires deployed workspace/Eventstream identity.'}
$processing=& (Join-Path $PSScriptRoot 'Start-EventstreamProcessing.ps1') `
    -WorkspaceId $inventory.workspaceId -EventstreamId $inventory.eventstreamId
if(-not $processing.running){throw 'Eventstream Running readback failed; generator will not start.'}
if(-not $PythonPath){
    $configuration=Get-Content (Join-Path $workingDirectory '.deployment\generator-task.json') -Raw | ConvertFrom-Json
    $PythonPath=[string]$configuration.pythonPath
}
if(-not $PythonPath -or -not(Test-Path -LiteralPath $PythonPath)){throw 'Registered generator Python runtime is unavailable; reinstall the task.'}
if($RunInline){
    & $PythonPath $producer --rate $Rate --minutes $Minutes --run-id $RunId --manifest $manifest
    if($LASTEXITCODE -ne 0){throw "Generator failed with exit code $LASTEXITCODE; retain its manifest."}
    return
}
$arguments = "-NoProfile -File `"$PSCommandPath`" -RunInline -PythonPath `"$PythonPath`" -Rate $Rate -Minutes $Minutes -RunId $RunId -PrivatePathProofPath `"$proofPath`" -InventoryPath `"$inventoryFile`""
if($OapOffApprovedTest){$arguments+=' -OapOffApprovedTest'}
$action = New-ScheduledTaskAction -Execute (Get-Process -Id $PID).Path `
    -Argument $arguments -WorkingDirectory $workingDirectory
Set-ScheduledTask -TaskName $taskName -Action $action | Out-Null
Start-ScheduledTask -TaskName $taskName
$deadline = [DateTime]::UtcNow.AddSeconds(15)
do {
    Start-Sleep -Seconds 1
    $state = (Get-ScheduledTask -TaskName $taskName).State
} while ($state -ne 'Running' -and [DateTime]::UtcNow -lt $deadline)
if ($state -ne 'Running') { throw "Generator scheduled task did not enter Running state (observed '$state')." }
Write-Output "Generator started: run_id=$RunId rate=$Rate/min minutes=$Minutes manifest=$manifest"
