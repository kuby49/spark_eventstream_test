$ErrorActionPreference='Stop'
$script:sessionState='Succeeded'
$script:processes=@()
$script:jobState='Completed'
$script:inventoryFailure=$false
function Invoke-RestMethod {
    param($Uri,$Headers,$TimeoutSec)
    if($Uri -like '*169.254*'){return @{access_token='test-only'}}
    if($script:inventoryFailure){throw 'TestInventoryUnavailable'}
    if($Uri -match '/items$'){return @{value=@(@{id='notebook';type='Notebook';displayName='TestNotebook'})}}
    if($Uri -match '/jobs/instances$'){return @{value=@(@{id='job';status=$script:jobState})}}
    if($Uri -match '/spark/livySessions$'){return @{value=@(@{state=$script:sessionState})}}
    throw 'UnexpectedRead'
}
function Get-CimInstance {param($ClassName,$ErrorAction);return $script:processes}
function Get-ScheduledTask {param($ErrorAction);return @()}
# The private DNS guard remains real; replace only that expression in the local mocked script.
$path=Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts\Read-PrivateWorkloadInventory.ps1'
$code=[IO.File]::ReadAllText($path).Replace(
    "[Net.Dns]::GetHostAddresses(`$apiHost)|ForEach-Object IPAddressToString",
    "'10.74.2.10'")
function Read-MockedInventory {
    $text=& ([scriptblock]::Create($code)) -WorkspaceId 'ddd993fe-8d65-46f6-90f5-7002e6aa5990'
    ($text -replace '^WORKLOAD_INVENTORY:','')|ConvertFrom-Json
}
$result=Read-MockedInventory
if(-not $result.complete -or -not $result.idle){throw 'Completed jobs and Succeeded monitoring sessions must not be misclassified as active.'}
$script:sessionState='Failed'
if(-not (Read-MockedInventory).idle){throw 'Failed monitoring sessions are terminal.'}
$script:sessionState='Running'
if((Read-MockedInventory).idle){throw 'Running session must block shutdown.'}
$script:sessionState='Succeeded'
$script:jobState='InProgress'
if((Read-MockedInventory).idle){throw 'Running item job must block shutdown.'}
$script:jobState='Completed'
$script:processes=@([pscustomobject]@{Name='explorer.exe';ProcessId=1})
$result=Read-MockedInventory
if($result.idle -or $result.desktopProcesses.Count -ne 1){throw 'Existing desktop must require explicit approval.'}
$script:processes=@([pscustomobject]@{Name='python.exe';ProcessId=2})
if((Read-MockedInventory).pythonProcesses.Count -ne 1){throw 'Python workload must be preserved independently of desktop approval.'}
$script:inventoryFailure=$true
$result=Read-MockedInventory
if($result.complete -or $result.idle){throw 'Unavailable inventory must fail closed.'}
Write-Output 'Terminal-session classification, active workloads, desktop ownership and unavailable inventory regressions passed.'
