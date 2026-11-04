[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenant = '4526cee1-4399-4e4b-9134-46fabd583527'
$root = Split-Path -Parent $PSScriptRoot
$directory = Join-Path $root '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json

function Invoke-AzJson {
    param([string[]] $Arguments)
    $result = & az @Arguments --subscription $subscription --only-show-errors --output json
    if ($LASTEXITCODE -ne 0) { throw "Azure evidence command failed: $($Arguments[0])." }
    return ($result | ConvertFrom-Json)
}

function Save-Evidence {
    param([string] $Name, [object] $Value)
    $Value | ConvertTo-Json -Depth 50 |
        Set-Content -LiteralPath (Join-Path $directory $Name) -Encoding utf8
}

$account = Invoke-AzJson @('account', 'show')
if ($account.id -ne $subscription -or $account.tenantId -ne $tenant -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') {
    throw 'The Azure CLI identity does not match the approved isolated MCAP administrator.'
}
$resources = @(Invoke-AzJson @('resource', 'list', '--resource-group', 'rg_esehspark',
    '--query', '[].{id:id,name:name,type:type,location:location}'))
Save-Evidence 'idle-resource-inventory.json' $resources
$endpoints = Invoke-AzJson @('network', 'private-endpoint', 'list', '--resource-group', 'rg_esehspark')
Save-Evidence 'idle-private-endpoints.json' $endpoints

$token = & az account get-access-token --subscription $subscription `
    --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Fabric token acquisition failed.' }
$headers = @{ Authorization = "Bearer $token"; 'x-ms-fabric-skill' = 'spark-cli' }
$base = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)"
$policy = Invoke-RestMethod -Uri "$base/networking/communicationPolicy" -Headers $headers
Save-Evidence 'idle-workspace-communication-policy.json' $policy
$jobs = [Collections.Generic.List[object]]::new()
$next = "$base/items/$($inventory.notebookId)/jobs/instances"
while ($next) {
    $page = Invoke-RestMethod -Uri $next -Headers $headers
    foreach ($job in @($page.value)) { $jobs.Add($job) }
    $next = $page.continuationUri
    if (-not $next -and $page.continuationToken) {
        $next = "$base/items/$($inventory.notebookId)/jobs/instances?continuationToken=" +
            [Uri]::EscapeDataString([string]$page.continuationToken)
    }
}
Save-Evidence 'idle-notebook-jobs.json' @($jobs.ToArray())
$terminal = @('Completed', 'Failed', 'Cancelled', 'Canceled', 'Deduped')
$active = @($jobs | Where-Object { $_.status -notin $terminal })
if ($active.Count) { throw 'A notebook job is not terminal; idle shutdown evidence was not issued.' }
$canary = Get-Content (Join-Path $directory 'spark-private-route-canary.json') -Raw | ConvertFrom-Json
$session = Invoke-RestMethod -Uri "$base/notebooks/$($inventory.notebookId)/livySessions/$($canary.sparkSessionId)" `
    -Headers $headers
Save-Evidence 'idle-failed-livy-session.json' $session
$token = $null
$headers = $null

$remote = @'
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$tasks = @(Get-ScheduledTask | Where-Object { $_.TaskName -like '*esehspark*' })
$activeTasks = @($tasks | Where-Object { $_.State -in @('Running', 'Queued') })
$processes = @(Get-CimInstance Win32_Process | Where-Object {
    $_.Name -match '^python(w)?\.exe$' -and $_.CommandLine -match 'producer\.py'
})
$dns = @([Net.Dns]::GetHostAddresses('esehusw3qzfnf3vqakins22j.servicebus.windows.net') |
    ForEach-Object { $_.IPAddressToString })
[ordered]@{
    checkedAtUtc = [DateTime]::UtcNow.ToString('o')
    tasks = @($tasks | Select-Object TaskName, @{n='state';e={[string]$_.State}})
    activeGeneratorTaskCount = $activeTasks.Count
    producerProcessCount = $processes.Count
    sourceHost = 'esehusw3qzfnf3vqakins22j.servicebus.windows.net'
    sourceDnsAddresses = $dns
    sourceAuthenticatedSendAttempted = $false
    note = 'Read-only diagnostic; no event was generated or sent.'
} | ConvertTo-Json -Depth 10 -Compress
'@
$encodedScript = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($remote))
$runner = "powershell.exe -NoProfile -NonInteractive -EncodedCommand $encodedScript"
$command = Invoke-AzJson @('vm', 'run-command', 'invoke', '--resource-group', 'rg_esehspark',
    '--name', 'vmesehspark', '--command-id', 'RunPowerShellScript', '--scripts', $runner)
Save-Evidence 'idle-vm-workload-diagnostic.json' $command
$stdout = @($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
$stderr = @($command.value | Where-Object code -eq 'ComponentStatus/StdErr/succeeded')
if ($stdout.Count -ne 1 -or [string]::IsNullOrWhiteSpace($stdout[0].message) -or
    @($stderr | Where-Object { -not [string]::IsNullOrWhiteSpace($_.message) }).Count) {
    throw 'VM evidence output was absent or reported an error.'
}
$vm = [string]$stdout[0].message | ConvertFrom-Json
if ($vm.activeGeneratorTaskCount -ne 0 -or $vm.producerProcessCount -ne 0) {
    throw 'A generator task or producer process exists; idle shutdown evidence was not issued.'
}
$evidence = [ordered]@{
    mode = 'approved-idle-shutdown'
    verifiedAtUtc = [DateTime]::UtcNow.ToString('o')
    generatorStopped = $true
    notebookStopped = $true
    processingDrained = $true
    evidenceExported = $true
    inventoryCaptured = $true
    fullPoliciesCaptured = $true
    canaryDiagnosticsCaptured = $true
    activeGeneratorTaskCount = 0
    activeNotebookJobCount = 0
    unresolvedCount = 0
    acceptancePassed = $false
    baselineRun = $false
    drainBasis = 'No generator or Kafka processing was started; fresh VM task/process and Fabric job checks show no active workload. No baseline messages exist to drain or mirror.'
}
Save-Evidence 'idle-shutdown-evidence.json' $evidence
Write-Output 'Refreshed MCAP inventory, full workspace policy, failed-session diagnostics, and no-active-work evidence. Acceptance remains unrun.'
