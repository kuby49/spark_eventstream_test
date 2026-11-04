[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$root = Split-Path -Parent $PSScriptRoot
$directory = Join-Path $root '.deployment'
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$capacityId = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
$sourceEndpoint = & az network private-endpoint show -g rg_esehspark -n pe-esehspark-fabric-workspace `
    --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
$vaultEndpoint = & az network private-endpoint show -g rg_esehspark -n pe-esehspark-kv `
    --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or
    $sourceEndpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved' -or
    $vaultEndpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved') {
    throw 'Required private endpoints are not approved.'
}
$sourceNic = & az network nic show --ids $sourceEndpoint.networkInterfaces[0].id --subscription $subscription `
    --only-show-errors -o json | ConvertFrom-Json
$sourceAddresses = @($sourceNic.ipConfigurations.privateIPAddress)
if ($LASTEXITCODE -ne 0 -or $sourceAddresses -notcontains '10.74.2.15') {
    throw 'The previously observed source private IP is not on the actual workspace PE NIC.'
}
$vaultIp = & az network nic show --ids $vaultEndpoint.networkInterfaces[0].id --subscription $subscription `
    --query 'ipConfigurations[0].privateIPAddress' -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $vaultIp) { throw 'Vault PE address unavailable.' }
$runId = [guid]::NewGuid().ToString()
$inventory=Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
$lifecycle=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $PSScriptRoot 'EventstreamLifecycle.ps1')))
$processing=[Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $PSScriptRoot 'Start-EventstreamProcessing.ps1')))
$source = [Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $root 'src\generator\private_source_canary.py')))
$requirements = [Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $root 'src\generator\private_canary_requirements.txt')))
$remote = @'
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$folder='C:\ProgramData\esehspark\diagnostics'
$python='C:\ProgramData\esehspark\Python313\python.exe'
New-Item -ItemType Directory -Force -Path $folder|Out-Null
$stage='python_install'
try{
    if(-not(Test-Path $python)){
        $installer=Join-Path $folder 'python-3.13.17-amd64.exe'
        Invoke-WebRequest -UseBasicParsing 'https://www.python.org/ftp/python/3.13.17/python-3.13.17-amd64.exe' -OutFile $installer -TimeoutSec 90
        $signature=Get-AuthenticodeSignature $installer
        if($signature.Status -ne 'Valid' -or $signature.SignerCertificate.Subject -notlike '*Python Software Foundation*'){
            throw 'Official Python installer signature failed verification'
        }
        $process=Start-Process -FilePath $installer -ArgumentList '/quiet InstallAllUsers=1 TargetDir=C:\ProgramData\esehspark\Python313 Include_test=0 PrependPath=0 Include_pip=1' -PassThru
        $null=$process.Handle
        if(-not $process.WaitForExit(180000)){Stop-Process -Id $process.Id;throw 'Python installer timed out'}
        if($process.ExitCode -notin @(0,3010) -or -not(Test-Path $python)){throw 'Python installation failed'}
        Remove-Item -LiteralPath $installer -Force
    }
    $code=Join-Path $folder 'private_source_canary.py'
    $requirements=Join-Path $folder 'private_canary_requirements.txt'
    [IO.File]::WriteAllBytes($code,[Convert]::FromBase64String('__SOURCE__'))
    [IO.File]::WriteAllBytes($requirements,[Convert]::FromBase64String('__REQUIREMENTS__'))
    $stage='dependency_probe'
    $probe=Start-Process -FilePath $python -ArgumentList '-c "import kafka"' -Wait -PassThru `
        -RedirectStandardOutput (Join-Path $folder 'dependency-probe.stdout') -RedirectStandardError (Join-Path $folder 'dependency-probe.stderr')
    if($probe.ExitCode -ne 0){
        $stage='dependency_install'
        $install=Start-Process -FilePath $python -ArgumentList "-m pip install --disable-pip-version-check --retries 1 --timeout 30 -r $requirements" -PassThru `
            -RedirectStandardOutput (Join-Path $folder 'dependency-install.stdout') -RedirectStandardError (Join-Path $folder 'dependency-install.stderr')
        $null=$install.Handle
        if(-not $install.WaitForExit(180000)){Stop-Process -Id $install.Id;throw 'Source dependency installation timed out'}
        if($null -eq $install.ExitCode){throw 'Source dependency exit code unavailable'}
        if($install.ExitCode -ne 0){throw 'Source canary dependency installation failed'}
    }
    $stage='eventstream_running'
    [IO.File]::WriteAllBytes((Join-Path $folder 'EventstreamLifecycle.ps1'),[Convert]::FromBase64String('__LIFECYCLE__'))
    [IO.File]::WriteAllBytes((Join-Path $folder 'Start-EventstreamProcessing.ps1'),[Convert]::FromBase64String('__PROCESSING__'))
    $running=& (Join-Path $folder 'Start-EventstreamProcessing.ps1') -WorkspaceId '__WORKSPACE__' -EventstreamId '__EVENTSTREAM__'
    if(-not $running.running){throw 'Eventstream running prerequisite failed'}
    $stage='source_canary'
    $canary=Start-Process -FilePath $python -ArgumentList "$code --expected-source-ip 10.74.2.15 --expected-vault-ip __VAULT_IP__ --run-id __RUN_ID__" -PassThru `
        -RedirectStandardOutput (Join-Path $folder 'source-canary.stdout') -RedirectStandardError (Join-Path $folder 'source-canary.stderr')
    $null=$canary.Handle
    if(-not $canary.WaitForExit(120000)){Stop-Process -Id $canary.Id;throw 'Source canary process timed out'}
    if($null -eq $canary.ExitCode){throw 'Source canary exit code unavailable'}
    Get-Content (Join-Path $folder 'source-canary.stdout')
    if($canary.ExitCode -ne 0){exit 1}
}catch{
    $reason=switch($_.Exception.Message){
        'Source dependency installation timed out' {'DependencyInstallTimeout'}
        'Source dependency exit code unavailable' {'DependencyExitCodeUnavailable'}
        'Source canary dependency installation failed' {'DependencyInstallNonzeroExit'}
        'Source canary process timed out' {'CanaryProcessTimeout'}
        'Source canary exit code unavailable' {'CanaryExitCodeUnavailable'}
        default {'BootstrapFailure'}
    }
    Write-Output ('PRIVATE_SOURCE_BOOTSTRAP_ERROR:'+(@{stage=$stage;errorType=$_.Exception.GetType().Name;reason=$reason}|ConvertTo-Json -Compress))
    exit 1
}
'@
$remote = $remote.Replace('__SOURCE__', $source).Replace('__REQUIREMENTS__', $requirements).
    Replace('__VAULT_IP__', $vaultIp).Replace('__RUN_ID__', $runId).
    Replace('__LIFECYCLE__',$lifecycle).Replace('__PROCESSING__',$processing).
    Replace('__WORKSPACE__',[string]$inventory.workspaceId).Replace('__EVENTSTREAM__',[string]$inventory.eventstreamId)
try {
    $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $capacity.sku.name -ne 'F8') { throw 'Exact F8 unavailable.' }
    if ($capacity.properties.state -eq 'Paused') {
        & az rest --method post --url "https://management.azure.com$capacityId/resume?api-version=2023-11-01" `
            --subscription $subscription --only-show-errors -o none
        if ($LASTEXITCODE -ne 0) { throw 'Source canary capacity resume failed.' }
    }
    $deadline = [DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or [DateTime]::UtcNow -gt $deadline) { throw 'F8 readiness unverified.' }
    } while ($capacity.properties.state -ne 'Active')
    & az vm start -g rg_esehspark -n vmesehspark --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'Source canary VM start failed.' }
    $runtimePath = Join-Path $directory 'private-source-runtime.ps1'
    [IO.File]::WriteAllText($runtimePath, $remote, [Text.UTF8Encoding]::new($false))
    $command = & az vm run-command invoke -g rg_esehspark -n vmesehspark --subscription $subscription `
        --command-id RunPowerShellScript --scripts "@$runtimePath" `
        --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Source canary Run Command failed.' }
    $command | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $directory 'private-source-command.json')
    $stdout = @($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if ($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'PRIVATE_SOURCE_CANARY:(\{[^\r\n]+\})') {
        throw 'Source canary returned no structured result; inspect redacted bootstrap evidence.'
    }
    $result = $Matches[1] | ConvertFrom-Json
    $result | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $directory 'private-source-canary.json')
    if (-not $result.acknowledged) { throw 'Private source canary did not acknowledge an event; no ingestion pass claimed.' }
}
finally {
    try {
        & az vm deallocate -g rg_esehspark -n vmesehspark --subscription $subscription --only-show-errors -o none
        if ($LASTEXITCODE -ne 0) { throw 'Source diagnostic VM deallocation failed.' }
    }
    finally {
        & az rest --method post --url "https://management.azure.com$capacityId/suspend?api-version=2023-11-01" `
            --subscription $subscription --only-show-errors -o none
        if ($LASTEXITCODE -ne 0) { throw 'Source diagnostic F8 suspension failed.' }
        $deadline = [DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 10
            $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or [DateTime]::UtcNow -gt $deadline) { throw 'Source diagnostic F8 idle state unverified.' }
        } while ($capacity.properties.state -ne 'Paused')
        $vmState = & az vm get-instance-view -g rg_esehspark -n vmesehspark --subscription $subscription `
            --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" -o tsv --only-show-errors
        if ($LASTEXITCODE -ne 0 -or $vmState -ne 'PowerState/deallocated') { throw 'Source diagnostic VM idle state unverified.' }
        @{verifiedAtUtc=[DateTime]::UtcNow.ToString('o');capacityState=$capacity.properties.state;
            vmPowerState=$vmState;acceptancePassed=$false;resourcesRetained=$true} | ConvertTo-Json |
            Set-Content (Join-Path $directory 'private-source-shutdown.json')
    }
}
