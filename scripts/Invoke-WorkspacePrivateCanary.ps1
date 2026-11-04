[CmdletBinding()]
param([switch] $BlockInboundAfterProof)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
$run = Get-Content (Join-Path $directory 'staged-diagnostic-run.json') -Raw | ConvertFrom-Json
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$token = & az account get-access-token --subscription $subscription --resource https://api.fabric.microsoft.com `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Fabric token unavailable.' }
$headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='spark-cli'}
$base = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)"
$principalId = & az vm show -g rg_esehspark -n vmesehspark --subscription $subscription `
    --query identity.principalId -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or -not $principalId) { throw 'VM identity unavailable.' }
$endpoint = & az network private-endpoint show -g rg_esehspark -n pe-esehspark-fabric-workspace `
    --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $endpoint.privateLinkServiceConnections[0].privateLinkServiceConnectionState.status -ne 'Approved') {
    throw 'Workspace private endpoint is not approved.'
}
$ips = & az network nic show --ids $endpoint.networkInterfaces[0].id --subscription $subscription `
    --query 'ipConfigurations[].privateIPAddress' -o json --only-show-errors | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $ips) { throw 'Workspace private endpoint addresses unavailable.' }
$roles = Invoke-RestMethod -Uri "$base/roleAssignments" -Headers $headers
$existing = @($roles.value | Where-Object { $_.principal.id -eq $principalId })
$createdRole = $null
$capacityId = "/subscriptions/$subscription/resourceGroups/rg_esehspark/providers/Microsoft.Fabric/capacities/fabricessparktest8"
$capacityUri = "https://management.azure.com$capacityId`?api-version=2023-11-01"
$remote = @'
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
$workspace='__WORKSPACE__'
$lakehouse='__LAKEHOUSE__'
$path='__RESULT_PATH__'
$expected=@(__IPS__)
$prefix=$workspace.Replace('-','')
$zone=$prefix.Substring(0,2)
$apiHost="$prefix.z$zone.w.api.fabric.microsoft.com"
$dfsHost="$prefix.z$zone.dfs.fabric.microsoft.com"
$results=@()
foreach($target in @(
    @{stage='workspace_api';host=$apiHost;resource='https://api.fabric.microsoft.com';url="https://$apiHost/v1/workspaces/$workspace"},
    @{stage='onelake_read';host=$dfsHost;resource='https://storage.azure.com/';url="https://$dfsHost/$workspace/$lakehouse/$path"}
)){
    $s=$target.stage
    try{
        $addresses=@([Net.Dns]::GetHostAddresses($target.host)|ForEach-Object IPAddressToString)
        if(-not $addresses.Count -or @($addresses|Where-Object {$_ -notin $expected}).Count){
            throw [InvalidOperationException]::new('WorkspaceEndpointAddressMismatch')
        }
        $resource=[Uri]::EscapeDataString($target.resource)
        $t=(Invoke-RestMethod "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=$resource" -Headers @{Metadata='true'} -TimeoutSec 15).access_token
        $h=@{Authorization="Bearer $t";'x-ms-version'='2021-06-08';'x-ms-fabric-skill'='spark-cli'}
        $r=Invoke-WebRequest -UseBasicParsing -Uri $target.url -Headers $h -TimeoutSec 30 -MaximumRedirection 0
        if($r.StatusCode -ne 200){throw [InvalidOperationException]::new('UnexpectedStatus')}
        $results+=@{stage=$s;success=$true;httpStatus=200;host=$target.host;privateAddresses=$addresses;tlsVerified=$true}
    }catch{
        $status=$null
        if($_.Exception.Response -and $_.Exception.Response.StatusCode){$status=[int]$_.Exception.Response.StatusCode}
        $code=$null
        if($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"'){$code=$Matches[1]}
        $results+=@{stage=$s;success=$false;httpStatus=$status;errorType=$_.Exception.GetType().Name;errorCode=$code}
    }
}
Write-Output ('WORKSPACE_PRIVATE_CANARY:'+(@{atUtc=[DateTime]::UtcNow.ToString('o');results=$results}|ConvertTo-Json -Depth 10 -Compress))
'@
$remote = $remote.Replace('__WORKSPACE__', $inventory.workspaceId).
    Replace('__LAKEHOUSE__', $inventory.lakehouseId).Replace('__RESULT_PATH__', $run.resultRelativePath).
    Replace('__IPS__', (@($ips | ForEach-Object { "'$_'" }) -join ','))
function Run-PrivateProbe {
    param([string] $Phase)
    $encoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($remote))
    $command = & az vm run-command invoke -g rg_esehspark -n vmesehspark --subscription $subscription `
        --command-id RunPowerShellScript --scripts "powershell.exe -NoProfile -NonInteractive -EncodedCommand $encoded" `
        --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'VM private probe Run Command failed.' }
    $command | ConvertTo-Json -Depth 20 | Set-Content (Join-Path $directory "workspace-private-$Phase-command.json")
    $stdout = @($command.value | Where-Object code -eq 'ComponentStatus/StdOut/succeeded')
    if ($stdout.Count -ne 1 -or $stdout[0].message -notmatch 'WORKSPACE_PRIVATE_CANARY:(\{[^\r\n]+\})') {
        throw 'VM private probe returned no structured evidence.'
    }
    $result = $Matches[1] | ConvertFrom-Json
    $result | ConvertTo-Json -Depth 15 | Set-Content (Join-Path $directory "workspace-private-$Phase.json")
    if (@($result.results | Where-Object { -not $_.success }).Count -or $result.results.Count -ne 2) {
        throw "Workspace private $Phase gate failed; inspect redacted evidence."
    }
}
try {
    if (-not $existing.Count) {
        $body = @{principal=@{id=$principalId;type='ServicePrincipal'};role='Contributor'} | ConvertTo-Json -Depth 5
        $createdRole = Invoke-RestMethod -Method Post -Uri "$base/roleAssignments" -Headers $headers `
            -ContentType application/json -Body $body
        if (-not $createdRole.id) { throw 'Workspace-scoped diagnostic role returned no ID.' }
        $createdRole | ConvertTo-Json -Depth 10 | Set-Content (Join-Path $directory 'workspace-vm-diagnostic-role.json')
    }
    $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $capacity.sku.name -ne 'F8') { throw 'Unable to verify exact F8 for OneLake readiness.' }
    if ($capacity.properties.state -eq 'Paused') {
        & az rest --method post --url "https://management.azure.com$capacityId/resume?api-version=2023-11-01" `
            --subscription $subscription --only-show-errors -o none
        if ($LASTEXITCODE -ne 0) { throw 'OneLake readiness capacity resume failed.' }
    }
    $readyDeadline = [DateTime]::UtcNow.AddMinutes(5)
    do {
        Start-Sleep -Seconds 10
        $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
        if ($LASTEXITCODE -ne 0 -or [DateTime]::UtcNow -gt $readyDeadline) { throw 'Capacity readiness unverified.' }
    } while ($capacity.properties.state -ne 'Active')
    & az vm start -g rg_esehspark -n vmesehspark --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'VM start failed.' }
    Run-PrivateProbe before
    if ($BlockInboundAfterProof) {
        $proofPath = Join-Path $directory 'workspace-private-cutover-proof.json'
        @{workspacePrivateApi=$true;workspacePrivateOneLake=$true;acceptancePassed=$false;diagnosticOnly=$true} |
            ConvertTo-Json | Set-Content $proofPath
        & (Join-Path $PSScriptRoot 'Set-WorkspaceNetworkPolicy.ps1') -WorkspaceId $inventory.workspaceId `
            -PrivatePathProofPath $proofPath -BlockInboundPublic -WorkspacePrivateCutover
        if (-not $?) { throw 'Workspace private-only cutover failed.' }
        Run-PrivateProbe after
        $negative = $null
        try {
            Invoke-WebRequest -Uri $base -Headers $headers -TimeoutSec 30 | Out-Null
            throw 'Authorized public workspace request unexpectedly succeeded after inbound Deny.'
        }
        catch [Microsoft.PowerShell.Commands.HttpResponseException] {
            $body = $_.ErrorDetails.Message | ConvertFrom-Json
            $negative = @{atUtc=[DateTime]::UtcNow.ToString('o');httpStatus=[int]$_.Exception.Response.StatusCode;
                errorCode=$body.errorCode;otherwiseAuthorized=$true}
            $negative | ConvertTo-Json | Set-Content (Join-Path $directory 'workspace-public-negative.json')
            if ($negative.errorCode -ne 'RequestDeniedByInboundPolicy') { throw 'Public denial was not the expected inbound-policy rejection.' }
        }
    }
}
finally {
    try {
    & az vm deallocate -g rg_esehspark -n vmesehspark --subscription $subscription --only-show-errors -o none
    if ($LASTEXITCODE -ne 0) { throw 'VM diagnostic deallocation failed.' }
    $vmState = & az vm get-instance-view -g rg_esehspark -n vmesehspark --subscription $subscription `
        --query "instanceView.statuses[?starts_with(code, 'PowerState/')].code | [0]" -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or $vmState -ne 'PowerState/deallocated') { throw 'VM diagnostic idle state unverified.' }
    @{verifiedAtUtc=[DateTime]::UtcNow.ToString('o');vmPowerState=$vmState;acceptancePassed=$false} |
        ConvertTo-Json | Set-Content (Join-Path $directory 'workspace-private-vm-shutdown.json')
    }
    finally {
        & az rest --method post --url "https://management.azure.com$capacityId/suspend?api-version=2023-11-01" `
            --subscription $subscription --only-show-errors -o none
        if ($LASTEXITCODE -ne 0) { throw 'Private workspace diagnostic capacity suspension failed.' }
        $stopDeadline = [DateTime]::UtcNow.AddMinutes(5)
        do {
            Start-Sleep -Seconds 10
            $capacity = & az rest --method get --url $capacityUri --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
            if ($LASTEXITCODE -ne 0 -or [DateTime]::UtcNow -gt $stopDeadline) { throw 'Final F8 suspension unverified.' }
        } while ($capacity.properties.state -ne 'Paused')
        @{verifiedAtUtc=[DateTime]::UtcNow.ToString('o');capacityState=$capacity.properties.state;acceptancePassed=$false} |
            ConvertTo-Json | Set-Content (Join-Path $directory 'workspace-private-capacity-shutdown.json')
    }
}
