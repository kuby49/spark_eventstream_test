[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
$directory = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment'
$inventory = Get-Content (Join-Path $directory 'fabric-inventory.json') -Raw | ConvertFrom-Json
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$accountText = & az account show --subscription $subscription --only-show-errors -o json
if ($LASTEXITCODE -ne 0) { throw 'MCAP account context read failed; no workload requests made.' }
$account = $accountText | ConvertFrom-Json
if ($account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or $account.id -ne $subscription) {
    throw 'MCAP tenant/subscription mismatch; no workload requests made.'
}
$report = [ordered]@{
    startedAtUtc=[DateTime]::UtcNow.ToString('o');tenantId=$account.tenantId;subscriptionId=$account.id;
    workspaceId=$inventory.workspaceId;eventstreamId=$inventory.eventstreamId;
    historicalFixtureStateReconstructed=$false;computeResumed=$false;mutationPerformed=$false;
    captureSucceeded=$false;definitionCaptured=$false;topologyCaptured=$false;mappingsCaptured=$false
}
$stage = 'private_api_dns'
$token = $null
$headers = $null
try {
    $prefix = ([string]$inventory.workspaceId).Replace('-','')
    $privateHost = "$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
    try { $addresses = @([Net.Dns]::GetHostAddresses($privateHost) | ForEach-Object IPAddressToString) }
    catch { $addresses = @(); $report.privateDnsErrorType = $_.Exception.GetType().Name }
    $report.privateApiHost = $privateHost
    $report.privateApiAddresses = $addresses
    $privateAvailable = $addresses.Count -gt 0 -and
        @($addresses | Where-Object { $_ -ne '10.74.2.10' }).Count -eq 0
    # Public management inspection may be denied by inbound policy; never reset policy or bypass it.
    $apiHost = if ($privateAvailable) { $privateHost } else { 'api.fabric.microsoft.com' }
    $report.managementApiHost = $apiHost
    $token = & az account get-access-token --subscription $subscription `
        --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'Fabric token acquisition failed.' }
    $headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventstream-cli'}
    $base = "https://$apiHost/v1/workspaces/$($inventory.workspaceId)/eventstreams/$($inventory.eventstreamId)"
    $stage = 'current_eventstream_topology'
    $topology = Invoke-RestMethod -Uri "$base/topology" -Headers $headers -TimeoutSec 30 -MaximumRedirection 0
    Save-FabricEvidence (Join-Path $directory 'current-eventstream-topology.json') @{
        capturedAtUtc=[DateTime]::UtcNow.ToString('o');topology=$topology;
        state=(Get-FabricComponentStateEvidence $topology);historicalFixtureStateReconstructed=$false
    }
    $report.topologyCaptured = $true
    $stage = 'current_eventstream_definition'
    $response = Invoke-WebRequest -Method Post -Uri "$base/getDefinition" -Headers $headers `
        -ContentType 'application/json' -Body '{}' -TimeoutSec 30 -MaximumRedirection 0
    if ($response.StatusCode -eq 202) {
        $location = [string]$response.Headers['Location']
        $uri = [uri]$location
        if (-not $location -or $uri.Scheme -ne 'https' -or $uri.Host -ne $apiHost) {
            throw 'Definition LRO does not remain on the authorized API host; refusing alternate route.'
        }
        $deadline = [DateTime]::UtcNow.AddSeconds(60)
        do {
            Start-Sleep -Seconds 5
            $operation = Invoke-RestMethod -Uri $location -Headers $headers -TimeoutSec 15 -MaximumRedirection 0
            if ($operation.status -in @('Failed','Cancelled')) { throw 'Read-only definition LRO failed.' }
            if ([DateTime]::UtcNow -ge $deadline) { throw 'Bounded definition read timed out.' }
        } while ($operation.status -ne 'Succeeded')
        $definition = Invoke-RestMethod -Uri "$location/result" -Headers $headers -TimeoutSec 15 -MaximumRedirection 0
    }
    else { $definition = $response.Content | ConvertFrom-Json }
    Save-FabricEvidence (Join-Path $directory 'current-eventstream-definition.json') @{
        capturedAtUtc=[DateTime]::UtcNow.ToString('o');definition=(Convert-FabricDefinitionEvidence $definition);
        historicalFixtureStateReconstructed=$false
    }
    $report.definitionCaptured = $true
    $stage = 'eventhouse_private_path'
    $kqlHost = ([uri]$inventory.kustoQueryEndpoint).Host
    $kqlAddresses = @([Net.Dns]::GetHostAddresses($kqlHost) | ForEach-Object IPAddressToString)
    $report.kqlAddresses = $kqlAddresses
    if (-not $kqlAddresses.Count -or @($kqlAddresses | Where-Object { $_ -ne '10.74.2.11' }).Count) {
        throw 'Current host cannot reach the previously approved private KQL DNS path; no public KQL call made.'
    }
    $token = & az account get-access-token --subscription $subscription `
        --resource 'https://kusto.kusto.windows.net' --query accessToken -o tsv --only-show-errors
    if ($LASTEXITCODE -ne 0 -or -not $token) { throw 'KQL token acquisition failed.' }
    $headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'}
    foreach ($scope in @('table RawEvents', "database $($inventory.kqlDatabaseName)")) {
        $stage = "current_mappings_$scope"
        $result = Invoke-RestMethod -Method Post -Uri (
            ([string]$inventory.kustoQueryEndpoint).TrimEnd('/') + '/v1/rest/mgmt') `
            -Headers $headers -ContentType 'application/json' -TimeoutSec 30 -MaximumRedirection 0 `
            -Body (@{db=$inventory.kqlDatabaseName;csl=".show $scope ingestion json mappings"} | ConvertTo-Json -Compress)
        $name = if ($scope.StartsWith('table')) { 'table' } else { 'database' }
        Save-FabricEvidence (Join-Path $directory "current-eventhouse-$name-mappings.json") @{
            capturedAtUtc=[DateTime]::UtcNow.ToString('o');databaseId=$inventory.kqlDatabaseId;
            databaseName=$inventory.kqlDatabaseName;command=".show $scope ingestion json mappings";response=$result
        }
        if ($result.error -or -not $result.Tables) { throw 'Mapping response lacks a valid result table.' }
    }
    $report.mappingsCaptured = $true
    $report.captureSucceeded = $true
}
catch {
    $report.failedStage = $stage
    $report.errorType = $_.Exception.GetType().Name
    if ($_.Exception.Response) { $report.httpStatus = [int]$_.Exception.Response.StatusCode }
    if ($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"') {
        $report.errorCode = $Matches[1]
    }
    $report.stoppedWithoutReopeningOrResuming = $true
}
finally {
    $token = $null
    $headers = $null
    $report.finishedAtUtc = [DateTime]::UtcNow.ToString('o')
    Save-FabricEvidence (Join-Path $directory 'current-ingestion-capture-result.json') $report
}
$report | ConvertTo-Json -Depth 10
