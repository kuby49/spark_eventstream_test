param(
    [Parameter(Mandatory)][string] $WorkspaceId,
    [Parameter(Mandatory)][string] $DatabaseId,
    [Parameter(Mandatory)][string] $RunId,
    [Parameter(Mandatory)][string] $MessageId,
    [Parameter(Mandatory)][string[]] $ExpectedPrivateAddresses
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$WorkspaceId = ([guid]$WorkspaceId).ToString()
$DatabaseId = ([guid]$DatabaseId).ToString()
$RunId = ([guid]$RunId).ToString()
$MessageId = ([guid]$MessageId).ToString()
$report = @{baselineRun=$false;acceptancePassed=$false;eventhouseIngestionProven=$false;
    run_id=$RunId;message_id=$MessageId}
$stage = 'workspace_private_metadata'

function Get-PrivateAddresses {
    param([string] $Hostname)
    $addresses = @([Net.Dns]::GetHostAddresses($Hostname) | ForEach-Object IPAddressToString)
    if (-not $addresses.Count -or @($addresses | Where-Object { $_ -notin $ExpectedPrivateAddresses }).Count) {
        $report.refusedHost = $Hostname
        $report.refusedAddresses = $addresses
        throw [InvalidOperationException]::new('EndpointNotOnApprovedFabricPrivateNic')
    }
    return $addresses
}

function Get-ManagedIdentityToken {
    param([string] $Audience)
    $resource = [Uri]::EscapeDataString($Audience)
    (Invoke-RestMethod -Uri (
        "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=$resource") `
        -Headers @{Metadata='true'} -TimeoutSec 15 -MaximumRedirection 0).access_token
}

function Get-CanaryResultTable {
    param([object] $Response)
    $requiredColumns = 'message_id,run_id,equipment_id,temperature,timestamp'
    $tables = @($Response.Tables | Where-Object {
        (@($_.Columns.ColumnName) -join ',') -eq $requiredColumns
    })
    if ($tables.Count -ne 1) { throw 'Canary query did not return exactly one table with the required projection.' }
    return $tables[0]
}

try {
    $prefix = $WorkspaceId.Replace('-', '')
    $apiHost = "$prefix.z$($prefix.Substring(0,2)).w.api.fabric.microsoft.com"
    $report.workspaceApiPrivateAddresses = @(Get-PrivateAddresses $apiHost)
    $token = Get-ManagedIdentityToken 'https://api.fabric.microsoft.com'
    $items = Invoke-RestMethod -Uri "https://$apiHost/v1/workspaces/$WorkspaceId/kqlDatabases" `
        -Headers @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'} -TimeoutSec 30 -MaximumRedirection 0
    $database = @($items.value | Where-Object id -eq $DatabaseId)
    if ($database.Count -ne 1) { throw 'Expected KQL Database was not found in the actual private metadata response.' }
    $details = Invoke-RestMethod -Uri "https://$apiHost/v1/workspaces/$WorkspaceId/kqlDatabases/$DatabaseId" `
        -Headers @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'} -TimeoutSec 30 -MaximumRedirection 0
    if ($details.id -ne $DatabaseId -or $details.workspaceId -ne $WorkspaceId -or $details.type -ne 'KQLDatabase') {
        throw 'KQL Database details do not match the verified workspace/item identity.'
    }
    $queryUri = [Uri]$details.properties.queryServiceUri
    $databaseName = [string]$details.displayName
    if ($queryUri.Scheme -ne 'https' -or $queryUri.Host -notlike '*.kusto.fabric.microsoft.com' -or
        -not $databaseName) { throw 'Actual KQL metadata did not supply a valid HTTPS query URI and database name.' }
    $report.queryUri = $queryUri.AbsoluteUri
    $report.databaseName = $databaseName
    $report.databaseNameSource = 'GA displayName, matching the previously deployed schema database name'
    $stage = 'kql_private_dns'
    $report.kqlPrivateAddresses = @(Get-PrivateAddresses $queryUri.Host)
    $stage = 'kql_authenticated_schema'
    $token = Get-ManagedIdentityToken 'https://kusto.kusto.windows.net'
    $headers = @{Authorization="Bearer $token";'x-ms-fabric-skill'='eventhouse-cli'}
    $url = $queryUri.AbsoluteUri.TrimEnd('/') + '/v1/rest/query'
    $schema = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -ContentType 'application/json' `
        -Body (@{db=$databaseName;csl='.show table RawEvents schema as json'} | ConvertTo-Json -Compress) -TimeoutSec 30 -MaximumRedirection 0
    if ($schema.error -or -not $schema.Tables) { throw 'RawEvents schema query returned no valid result.' }
    $report.schemaQuerySucceeded = $true
    $stage = 'eventhouse_ingestion_readback'
    $query = "RawEvents | where timestamp > ago(1h) | where run_id == '$RunId' and message_id == '$MessageId' | project message_id, run_id, equipment_id, temperature, timestamp | take 2"
    $deadline = [DateTime]::UtcNow.AddMinutes(3)
    do {
        $response = Invoke-RestMethod -Method Post -Uri $url -Headers $headers -ContentType 'application/json' `
            -Body (@{db=$databaseName;csl=$query} | ConvertTo-Json -Compress) -TimeoutSec 30 -MaximumRedirection 0
        if ($response.error -or -not $response.Tables) { throw 'Canary query returned no valid result.' }
        $report.resultTableSchemas = @($response.Tables | ForEach-Object {
            @{tableName=$_.TableName;columns=@($_.Columns.ColumnName)}
        })
        $table = Get-CanaryResultTable $response
        $report.rawResult = $table
        $report.rawRowCount = @($table.Rows).Count
        if ($report.rawRowCount -eq 1) {
            $columns = @($table.Columns.ColumnName)
            $row = $table.Rows[0]
            if ($row[[Array]::IndexOf($columns,'message_id')] -ne $MessageId -or
                $row[[Array]::IndexOf($columns,'run_id')] -ne $RunId -or
                $row[[Array]::IndexOf($columns,'equipment_id')] -ne 17 -or
                $row[[Array]::IndexOf($columns,'temperature')] -ne 20) {
                throw 'The independently read canary row did not match the acknowledged fixture.'
            }
            $report.eventhouseIngestionProven = $true
            $report.tlsVerified = $true
            break
        }
        Start-Sleep -Seconds 10
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $report.eventhouseIngestionProven) { throw 'No unique matching Eventhouse fixture was observed within the bounded readback window.' }
}
catch {
    $report.failedStage = $stage
    $report.errorType = $_.Exception.GetType().Name
    if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
        $report.httpStatus = [int]$_.Exception.Response.StatusCode
    }
    if ($_.ErrorDetails.Message -match '"errorCode"\s*:\s*"([A-Za-z0-9_]{1,100})"') {
        $report.errorCode = $Matches[1]
    }
}
finally {
    $token = $null
    $headers = $null
}
$report.checkedAtUtc = [DateTime]::UtcNow.ToString('o')
Write-Output ('PRIVATE_EVENTHOUSE_CANARY:' + ($report | ConvertTo-Json -Depth 15 -Compress))
