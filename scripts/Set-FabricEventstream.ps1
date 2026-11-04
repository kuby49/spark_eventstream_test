[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json'),
    [switch] $RequireComponentState
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
$root = Split-Path -Parent $PSScriptRoot
. (Join-Path $PSScriptRoot 'FabricEvidence.ps1')
. (Join-Path $PSScriptRoot 'EventstreamLifecycle.ps1')
$evidenceDirectory = Join-Path $root '.deployment'
New-Item -ItemType Directory -Force -Path $evidenceDirectory | Out-Null
$templatePath = Join-Path $root 'fabric\eventstream\eventstream-definition.template.json'
$topology = Get-Content -LiteralPath $templatePath -Raw
$topology = $topology.Replace('{{WORKSPACE_ID}}', [string]$inventory.workspaceId)
$topology = $topology.Replace('{{KQL_DATABASE_ID}}', [string]$inventory.kqlDatabaseId)
$topologyObject = $topology | ConvertFrom-Json
if (@($topologyObject.streams | Where-Object type -eq 'DefaultStream').Count -ne 1) {
    throw 'Eventstream topology must contain exactly one DefaultStream.'
}

$topologyBytes = [Text.Encoding]::UTF8.GetBytes(($topologyObject | ConvertTo-Json -Depth 30 -Compress))
$propertiesBytes = [Text.Encoding]::UTF8.GetBytes(
    (@{ retentionTimeInDays = 1; eventThroughputLevel = 'Low' } | ConvertTo-Json -Compress)
)
$body = @{
    definition = @{
        parts = @(
            @{
                path = 'eventstream.json'
                payload = [Convert]::ToBase64String($topologyBytes)
                payloadType = 'InlineBase64'
            },
            @{
                path = 'eventstreamProperties.json'
                payload = [Convert]::ToBase64String($propertiesBytes)
                payloadType = 'InlineBase64'
            }
        )
    }
} | ConvertTo-Json -Depth 10 -Compress

$token = & az account get-access-token --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'eventstream-cli'
}
$uri = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/eventstreams/$($inventory.eventstreamId)/updateDefinition"
$response = Invoke-WebRequest -Method Post -Uri $uri -Headers $headers `
    -ContentType 'application/json' -Body $body -ErrorAction Stop
if ($response.StatusCode -eq 202) {
    $operationUrl = $response.Headers['Location']
    if (-not $operationUrl) { throw 'Eventstream definition update returned 202 without a Location header.' }
    $deadline = [DateTime]::UtcNow.AddMinutes(30)
    do {
        $retryAfter = 5
        [int]::TryParse([string]$response.Headers['Retry-After'], [ref]$retryAfter) | Out-Null
        Start-Sleep -Seconds ([Math]::Max(1, $retryAfter))
        $poll = Invoke-WebRequest -Method Get -Uri $operationUrl -Headers $headers -ErrorAction Stop
        $status = $poll.Content | ConvertFrom-Json
        $response = $poll
        if ($status.status -in @('Failed', 'Cancelled')) {
            throw "Eventstream update operation failed with status '$($status.status)'."
        }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Timed out waiting for Eventstream definition update.' }
    } while ($status.status -ne 'Succeeded')
}

$readUri = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/eventstreams/$($inventory.eventstreamId)/getDefinition"
$definitionResponse = Invoke-WebRequest -Method Post -Uri $readUri -Headers $headers `
    -ContentType 'application/json' -Body '{}' -ErrorAction Stop
if ($definitionResponse.StatusCode -eq 202) {
    $operationUrl = $definitionResponse.Headers['Location']
    if (-not $operationUrl) { throw 'Eventstream getDefinition returned 202 without a Location header.' }
    $deadline = [DateTime]::UtcNow.AddMinutes(30)
    do {
        $retryAfter = 5
        [int]::TryParse([string]$definitionResponse.Headers['Retry-After'], [ref]$retryAfter) | Out-Null
        Start-Sleep -Seconds ([Math]::Max(1, $retryAfter))
        $poll = Invoke-WebRequest -Method Get -Uri $operationUrl -Headers $headers -ErrorAction Stop
        $status = $poll.Content | ConvertFrom-Json
        $definitionResponse = $poll
        if ($status.status -in @('Failed', 'Cancelled')) {
            throw "Eventstream getDefinition operation failed with status '$($status.status)'."
        }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Timed out waiting for Eventstream getDefinition.' }
    } while ($status.status -ne 'Succeeded')
    $definitionResponse = Invoke-WebRequest -Method Get -Uri "$operationUrl/result" -Headers $headers -ErrorAction Stop
}
$definition = $definitionResponse.Content | ConvertFrom-Json
Save-FabricEvidence -Path (Join-Path $evidenceDirectory 'eventstream-deployed-definition.json') -Value @{
    capturedAtUtc=[DateTime]::UtcNow.ToString('o');workspaceId=$inventory.workspaceId;
    eventstreamId=$inventory.eventstreamId;definition=(Convert-FabricDefinitionEvidence $definition)
}
$part = $definition.definition.parts | Where-Object path -eq 'eventstream.json' | Select-Object -First 1
if (-not $part) { throw 'Eventstream definition read-back did not include eventstream.json.' }
$savedTopology = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String($part.payload)) | ConvertFrom-Json
Assert-FabricEventstreamGraph -Actual $savedTopology -Expected $topologyObject
$topologyUri = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/eventstreams/$($inventory.eventstreamId)/topology"
try {
    $liveTopology = Invoke-RestMethod -Method Get -Uri $topologyUri -Headers $headers -ErrorAction Stop
}
catch {
    Save-FabricEvidence -Path (Join-Path $evidenceDirectory 'eventstream-component-state.json') -Value @{
        capturedAtUtc=[DateTime]::UtcNow.ToString('o');available=$false;
        errorType=$_.Exception.GetType().Name;historicalFixtureStateReconstructed=$false;
        documentedApi='GET /v1/workspaces/{workspaceId}/eventstreams/{eventstreamId}/topology'
    }
    throw 'Documented Eventstream topology/state read failed; no alternate or unsupported state API was attempted.'
}
Save-FabricEvidence -Path (Join-Path $evidenceDirectory 'eventstream-deployed-topology.json') -Value @{
    capturedAtUtc=[DateTime]::UtcNow.ToString('o');workspaceId=$inventory.workspaceId;
    eventstreamId=$inventory.eventstreamId;topology=$liveTopology
}
Save-FabricEvidence -Path (Join-Path $evidenceDirectory 'eventstream-component-state.json') `
    -Value @{
        capturedAtUtc=[DateTime]::UtcNow.ToString('o');workspaceId=$inventory.workspaceId;
        eventstreamId=$inventory.eventstreamId;state=(Get-FabricComponentStateEvidence $liveTopology)
    }
Assert-FabricEventstreamGraph -Actual $liveTopology -Expected $topologyObject
$running=Ensure-EventstreamProcessingRunning -ReadTopology {
    Invoke-RestMethod -Method Get -Uri $topologyUri -Headers $headers -ErrorAction Stop
} -Resume {
    param($resumeBody)
    Invoke-RestMethod -Method Post -Uri ($topologyUri -replace '/topology$','/resume') `
        -Headers $headers -ContentType 'application/json' -Body ($resumeBody|ConvertTo-Json -Compress) -ErrorAction Stop
} -ResumeDestination {
    param($id,$resumeBody)
    $resumeUri=($topologyUri -replace '/topology$',"/destinations/$id/resume")
    Invoke-RestMethod -Method Post -Uri $resumeUri -Headers $headers -ContentType 'application/json' `
        -Body ($resumeBody|ConvertTo-Json -Compress) -ErrorAction Stop
}
Assert-FabricEventstreamGraph -Actual $running.topology -Expected $topologyObject
Save-FabricEvidence -Path (Join-Path $evidenceDirectory 'eventstream-running-readback.json') -Value $running
if ($RequireComponentState) { $null = Get-FabricComponentStateEvidence $running.topology -RequireState }
$token = $null
$headers = $null
Write-Output "Eventstream topology persisted and read back for item ID $($inventory.eventstreamId)."
