[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$workspaceName = 'ES_EH_Spark_test'
$capacityName = 'fabricessparktest8'
$inventoryPath = Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json'

$token = & az account get-access-token --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'eventstream-cli'
}

function Invoke-FabricRequest {
    param(
        [Parameter(Mandatory)] [ValidateSet('Get', 'Post')] [string] $Method,
        [Parameter(Mandatory)] [string] $Uri,
        [object] $Body
    )

    $requestUri = [Uri]::new([string]$Uri)
    $request = @{
        Method = $Method
        Uri = $requestUri
        Headers = $headers
        ErrorAction = 'Stop'
    }
    if ($PSBoundParameters.ContainsKey('Body')) {
        $request.ContentType = 'application/json'
        $request.Body = $Body | ConvertTo-Json -Depth 30 -Compress
    }
    try {
        $response = Invoke-WebRequest @request
    }
    catch {
        throw "Fabric request failed ($Method $Uri): $($_.Exception.Message)"
    }
    if ($response.StatusCode -eq 202) {
        $operationUrl = $response.Headers['Location']
        if (-not $operationUrl) {
            throw "Fabric accepted $Method $Uri without an LRO Location header."
        }
        $deadline = [DateTime]::UtcNow.AddMinutes(30)
        do {
            $retryAfter = 5
            [int]::TryParse([string]$response.Headers['Retry-After'], [ref]$retryAfter) | Out-Null
            Start-Sleep -Seconds ([Math]::Max(1, $retryAfter))
            $response = Invoke-WebRequest -Method Get -Uri $operationUrl -Headers $headers -ErrorAction Stop
            $operation = $response.Content | ConvertFrom-Json
            if ($operation.status -in @('Failed', 'Cancelled')) {
                throw "Fabric operation failed with status '$($operation.status)'."
            }
            if ([DateTime]::UtcNow -gt $deadline) {
                throw "Timed out waiting for Fabric operation from $Uri."
            }
        } while ($operation.status -ne 'Succeeded')
        return $operation
    }
    if ([string]::IsNullOrWhiteSpace($response.Content)) { return $null }
    return $response.Content | ConvertFrom-Json
}

function Get-FabricPages {
    param([Parameter(Mandatory)] [string] $Uri)
    $items = [System.Collections.Generic.List[object]]::new()
    $next = $Uri
    while ($next) {
        $page = Invoke-FabricRequest -Method Get -Uri $next
        foreach ($item in @($page.value)) { $items.Add($item) }
        if ($page.continuationUri) {
            $next = [string]$page.continuationUri
        }
        elseif ($page.continuationToken) {
            $separator = if ($Uri.Contains('?')) { '&' } else { '?' }
            $next = "$Uri$separator" + 'continuationToken=' + [Uri]::EscapeDataString([string]$page.continuationToken)
        }
        else {
            $next = $null
        }
    }
    return @($items)
}

function Find-ExactItem {
    param(
        [Parameter(Mandatory)] [AllowEmptyCollection()] [AllowNull()] [object[]] $Items,
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string] $Kind
    )
    $matches = @($Items | Where-Object { $_.displayName -ieq $Name })
    if ($matches.Count -gt 1) { throw "More than one $Kind has the fixed name '$Name'." }
    if ($matches.Count -eq 1 -and $matches[0].displayName -cne $Name) {
        throw "A $Kind exists with a case-variant of the fixed name '$Name'; refusing to create or rename it."
    }
    return $matches | Select-Object -First 1
}

function Ensure-FabricItem {
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string] $ListPath,
        [Parameter(Mandatory)] [string] $CreatePath,
        [Parameter(Mandatory)] [hashtable] $CreateBody,
        [Parameter(Mandatory)] [string] $Kind,
        [Parameter(Mandatory)] [string] $Skill
    )
    $headers['x-ms-fabric-skill'] = $Skill
    $items = Get-FabricPages -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/$ListPath"
    $item = Find-ExactItem -Items $items -Name $Name -Kind $Kind
    if (-not $item) {
        Invoke-FabricRequest -Method Post `
            -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/$CreatePath" `
            -Body $CreateBody | Out-Null
        $items = Get-FabricPages -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/$ListPath"
        $item = Find-ExactItem -Items $items -Name $Name -Kind $Kind
        if (-not $item) { throw "$Kind creation returned without the fixed-name item appearing in list read-back." }
    }
    return $item
}

$capacities = Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/capacities'
$capacity = Find-ExactItem -Items $capacities -Name $capacityName -Kind 'Fabric capacity'
if (-not $capacity) { throw "The fixed Fabric capacity '$capacityName' is not visible in Fabric REST." }
if ($capacity.sku -ne 'F8' -or $capacity.state -ne 'Active') {
    throw "The fixed Fabric capacity is not active F8 (observed SKU '$($capacity.sku)', state '$($capacity.state)')."
}

$workspaces = Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/workspaces'
$workspace = Find-ExactItem -Items $workspaces -Name $workspaceName -Kind 'workspace'
if (-not $workspace) {
    Invoke-FabricRequest -Method Post -Uri 'https://api.fabric.microsoft.com/v1/workspaces' -Body @{
        displayName = $workspaceName
        description = 'Private Eventstream custom-endpoint and Spark acceptance test workspace.'
        capacityId = $capacity.id
    } | Out-Null
    $workspaces = Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/workspaces'
    $workspace = Find-ExactItem -Items $workspaces -Name $workspaceName -Kind 'workspace'
    if (-not $workspace) { throw 'Workspace creation returned without a fixed-name workspace in list read-back.' }
}
if ($workspace.capacityId -ne $capacity.id) {
    throw "The existing fixed-name workspace is assigned to another capacity; refusing implicit reassignment."
}
$workspaceId = [string]$workspace.id

$eventhouse = Ensure-FabricItem -Name 'ehspark' -ListPath 'eventhouses' -CreatePath 'eventhouses' `
    -CreateBody @{ displayName = 'ehspark'; description = 'Raw Eventstream ingestion and event-time query database.' } `
    -Kind 'Eventhouse' -Skill 'eventhouse-cli'
$database = Ensure-FabricItem -Name 'ehspark' -ListPath 'kqlDatabases' -CreatePath 'kqlDatabases' `
    -CreateBody @{
        displayName = 'ehspark'
        creationPayload = @{
            databaseType = 'ReadWrite'
            parentEventhouseItemId = $eventhouse.id
        }
    } -Kind 'KQL database' -Skill 'eventhouse-cli'
$lakehouse = Ensure-FabricItem -Name 'lhspark' -ListPath 'items?type=Lakehouse' -CreatePath 'items' `
    -CreateBody @{ displayName = 'lhspark'; type = 'Lakehouse' } -Kind 'Lakehouse' -Skill 'spark-cli'
$notebook = Ensure-FabricItem -Name 'esehnotebookspark' -ListPath 'items?type=Notebook' -CreatePath 'items' `
    -CreateBody @{ displayName = 'esehnotebookspark'; type = 'Notebook' } -Kind 'Notebook' -Skill 'spark-cli'
$eventstream = Ensure-FabricItem -Name 'esspark' -ListPath 'eventstreams' -CreatePath 'eventstreams' `
    -CreateBody @{ displayName = 'esspark'; description = 'Private generator ingress and filtered Kafka destination.' } `
    -Kind 'Eventstream' -Skill 'eventstream-cli'

$headers['x-ms-fabric-skill'] = 'eventhouse-cli'
$databaseDetails = Invoke-FabricRequest -Method Get `
    -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/kqlDatabases/$($database.id)"
if ($databaseDetails.properties.parentEventhouseItemId -and
    $databaseDetails.properties.parentEventhouseItemId -ne $eventhouse.id) {
    throw 'The fixed-name KQL database belongs to a different Eventhouse.'
}
if (-not $databaseDetails.properties.queryServiceUri) {
    throw 'KQL database read-back did not include its query service URI.'
}

$inventory = [ordered]@{
    workspaceName = $workspaceName
    workspaceId = $workspaceId
    capacityName = $capacityName
    capacityId = $capacity.id
    eventhouseName = 'ehspark'
    eventhouseId = $eventhouse.id
    kqlDatabaseName = 'ehspark'
    kqlDatabaseId = $database.id
    kustoQueryEndpoint = $databaseDetails.properties.queryServiceUri
    lakehouseName = 'lhspark'
    lakehouseId = $lakehouse.id
    notebookName = 'esehnotebookspark'
    notebookId = $notebook.id
    eventstreamName = 'esspark'
    eventstreamId = $eventstream.id
    createdAtUtc = [DateTime]::UtcNow.ToString('o')
}
$parent = Split-Path -Parent $inventoryPath
New-Item -ItemType Directory -Force -Path $parent | Out-Null
[IO.File]::WriteAllText(
    $inventoryPath,
    ($inventory | ConvertTo-Json -Depth 20),
    [Text.UTF8Encoding]::new($false)
)
Write-Output "Fabric workspace and item inventory recorded (IDs only; no secrets): $inventoryPath"
