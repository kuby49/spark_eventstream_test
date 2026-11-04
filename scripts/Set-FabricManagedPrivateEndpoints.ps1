[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json')
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$root = Split-Path -Parent $PSScriptRoot
$statePath = Join-Path $root '.deployment\fabric-managed-private-endpoints.json'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json

function Invoke-AzJson {
    param([Parameter(Mandatory)][string[]] $Arguments)

    $output = & az @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "Azure CLI command failed: az $($Arguments -join ' ')`n$($output -join "`n")"
    }
    return (($output -join "`n") | ConvertFrom-Json)
}

$account = Invoke-AzJson -Arguments @(
    'account', 'show', '--subscription', $subscriptionId, '--only-show-errors', '--output', 'json'
)
if ($account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

$workspaceResponse = Invoke-AzJson -Arguments @(
    'rest', '--method', 'get', '--resource', 'https://api.fabric.microsoft.com',
    '--url', "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)",
    '--headers', 'x-ms-fabric-skill=spark-cli', '--only-show-errors', '--output', 'json'
)
if ($workspaceResponse.id -ne $inventory.workspaceId -or
    $workspaceResponse.capacityId -ne $inventory.capacityId) {
    throw 'The discovered Fabric workspace is not assigned to the approved capacity.'
}

$capacities = Invoke-AzJson -Arguments @(
    'rest', '--method', 'get', '--resource', 'https://api.fabric.microsoft.com',
    '--url', 'https://api.fabric.microsoft.com/v1/capacities',
    '--headers', 'x-ms-fabric-skill=spark-cli', '--only-show-errors', '--output', 'json'
)
$capacity = @($capacities.value | Where-Object id -eq $inventory.capacityId)
if ($capacity.Count -ne 1 -or $capacity[0].sku -ne 'F8' -or $capacity[0].state -ne 'Active') {
    throw 'The approved Fabric capacity must be the active F8 before creating Spark routes.'
}

$functionApp = Invoke-AzJson -Arguments @(
    'functionapp', 'show', '--resource-group', $resourceGroup, '--name', 'esehspark-score-api',
    '--subscription', $subscriptionId, '--query', '{id:id,host:defaultHostName}',
    '--only-show-errors', '--output', 'json'
)
$vault = Invoke-AzJson -Arguments @(
    'keyvault', 'show', '--resource-group', $resourceGroup, '--name', 'esehsparkkv',
    '--subscription', $subscriptionId, '--query', '{id:id,uri:properties.vaultUri}',
    '--only-show-errors', '--output', 'json'
)
if (-not $functionApp.id -or -not $functionApp.host -or -not $vault.id -or -not $vault.uri) {
    throw 'Azure did not return the exact Function App and Key Vault resource metadata.'
}

function Get-TargetSubresource {
    param(
        [Parameter(Mandatory)][string] $ResourceId,
        [Parameter(Mandatory)][string] $ExpectedGroupId
    )

    $resources = Invoke-AzJson -Arguments @(
        'network', 'private-link-resource', 'list', '--id', $ResourceId,
        '--subscription', $subscriptionId, '--only-show-errors', '--output', 'json'
    )
    $matches = @($resources | Where-Object { $_.properties.groupId -eq $ExpectedGroupId })
    if ($matches.Count -ne 1) {
        throw "Azure did not return exactly one '$ExpectedGroupId' private-link subresource for '$ResourceId'."
    }
    return [string]$matches[0].properties.groupId
}

$functionGroupId = Get-TargetSubresource -ResourceId $functionApp.id -ExpectedGroupId 'sites'
$vaultGroupId = Get-TargetSubresource -ResourceId $vault.id -ExpectedGroupId 'vault'
$functionHost = ([Uri]::new("https://$($functionApp.host)")).Host
$vaultHost = ([Uri]::new([string]$vault.uri)).Host
$targets = @(
    [ordered]@{
        name = 'mpe-esehspark-score-api'
        resourceId = [string]$functionApp.id
        subresource = $functionGroupId
        fqdn = $functionHost
    },
    [ordered]@{
        name = 'mpe-esehsparkkv'
        resourceId = [string]$vault.id
        subresource = $vaultGroupId
        fqdn = $vaultHost
    }
)

$fabricToken = & az account get-access-token --resource 'https://api.fabric.microsoft.com' `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($fabricToken)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $fabricToken"
    'x-ms-fabric-skill' = 'spark-cli'
}

function Invoke-FabricRequest {
    param(
        [Parameter(Mandatory)][ValidateSet('Get', 'Post')][string] $Method,
        [Parameter(Mandatory)][string] $Uri,
        [object] $Body
    )

    $request = @{
        Method = $Method
        Uri = $Uri
        Headers = $headers
        ErrorAction = 'Stop'
    }
    if ($PSBoundParameters.ContainsKey('Body')) {
        $request.ContentType = 'application/json'
        $request.Body = $Body | ConvertTo-Json -Depth 20 -Compress
    }
    return Invoke-WebRequest @request
}

function Get-FabricManagedPrivateEndpoints {
    $all = [System.Collections.Generic.List[object]]::new()
    $next = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/managedPrivateEndpoints"
    while ($next) {
        $response = Invoke-FabricRequest -Method Get -Uri $next
        $page = $response.Content | ConvertFrom-Json
        foreach ($endpoint in @($page.value)) {
            $all.Add($endpoint)
        }
        if ($page.continuationUri) {
            $next = [string]$page.continuationUri
        }
        elseif ($page.continuationToken) {
            $next = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/managedPrivateEndpoints?continuationToken=$([Uri]::EscapeDataString([string]$page.continuationToken))"
        }
        else {
            $next = $null
        }
    }
    return @($all)
}

function Get-PrivateEndpointConnections {
    param([Parameter(Mandatory)][string] $ResourceId)
    return @(Invoke-AzJson -Arguments @(
        'network', 'private-endpoint-connection', 'list', '--id', $ResourceId,
        '--subscription', $subscriptionId, '--only-show-errors', '--output', 'json'
    ))
}

function Get-ManagedPrivateEndpoint {
    param([Parameter(Mandatory)][string] $EndpointId)
    $response = Invoke-FabricRequest -Method Get `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/managedPrivateEndpoints/$EndpointId"
    return $response.Content | ConvertFrom-Json
}

function Ensure-ApprovedManagedPrivateEndpoint {
    param(
        [Parameter(Mandatory)][System.Collections.IDictionary] $Target,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]] $ExistingEndpoints
    )

    $matches = @($ExistingEndpoints | Where-Object name -eq $Target.name)
    if ($matches.Count -gt 1) {
        throw "More than one Fabric managed private endpoint is named '$($Target.name)'."
    }
    if ($matches.Count -eq 1) {
        $endpoint = $matches[0]
        if ($endpoint.targetPrivateLinkResourceId -ne $Target.resourceId -or
            $endpoint.targetSubresourceType -ne $Target.subresource) {
            throw "Existing managed private endpoint '$($Target.name)' targets a different Azure resource; refusing replacement."
        }
    }
    else {
        $baselineIds = @(
            Get-PrivateEndpointConnections -ResourceId $Target.resourceId |
            ForEach-Object id
        )
        $body = @{
            name = $Target.name
            targetPrivateLinkResourceId = $Target.resourceId
            targetSubresourceType = $Target.subresource
            targetFQDNs = @($Target.fqdn)
            requestMessage = "Fabric Spark route for $($Target.name) in $($inventory.workspaceName)."
        }
        try {
            $createdResponse = Invoke-FabricRequest -Method Post `
                -Uri "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)/managedPrivateEndpoints" `
                -Body $body
            if ($createdResponse.StatusCode -ne 201) {
                throw "Create managed private endpoint returned HTTP $($createdResponse.StatusCode), expected 201."
            }
            $endpoint = $createdResponse.Content | ConvertFrom-Json
        }
        catch {
            $existingAfterError = @(Get-FabricManagedPrivateEndpoints |
                Where-Object name -eq $Target.name)
            if ($existingAfterError.Count -ne 1) {
                throw "Creating managed private endpoint '$($Target.name)' failed: $($_.Exception.Message)"
            }
            $endpoint = $existingAfterError[0]
        }
        if (-not $endpoint.id) {
            $endpoint = @(Get-FabricManagedPrivateEndpoints |
                Where-Object name -eq $Target.name | Select-Object -First 1)[0]
        }
        if (-not $endpoint.id) {
            throw "The create response for '$($Target.name)' did not include a readable endpoint ID."
        }
        $endpoint | Add-Member -NotePropertyName _baselineConnectionIds `
            -NotePropertyValue $baselineIds -Force
    }

    $baselineIds = if ($endpoint.PSObject.Properties['_baselineConnectionIds']) {
        @($endpoint._baselineConnectionIds)
    }
    else {
        $null
    }
    $deadline = [DateTime]::UtcNow.AddMinutes(20)
    $approvalIssued = $false
    do {
        $endpoint = Get-ManagedPrivateEndpoint -EndpointId ([string]$endpoint.id)
        if ($endpoint.provisioningState -eq 'Failed') {
            throw "Fabric managed private endpoint '$($Target.name)' provisioning failed."
        }
        if ($endpoint.connectionState.status -eq 'Rejected') {
            throw "Fabric managed private endpoint '$($Target.name)' was rejected by the target resource."
        }
        if ($endpoint.provisioningState -eq 'Succeeded' -and
            $endpoint.connectionState.status -eq 'Approved') {
            return $endpoint
        }
        if (-not $approvalIssued) {
            $connections = Get-PrivateEndpointConnections -ResourceId $Target.resourceId
            $pending = @($connections | Where-Object {
                $_.properties.privateLinkServiceConnectionState.status -eq 'Pending'
            })
            if ($baselineIds) {
                $candidates = @($pending | Where-Object { $_.id -notin $baselineIds })
            }
            else {
                $targetEndpoints = @($ExistingEndpoints | Where-Object {
                    $_.targetPrivateLinkResourceId -eq $Target.resourceId
                })
                if ($targetEndpoints.Count -gt 1 -or $pending.Count -gt 1) {
                    throw "Cannot safely map a pending target connection to '$($Target.name)'; refusing an ambiguous approval."
                }
                $candidates = $pending
            }
            if ($candidates.Count -gt 1) {
                throw "Multiple new pending connections appeared for '$($Target.name)'; refusing an ambiguous approval."
            }
            if ($candidates.Count -eq 1) {
                $approval = & az network private-endpoint-connection approve `
                    --id $candidates[0].id `
                    --description "Approved for the $($inventory.workspaceName) Fabric Spark test." `
                    --subscription $subscriptionId `
                    --only-show-errors --output none 2>&1
                if ($LASTEXITCODE -ne 0) {
                    throw "Azure rejected approval of the discovered '$($Target.name)' connection: $($approval -join "`n")"
                }
                $approvalIssued = $true
            }
        }
        if ([DateTime]::UtcNow -ge $deadline) {
            throw "Timed out waiting for '$($Target.name)' to reach provisioning Succeeded and connection Approved."
        }
        Start-Sleep -Seconds 5
    } while ($true)
}

$existingEndpoints = @(Get-FabricManagedPrivateEndpoints)
$results = [System.Collections.Generic.List[object]]::new()
foreach ($target in $targets) {
    $endpoint = Ensure-ApprovedManagedPrivateEndpoint `
        -Target $target -ExistingEndpoints $existingEndpoints
    $existingEndpoints = @($existingEndpoints | Where-Object name -ne $target.name) + @($endpoint)
    $results.Add([pscustomobject]@{
        name = $endpoint.name
        id = $endpoint.id
        targetPrivateLinkResourceId = $endpoint.targetPrivateLinkResourceId
        targetSubresourceType = $endpoint.targetSubresourceType
        targetFqdn = $target.fqdn
        provisioningState = $endpoint.provisioningState
        connectionStatus = $endpoint.connectionState.status
    })
}

$state = [ordered]@{
    workspaceId = $inventory.workspaceId
    capturedAtUtc = [DateTime]::UtcNow.ToString('o')
    endpoints = @($results)
}
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $statePath) | Out-Null
[IO.File]::WriteAllText(
    $statePath,
    ($state | ConvertTo-Json -Depth 20),
    [Text.UTF8Encoding]::new($false)
)
$headers = $null
$fabricToken = $null
Write-Output "Fabric Spark managed private endpoints are approved. Redacted inventory: $statePath"
