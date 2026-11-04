[CmdletBinding()]
param(
    [string] $WorkspaceName = 'ES_EH_Spark_test',
    [string] $EventstreamName = 'esspark',
    [string] $VaultName = 'esehsparkkv'
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscriptionId = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$tenantId = '4526cee1-4399-4e4b-9134-46fabd583527'
$resourceGroup = 'rg_esehspark'
$root = Split-Path -Parent $PSScriptRoot
$metadataPath = Join-Path $root '.deployment\eventstream-connections.json'
$templatePath = Join-Path $root 'infra\eventstream-connection-secret.bicep'
$account = & az account show --subscription $subscriptionId --only-show-errors --output json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.id -ne $subscriptionId -or $account.tenantId -ne $tenantId) {
    throw 'Azure CLI context is not the approved MCAP tenant and subscription.'
}

$token = & az account get-access-token --resource 'https://api.fabric.microsoft.com' `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'eventstream-cli'
}

function Get-FabricPages {
    param([Parameter(Mandatory)][string] $Uri)
    $items = [System.Collections.Generic.List[object]]::new()
    $next = $Uri
    while ($next) {
        $page = Invoke-RestMethod -Method Get -Uri $next -Headers $headers -ErrorAction Stop
        foreach ($item in @($page.value)) { $items.Add($item) }
        $next = if ($page.continuationUri) { [string]$page.continuationUri } else { $null }
    }
    return @($items)
}

function Get-SecretResourceId {
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][string] $Vault
    )
    return "/subscriptions/$subscriptionId/resourceGroups/$resourceGroup/providers/Microsoft.KeyVault/vaults/$Vault/secrets/$Name"
}

function Test-SecretExists {
    param([Parameter(Mandatory)][string] $SecretResourceId)
    $result = & az resource show --ids $SecretResourceId --api-version '2024-11-01' `
        --query id -o tsv --only-show-errors 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -eq 0) { return ([string]$result -eq $SecretResourceId) }
    if (($result -join ' ') -match 'ResourceNotFound|NotFound|was not found') { return $false }
    throw 'Unable to determine whether the Key Vault secret already exists.'
}

function Set-SecretThroughArm {
    param(
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][string] $Value,
        [Parameter(Mandatory)][string] $Vault
    )
    $parameterPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-eventstream-$([guid]::NewGuid()).json"
    $parameters = @{
        '$schema' = 'https://schema.management.azure.com/schemas/2019-04-01/deploymentParameters.json#'
        contentVersion = '1.0.0.0'
        parameters = @{
            keyVaultName = @{ value = $Vault }
            secretName = @{ value = $Name }
            connectionString = @{ value = $Value }
        }
    }
    try {
        New-Item -ItemType File -Path $parameterPath -Force | Out-Null
        $acl = Get-Acl -LiteralPath $parameterPath
        $acl.SetAccessRuleProtection($true, $false)
        $identity = [Security.Principal.WindowsIdentity]::GetCurrent().Name
        $rule = [Security.AccessControl.FileSystemAccessRule]::new(
            $identity,
            'FullControl',
            'Allow'
        )
        $acl.SetAccessRule($rule)
        Set-Acl -LiteralPath $parameterPath -AclObject $acl
        [IO.File]::WriteAllText(
            $parameterPath,
            ($parameters | ConvertTo-Json -Depth 10),
            [Text.UTF8Encoding]::new($false)
        )

        & az deployment group create `
            --subscription $subscriptionId `
            --resource-group $resourceGroup `
            --name "esehspark-$Name" `
            --template-file $templatePath `
            --parameters "@$parameterPath" `
            --only-show-errors `
            --output none 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "Secure Key Vault deployment failed for secret '$Name'; secret content was not emitted."
        }
    }
    finally {
        $parameters = $null
        $Value = $null
        Remove-Item -LiteralPath $parameterPath -Force -ErrorAction SilentlyContinue
    }

    $verified = & az resource show --ids (Get-SecretResourceId -Name $Name -Vault $Vault) `
        --api-version '2024-11-01' --query id -o tsv --only-show-errors 2>$null
    if ($LASTEXITCODE -ne 0 -or [string]$verified -ne (Get-SecretResourceId -Name $Name -Vault $Vault)) {
        throw "Key Vault secret '$Name' was not found on ARM read-back."
    }
}

try {
    $workspaceMatches = @(Get-FabricPages -Uri 'https://api.fabric.microsoft.com/v1/workspaces' |
        Where-Object displayName -eq $WorkspaceName)
    if ($workspaceMatches.Count -ne 1) {
        throw "Expected one Fabric workspace named '$WorkspaceName'; found $($workspaceMatches.Count)."
    }
    $workspaceId = [string]$workspaceMatches[0].id
    $eventstreamMatches = @(Get-FabricPages `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/eventstreams" |
        Where-Object displayName -eq $EventstreamName)
    if ($eventstreamMatches.Count -ne 1) {
        throw "Expected one Eventstream named '$EventstreamName'; found $($eventstreamMatches.Count)."
    }
    $eventstreamId = [string]$eventstreamMatches[0].id
    $topology = Invoke-RestMethod -Method Get `
        -Uri "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/eventstreams/$eventstreamId/topology" `
        -Headers $headers -ErrorAction Stop

    $sourceMatches = @($topology.sources | Where-Object {
        $_.type -eq 'CustomEndpoint' -and $_.name -eq 'VmKafkaIngress'
    })
    $destinationMatches = @($topology.destinations | Where-Object {
        $_.type -eq 'CustomEndpoint' -and $_.name -eq 'HighTemperatureKafka'
    })
    if ($sourceMatches.Count -ne 1 -or $destinationMatches.Count -ne 1) {
        throw 'The approved Eventstream topology must contain exactly one named Custom Endpoint source and destination.'
    }

    $endpoints = @(
        @{
            node = $sourceMatches[0]
            secretName = 'eventstream-source-connection'
            apiPath = 'sources'
        }
        @{
            node = $destinationMatches[0]
            secretName = 'eventstream-destination-connection'
            apiPath = 'destinations'
        }
    )
    $metadata = [ordered]@{
        workspaceId = $workspaceId
        eventstreamId = $eventstreamId
        retrievedAtUtc = [DateTime]::UtcNow.ToString('o')
        source = $null
        destination = $null
    }

    foreach ($endpoint in $endpoints) {
        $connectionUri = "https://api.fabric.microsoft.com/v1/workspaces/$workspaceId/eventstreams/$eventstreamId/$($endpoint.apiPath)/$($endpoint.node.id)/connection"
        $connection = Invoke-RestMethod -Method Get -Uri $connectionUri -Headers $headers -ErrorAction Stop
        $connectionString = [string]$connection.accessKeys.primaryConnectionString
        $namespace = [string]$connection.fullyQualifiedNamespace
        $entityName = [string]$connection.eventHubName
        if ([string]::IsNullOrWhiteSpace($namespace) -or
            [string]::IsNullOrWhiteSpace($entityName) -or
            [string]::IsNullOrWhiteSpace($connectionString)) {
            throw "The Eventstream $($endpoint.apiPath) connection response was incomplete; no secret was stored."
        }
        if ($connectionString -notmatch [regex]::Escape("Endpoint=sb://$namespace/") -or
            $connectionString -notmatch [regex]::Escape("EntityPath=$entityName")) {
            throw "The Eventstream $($endpoint.apiPath) connection string did not match its discovered Kafka metadata."
        }

        $secretId = Get-SecretResourceId -Name $endpoint.secretName -Vault $VaultName
        if (-not (Test-SecretExists -SecretResourceId $secretId)) {
            Set-SecretThroughArm -Name $endpoint.secretName -Value $connectionString -Vault $VaultName
        }
        $entry = [ordered]@{
            name = $endpoint.node.name
            nodeId = [string]$endpoint.node.id
            fullyQualifiedNamespace = $namespace
            eventHubName = $entityName
            bootstrapServers = "${namespace}:9093"
            secretName = $endpoint.secretName
            secretStored = $true
        }
        if ($endpoint.apiPath -eq 'sources') { $metadata.source = $entry }
        else { $metadata.destination = $entry }
        $connectionString = $null
        $connection = $null
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $metadataPath) | Out-Null
    [IO.File]::WriteAllText(
        $metadataPath,
        ($metadata | ConvertTo-Json -Depth 10),
        [Text.UTF8Encoding]::new($false)
    )
    Write-Output "Eventstream connection metadata saved to $metadataPath. Source=$($metadata.source.fullyQualifiedNamespace)/$($metadata.source.eventHubName); destination=$($metadata.destination.fullyQualifiedNamespace)/$($metadata.destination.eventHubName). Credentials were stored only in Key Vault."
}
finally {
    $headers = $null
    $token = $null
    $connectionString = $null
    $connection = $null
}
