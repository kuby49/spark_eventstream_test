[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [Parameter(Mandatory)] [guid] $LakehouseId,
    [Parameter(Mandatory)] [string] $KeyVaultUrl,
    [Parameter(Mandatory)] [string] $KustoEndpoint,
    [Parameter(Mandatory)] [string] $FunctionUrl,
    [Parameter(Mandatory)] [guid] $RunId,
    [ValidateSet('streaming', 'private-canary')] [string] $RunMode = 'streaming'
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$subscription = '0a0cf761-9d5d-4862-9165-7b69c188a0ee'
$account = & az account show --subscription $subscription --only-show-errors -o json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or $account.tenantId -ne '4526cee1-4399-4e4b-9134-46fabd583527' -or
    $account.user.name -ne 'admin@MngEnvMCAP885977.onmicrosoft.com') { throw 'Incorrect MCAP identity.' }
$root = Split-Path -Parent $PSScriptRoot
$output = Join-Path $root '.deployment\streaming_pipeline.ipynb'
& (Join-Path $PSScriptRoot 'Build-FabricNotebook.ps1') `
    -WorkspaceId $WorkspaceId `
    -LakehouseId $LakehouseId `
    -KeyVaultUrl $KeyVaultUrl `
    -KustoEndpoint $KustoEndpoint `
    -FunctionUrl $FunctionUrl `
    -RunId $RunId `
    -RunMode $RunMode `
    -OutputPath $output

$accessToken = (& az account get-access-token --subscription $subscription --resource 'https://api.fabric.microsoft.com' --query accessToken -o tsv --only-show-errors)
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($accessToken)) {
    throw 'Could not obtain a Fabric API token from the authenticated Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $accessToken"
    'x-ms-fabric-skill' = 'spark-cli'
}
$base = "https://api.fabric.microsoft.com/v1/workspaces/$WorkspaceId"
$itemsUrl = "$base/items?type=Notebook"
$items = Invoke-RestMethod -Method Get -Uri $itemsUrl -Headers $headers
$notebook = $items.value | Where-Object displayName -eq 'esehnotebookspark' | Select-Object -First 1
$definition = [Convert]::ToBase64String([IO.File]::ReadAllBytes($output))
$createBody = @{
    displayName = 'esehnotebookspark'
    type = 'Notebook'
    description = 'Continuous Eventstream custom-endpoint Kafka consumer and per-event Eventhouse scoring.'
} | ConvertTo-Json -Depth 10 -Compress
if (-not $notebook) {
    $created = Invoke-WebRequest -Method Post -Uri "$base/items" -Headers $headers `
        -ContentType 'application/json' -Body $createBody
    $responseHeaders = $created.Headers
    if ($created.StatusCode -eq 202) {
        $operationUrl = [string](@($responseHeaders['Location'])[0])
        if (-not $operationUrl) { throw 'Fabric notebook creation returned 202 without an LRO Location header.' }
        $deadline = [DateTime]::UtcNow.AddMinutes(20)
        do {
            $waitSeconds = 5
            [int]::TryParse([string](@($responseHeaders['Retry-After'])[0]), [ref]$waitSeconds) | Out-Null
            Start-Sleep -Seconds ([Math]::Max(1, $waitSeconds))
            $statusResponse = Invoke-WebRequest -Method Get -Uri $operationUrl -Headers $headers
            $responseHeaders = $statusResponse.Headers
            $status = $statusResponse.Content | ConvertFrom-Json
            if ($status.status -in @('Failed', 'Cancelled')) {
                throw "Fabric notebook creation failed with status '$($status.status)'."
            }
            if ([DateTime]::UtcNow -gt $deadline) { throw 'Timed out waiting for Fabric notebook creation.' }
        } while ($status.status -ne 'Succeeded')
    }
    $items = Invoke-RestMethod -Method Get -Uri $itemsUrl -Headers $headers
    $notebook = $items.value | Where-Object displayName -eq 'esehnotebookspark' | Select-Object -First 1
    if (-not $notebook) { throw 'Fabric notebook creation completed without the fixed-name item appearing in read-back.' }
}
$body = @{
    definition = @{
        format = 'ipynb'
        parts = @(@{
            path = 'notebook-content.ipynb'
            payload = $definition
            payloadType = 'InlineBase64'
        })
    }
} | ConvertTo-Json -Depth 20 -Compress
$writeUrl = "$base/notebooks/$($notebook.id)/updateDefinition"
$response = Invoke-WebRequest -Method Post -Uri $writeUrl -Headers $headers `
    -ContentType 'application/json' -Body $body
$responseHeaders = $response.Headers
if ($response.StatusCode -eq 202) {
    $operationUrl = [string](@($responseHeaders['Location'])[0])
    if (-not $operationUrl) { throw 'Fabric accepted notebook write without an LRO Location header.' }
    $deadline = [DateTime]::UtcNow.AddMinutes(20)
    do {
        $waitSeconds = 5
        $retryAfter = [string](@($responseHeaders['Retry-After'])[0])
        if ($retryAfter) {
            [int]::TryParse($retryAfter, [ref]$waitSeconds) | Out-Null
        }
        Start-Sleep -Seconds ([Math]::Max(1, $waitSeconds))
        $statusResponse = Invoke-WebRequest -Method Get -Uri $operationUrl -Headers $headers
        $responseHeaders = $statusResponse.Headers
        $status = $statusResponse.Content | ConvertFrom-Json
        if ($status.status -in @('Failed', 'Cancelled')) {
            throw "Fabric notebook operation finished with status '$($status.status)'."
        }
        if ([DateTime]::UtcNow -gt $deadline) { throw 'Timed out waiting for Fabric notebook operation.' }
    } while ($status.status -ne 'Succeeded')
}
$items = Invoke-RestMethod -Method Get -Uri $itemsUrl -Headers $headers
$notebook = $items.value | Where-Object displayName -eq 'esehnotebookspark' | Select-Object -First 1
if (-not $notebook) { throw 'Fabric notebook write succeeded but the fixed-name item was not found on read-back.' }
Write-Output "Notebook item ID: $($notebook.id)"
