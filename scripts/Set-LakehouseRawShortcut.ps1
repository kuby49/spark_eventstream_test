[CmdletBinding()]
param(
    [string] $InventoryPath = (Join-Path (Split-Path -Parent $PSScriptRoot) '.deployment\fabric-inventory.json')
)

$ErrorActionPreference = 'Stop'
$env:AZURE_CONFIG_DIR = Join-Path $HOME '.azure-mcap'
$inventory = Get-Content -LiteralPath $InventoryPath -Raw | ConvertFrom-Json
foreach ($field in @('workspaceId', 'lakehouseId', 'kqlDatabaseId')) {
    try { $null = [guid]::Parse([string]$inventory.$field) }
    catch {
        throw "Fabric inventory is missing a valid $field."
    }
}

$token = & az account get-access-token --resource 'https://api.fabric.microsoft.com' `
    --query accessToken -o tsv --only-show-errors
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($token)) {
    throw 'Unable to acquire the Fabric API token using the approved Azure CLI context.'
}
$headers = @{
    Authorization = "Bearer $token"
    'x-ms-fabric-skill' = 'spark-cli'
}
$base = "https://api.fabric.microsoft.com/v1/workspaces/$($inventory.workspaceId)"
$shortcutUri = "$base/items/$($inventory.lakehouseId)/shortcuts"

function Get-ShortcutPages {
    param([Parameter(Mandatory)][string] $Uri)
    $results = [System.Collections.Generic.List[object]]::new()
    $next = $Uri
    while ($next) {
        $page = Invoke-RestMethod -Method Get -Uri $next -Headers $headers -ErrorAction Stop
        foreach ($shortcut in @($page.value)) { $results.Add($shortcut) }
        if ($page.continuationUri) {
            $next = [string]$page.continuationUri
        }
        elseif ($page.continuationToken) {
            $separator = if ($Uri.Contains('?')) { '&' } else { '?' }
            $next = "$Uri$separator" + 'continuationToken=' +
                [Uri]::EscapeDataString([string]$page.continuationToken)
        }
        else {
            $next = $null
        }
    }
    return @($results)
}

function Test-ExpectedTarget {
    param([Parameter(Mandatory)][object] $Shortcut)
    $target = $Shortcut.target.oneLake
    return (
        ([string]$Shortcut.path).TrimStart('/') -eq 'Tables' -and
        $Shortcut.name -eq 'RawEvents' -and
        $Shortcut.target.type -eq 'OneLake' -and
        $target.workspaceId -eq $inventory.workspaceId -and
        $target.itemId -eq $inventory.kqlDatabaseId -and
        $target.path -eq 'Tables/RawEvents'
    )
}

$shortcuts = Get-ShortcutPages -Uri $shortcutUri
$matches = @($shortcuts | Where-Object {
    ([string]$_.path).TrimStart('/') -eq 'Tables' -and $_.name -eq 'RawEvents'
})
if ($matches.Count -gt 1) { throw 'More than one Tables/RawEvents shortcut exists; refusing ambiguous state.' }
if ($matches.Count -eq 1) {
    if (-not (Test-ExpectedTarget -Shortcut $matches[0])) {
        throw 'Tables/RawEvents exists but targets a different OneLake item or path.'
    }
    Write-Output 'The RawEvents shortcut already matches the approved Eventhouse table target.'
    return
}

$body = @{
    path = 'Tables'
    name = 'RawEvents'
    target = @{
        oneLake = @{
            workspaceId = [string]$inventory.workspaceId
            itemId = [string]$inventory.kqlDatabaseId
            path = 'Tables/RawEvents'
        }
    }
} | ConvertTo-Json -Depth 10 -Compress
$created = Invoke-RestMethod -Method Post -Uri $shortcutUri -Headers $headers `
    -ContentType 'application/json' -Body $body -ErrorAction Stop
if (-not (Test-ExpectedTarget -Shortcut $created)) {
    throw 'Shortcut create response did not match the approved source workspace, KQL database, and RawEvents path.'
}

$readback = Get-ShortcutPages -Uri $shortcutUri
$verified = @($readback | Where-Object {
    ([string]$_.path).TrimStart('/') -eq 'Tables' -and $_.name -eq 'RawEvents'
})
if ($verified.Count -ne 1 -or -not (Test-ExpectedTarget -Shortcut $verified[0])) {
    throw 'RawEvents shortcut was not found with the expected target on read-back.'
}
Write-Output 'Read-only Delta shortcut Tables/RawEvents now targets the mirrored ehspark.RawEvents table.'
