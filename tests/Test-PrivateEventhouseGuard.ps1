$ErrorActionPreference = 'Stop'
$path = Join-Path $PSScriptRoot '..\scripts\Read-PrivateEventhouseCanary.ps1'
$tokens = $null
$errors = $null
$ast = [Management.Automation.Language.Parser]::ParseFile($path, [ref]$tokens, [ref]$errors)
if ($errors.Count) { throw 'Eventhouse reader syntax is invalid.' }
$definition = $ast.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-PrivateAddresses'
}, $true)
. ([scriptblock]::Create($definition.Extent.Text))
$resultDefinition = $ast.Find({
    param($node)
    $node -is [Management.Automation.Language.FunctionDefinitionAst] -and $node.Name -eq 'Get-CanaryResultTable'
}, $true)
. ([scriptblock]::Create($resultDefinition.Extent.Text))
try {
    $report = @{}
    $ExpectedPrivateAddresses = @([Net.Dns]::GetHostAddresses('localhost') | ForEach-Object IPAddressToString)
    $observed = @(Get-PrivateAddresses 'localhost')
    if (@(Compare-Object $ExpectedPrivateAddresses $observed).Count) {
        throw 'The guard must preserve actual DNS results, not redirect resolution.'
    }
    $ExpectedPrivateAddresses = @('10.74.2.15')
    $rejected = $false
    try { $null = Get-PrivateAddresses 'localhost' }
    catch {
        if ($_.Exception.Message -ne 'EndpointNotOnApprovedFabricPrivateNic') { throw }
        $rejected = $true
    }
    if (-not $rejected -or -not $report.refusedAddresses.Count -or $report.refusedHost -ne 'localhost') {
        throw 'A host outside the approved private NIC must be refused with evidence before any HTTP request.'
    }
    $source = Get-Content $path -Raw
    if ([regex]::Matches($source, '-MaximumRedirection 0').Count -ne 5) {
        throw 'Private metadata/token/KQL calls must not follow a redirect to an unguarded host.'
    }
    if ($source -match '\.properties\.databaseName' -or $source -notmatch '\$details\.displayName') {
        throw 'GA KQL metadata has no databaseName property; use the actual verified database display name.'
    }
    $legacyTable = [pscustomobject]@{
        TableName='Table_0'
        Columns=@('message_id','run_id','equipment_id','temperature','timestamp' | ForEach-Object {
            [pscustomobject]@{ColumnName=$_}
        })
        Rows=@()
    }
    $metadata = [pscustomobject]@{TableName='QueryProperties';Columns=@();Rows=@()}
    $selected = Get-CanaryResultTable ([pscustomobject]@{Tables=@($legacyTable,$metadata)})
    if ($selected.TableName -ne 'Table_0' -or $selected.Rows.Count -ne 0) {
        throw 'Kusto v1 uses Table_0 rather than a mandatory PrimaryResult name; empty rows remain empty.'
    }
    $rejected = $false
    try { $null = Get-CanaryResultTable ([pscustomobject]@{Tables=@($metadata)}) }
    catch { $rejected = $true }
    if (-not $rejected) { throw 'A response with no projected result table must not become an empty-success fallback.' }
    Write-Output 'Eventhouse private NIC/DNS guard and no-redirect checks passed.'
}
finally {
    Remove-Item Function:\Get-PrivateAddresses
    Remove-Item Function:\Get-CanaryResultTable
}
