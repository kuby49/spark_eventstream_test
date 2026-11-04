$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot

foreach ($path in Get-ChildItem (Join-Path $root 'scripts\*.ps1')) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        $path.FullName, [ref]$tokens, [ref]$errors
    ) | Out-Null
    if ($errors.Count) {
        throw "PowerShell parse failed for $($path.Name): $($errors[0].Message)"
    }
}

$notebookPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-notebook-$([guid]::NewGuid()).ipynb"
$canaryNotebookPath = Join-Path ([IO.Path]::GetTempPath()) "esehspark-canary-notebook-$([guid]::NewGuid()).ipynb"
try {
    & (Join-Path $root 'scripts\Build-FabricNotebook.ps1') `
        -WorkspaceId '11111111-1111-4111-8111-111111111111' `
        -LakehouseId '22222222-2222-4222-8222-222222222222' `
        -KeyVaultUrl 'https://example.vault.azure.net/' `
        -KustoEndpoint 'https://example.kusto.fabric.microsoft.com' `
        -FunctionUrl 'https://example.azurewebsites.net' `
        -RunId '33333333-3333-4333-8333-333333333333' `
        -OutputPath $notebookPath | Out-Null
    $notebook = Get-Content -LiteralPath $notebookPath -Raw | ConvertFrom-Json
    if ($notebook.metadata.trident.lakehouse.default_lakehouse -ne '22222222-2222-4222-8222-222222222222') {
        throw 'Notebook Lakehouse metadata binding is incorrect.'
    }
    if($notebook.cells.Count -ne 2 -or $notebook.cells[0].source[0].Trim() -ne '%%configure'){
        throw 'Session configuration must precede every production/diagnostic Spark action.'
    }
    $configuration=([string]::Join('', $notebook.cells[0].source)) -replace '^%%configure\s*',''
    $configuration=$configuration|ConvertFrom-Json
    if($configuration.numExecutors -ne 1 -or
        $configuration.conf.'spark.dynamicAllocation.enabled' -ne 'false' -or
        $configuration.driverCores -ne 8 -or $configuration.executorCores -ne 8 -or
        $configuration.driverMemory -ne '56g' -or $configuration.executorMemory -ne '56g'){
        throw 'Fixed executor setting must not expand the observed 16-core/56g-per-process envelope.'
    }
    $source = [string]::Join('', $notebook.cells[1].source)
    if ($source -match '<injected-[^>]+>') {
        throw 'Notebook contains an uninjected deployment value.'
    }
    if ($source -notmatch 'password=') {
        throw 'Notebook Kafka authentication configuration is absent.'
    }
    if ($source -notmatch 'RUN_MODE = "streaming"' -or
        $source -notmatch 'notebookutils\.notebook\.exit') {
        throw 'Notebook package must default to streaming and support a bounded private-route canary mode.'
    }

    & (Join-Path $root 'scripts\Build-FabricNotebook.ps1') `
        -WorkspaceId '11111111-1111-4111-8111-111111111111' `
        -LakehouseId '22222222-2222-4222-8222-222222222222' `
        -KeyVaultUrl 'https://example.vault.azure.net/' `
        -KustoEndpoint 'https://example.kusto.fabric.microsoft.com' `
        -FunctionUrl 'https://example.azurewebsites.net' `
        -RunId '33333333-3333-4333-8333-333333333333' `
        -RunMode 'private-canary' `
        -OutputPath $canaryNotebookPath | Out-Null
    $canaryNotebook = Get-Content -LiteralPath $canaryNotebookPath -Raw | ConvertFrom-Json
    $canarySource = [string]::Join('', $canaryNotebook.cells[1].source)
    if ($canarySource -notmatch 'RUN_MODE = "private-canary"' -or
        $canarySource -notmatch 'PrivateRouteCanaryResolvedPublicAddress' -or
        $canarySource -notmatch 'PRIVATE_CANARY') {
        throw 'The private-route canary notebook package is missing its private DNS/API/audit checks.'
    }
}
finally {
    Remove-Item -LiteralPath $notebookPath -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $canaryNotebookPath -Force -ErrorAction SilentlyContinue
}

$topology = Get-Content -LiteralPath (Join-Path $root 'fabric\eventstream\eventstream-definition.template.json') -Raw | ConvertFrom-Json
$rawDestination = $topology.destinations | Where-Object name -eq 'RawEvents'
$customDestination = $topology.destinations | Where-Object name -eq 'HighTemperatureKafka'
$filter = $topology.operators | Where-Object name -eq 'HighTemperatureFilter'
if ($rawDestination.properties.dataIngestionMode -ne 'ProcessedIngestion') {
    throw 'Eventhouse Eventstream destination must use the API-supported ProcessedIngestion mode.'
}
if ($rawDestination.properties.itemId -ne '{{KQL_DATABASE_ID}}') {
    throw 'Eventhouse destination must receive the KQL Database ID, not an Eventhouse ID.'
}
if ($customDestination.type -ne 'CustomEndpoint' -or $filter.properties.conditions[0].operatorType -ne 'GreaterThan' -or $filter.properties.conditions[0].value.value -ne '80.0') {
    throw 'Eventstream filter or custom endpoint destination contract is incorrect.'
}

$kql = Get-Content -LiteralPath (Join-Path $root 'fabric\kql\DatabaseSchema.kql') -Raw
if ($kql -notmatch '\.create-merge table RawEvents' -or $kql -notmatch 'policy streamingingestion enable') {
    throw 'RawEvents schema and streaming-ingestion policy must both be present.'
}
$schemaScript = Get-Content -LiteralPath (Join-Path $root 'scripts\Set-EventhouseSchema.ps1') -Raw
if ($schemaScript -notmatch 'TargetLatencyInMinutes=5' -or
    $schemaScript -notmatch '\.show table RawEvents policy mirroring') {
    throw 'RawEvents OneLake availability must use and read back the approved five-minute mirror policy.'
}
$functionDeployment = Get-Content -LiteralPath (Join-Path $root 'scripts\Deploy-ScoringFunction.ps1') -Raw
$functionCutover = Get-Content -LiteralPath (Join-Path $root 'scripts\Set-ScoringFunctionPrivateOnly.ps1') -Raw
$azureDeployment = Get-Content -LiteralPath (Join-Path $root 'scripts\Deploy-Azure.ps1') -Raw
$entrypoint = Get-Content -LiteralPath (Join-Path $root 'infra\entrypoint.bicep') -Raw
if ($functionCutover -notmatch 'properties\.publicNetworkAccess=Disabled' -or
    $functionCutover -notmatch 'networkRestrictionVerified' -or
    $functionCutover -notmatch 'functionKeyAuthenticated' -or
    $azureDeployment -notmatch 'functionPublicAccess = @\{ value = \$functionPublicAccess \}' -or
    $azureDeployment -notmatch '\$existingApp\.publicNetworkAccess' -or
    $entrypoint -notmatch 'functionPublicAccess: functionPublicAccess') {
    throw 'Function private-only cutover must require private proof and preserve public-network denial on redeployment.'
}
$mainTemplate = Get-Content -LiteralPath (Join-Path $root 'infra\main.bicep') -Raw
if ($functionDeployment -notmatch '--build-remote true' -or
    $functionDeployment -notmatch 'WEBSITE_RUN_FROM_PACKAGE' -or
    $functionDeployment -notmatch '--vnet-route-all-enabled true' -or
    $functionDeployment -notmatch 'config show' -or
    $mainTemplate -match "name:\s*'WEBSITE_RUN_FROM_PACKAGE'") {
    throw 'Linux Function deployment must enable remote Oryx build and VNet route-all without run-from-package.'
}
$managedPrivateEndpoints = Get-Content -LiteralPath (Join-Path $root 'scripts\Set-FabricManagedPrivateEndpoints.ps1') -Raw
if ($managedPrivateEndpoints -notmatch 'targetPrivateLinkResourceId' -or
    $managedPrivateEndpoints -notmatch 'private-link-resource' -or
    $managedPrivateEndpoints -notmatch 'x-ms-fabric-skill.*spark-cli' -or
    $managedPrivateEndpoints -notmatch 'private-endpoint-connection approve' -or
    $managedPrivateEndpoints -match 'Microsoft\.EventHub/namespaces') {
    throw 'Spark MPE automation must discover exact Azure resource IDs/subresources and avoid invented Event Hubs targets.'
}
$shortcutScript = Get-Content -LiteralPath (Join-Path $root 'scripts\Set-LakehouseRawShortcut.ps1') -Raw
if ($shortcutScript -notmatch 'Tables/RawEvents' -or
    $shortcutScript -notmatch 'kqlDatabaseId' -or
    $shortcutScript -notmatch 'spark-cli') {
    throw 'The Lakehouse RawEvents shortcut must target the discovered KQL database table and use the Spark authoring API attribution.'
}
$notebookStart = Get-Content -LiteralPath (Join-Path $root 'scripts\Start-FabricNotebook.ps1') -Raw
$notebookStop = Get-Content -LiteralPath (Join-Path $root 'scripts\Stop-FabricNotebook.ps1') -Raw
if ($notebookStart -notmatch 'jobs/instances' -or
    $notebookStart -notmatch 'reusedRecentJob' -or
    $notebookStart -notmatch 'jobInstanceId' -or
    $notebookStop -notmatch '\$base/cancel') {
    throw 'Notebook lifecycle scripts must prevent duplicate starts and cancel the recorded job instance.'
}
$generatorStart = Get-Content -LiteralPath (Join-Path $root 'scripts\Start-Generator.ps1') -Raw
if ($generatorStart -notmatch 'private-path-proof\.json' -or
    $generatorStart -notmatch 'unapprovedPublicRejected') {
    throw 'The generator must require final-policy private-path and public-rejection evidence.'
}
$shutdown = Get-Content -LiteralPath (Join-Path $root 'scripts\Stop-TestEnvironment.ps1') -Raw
if ($shutdown -notmatch 'rawMirrorPublished' -or
    $shutdown -notmatch 'unresolvedCount' -or
    $shutdown -notmatch 'providers/Microsoft.Fabric/capacities/fabricessparktest8' -or
    $shutdown -notmatch 'IdleShutdownEvidencePath' -or
    $shutdown -notmatch 'activeGeneratorTaskCount' -or
    $shutdown -notmatch 'activeNotebookJobCount' -or
    $shutdown -notmatch 'fullPoliciesCaptured' -or
    $shutdown -notmatch 'canaryDiagnosticsCaptured') {
    throw 'Environment shutdown must require acceptance or fresh no-workload evidence and target only the approved Fabric capacity.'
}

Write-Output 'PowerShell syntax, notebook package, Eventstream graph, and KQL schema checks passed.'
