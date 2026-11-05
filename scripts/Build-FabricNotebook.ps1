[CmdletBinding()]
param(
    [Parameter(Mandatory)] [guid] $WorkspaceId,
    [Parameter(Mandatory)] [guid] $LakehouseId,
    [Parameter(Mandatory)] [string] $KeyVaultUrl,
    [Parameter(Mandatory)] [string] $KustoEndpoint,
    [Parameter(Mandatory)] [string] $FunctionUrl,
    [Parameter(Mandatory)] [guid] $RunId,
    [ValidateSet('streaming', 'private-canary')] [string] $RunMode = 'streaming',
    [Parameter(Mandatory)] [string] $OutputPath
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
$sourcePath = Join-Path $root 'fabric\notebooks\streaming_pipeline.py'
$source = Get-Content -LiteralPath $sourcePath -Raw
$replacements = @{
    '<injected-workspace-id>' = $WorkspaceId.ToString()
    '<injected-lakehouse-id>' = $LakehouseId.ToString()
    '<injected-run-id>' = $RunId.ToString()
    '<injected-run-mode>' = $RunMode
    '<injected-private-key-vault-url>' = $KeyVaultUrl.TrimEnd('/')
    '<injected-kql-query-endpoint>' = $KustoEndpoint.TrimEnd('/')
    '<injected-private-function-url>' = $FunctionUrl.TrimEnd('/')
}
foreach ($key in $replacements.Keys) {
    $source = $source.Replace($key, $replacements[$key])
}
if ($source -match '<injected-[^>]+>') {
    throw 'Notebook source still contains an uninjected deployment value.'
}

$cellSource = @($source -split "`n" | ForEach-Object { "$_`n" })
$configuration = Get-Content (Join-Path $root 'fabric\notebooks\spark_session_configuration.json') -Raw
$configurationSource = @("%%configure`n") + @($configuration -split "`n" | ForEach-Object { "$_`n" })
$notebook = [ordered]@{
    cells = @(
        [ordered]@{
            cell_type = 'code'
            execution_count = $null
            metadata = @{}
            outputs = @()
            source = $configurationSource
        },
        [ordered]@{
            cell_type = 'code'
            execution_count = $null
            metadata = @{}
            outputs = @()
            source = $cellSource
        }
    )
    metadata = @{
        kernelspec = @{
            display_name = 'Synapse PySpark'
            language = 'Python'
            name = 'synapse_pyspark'
        }
        language_info = @{ name = 'python' }
        dependencies = @{
            lakehouse = @{
                default_lakehouse = $LakehouseId.ToString()
                default_lakehouse_name = 'lhspark'
                default_lakehouse_workspace_id = $WorkspaceId.ToString()
            }
        }
        trident = @{
            lakehouse = @{
                default_lakehouse = $LakehouseId.ToString()
                default_lakehouse_name = 'lhspark'
                default_lakehouse_workspace_id = $WorkspaceId.ToString()
                known_lakehouses = @(@{ id = $LakehouseId.ToString() })
            }
        }
    }
    nbformat = 4
    nbformat_minor = 5
}

$resolvedOutput = [IO.Path]::GetFullPath($OutputPath)
$parent = Split-Path -Parent $resolvedOutput
New-Item -ItemType Directory -Force -Path $parent | Out-Null
$json = $notebook | ConvertTo-Json -Depth 30
[IO.File]::WriteAllText($resolvedOutput, $json, [Text.UTF8Encoding]::new($false))
Write-Output "Notebook package created at $resolvedOutput"
