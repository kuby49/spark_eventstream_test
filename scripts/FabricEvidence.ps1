function Protect-FabricEvidence {
    param([AllowNull()][object] $Value,[string] $EvidencePath='$')
    if ($null -eq $Value) { return $null }
    if ($Value -is [DateTime]) { return $Value.ToString('o') }
    if ($Value -is [DateTimeOffset]) { return $Value.ToString('o') }
    if ($Value.GetType().IsPrimitive -or $Value -is [decimal]) { return $Value.PSObject.BaseObject }
    if ($Value -is [string]) {
        if ($Value -eq '[REDACTED]') { return $Value }
        # Kusto policy EntityName is a qualified bracketed identifier, not JSON.
        if ($Value -match '^\[[A-Za-z0-9_-]+\]\.\[[A-Za-z_][A-Za-z0-9_]*\]$') { return $Value }
        $text = $Value.TrimStart()
        if ($text.StartsWith('{') -or $text.StartsWith('[')) {
            try { $json = $Value | ConvertFrom-Json -ErrorAction Stop }
            catch { throw "Evidence contains malformed embedded JSON at $EvidencePath; refusing to persist it." }
            $protectedJson=if($text.StartsWith('[')){
                Protect-FabricEvidence -Value @($json) -EvidencePath $EvidencePath
            }else{Protect-FabricEvidence -Value $json -EvidencePath $EvidencePath}
            return (ConvertTo-Json -InputObject $protectedJson -Depth 90 -Compress)
        }
        if ($Value -match '(?i)SharedAccessKey\s*=|SharedAccessSignature\s*=|AccountKey\s*=|Password\s*=|Bearer\s+\S+|-----BEGIN|[?&](sig|code|token|key|secret|password)=' -or
            $Value -match '^eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$') {
            return '[REDACTED]'
        }
        return $Value
    }
    if ($Value -is [Collections.IEnumerable] -and $Value -isnot [Collections.IDictionary]) {
        $result = @($Value | ForEach-Object { Protect-FabricEvidence -Value $_ -EvidencePath "$EvidencePath[]" })
        return ,$result
    }
    if ($Value -is [Collections.IDictionary] -or $Value -is [pscustomobject]) {
        $result = [ordered]@{}
        $names = if ($Value -is [Collections.IDictionary]) { @($Value.Keys) } else { @($Value.PSObject.Properties.Name) }
        foreach ($name in $names) {
            if ($name -match '(?i)password|secret|token|credential|authorization|connectionstring|sharedaccess|accountkey|apikey|functionkey|^key$|^sas$|^sig$') {
                $result[$name] = '[REDACTED]'
            }
            else { $result[$name] = Protect-FabricEvidence -Value $Value.$name -EvidencePath "$EvidencePath.$name" }
        }
        return $result
    }
    return $Value
}

function Save-FabricEvidence {
    param([string] $Path, [object] $Value)
    $protected = Protect-FabricEvidence $Value
    $protected | ConvertTo-Json -Depth 90 |
        Set-Content -LiteralPath $Path -Encoding utf8
}

function Convert-FabricDefinitionEvidence {
    param([object] $Definition)
    $parts = @($Definition.definition.parts)
    if (-not $parts.Count) { throw 'No definition parts returned; evidence is incomplete.' }
    $decoded = foreach ($part in $parts) {
        if ($part.payloadType -ne 'InlineBase64' -or ($part.path -notlike '*.json' -and $part.path -ne '.platform')) {
            throw 'Unsupported definition part encoding/format; refusing to persist opaque credentials.'
        }
        try {
            $bytes = [Convert]::FromBase64String($part.payload)
            $content = [Text.UTF8Encoding]::new($false, $true).GetString($bytes) | ConvertFrom-Json -ErrorAction Stop
        }
        catch { throw 'Definition part is not valid UTF8 JSON; evidence was not persisted.' }
        @{path=$part.path;content=$content}
    }
    return @{parts=@($decoded);format='DecodedJsonRedacted';opaquePayloadsRetained=$false}
}

function Assert-FabricEventstreamGraph {
    param([object] $Actual, [object] $Expected)
    foreach ($group in @('sources','streams','operators','destinations')) {
        if (@($Actual.$group).Count -ne @($Expected.$group).Count) {
            throw "Eventstream $group count differs from the approved graph."
        }
        foreach ($node in $Expected.$group) {
            $found = @($Actual.$group | Where-Object name -eq $node.name)
            if ($found.Count -ne 1 -or $found[0].type -ne $node.type -or
                (@($found[0].inputNodes.name) -join '|') -cne (@($node.inputNodes.name) -join '|')) {
                throw "Eventstream node type/edge mismatch: $($node.name)."
            }
            if ($group -eq 'operators') {
                $actualConditions=@($found[0].properties.conditions)
                $expectedConditions=@($node.properties.conditions)
                if($actualConditions.Count -ne $expectedConditions.Count){throw "Eventstream operator condition count mismatch: $($node.name)."}
                for($index=0;$index -lt $expectedConditions.Count;$index++){
                    $actualColumn=$actualConditions[$index].column
                    $expectedColumn=$expectedConditions[$index].column
                    $actualPath=if($actualColumn.PSObject.Properties['columnPathSegments']){$actualColumn.columnPathSegments}else{$actualColumn.columnPath}
                    $expectedPath=if($expectedColumn.PSObject.Properties['columnPathSegments']){$expectedColumn.columnPathSegments}else{$expectedColumn.columnPath}
                    if((ConvertTo-Json -InputObject $actualPath -Depth 20 -Compress) -cne
                        (ConvertTo-Json -InputObject $expectedPath -Depth 20 -Compress)){
                        throw "Eventstream operator column path mismatch: $($node.name)."
                    }
                    foreach($property in @('operatorType','column.columnName','column.expressionType','column.node',
                        'value.expressionType','value.dataType','value.value')){
                        $actualValue=$actualConditions[$index]
                        $expectedValue=$expectedConditions[$index]
                        foreach($segment in $property.Split('.')){
                            $actualValue=$actualValue.$segment
                            $expectedValue=$expectedValue.$segment
                        }
                        if($actualValue -cne $expectedValue){throw "Eventstream operator condition mismatch: $($node.name)/$property."}
                    }
                }
            }
        }
    }
    $raw = @($Actual.destinations | Where-Object name -eq 'RawEvents')[0]
    $expectedRaw = @($Expected.destinations | Where-Object name -eq 'RawEvents')[0]
    foreach ($property in @('workspaceId','itemId','databaseName','tableName','dataIngestionMode')) {
        if ($raw.properties.$property -cne $expectedRaw.properties.$property) {
            throw "Eventhouse destination identity/mode mismatch: $property."
        }
        if (($raw.properties.inputSerialization | ConvertTo-Json -Depth 20 -Compress) -cne
            ($expectedRaw.properties.inputSerialization | ConvertTo-Json -Depth 20 -Compress)) {
            throw 'Eventhouse input serialization differs from the approved JSON contract.'
        }
    }
}

function Get-FabricComponentStateEvidence {
    param([object] $Topology, [switch] $RequireState)
    $components = foreach ($group in @('sources','destinations','streams','operators')) {
        foreach ($node in @($Topology.$group)) {
            if ($null -eq $node) { continue }
            @{kind=$group;id=$node.id;name=$node.name;status=$node.status;statusAvailable=($null -ne $node.status)}
        }
    }
    $missing = @($components | Where-Object { -not $_.statusAvailable -and $_.kind -in @('sources','destinations') })
    if ($RequireState -and $missing.Count) {
        throw 'Documented topology response omitted required source/destination status; no guessed state API was called.'
    }
    return @{components=@($components);sourceDestinationStatusComplete=($missing.Count -eq 0);
        historicalFixtureStateReconstructed=$false;basis='GA GET Eventstream topology status fields, only when returned'}
}
