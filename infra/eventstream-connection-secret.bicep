targetScope = 'resourceGroup'

@description('Name of the existing test Key Vault.')
param keyVaultName string = 'esehsparkkv'

@description('Approved Eventstream connection secret name.')
@minLength(1)
@maxLength(127)
param secretName string

@secure()
@minLength(1)
@description('Custom Endpoint connection string; supplied only through a protected deployment parameter file.')
param connectionString string

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' existing = {
  name: keyVaultName
}

resource eventstreamConnection 'Microsoft.KeyVault/vaults/secrets@2024-11-01' = {
  parent: keyVault
  name: secretName
  properties: {
    value: connectionString
    contentType: 'Fabric Eventstream Custom Endpoint SAS connection string'
    attributes: {
      enabled: true
    }
  }
}
