targetScope = 'resourceGroup'

@description('Name of the existing test Key Vault.')
param keyVaultName string = 'esehsparkkv'

@secure()
@minLength(32)
@description('Function-specific API key, supplied only through a protected deployment parameter file.')
param functionKey string

resource keyVault 'Microsoft.KeyVault/vaults@2024-11-01' existing = {
  name: keyVaultName
}

resource scoreKey 'Microsoft.KeyVault/vaults/secrets@2024-11-01' = {
  parent: keyVault
  name: 'score-api-function-key'
  properties: {
    value: functionKey
    contentType: 'Azure Function key for the score endpoint'
    attributes: {
      enabled: true
    }
  }
}
