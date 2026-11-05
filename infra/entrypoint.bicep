targetScope = 'subscription'

@description('Azure region for the approved test resource group.')
param location string = 'westus3'

@description('Fabric capacity administrator user principal name from the selected tenant.')
param capacityAdminUpn string

@description('Local administrator account for the private Windows generator VM.')
param vmAdminUsername string

@secure()
@minLength(12)
@description('VM local administrator password; provided securely at deploy time.')
param vmAdminPassword string

@allowed([
  'Enabled'
  'Disabled'
])
@description('Function public access during initial proof or preserved private-only state on redeployment.')
param functionPublicAccess string = 'Enabled'

resource targetGroup 'Microsoft.Resources/resourceGroups@2024-03-01' = {
  name: 'rg_esehspark'
  location: location
}

module resources 'main.bicep' = {
  name: 'esehspark-resources'
  scope: targetGroup
  params: {
    location: location
    capacityAdminUpn: capacityAdminUpn
    vmAdminUsername: vmAdminUsername
    vmAdminPassword: vmAdminPassword
    functionPublicAccess: functionPublicAccess
  }
}

output resourceGroupId string = targetGroup.id
output fabricCapacityId string = resources.outputs.fabricCapacityId
output virtualNetworkId string = resources.outputs.virtualNetworkId
output functionAppId string = resources.outputs.functionAppId
output functionHostName string = resources.outputs.functionHostName
output keyVaultUri string = resources.outputs.keyVaultUri
output storageAccountName string = resources.outputs.storageAccountName
output vmId string = resources.outputs.vmId
