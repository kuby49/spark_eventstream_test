targetScope = 'resourceGroup'

@description('Existing Fabric workspace ID. Obtain it from the Fabric create/list API.')
param workspaceId string

@description('Fabric tenant ID from the approved environment.')
param tenantId string = '4526cee1-4399-4e4b-9134-46fabd583527'

@description('Private DNS virtual network ID.')
param virtualNetworkId string

param location string = 'westus3'

resource fabricPrivateDns 'Microsoft.Network/privateDnsZones@2024-06-01' = {
  name: 'privatelink.fabric.microsoft.com'
  location: 'global'
}

resource fabricDnsLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2024-06-01' = {
  parent: fabricPrivateDns
  name: 'vnet-link'
  location: 'global'
  properties: {
    registrationEnabled: false
    virtualNetwork: {
      id: virtualNetworkId
    }
  }
}

resource fabricPrivateLink 'Microsoft.Fabric/privateLinkServicesForFabric@2024-06-01' = {
  name: 'pl-fabric-esehspark'
  location: 'global'
  properties: {
    tenantId: tenantId
    workspaceId: workspaceId
  }
}

resource fabricWorkspaceEndpoint 'Microsoft.Network/privateEndpoints@2024-07-01' = {
  name: 'pe-esehspark-fabric-workspace'
  location: location
  properties: {
    subnet: {
      id: '${virtualNetworkId}/subnets/private-endpoints'
    }
    privateLinkServiceConnections: [
      {
        name: 'fabric-workspace'
        properties: {
          privateLinkServiceId: fabricPrivateLink.id
          groupIds: [
            'workspace'
          ]
        }
      }
    ]
  }
}

resource fabricWorkspaceDnsGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2024-07-01' = {
  parent: fabricWorkspaceEndpoint
  name: 'default'
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'fabric'
        properties: {
          privateDnsZoneId: fabricPrivateDns.id
        }
      }
    ]
  }
}

output fabricPrivateLinkId string = fabricPrivateLink.id
output fabricEndpointId string = fabricWorkspaceEndpoint.id
