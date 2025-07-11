data "azurerm_virtual_network" "vnet" {
  name                = "sentiment-vnet"
  resource_group_name = var.resource_group_name
}

data "azurerm_subnet" "subnet" {
  name                 = "default"
  virtual_network_name = data.azurerm_virtual_network.vnet.name
  resource_group_name  = var.resource_group_name
}

data "azurerm_network_security_group" "nsg" {
  name                = "sentiment-nsg"
  resource_group_name = var.resource_group_name
}