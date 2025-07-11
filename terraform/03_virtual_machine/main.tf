# ---------------------------
# 1. Public IP for the VM
# ---------------------------
resource "azurerm_public_ip" "main" {
  name                = "sentiment-vm-ip"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Basic"
}

# ---------------------------
# 2. Linux Virtual Machine (B1s)
# ---------------------------
resource "azurerm_linux_virtual_machine" "vm" {
  name                = "sentiment-vm"
  resource_group_name = var.resource_group_name
  location            = var.location
  size                = "Standard_B1s"
  admin_username      = "azureuser"
  network_interface_ids = [
    azurerm_network_interface.nic.id
  ]

  admin_ssh_key {
    username   = "azureuser"
    public_key = file(".ssh/id_rsa.pub") # Passe Pfad ggf. an
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
  publisher = "Canonical"
  offer     = "0001-com-ubuntu-server-focal"
  sku       = "20_04-lts"
  version   = "latest"
  }
}


resource "azurerm_network_interface" "nic" {
  name                = "sentiment-nic"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = data.azurerm_subnet.subnet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.main.id
  }
}


resource "azurerm_network_interface_security_group_association" "assoc" {
  network_interface_id      = azurerm_network_interface.nic.id
  network_security_group_id = data.azurerm_network_security_group.nsg.id
}

resource "azurerm_dev_test_global_vm_shutdown_schedule" "shutdown" {
  virtual_machine_id = azurerm_linux_virtual_machine.vm.id
  location           = var.location
  enabled            = true
  daily_recurrence_time = "2000" 
  timezone              = "W. Europe Standard Time"
  notification_settings {
    enabled         = false
  }
}