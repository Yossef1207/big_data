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
  size                = "Standard_B4ms" 
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
  custom_data = base64encode(<<-EOF
              #!/bin/bash
              apt-get update
              apt-get install -y apt-transport-https ca-certificates curl software-properties-common git

              # Docker installieren
              curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
              add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu focal stable"
              apt-get update
              apt-get install -y docker-ce docker-ce-cli containerd.io

              # Docker Compose installieren
              curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
              chmod +x /usr/local/bin/docker-compose

              # Git-Repo klonen (ersetze mit deinem echten Repo-Link)
              #cd /home/azureuser
              #git clone git@collaborating.tuhh.de:e-19/teaching/bd25_project_m8_a.git

              # Rechte setzen
              usermod -aG docker azureuser
              chown -R azureuser:azureuser /home/azureuser
              EOF
  )

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