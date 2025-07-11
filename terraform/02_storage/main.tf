# ---------------------------
# 1. Storage Account for Reddit Data
# ---------------------------
resource "azurerm_storage_account" "storage" {
  name                     = "sentimentstore123" # must be globally unique
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}
# ---------------------------
# 2. Storage Container
# ---------------------------
resource "azurerm_storage_container" "data" {
  name                  = "reddit-data"
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = "private"
}
