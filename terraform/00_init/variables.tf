variable "project_name" {
  default = "sentiment-project"
}

variable "resource_group_name" {
  default = "sentiment-project"
}


variable "location" {
  default = "westeurope"
}

variable "subscription_id" {
  description = "Azure Subscription ID"
  type        = string
}

variable "client_id" {
  description = "Azure Client ID (App ID)"
  type        = string
}

variable "client_secret" {
  description = "Azure Client Secret"
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Azure Tenant ID"
  type        = string
}