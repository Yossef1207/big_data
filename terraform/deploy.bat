@echo off
cd 00_init
terraform apply -var-file="secrets.tfvars" --auto-approve
cd ..\01_network
terraform apply -var-file="secrets.tfvars" --auto-approve
cd ..\02_storage
terraform apply -var-file="secrets.tfvars" --auto-approve
cd ..\03_virtual_machine
terraform apply -var-file="secrets.tfvars" --auto-approve