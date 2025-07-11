@echo off
cd 03_virtual_machine
terraform destroy -var-file="secrets.tfvars" --auto-approve
cd ..\02_storage
terraform destroy -var-file="secrets.tfvars" --auto-approve
cd ..\01_network
terraform destroy -var-file="secrets.tfvars" --auto-approve
cd ..\00_init
terraform destroy -var-file="secrets.tfvars" --auto-approve


