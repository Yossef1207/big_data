# Guide: Running Terraform Code & Connecting to the Cloud VM

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) installed
- Cloud credentials (e.g., Azure, AWS, GCP)
- SSH key pair for the VM
- Be Aware that you must run 00_init and 01_network terraform code to be able to run this code without errors.

## Running the Terraform Code

1. **Change to the directory:**
    ```bash
    /terraform/03_virtual_machine/
    ```

2. **Initialize Terraform:**
    ```bash
    terraform init
    ```

3. **Check the configuration:**
    ```bash
    terraform plan
    ```

4. **Create resources:**
    ```bash
    terraform apply
    ```
    Confirm with `yes` when prompted.

## Connecting to the VM

1. **Find the public IP of the VM:**
    After `terraform apply`, the IP will be shown in the output

2. **Connect via SSH:**
    ```bash
    ssh -i .ssh/id_rsa azureuser@<public_ip>
    ```
> **Note:** Username and SSH key must match your Terraform configuration.

## Removing Resources after finishing working with the VM

```bash
terraform destroy
```