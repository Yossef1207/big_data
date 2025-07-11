# Guide: Running Terraform Code & Connecting to the Cloud VM

## Prerequisites

- [Terraform](https://www.terraform.io/downloads.html) installed
- Cloud credentials (e.g., Azure, AWS, GCP)
- SSH key pair for the VM
- Be Aware that you must run **00_init** terraform code to be able to run this code without errors.

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

5. **Removing Resources after finishing working with the VM**

    ```bash
    terraform destroy
    ```