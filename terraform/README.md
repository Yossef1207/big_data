# deploy.bat

## Overview

The **deploy.bat** file is a Windows batch script designed for the automated deployment of Azure infrastructure using Terraform.  
It sequentially executes the Terraform configurations in multiple subfolders to create resources in the correct order.

## How it works

The script performs the following steps:

1. **Change to the `00_init` directory**  
   Runs `terraform apply` with the variables defined in `secrets.tfvars`.

2. **Change to the `01_network` directory**  
   Runs `terraform apply` again with the same variables to provision the network.

3. **Change to the `02_storage` directory**  
   Runs `terraform apply` to create storage resources.

4. **Change to the `03_virtual_machine` directory**  
   Runs `terraform apply` to provision the virtual machine.

Each step is executed automatically and without further prompts (`--auto-approve`).

## Prerequisites

- **Terraform** must be installed and available in the system path.
- The **`secrets.tfvars`** file with all required variables must be present in each module directory or accessible from there.
- The directory structure must exist as specified in the script.

## Usage

1. Open a command prompt (CMD) in the `terraform` directory.
2. Run the script:
   ```
   deploy.bat
   ```

The script will then create the entire infrastructure in the correct order.

---

**Note:**  
This script is intended for Windows operating systems. For Linux/MacOS, use a shell script (`.sh`) instead.

--- 

# destroy.bat

## Overview

The **destroy.bat** file is a Windows batch script for automated teardown of the Azure infrastructure created with Terraform.  
It sequentially destroys resources in the reverse order of their creation, ensuring dependencies are handled correctly.

## How it works

The script performs the following steps:

1. **Change to the `03_virtual_machine` directory**  
   Runs `terraform destroy` to remove the virtual machine resources.

2. **Change to the `02_storage` directory**  
   Runs `terraform destroy` to remove storage resources.

3. **Change to the `01_network` directory**  
   Runs `terraform destroy` to remove network resources.

4. **Change to the `00_init` directory**  
   Runs `terraform destroy` to remove initial resources (such as the resource group).

Each step is executed automatically and without further prompts (`--auto-approve`).

## Prerequisites

- **Terraform** must be installed and available in the system path.
- The **`secrets.tfvars`** file with all required variables must be present in each module directory or accessible from there.
- The directory structure must exist as specified in the script.

## Usage

1. Open a command prompt (CMD) in the `terraform` directory.
2. Run the script:
   ```
   destroy.bat
   ```

The script will then destroy the entire infrastructure in the correct order.

---

**Note:**  
This script is intended for Windows operating systems. For Linux/MacOS, use a shell script (`.sh`) instead.