# Terraform

## Installation
Install Terraform on Ubuntu 22.04
[reference](https://computingforgeeks.com/how-to-install-terraform-on-ubuntu/)
```shell
# install repository addition dependencies
sudo apt update
sudo apt install software-properties-common gnupg2 curl

# import repository GPG key
curl https://apt.releases.hashicorp.com/gpg | gpg --dearmor > hashicorp.gpg
sudo install -o root -g root -m 644 hashicorp.gpg /etc/apt/trusted.gpg.d/

# With the key imported now add Hashicorp repository to your Ubuntu system
sudo apt-add-repository "deb [arch=$(dpkg --print-architecture)] https://apt.releases.hashicorp.com focal main"
# because apt repository has packages for Ubuntu 22.04 "jammy" yet, so here use "focal" (Ubuntu 20.04)

sudo apt install terraform

# check
terraform --version
#Terraform v1.5.7
#on linux_amd64
```

## Usage
```shell
# Initialize state file (.tfstate)
terraform init

# List all workspaces
terraform workspace list

# Create a new workspace
terraform workspace new dev

# Select a workspace
terraform workspace select dev

# Need to Enable Identity and Access Management (IAM) API and Cloud Resource Manager API first
# Check changes to new infra plan
terraform plan

# Create new infra
terraform apply

# Verify the workspace
terraform workspace show

# Delete infra after your work, to avoid costs on any running services
terraform destroy
```
