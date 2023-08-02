# Ansible usage guide

## Requirements 

Run `pip3 install -r requirements.txt` to install python dependencies

Run `ansible-galaxy install -r requirements.yml` to install Ansible collections

Add service account JSON file to main directory and file name to variable `service_account_file` in group_vars/all

## Usage

Run `ansible-playbook start-playbook.yaml` to create and start resources or `ansible-playbook shutdown-playbook.yaml` to stop and delete resources