#!/usr/bin/env python3
import logging

from pathlib import Path
from gcp_data_collection import vm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

# Create a VM instance with specified parameters, adjust as needed
def main():
    
    vm.create_vm(
        instance_name="glamira-mongo-vm",
        machine_type="n2-standard-8",
        boot_disk_size_gb=200,  # change if you need a different disk size
        image_project="ubuntu-os-cloud",
        image_family="ubuntu-2204-lts",  # or use a specific image
        startup_script= Path("scripts/install_mongo.sh"), # path to install MongoDB
        tags=["allow-ssh"],
    )
    print("VM creation initiated.")

if __name__ == "__main__":
    main()