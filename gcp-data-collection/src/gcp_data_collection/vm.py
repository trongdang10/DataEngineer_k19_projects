"""
Helper functions for Google Compute Engine (GCE) VM management.
"""
from __future__ import annotations

import logging
import time
import json
from pathlib import Path
from typing import Optional

from google.oauth2 import service_account
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from config.settings import settings

logger = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/cloud-platform"]


# Helper to extract service account email from the JSON key file
def service_account_email() -> str:
    data = json.loads(Path(settings.service_account_key).read_text())
    return data["client_email"]


def _compute_client():
    """Build a Compute Engine client using the service-account JSON."""
    creds = service_account.Credentials.from_service_account_file(
        str(settings.service_account_key),
        scopes=_SCOPES,
    )
    return discovery.build("compute", "v1", credentials=creds, cache_discovery=False)


def _machine_type(zone: str, machine_type: str) -> str:
    return f"zones/{zone}/machineTypes/{machine_type}"


def instance_exists(instance_name: str) -> bool:
    """Return True if the instance already exists in the project+zone."""
    client = _compute_client()
    try:
        client.instances().get(
            project=settings.gcp_project_id,
            zone=settings.gcp_zone,
            instance=instance_name,
        ).execute()
        return True
    except HttpError as exc:  # 404 => not found
        if exc.resp.status == 404:
            return False
        raise


def _wait_for_operation(operation_name: str) -> None:
    """Poll the compute API until the long-running operation is done."""
    client = _compute_client()
    project = settings.gcp_project_id
    zone = settings.gcp_zone
    while True:
        result = client.zoneOperations().get(
            project=project,
            zone=zone,
            operation=operation_name,
        ).execute()

        if result.get("status") == "DONE":
            if "error" in result:
                raise RuntimeError(result["error"])
            logger.info("Operation %s completed.", operation_name)
            return

        time.sleep(3)


def create_vm(
    instance_name: str,
    *,
    machine_type: str = "e2-standard-2",
    boot_disk_size_gb: int = 50,
    image_project: str = "debian-cloud",
    image_family: str = "debian-12",
    startup_script: Optional[Path] = None,
    tags: Optional[list[str]] = None,
) -> None:
    """
    Create a VM instance if it does not exist.

    startup_script: optional path to a script that installs MongoDB, etc.
    tags: optional list of network tags (e.g. ["allow-mongo"])
    """
    if instance_exists(instance_name):
        logger.info("Instance %s already exists; skipping create.", instance_name)
        return

    client = _compute_client()
    image_response = client.images().getFromFamily(
        project=image_project,
        family=image_family,
    ).execute()
    source_disk_image = image_response["selfLink"]

    metadata_items = []
    if startup_script:
        script_text = Path(startup_script).read_text()
        metadata_items.append({"key": "startup-script", "value": script_text})

    config = {
        "name": instance_name,
        "machineType": _machine_type(settings.gcp_zone, machine_type),
        "tags": {"items": tags or []},
        "disks": [
            {
                "boot": True,
                "autoDelete": True,
                "initializeParams": {
                    "sourceImage": source_disk_image,
                    "diskSizeGb": boot_disk_size_gb,
                },
            }
        ],
        "networkInterfaces": [
            {
                "network": "global/networks/default",
                "accessConfigs": [{"type": "ONE_TO_ONE_NAT", "name": "External NAT"}],
            }
        ],
        "serviceAccounts": [
            {
                "email": service_account_email(),
                "scopes": _SCOPES,
            }
        ],
        "metadata": {"items": metadata_items},
    }

    logger.info("Creating VM %s in %s (%s)", instance_name, settings.gcp_zone, machine_type)
    start = time.monotonic()
    operation = client.instances().insert(
        project=settings.gcp_project_id,
        zone=settings.gcp_zone,
        body=config,
    ).execute()
    _wait_for_operation(operation["name"])
    duration = time.monotonic() - start
    logger.info("VM %s created in %.1f seconds.", instance_name, duration)
