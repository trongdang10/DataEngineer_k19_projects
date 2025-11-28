"""
helper functions for Google Cloud Storage (GCS) interactions.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from google.cloud import storage
from config.settings import Settings

import time

logger = logging.getLogger(__name__)

def _client() -> storage.Client:
    settings = Settings()
    return storage.Client.from_service_account_json(
        str(settings.service_account_key),
        project=settings.gcp_project_id,
    )

def create_bucket(
        bucket_name: Optional[str] = None,
        region: Optional[str] = None,
        storage_class: str = "STANDARD",
) -> None:
    """Create a new GCS bucket."""
    # Initialize client
    settings = Settings()
    client = _client()

    # Use default settings if not provided
    bucket_name = bucket_name or settings.gcs_raw_bucket
    region = region or settings.gcp_region

    # Check if bucket already exists
    existing = client.lookup_bucket(bucket_name)
    if existing:
        logger.info("Bucket %s already exists; skipping create.", bucket_name)
        return

    # Create the bucket
    bucket = storage.Bucket(client, name=bucket_name)
    bucket.storage_class = storage_class

    logger.info("Creating bucket %s in region %s", bucket_name, region)
    
    # Measuring time taken to create the bucket
    start = time.monotonic()
    client.create_bucket(bucket, location=region)
    duration = time.monotonic() - start
    logger.info("Bucket %s created in %.2f seconds.", bucket_name, duration)

def upload_file(
        local_path: str | Path,
        dest_path: str,
        bucket_name: Optional[str] = None,
) -> None:
    """Upload a file to a GCS bucket."""

    settings = Settings()
    client = _client()

    # Use default bucket if not provided
    bucket_name = bucket_name or settings.gcs_raw_bucket
    local_path = Path(local_path)

    # Validate local file exists
    if not local_path.is_file():
        raise FileNotFoundError(f"Local file {local_path} does not exist.")
    
    # Upload the file
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dest_path)

    logger.info("Uploading %s to gs://%s/%s",
               local_path, 
               bucket_name, 
               dest_path)
    
    start = time.monotonic()
    blob.upload_from_filename(str(local_path))
    duration = time.monotonic() - start
    logger.info("Upload complete in %.2f seconds.", duration)
    
    
    

