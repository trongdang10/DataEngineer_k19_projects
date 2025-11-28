#!/usr/bin/env python3
"""
Sanity-check config/settings.py against environment variables and key files.
Run with: poetry run python scripts/validate_settings.py
"""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


from config.settings import settings

def check_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{label} does not exist: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"{label} is not a file: {path}")


def main() -> None:
    print("Loaded settings:")
    print(f"  GCP project:  {settings.gcp_project_id}")
    print(f"  Region/Zone:  {settings.gcp_region} / {settings.gcp_zone}")
    print(f"  Raw bucket:   {settings.gcs_raw_bucket}")
    print(f"  Mongo URI:    {settings.mongo_uri}")
    print(f"  Mongo DB:     {settings.mongo_db}")

    check_file(settings.service_account_key, "Service-account key")

    print("All required environment variables and files are present.")


if __name__ == "__main__":
    main()
