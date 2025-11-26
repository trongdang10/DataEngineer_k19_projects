from dataclasses import dataclass
from pathlib import Path
import os

@dataclass
class Settings:
    gcp_project_id: str = os.environ["GCP_PROJECT_ID"]
    gcp_region: str = os.environ["GCP_REGION"]
    gcp_zone: str = os.environ["GCP_ZONE"]
    gcs_raw_bucket: str = os.environ["GCS_RAW_BUCKET"]
    service_account_key: Path = Path(os.environ["GCP_SERVICE_ACCOUNT_KEY"])
    mongo_uri: str = os.environ["MONGO_URI"]
    mongo_db: str = os.environ["MONGO_DB"]

settings = Settings()