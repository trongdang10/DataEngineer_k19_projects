# Glamira Data Pipeline

This project ingests the Glamira dataset, restores it into MongoDB, profiles the schema, enriches IPs, builds product URL mappings, and crawls product names. Below are the prerequisites, setup, and step-by-step commands to run each stage.

## Prerequisites
- Python 3.12+, Poetry, MongoDB CLI tools (`mongorestore`, `mongosh`).
- GCP CLI (`gsutil` or `gcloud storage`) to pull the dump from GCS.
- IP2Location BIN file (set `BIN_PATH` in scripts/7_process_ip_location.py).
- Network access to the Glamira sites if running the product-name crawler.

## Environment setup
Export these variables before running scripts (adjust as needed):
```bash
export GCP_PROJECT_ID=$(gcloud config get-value project)
export GCP_REGION=$(gcloud config get-value compute/region)
export GCP_ZONE=$(gcloud config get-value compute/zone)
export GCS_RAW_BUCKET="raw_glamira_data"
export GCP_SERVICE_ACCOUNT_KEY="$PWD/credentials/de-k19-key.json"
export MONGO_URI="mongodb://localhost:27017"
export MONGO_DB="countly"
```
Install dependencies:
```bash
poetry install
```

## Procedure Steps
1) **Validate config**  
   ```bash
   poetry run python scripts/1_validate_settings.py
   ```

2) **Upload/download raw Glamira data**  
   - Upload to GCS if needed: `poetry run python scripts/2_upload_glamira.py`
   - (VM) Download/restore: see steps 4–5.

3) **Provision VM / install MongoDB**  
   - Create VM (if using script): `poetry run python scripts/3_create_vm.py`
   - Install MongoDB on VM: `bash scripts/4_install_mongo.sh`

4) **Restore MongoDB from GCS dump** (on VM)  
   ```bash
   bash scripts/5_mongodb_restore.sh
   ```
   - Defaults: `gs://raw_glamira_data/glamira_ubl_oct2019_nov2019.tar.gz` → database `glamira`.
   - Override via env vars: `GCS_URI`, `LOCAL_TAR`, `EXTRACT_DIR`, `MONGO_URI`, `MONGO_DB`.

5) **Schema profiling / data dictionary**  
   ```bash
   poetry run python scripts/6_data_dict.py
   ```
   Outputs: `output/schema_fields.txt`, `output/schema_profile.csv`.

6) **Build product URL mappings in Mongo**  
   ```bash
   bash scripts/8_build_product_urls.sh
   ```
   Creates collections `product_urls` and `product_urls_reco` in Mongo.

7) **Export product URLs to CSV**  
   ```bash
   poetry run python scripts/9_export_product_urls.py
   ```
   Output: `output/product_id_urls.csv` (product_id → url).

8) **IP location enrichment**  
   Set `BIN_PATH` in `scripts/7_process_ip_location.py` to your IP2Location BIN, then run:
   ```bash
   poetry run python scripts/7_process_ip_location.py
   ```
   Outputs rotated CSVs `output/ip_locations_*.csv` and upserts into Mongo `ip_locations` collection.

9) **Product name crawler**  
   Uses `output/product_id_urls.csv`, fetches product pages, parses `react_data`, falls back to slug names:
   ```bash
   poetry run python scripts/10_product_name_crawler.py
   ```
   Outputs rotated CSVs `output/product_names_*.csv` with a `source` column (`react_data` or `slug`).

## Outputs
- `output/schema_fields.txt`, `output/schema_profile.csv` — schema profiling.
- `output/product_id_urls.csv` — product_id → url mapping.
- `output/ip_locations_*.csv` — enriched IP geolocation data.
- `output/product_names_*.csv` — crawled product names from product id.
- Logs in `logs/` for crawlers and enrichment scripts.
