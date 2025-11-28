#!/usr/bin/env python3
"""
IP enrichment pipeline:
- Loads distinct IPs from Mongo summary collection.
- Looks up geo data via IP2Location BIN.
- Writes rotated CSVs and upserts into Mongo ip_locations collection.
"""
import sys
import csv
import logging
import time
from datetime import datetime
from pathlib import Path

import IP2Location
from pymongo import UpdateOne

# Ensure src/ is on the path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from gcp_data_collection.ip_location import iter_distinct_ips, _client  # noqa: E402
from config.settings import settings  # noqa: E402

# Config
BIN_PATH = Path("input/IP-COUNTRY-REGION-CITY.BIN")  # adjust if stored elsewhere
CSV_OUTPUT = Path("output/ip_locations.csv")        # base name; rotates
BATCH_SIZE = 1000
MAX_ROWS_PER_FILE = 500_000  # rotate to keep files manageable
LOG_LEVEL = logging.INFO
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
handlers = [
    logging.StreamHandler(),
    logging.FileHandler(LOG_DIR / "process_ip_location.log", encoding="utf-8"),
]
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s", handlers=handlers)


def open_writer(idx: int):
    """Create a CSV writer for the current rotation file."""
    path = CSV_OUTPUT.with_name(f"ip_locations_{idx:04d}.csv")
    f = path.open("w", newline="", encoding="utf-8")
    w = csv.writer(f)
    w.writerow(["ip", "country_code", "country_name", "region", "city", "latitude", "longitude", "processed_at"])
    return f, w, path


def main():
    """Drive the enrichment: iterate IPs, lookup geo, write CSV and Mongo upserts."""
    if not BIN_PATH.is_file():
        raise FileNotFoundError(f"IP2Location BIN not found: {BIN_PATH}")

    t0 = time.monotonic()
    CSV_OUTPUT.parent.mkdir(exist_ok=True)

    client = _client()
    src = client[settings.mongo_db]["summary"]
    dest = client[settings.mongo_db]["ip_locations"]
    dest.create_index([("ip", 1)], unique=True)

    ipdb = IP2Location.IP2Location(str(BIN_PATH))

    ops = []
    total = skipped = errors = 0
    file_idx = 1
    current_rows = 0
    csv_file, writer, current_path = open_writer(file_idx)
    logging.info("Writing to %s", current_path)

    try:
        for ip in iter_distinct_ips(src):
            # Skip invalid/placeholder IPs
            if not ip or ip in ("", "0.0.0.0", "::1", "127.0.0.1"):
                skipped += 1
                continue
            try:
                rec = ipdb.get_all(ip)
                country = rec.country_short
                if not country:
                    errors += 1
                    continue
                processed_at = datetime.utcnow()
                row = [
                    ip,
                    country,
                    rec.country_long,
                    rec.region,
                    rec.city,
                    rec.latitude,
                    rec.longitude,
                    processed_at.isoformat(),
                ]
                writer.writerow(row)

                ops.append(
                    UpdateOne(
                        {"ip": ip},
                        {"$set": {
                            "ip": ip,
                            "country_code": country,
                            "country_name": rec.country_long,
                            "region": rec.region,
                            "city": rec.city,
                            "latitude": rec.latitude,
                            "longitude": rec.longitude,
                            "source": "ip2location",
                            "processed_at": processed_at,
                        }},
                        upsert=True,
                    )
                )
                total += 1
                current_rows += 1

                if len(ops) >= BATCH_SIZE:
                    dest.bulk_write(ops, ordered=False)
                    ops = []
                    if total % 1000 == 0:
                        logging.info("Processed %d (skipped %d, errors %d)", total, skipped, errors)

                if current_rows >= MAX_ROWS_PER_FILE:
                    csv_file.close()
                    file_idx += 1
                    current_rows = 0
                    csv_file, writer, current_path = open_writer(file_idx)
                    logging.info("Rolling to %s", current_path)

            except Exception:
                errors += 1
                continue

    finally:
        if ops:
            dest.bulk_write(ops, ordered=False)
        csv_file.close()

    dt = time.monotonic() - t0
    logging.info(
        "Finished: processed %d, skipped %d, errors %d. Time: %.2f min (%.2f s)",
        total, skipped, errors, dt / 60, dt,
    )


if __name__ == "__main__":
    main()
