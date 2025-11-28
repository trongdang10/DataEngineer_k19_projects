#!/usr/bin/env python3
"""Export product_id → URL mappings from Mongo to CSV."""
import csv
import logging
import time
from pathlib import Path

from pymongo import MongoClient
from config.settings import settings

OUTPUT = Path("output/product_id_urls.csv")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def export_product_urls():
    """Merge product_urls and product_urls_reco into one CSV."""
    t0 = time.monotonic()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]

    urls = {}

    # Prefer entries from product_urls
    logging.info("Loading product_urls …")
    for doc in db["product_urls"].find({}, {"_id": 1, "url": 1}):
        pid = str(doc["_id"])
        url = doc.get("url")
        if pid and url:
            urls[pid] = url

    # Fill gaps from product_urls_reco
    logging.info("Loading product_urls_reco …")
    for doc in db["product_urls_reco"].find({}, {"_id": 1, "url": 1}):
        pid = str(doc["_id"])
        url = doc.get("url")
        if pid and url and pid not in urls:
            urls[pid] = url

    OUTPUT.parent.mkdir(exist_ok=True)
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "url"])
        for pid, url in urls.items():
            writer.writerow([pid, url])

    dt = time.monotonic() - t0
    logging.info("Wrote %d rows to %s in %.2f seconds", len(urls), OUTPUT, dt)

if __name__ == "__main__":
    export_product_urls()
