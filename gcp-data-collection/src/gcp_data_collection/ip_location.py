"""Helper functions for IP location enrichment using IP2Location database."""
from __future__ import annotations
import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable

import IP2Location
from pymongo import MongoClient, UpdateOne
from config.settings import settings

logger = logging.getLogger(__name__)

def _client() -> MongoClient:
    return MongoClient(settings.mongo_uri)

def iter_distinct_ips(coll, batch_size: int = 50_000) -> Iterable[str]:
    pipeline = [
        {"$match": {"ip": {"$exists": True, "$ne": ""}}},
        {"$group": {"_id": "$ip"}},
        {"$project": {"ip": "$_id", "_id": 0}},
    ]
    cursor = coll.aggregate(pipeline, allowDiskUse=True, batchSize=batch_size)
    for doc in cursor:
        yield doc["ip"]

def enrich_ips(
    bin_path: Path,
    source_coll: str = "summary",
    dest_coll: str = "ip_locations",
    batch_size: int = 1_000,
) -> int:
    client = _client()
    src = client[settings.mongo_db][source_coll]
    dest = client[settings.mongo_db][dest_coll]

    ipdb = IP2Location.IP2Location(str(bin_path))
    ops = []
    processed = 0

    for ip in iter_distinct_ips(src):
        try:
            rec = ipdb.get_all(ip)
        except Exception as exc:
            logger.warning("Skipping IP %s: %s", ip, exc)
            continue

        doc = {
            "ip": ip,
            "country_code": rec.country_short,
            "country_name": rec.country_long,
            "region": rec.region,
            "city": rec.city,
            "latitude": rec.latitude,
            "longitude": rec.longitude,
            "timezone": getattr(rec, "timezone", None),
            "source": "ip2location",
            "processed_at": datetime.utcnow(),
        }
        ops.append(UpdateOne({"ip": ip}, {"$set": doc}, upsert=True))
        processed += 1

        if len(ops) >= batch_size:
            dest.bulk_write(ops, ordered=False)
            logger.info("Upserted %d IPs (total %d)", len(ops), processed)
            ops = []

    if ops:
        dest.bulk_write(ops, ordered=False)
        logger.info("Upserted %d IPs (total %d)", len(ops), processed)

    return processed
