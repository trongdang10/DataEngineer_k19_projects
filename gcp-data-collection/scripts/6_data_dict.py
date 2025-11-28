#!/usr/bin/env python3
"""
Schema profiler for MongoDB collections.
- Discovers fields via streamed scan (up to SCAN_LIMIT).
- Samples documents to compute observed types and missing percentages.
- Writes outputs into output/schema_fields.txt and output/schema_profile.csv.
"""
import csv
import logging
import time
from collections import Counter, defaultdict
from pathlib import Path

from bson import ObjectId
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "countly"
COLL_NAME = "summary"

SAMPLE_SIZE = 50000      # for type/missing rates
SCAN_LIMIT = 500000     # max docs to scan for field discovery
BATCH_SIZE = 5000

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
OUTPUT_DIR = Path("output")
OUTPUT_DIR.mkdir(exist_ok=True)

def bson_type(v):
    """Map Python/BSON value to a simple type label."""
    if isinstance(v, ObjectId): return "objectId"
    if isinstance(v, list): return "array"
    if isinstance(v, dict): return "object"
    if v is None: return "null"
    if isinstance(v, bool): return "bool"
    if isinstance(v, (int, float)): return "number"
    return "string"

def discover_fields(coll):
    """Stream documents to collect all seen field names up to SCAN_LIMIT."""
    seen = set()
    scanned = 0
    t0 = time.monotonic()
    cursor = coll.find({}, projection=None, batch_size=BATCH_SIZE)
    for doc in cursor:
        scanned += 1
        seen.update(doc.keys())
        if scanned % BATCH_SIZE == 0:
            logging.info("Scanned %d docs, fields so far: %d", scanned, len(seen))
        if scanned >= SCAN_LIMIT:
            break
    logging.info("Field discovery done: %d docs scanned, %d fields found in %.2f min",
                 scanned, len(seen), (time.monotonic() - t0) / 60)
    return sorted(seen)

def profile_collection(coll):
    """Sample documents to compute type counts and missing percentages."""
    cursor = coll.aggregate([{"$sample": {"size": SAMPLE_SIZE}}])
    type_counts = defaultdict(Counter)
    present_counts = Counter()
    total = 0
    for doc in cursor:
        total += 1
        for k, v in doc.items():
            present_counts[k] += 1
            type_counts[k][bson_type(v)] += 1
    rows = []
    for field in sorted(present_counts):
        types = ";".join(f"{t}:{c}" for t, c in type_counts[field].items())
        missing_pct = 100 * (1 - present_counts[field] / total)
        rows.append({
            "field": field,
            "types(count)": types,
            "present": present_counts[field],
            "total_sampled": total,
            "missing_pct": f"{missing_pct:.2f}",
        })
    return rows

def write_csv(rows, filename):
    path = OUTPUT_DIR / filename
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return path

if __name__ == "__main__":
    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_NAME]

    # Field discovery
    fields = discover_fields(coll)
    fields_path = OUTPUT_DIR / "schema_fields.txt"
    fields_path.write_text("\n".join(fields))
    logging.info("Wrote field list to %s", fields_path)

    # Sampled profile
    start = time.monotonic()
    data = profile_collection(coll)
    csv_path = write_csv(data, "schema_profile.csv")
    dur_sec = time.monotonic() - start
    logging.info("Wrote %d rows to %s in %.2f minutes (%.2f seconds)",
                 len(data), csv_path, dur_sec / 60, dur_sec)
