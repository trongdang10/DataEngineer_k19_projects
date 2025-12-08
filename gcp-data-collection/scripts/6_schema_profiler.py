#!/usr/bin/env python3
"""
CLI entrypoint for MongoDB schema profiling.

Responsibilities:
- Parse CLI arguments (MongoDB connection + scan limits + output directory).
- Connect to MongoDB.
- Orchestrate field discovery and profiling using helper functions.
- Emit schema_fields.txt and schema_profile.csv into the output directory.
"""

import argparse
import logging
import time
from pathlib import Path

from pymongo import MongoClient

from gcp_data_collection.schema_profiler_helpers import (
    discover_fields,
    profile_collection,
    write_schema_fields,
    write_schema_profile_csv,
)


# Centralized CLI construction for better maintainability
def build_arg_parser() -> argparse.ArgumentParser:
    """
    Configure the command-line interface for the schema profiler.
    This is the single source of truth for runtime parameters.
    """
    parser = argparse.ArgumentParser(
        description="MongoDB schema profiler (including nested fields via dot-notation)."
    )

    parser.add_argument(
        "--uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection string. Default: mongodb://localhost:27017",
    )
    parser.add_argument(
        "--db-name",
        required=True,
        help="Target MongoDB database name.",
    )
    parser.add_argument(
        "--collection-name",
        required=True,
        help="Target MongoDB collection name to profile.",
    )
    parser.add_argument(
        "--scan-limit",
        type=int,
        default=100000,
        help=(
            "Max number of documents to scan for discovery and profiling. "
            "Use a positive integer for sampling, or 0/negative for full collection."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory where schema_fields.txt and schema_profile.csv will be written.",
    )

    return parser


# Core orchestration function: wires up MongoDB and helper utilities
def run() -> None:
    """
    Orchestration for the schema profiling workflow:
    1) Connect to MongoDB.
    2) Discover all field paths (including nested) up to scan_limit.
    3) Profile field usage, missingness, and type distribution.
    4) Write schema_fields.txt and schema_profile.csv to the output directory.
    """
    parser = build_arg_parser()
    args = parser.parse_args()

    # Configure basic logging for operational visibility
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    output_dir = Path(args.output_dir)

    logging.info(
        "Starting schema profiling: db=%s collection=%s scan_limit=%s output_dir=%s",
        args.db_name,
        args.collection_name,
        args.scan_limit,
        output_dir,
    )

    # Normalize scan_limit semantics
    scan_limit = args.scan_limit if args.scan_limit and args.scan_limit > 0 else None

    # Create MongoDB client – context-managed for clean shutdown
    client = MongoClient(args.uri)
    db = client[args.db_name]
    coll = db[args.collection_name]

    # 1) Field discovery
    logging.info("Discovering field paths (including nested fields)...")
    start = time.monotonic()
    fields = discover_fields(coll, scan_limit=scan_limit)
    fields_path = write_schema_fields(fields, output_dir=output_dir)
    discover_duration = time.monotonic() - start
    logging.info(
        "Field discovery completed: %d fields written to %s in %.2f seconds",
        len(fields),
        fields_path,
        discover_duration,
    )

    # 2) Detailed field profiling
    logging.info("Profiling field usage and data types...")
    start = time.monotonic()
    profiles = profile_collection(coll, scan_limit=scan_limit)
    csv_path = write_schema_profile_csv(profiles, output_dir=output_dir)
    profile_duration = time.monotonic() - start
    logging.info(
        "Profiling completed: %d rows written to %s in %.2f seconds",
        len(profiles),
        csv_path,
        profile_duration,
    )

    logging.info("Schema profiling workflow completed successfully.")


# Standard Python entrypoint guard
if __name__ == "__main__":
    run()
