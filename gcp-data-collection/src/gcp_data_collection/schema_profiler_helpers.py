"""
Helper utilities for MongoDB schema discovery and profiling.

Key capabilities:
- Recursively discover field paths (including nested objects/arrays) in dot-notation.
- Aggregate per-field statistics: presence counts, non-null counts, type distribution.
- Persist results into schema_fields.txt and schema_profile.csv.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple

from bson import ObjectId
from pymongo.collection import Collection


# Data class to hold per-field aggregate metrics
@dataclass
class FieldProfile:
    field: str
    total_docs: int
    present_count: int
    non_null_count: int
    type_counts: Dict[str, int]

    @property
    def missing_count(self) -> int:
        """Number of documents where this field path is not present at all."""
        return self.total_docs - self.present_count

    @property
    def missing_pct(self) -> float:
        """Percentage of documents where this field path is missing."""
        if self.total_docs == 0:
            return 0.0
        return self.missing_count * 100.0 / self.total_docs

    @property
    def types_summary(self) -> str:
        """
        Human-readable summary of type distribution,
        e.g. "string=1200; int=34; NoneType=5".
        """
        if not self.type_counts:
            return ""
        parts = [f"{t}={c}" for t, c in sorted(self.type_counts.items())]
        return "; ".join(parts)


# Utility to normalize Python types to simple labels for reporting
def get_type_name(value: Any) -> str:
    """
    Map Python values to a stable, business-readable type name.
    Handles MongoDB specific types like ObjectId explicitly.
    """
    if value is None:
        return "NoneType"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int) and not isinstance(value, bool):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, ObjectId):
        return "ObjectId"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    # Fallback for exotic types (dates, binaries, etc.)
    return type(value).__name__


# Core recursive engine that discovers all field paths in a document
def iter_field_paths(obj: Any, prefix: str | None = None) -> Iterator[Tuple[str, Any]]:
    """
    Recursively yield (field_path, value) pairs from a MongoDB document.

    - For dicts: descend into nested keys and build dot-notation.
      Example:
        {"option": {"alloy": ""}}
      -> yields ("option", {...}), ("option.alloy", "")

    - For lists: treat the list as one logical field (prefix),
      and descend into dict/list items without adding indexes.
      Example:
        {"option": [{"alloy": ""}, {"diamond": ""}]}
      -> yields ("option", [...]), ("option.alloy", ""), ("option.diamond", "")
    """
    if isinstance(obj, dict):
        for key, value in obj.items():
            path = f"{prefix}.{key}" if prefix else key
            # Emit the current field path itself
            yield path, value

            # If nested structure, keep drilling down
            if isinstance(value, (dict, list)):
                yield from iter_field_paths(value, path)

    elif isinstance(obj, list):
        # For arrays, keep the same prefix; descend into nested structures.
        for item in obj:
            if isinstance(item, (dict, list)):
                # Same logical field (e.g. "option"), but expand its children.
                yield from iter_field_paths(item, prefix)
            else:
                # Scalar array elements are already represented by the prefix only.
                # Example: tags=["a","b"] => "tags" is array<string>, no extra path.
                continue


# Field discovery – returns a sorted list of all unique field paths
def discover_fields(
    coll: Collection,
    scan_limit: int | None = None,
) -> List[str]:
    """
    Stream documents from the collection and discover all field paths
    (including nested ones) up to scan_limit documents.

    This is the entry point for building schema_fields.txt.
    """
    fields: set[str] = set()
    cursor = coll.find({}, batch_size=1000)

    count = 0
    for doc in cursor:
        count += 1
        # Stop early once we hit the configured discovery limit
        if scan_limit is not None and scan_limit > 0 and count > scan_limit:
            break

        # Recursively collect all field paths from this document
        for path, _ in iter_field_paths(doc):
            fields.add(path)

    return sorted(fields)


# Main aggregation engine for per-field profiling
def profile_collection(
    coll: Collection,
    scan_limit: int | None = None,
) -> List[FieldProfile]:
    """
    Profile the collection by scanning up to scan_limit documents and
    aggregating:

    - total_docs: total documents scanned
    - present_count: number of docs where the field path is present
    - non_null_count: number of docs where field path appears with a non-None value
    - type_counts: distribution of observed value types

    This function is used to generate schema_profile.csv.
    """
    # Structure: field_path -> stats dict
    stats: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "present_count": 0,
            "non_null_count": 0,
            "type_counts": Counter(),
        }
    )

    total_docs = 0
    cursor = coll.find({}, batch_size=1000)

    for doc in cursor:
        total_docs += 1
        if scan_limit is not None and scan_limit > 0 and total_docs > scan_limit:
            break

        # Track which field paths appeared in this specific document
        # to ensure present_count is doc-level, not occurrence-level.
        seen_paths: set[str] = set()

        for path, value in iter_field_paths(doc):
            seen_paths.add(path)

            # Increment type counters for each observed value
            type_name = get_type_name(value)
            stats[path]["type_counts"][type_name] += 1

            # Increment non-null counter if value is not None
            if value is not None:
                stats[path]["non_null_count"] += 1

        # After processing the document, update presence counts once per path
        for path in seen_paths:
            stats[path]["present_count"] += 1

    # Convert raw stats into a list of FieldProfile objects
    profiles: List[FieldProfile] = []
    for field_path, s in stats.items():
        profiles.append(
            FieldProfile(
                field=field_path,
                total_docs=total_docs,
                present_count=s["present_count"],
                non_null_count=s["non_null_count"],
                type_counts=dict(s["type_counts"]),
            )
        )

    # Sort output by field path for deterministic downstream usage
    profiles.sort(key=lambda p: p.field)
    return profiles


# Persist raw field list into schema_fields.txt
def write_schema_fields(
    fields: Iterable[str],
    output_dir: Path,
    filename: str = "schema_fields.txt",
) -> Path:
    """
    Write the discovered field paths into a plain-text file (one per line).
    Returns the final path for logging / orchestration.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    sorted_fields = sorted(fields)
    path.write_text("\n".join(sorted_fields), encoding="utf-8")
    return path


# Persist profiling metrics into schema_profile.csv
def write_schema_profile_csv(
    profiles: Iterable[FieldProfile],
    output_dir: Path,
    filename: str = "schema_profile.csv",
) -> Path:
    """
    Write the per-field profile into a CSV file.
    Columns are optimized for downstream analytics and documentation.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "field_name",
                "total_docs",
                "present_count",
                "non_null_count",
                "missing_count",
                "missing_pct",
                "types_summary",
            ]
        )

        for p in profiles:
            writer.writerow(
                [
                    p.field,
                    p.total_docs,
                    p.present_count,
                    p.non_null_count,
                    p.missing_count,
                    round(p.missing_pct, 2),
                    p.types_summary,
                ]
            )

    return path
