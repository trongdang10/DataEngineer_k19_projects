#!/usr/bin/env python3
"""
Crawl product pages to extract names and related fields:
- Reads unique product_id + url pairs from output/product_id_urls.csv.
- Tries to parse react_data JSON from each page; falls back to slug-based names.
- Writes rotated CSVs under output/ and logs progress.
"""
import asyncio
import csv
import json
import logging
import random
import re
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

import aiohttp

INPUT = Path("output/product_id_urls.csv")
OUTPUT_DIR = Path("output")
LOG_DIR = Path("logs")

BATCH_SIZE = 500           # rotate CSV after this many rows
CONCURRENCY = 15           # reduce if you see timeouts
REQUEST_TIMEOUT = 30

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
]

LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "crawl_product_names.log", encoding="utf-8"),
    ],
)

RE_REACT = re.compile(r"var\s+react_data\s*=\s*(\{.*?\});", re.DOTALL)


def clean_react_data(raw: str) -> str:
    """Normalize JS-like object text into valid JSON."""
    raw = raw.replace("undefined", "null")
    raw = re.sub(r"(\w+)\s*:", r'"\1":', raw)
    raw = re.sub(r":\s*'([^']*)'", r': "\1"', raw)
    raw = re.sub(r",\s*}", "}", raw)
    raw = re.sub(r",\s*]", "]", raw)
    return raw


def parse_react_data(html: str):
    """Extract react_data JSON from HTML and return selected fields."""
    m = RE_REACT.search(html)
    if not m:
        return None
    txt = m.group(1)
    try:
        data = json.loads(txt)
    except json.JSONDecodeError:
        data = json.loads(clean_react_data(txt))
    fields = [
        "product_id", "name", "product_type", "sku",
        "price", "min_price", "max_price", "qty",
        "collection_id", "collection", "category",
        "category_name", "store_code", "gender",
    ]
    return {f: data.get(f) for f in fields}


def slug_to_name(url: str) -> str | None:
    """Fallback name extraction from the URL slug."""
    if not url:
        return None
    try:
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        if not parts:
            return None
        slug = parts[-1].split(".")[0]
        slug = unquote(slug)
        name = slug.replace("-", " ").replace("_", " ").strip()
        return name.title() if name else None
    except Exception:
        return None


async def fetch_one(session: aiohttp.ClientSession, sem: asyncio.Semaphore, pid: str, url: str):
    """Fetch a single product page with limited retries; return parsed data or status."""
    async with sem:
        for attempt in range(1, 3):
            try:
                async with session.get(
                    url,
                    headers={
                        "User-Agent": random.choice(USER_AGENTS),
                        "Accept-Language": "en-US,en;q=0.9",
                    },
                    timeout=REQUEST_TIMEOUT,
                ) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        data = parse_react_data(html)
                        return data, f"HTTP 200 attempt {attempt}"
                    elif resp.status == 404:
                        return None, "HTTP 404"
                    else:
                        if attempt >= 2:
                            return None, f"HTTP {resp.status}"
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                if attempt >= 2:
                    return None, f"Exception: {e}"
                await asyncio.sleep(2 ** attempt)
    return None, "Unknown"


def open_writer(idx: int):
    """Create a new CSV writer for rotated output."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    path = OUTPUT_DIR / f"product_names_{idx:04d}.csv"
    f = path.open("w", newline="", encoding="utf-8")
    w = csv.writer(f)
    w.writerow([
        "product_id", "name", "product_type", "sku",
        "price", "min_price", "max_price", "qty",
        "collection_id", "collection", "category", "category_name",
        "store_code", "gender", "source"
    ])
    return f, w, path


async def main():
    """Load IDs/URLs, crawl pages concurrently, write rotated CSVs, and log stats."""
    if not INPUT.is_file():
        raise FileNotFoundError(f"Input file not found: {INPUT}")

    pairs = []
    seen = set()
    with INPUT.open("r", newline="", encoding="utf-8") as fin:
        reader = csv.DictReader(fin)
        for row in reader:
            pid = row.get("product_id")
            url = row.get("url", "")
            if pid and url and pid not in seen:
                seen.add(pid)
                pairs.append((pid, url))
    total_ids = len(pairs)
    success = 0
    fail = 0
    logging.info("Loaded %d unique product IDs from %s", total_ids, INPUT)

    sem = asyncio.Semaphore(CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 10)
    t0 = time.monotonic()

    file_idx = 1
    current_rows = 0
    csv_file, writer, current_path = open_writer(file_idx)
    logging.info("Writing to %s", current_path)

    try:
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            tasks = (fetch_one(session, sem, pid, url) for pid, url in pairs)
            for (pid, url), coro in zip(pairs, asyncio.as_completed(tasks)):
                data, status = await coro
                if data and data.get("name"):
                    source = "react_data"
                    row = [data.get(k) for k in [
                        "product_id", "name", "product_type", "sku",
                        "price", "min_price", "max_price", "qty",
                        "collection_id", "collection", "category", "category_name",
                        "store_code", "gender"
                    ]]
                    success += 1
                else:
                    # fallback: use slug name if available
                    slug_name = slug_to_name(url)
                    if not slug_name:
                        logging.debug("Skip %s (%s)", pid, status)
                        fail += 1
                        continue
                    source = "slug"
                    row = [pid, slug_name] + [None] * 12
                    success += 1 # counted as written via slug

                writer.writerow(row + [source])
                current_rows += 1

                if current_rows >= BATCH_SIZE:
                    csv_file.close()
                    file_idx += 1
                    current_rows = 0
                    csv_file, writer, current_path = open_writer(file_idx)
                    logging.info("Rolling to %s", current_path)
    finally:
        csv_file.close()

    dt = time.monotonic() - t0
    logging.info(
        "Done. Total IDs: %d, Success: %d, Fail: %d. Time: %.2f min (%.2f s). Files in %s",
        total_ids, success, fail, dt / 60, dt, OUTPUT_DIR
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
