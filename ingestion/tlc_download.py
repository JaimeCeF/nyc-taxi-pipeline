"""Download NYC TLC trip record Parquet files into the landing zone.

Files are fetched from the TLC's public CloudFront distribution and written
byte-for-byte to a destination directory (on Databricks, a Unity Catalog
volume such as /Volumes/nyc_taxi/bronze/landing/yellow). Auto Loader picks
them up from there into the Bronze Delta table.

Downloads are:
- idempotent: a month already present in the destination is skipped
- atomic: data is written to a sibling ``_tmp`` directory and only moved into
  the destination once complete and validated, so a failed download never
  leaves a truncated file where Auto Loader can see it
- tolerant of unpublished months: TLC publishes with a ~2 month lag and
  CloudFront answers 403/404 for missing objects; those months are skipped

Usage (locally or as a Databricks Python script task):

    python ingestion/tlc_download.py --start 2024-01 --end 2024-12 \\
        --dest /Volumes/nyc_taxi/bronze/landing/yellow
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import uuid

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TRIP_TYPES = ("yellow", "green", "fhv", "fhvhv")
NOT_PUBLISHED_STATUSES = (403, 404)
PARQUET_MAGIC = b"PAR1"
CHUNK_SIZE = 8 * 1024 * 1024
USER_AGENT = "nyc-taxi-pipeline/0.1 (python-requests)"

logger = logging.getLogger(__name__)


class DownloadError(RuntimeError):
    """Raised when a download completes but the result is not a valid file."""


def build_filename(trip_type: str, year: int, month: int) -> str:
    if trip_type not in TRIP_TYPES:
        raise ValueError(f"trip_type must be one of {TRIP_TYPES}, got {trip_type!r}")
    if not 1 <= month <= 12:
        raise ValueError(f"month must be in 1..12, got {month}")
    return f"{trip_type}_tripdata_{year:04d}-{month:02d}.parquet"


def build_url(trip_type: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{build_filename(trip_type, year, month)}"


def make_session(total_retries: int = 3) -> requests.Session:
    """Session that retries connection errors and transient 5xx/429 responses."""
    retry = Retry(
        total=total_retries,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    return session


def _validate_parquet(path: str, expected_size: int | None) -> None:
    size = os.path.getsize(path)
    if expected_size is not None and size != expected_size:
        raise DownloadError(f"size mismatch: expected {expected_size} bytes, got {size}")
    if size < 2 * len(PARQUET_MAGIC):
        raise DownloadError(f"file too small to be Parquet ({size} bytes)")
    with open(path, "rb") as f:
        head = f.read(len(PARQUET_MAGIC))
        f.seek(-len(PARQUET_MAGIC), os.SEEK_END)
        tail = f.read(len(PARQUET_MAGIC))
    if head != PARQUET_MAGIC or tail != PARQUET_MAGIC:
        raise DownloadError("missing Parquet magic bytes (truncated or not a Parquet file)")


def download_month(
    year: int,
    month: int,
    dest_dir: str,
    trip_type: str = "yellow",
    session: requests.Session | None = None,
    timeout: float = 60,
) -> str | None:
    """Download one month of trip data into ``dest_dir``.

    Returns the destination path if the file was downloaded or already
    present, or None if TLC has not published that month.
    """
    filename = build_filename(trip_type, year, month)
    dest_path = os.path.join(dest_dir, filename)

    if os.path.exists(dest_path):
        logger.info("skip %s: already present", filename)
        return dest_path

    # Sibling of dest_dir, so Auto Loader watching dest_dir never sees partial files.
    tmp_dir = os.path.join(os.path.dirname(os.path.normpath(dest_dir)), "_tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    os.makedirs(dest_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, f"{filename}.{uuid.uuid4().hex}.part")

    session = session or make_session()
    url = build_url(trip_type, year, month)

    try:
        with session.get(url, stream=True, timeout=timeout) as resp:
            if resp.status_code in NOT_PUBLISHED_STATUSES:
                logger.warning("skip %s: not published (HTTP %s)", filename, resp.status_code)
                return None
            resp.raise_for_status()

            content_length = resp.headers.get("Content-Length")
            expected_size = int(content_length) if content_length else None

            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    f.write(chunk)

        _validate_parquet(tmp_path, expected_size)
        # shutil.move falls back to copy + delete where rename is unsupported.
        shutil.move(tmp_path, dest_path)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    logger.info("downloaded %s (%d bytes)", filename, os.path.getsize(dest_path))
    return dest_path


def parse_year_month(value: str) -> tuple[int, int]:
    try:
        year_str, month_str = value.split("-")
        year, month = int(year_str), int(month_str)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}") from None
    if not 1 <= month <= 12:
        raise argparse.ArgumentTypeError(f"month must be in 1..12, got {value!r}")
    return year, month


def month_range(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    """Inclusive list of (year, month) pairs from ``start`` to ``end``."""
    if start > end:
        raise ValueError(f"start {start} is after end {end}")
    months = []
    year, month = start
    while (year, month) <= end:
        months.append((year, month))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return months


def download_range(
    start: tuple[int, int],
    end: tuple[int, int],
    dest_dir: str,
    trip_type: str = "yellow",
) -> dict[str, list]:
    """Download every month in [start, end]. Failures are logged, not raised."""
    session = make_session()
    result: dict[str, list] = {"downloaded": [], "not_published": [], "failed": []}
    for year, month in month_range(start, end):
        try:
            path = download_month(year, month, dest_dir, trip_type, session=session)
        except (requests.RequestException, DownloadError, OSError) as exc:
            logger.error("failed %d-%02d: %s", year, month, exc)
            result["failed"].append((year, month))
            continue
        key = "downloaded" if path else "not_published"
        result[key].append(path or (year, month))
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=parse_year_month, required=True, help="YYYY-MM")
    parser.add_argument("--end", type=parse_year_month, required=True, help="YYYY-MM")
    parser.add_argument("--dest", required=True, help="destination directory")
    parser.add_argument("--trip-type", choices=TRIP_TYPES, default="yellow")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    result = download_range(args.start, args.end, args.dest, args.trip_type)
    logger.info(
        "done: %d present/downloaded, %d not published, %d failed",
        len(result["downloaded"]),
        len(result["not_published"]),
        len(result["failed"]),
    )
    # Non-zero exit makes a Databricks job task fail visibly.
    return 1 if result["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
