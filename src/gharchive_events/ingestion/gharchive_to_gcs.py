"""Ingest GitHub Archive hourly dumps into Google Cloud Storage.

Downloads gzip-compressed JSON files from data.gharchive.org for each UTC hour
in a configurable range (default: last 14 days through the current hour), then
uploads them under a Hive-style prefix (year=/month=/day=/hour=). Supports
--skip-if-exists to avoid re-uploading objects already in the bucket.

"""
from __future__ import annotations

import argparse
import os
import tempfile
from datetime import datetime, timedelta, timezone
from typing import Iterable

import requests
from google.cloud import storage

from gharchive_events.utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_RAW_PREFIX,
)

BASE_URL = "https://data.gharchive.org"


def current_utc_hour() -> datetime:
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def default_available_utc_hour() -> datetime:
    # GHArchive hourly files are usually not available immediately after the hour ends.
    # To avoid requesting files that have not been published yet, use a 3-hour buffer.
    return current_utc_hour() - timedelta(hours=3)


def parse_utc_datetime_hour(value: str) -> datetime:
    if value.endswith("Z"):
        value = value.replace("Z", "+00:00")

    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        raise ValueError("Datetime must include timezone information (e.g. Z or +00:00).")
    return dt.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)


def generate_hour_range(start_dt: datetime, end_dt: datetime) -> Iterable[datetime]:
    current = start_dt
    while current <= end_dt:
        yield current
        current += timedelta(hours=1)


def parse_args() -> argparse.Namespace:
    end_default = default_available_utc_hour()
    start_default = end_default - timedelta(days=14)

    parser = argparse.ArgumentParser(
        description="Download GH Archive hourly files and upload them to GCS raw bucket."
    )
    parser.add_argument(
        "--start",
        default=start_default.isoformat().replace("+00:00", "Z"),
        help="Start datetime in ISO-8601 UTC format, for example 2026-03-20T00:00:00Z.",
    )
    parser.add_argument(
        "--end",
        default=end_default.isoformat().replace("+00:00", "Z"),
        help="End datetime in ISO-8601 UTC format, for example 2026-03-28T23:00:00Z.",
    )
    parser.add_argument(
        "--skip-if-exists",
        action="store_true",
        help="Skip upload if the target object already exists in GCS.",
    )
    return parser.parse_args()


def build_filename(dt: datetime) -> str:
    # GH Archive format: YYYY-MM-DD-H.json.gz
    return f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d}-{dt.hour}.json.gz"


def build_url(dt: datetime) -> str:
    return f"{BASE_URL}/{build_filename(dt)}"


def build_gcs_object_name(dt: datetime) -> str:
    filename = build_filename(dt)
    return (
        f"{GCS_RAW_PREFIX}/"
        f"year={dt.year:04d}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"{filename}"
    )


def gcs_object_exists(client: storage.Client, object_name: str) -> bool:
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(object_name)
    return blob.exists()


def download_file(url: str, local_path: str, timeout: int = 60) -> None:
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def upload_file_to_gcs(
    client: storage.Client,
    bucket_name: str,
    source_file_path: str,
    destination_blob_name: str,
) -> None:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)


def process_one_hour_file(
    dt: datetime,
    client: storage.Client,
    bucket_name: str,
    skip_if_exists: bool,
) -> None:
    url = build_url(dt)
    filename = build_filename(dt)
    object_name = build_gcs_object_name(dt)

    if skip_if_exists and gcs_object_exists(client, object_name):
        print(f"Skipping existing object: gs://{bucket_name}/{object_name}")
        return

    # create temporary files to store the data, and remove the temp file after the
    # the data upload to gcs
    with tempfile.TemporaryDirectory() as tmp_dir:
        local_path = os.path.join(tmp_dir, filename)

        print(f"Downloading: {url}")
        download_file(url, local_path)

        print(f"Uploading to: gs://{bucket_name}/{object_name}")
        upload_file_to_gcs(client, bucket_name, local_path, object_name)

        print(f"Done: {filename}")


def ingest_gharchive_to_gcs() -> None:
    args = parse_args()
    start_dt = parse_utc_datetime_hour(args.start)
    end_dt = parse_utc_datetime_hour(args.end)
    if end_dt < start_dt:
        raise ValueError("--end must be greater than or equal to --start")

    client = storage.Client(project=GCP_PROJECT_ID)

    for dt in generate_hour_range(start_dt, end_dt):
        process_one_hour_file(
            dt=dt,
            client=client,
            bucket_name = GCS_BUCKET_NAME,
            skip_if_exists=args.skip_if_exists,
        )

def main():
    ingest_gharchive_to_gcs()


if __name__ == "__main__":
    main()