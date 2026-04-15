from __future__ import annotations

import argparse
from datetime import date, timedelta

from google.cloud import bigquery, storage

from gharchive_events.utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_PROCESSED_PREFIX,
    BQ_DATASET,
)


EVENT_TYPE_TO_TABLE = {
    "watch": "source_watch_events",
    "fork": "source_fork_events",
}

WRITE_DISPOSITION_MAP = {
    "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE,
    "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND,
    "WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY,
}


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def date_range(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def build_source_uris(event_type: str, start_date: date, end_date: date) -> list[str]:
    """Resolve concrete gs:// URIs for each Parquet object (Spark may nest partitions)."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    base = f"{GCS_PROCESSED_PREFIX.strip('/')}/{event_type}/"
    uris: list[str] = []

    for dt in date_range(start_date, end_date):
        partition_prefix = f"{base}event_date={dt.isoformat()}/"
        for blob in bucket.list_blobs(prefix=partition_prefix):
            if blob.name.endswith(".parquet"):
                uris.append(f"gs://{GCS_BUCKET_NAME}/{blob.name}")

    return uris


def load_processed_to_bq(
    event_type: str,
    start_date: str,
    end_date: str,
    write_disposition: str,
) -> None:
    if event_type not in EVENT_TYPE_TO_TABLE:
        raise ValueError(f"Unsupported event_type: {event_type}")

    start_dt = parse_date(start_date)
    end_dt = parse_date(end_date)

    table_name = EVENT_TYPE_TO_TABLE[event_type]
    destination_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"

    source_uris = build_source_uris(event_type, start_dt, end_dt)

    if not source_uris:
        raise SystemExit(
            f"No Parquet files under gs://{GCS_BUCKET_NAME}/"
            f"{GCS_PROCESSED_PREFIX.strip('/')}/{event_type}/event_date="
            f"[{start_dt.isoformat()}..{end_dt.isoformat()}]. Run transform first."
        )

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Plain Parquet load only: Spark partitionBy("event_date") drops that column from
    # file bodies, so autodetect has no event_date — BQ errors if the load job requests
    # partitioning on event_date. Clustering/partitioning on load also conflicts with
    # many existing Terraform tables (partitioning none).
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=WRITE_DISPOSITION_MAP[write_disposition],
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    print(f"Loading {event_type} processed data to {destination_table}")
    print(f"Source objects: {len(source_uris)} Parquet file(s)")
    for uri in source_uris[:20]:
        print(f"  {uri}")
    if len(source_uris) > 20:
        print(f"  ... and {len(source_uris) - 20} more")

    load_job = client.load_table_from_uri(
        source_uris,
        destination_table,
        job_config=job_config,
    )

    load_job.result()

    table = client.get_table(destination_table)
    print(f"Loaded {table.num_rows} rows into {destination_table}.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load processed GH Archive parquet files from GCS to BigQuery."
    )
    parser.add_argument(
        "--event-type",
        required=True,
        choices=["watch", "fork"],
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--write-disposition",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_processed_to_bq(
        event_type=args.event_type,
        start_date=args.start_date,
        end_date=args.end_date,
        write_disposition=args.write_disposition,
    )


if __name__ == "__main__":
    main()