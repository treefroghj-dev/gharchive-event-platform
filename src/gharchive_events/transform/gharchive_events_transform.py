from __future__ import annotations

from datetime import datetime, timedelta, UTC
import argparse
from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import storage

from gharchive_events.utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_PREFIX,
    BQ_DATASET,
    GOOGLE_APPLICATION_CREDENTIALS,
    LOCAL_TMP_DIR,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read GH Archive raw data from GCS and build GH Archive source tables in BigQuery."
    )

    parser.add_argument(
        "--execution-date",
        type=str,
        default=None,
        help="Logical execution date in YYYY-MM-DD format. Defaults to current UTC date.",
    )

    return parser.parse_args()


def get_execution_date(execution_date_str: str | None):
    if execution_date_str:
        return datetime.strptime(execution_date_str, "%Y-%m-%d").date()
    return datetime.now(UTC).date()


def build_input_paths(execution_date) -> list[str]:
    dates_to_read = [
        execution_date - timedelta(days=i)
        for i in range(1, 16)
    ]

    return [
        f"gs://{GCS_BUCKET_NAME}/{GCS_PREFIX}/year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        for d in dates_to_read
    ]


def split_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Expected gs:// URI, got: {gs_uri}")
    bucket_and_path = gs_uri[len("gs://"):]
    bucket, _, object_path = bucket_and_path.partition("/")
    if not bucket or not object_path:
        raise ValueError(f"Invalid gs:// URI: {gs_uri}")
    return bucket, object_path


def download_inputs_to_local(input_paths: list[str], execution_date) -> list[str]:
    client = storage.Client(project=GCP_PROJECT_ID)
    local_root = Path(LOCAL_TMP_DIR) / "transform_inputs" / execution_date.strftime("%Y-%m-%d")
    local_root.mkdir(parents=True, exist_ok=True)

    downloaded: list[str] = []

    for input_path in input_paths:
        bucket_name, object_path = split_gs_uri(input_path)
        bucket = client.bucket(bucket_name)

        if input_path.endswith("/"):
            for blob in client.list_blobs(bucket, prefix=object_path):
                if blob.name.endswith("/") or not blob.name.endswith((".json", ".json.gz")):
                    continue
                target = local_root / Path(blob.name).name
                blob.download_to_filename(str(target))
                downloaded.append(str(target))
        else:
            blob = bucket.blob(object_path)
            target = local_root / Path(object_path).name
            blob.download_to_filename(str(target))
            downloaded.append(str(target))

    if not downloaded:
        raise FileNotFoundError("No input files downloaded from GCS.")

    return downloaded


def build_spark(include_bq: bool = True) -> SparkSession:
    # Single fat connector: avoids mixing gcs-connector + BQ jars (Guava / google-api-client clashes).
    # Reads use local files (downloaded via google-cloud-storage); writes use Storage Write API (no gs:// staging).
    packages = [
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.43.1",
    ]

    conf = (
        SparkConf()
        .setMaster("local[1]")
        .set("spark.driver.memory", "512m")
        .set("spark.executor.memory", "512m")
        .setAppName("gharchive_events_transform")
        .set("spark.jars.packages", ",".join(packages))
    )

    builder = (
        SparkSession.builder \
        .config(conf=conf) \
        .config("spark.sql.session.timeZone", "UTC")
    )

    if GOOGLE_APPLICATION_CREDENTIALS:
        builder = (
            builder
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                GOOGLE_APPLICATION_CREDENTIALS,
            )
        )

    return builder.getOrCreate()


def write_to_bq(df, *, table_name: str):
    (
        df.write.format("bigquery")
        .option("table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}")
        .option("writeMethod", "direct")
        .option("writeAtLeastOnce", "true")
        .mode("overwrite")
        .save()
    )
    print(f"Spark successfully wrote data into BigQuery {GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}")

def transform_watch_events(df_raw):
    return (
        df_raw.filter(F.col("type") == "WatchEvent")
        .withColumn("event_ts", F.to_timestamp("created_at"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("event_ts"),
            F.col("event_date"),
            F.col("event_hour"),
            F.col("repo.id").alias("repo_id"),
            F.col("repo.name").alias("repo_name"),
            F.col("actor.id").alias("actor_id"),
            F.col("actor.login").alias("actor_login"),
            F.col("org.id").alias("org_id"),
            F.col("org.login").alias("org_login"),
            F.col("payload.action").alias("action"),
            F.col("public").alias("is_public"),
        )
    )


def transform_fork_events(df_raw):
    return (
        df_raw.filter(F.col("type") == "ForkEvent")
        .withColumn("event_ts", F.to_timestamp("created_at"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.col("event_ts"),
            F.col("event_date"),
            F.col("event_hour"),
            F.col("repo.id").alias("repo_id"),
            F.col("repo.name").alias("repo_name"),
            F.col("actor.id").alias("actor_id"),
            F.col("actor.login").alias("actor_login"),
            F.col("org.id").alias("org_id"),
            F.col("org.login").alias("org_login"),
            F.col("payload.action").alias("action"),
            F.col("payload.forkee.id").alias("forkee_repo_id"),
            F.col("payload.forkee.full_name").alias("forkee_repo_name"),
            F.col("public").alias("is_public"),
        )
    )


def spark_create_source_tbl():
    args = parse_args()
    execution_date = get_execution_date(args.execution_date)
    input_paths = build_input_paths(execution_date)

    print(f"Execution date: {execution_date}")
    print("Reading input paths:")
    for path in input_paths:
        print(path)

    local_input_paths = download_inputs_to_local(input_paths, execution_date)
    print(f"Downloaded {len(local_input_paths)} local input file(s)")

    spark = build_spark()

    try:
        df_raw = spark.read.json(local_input_paths)
        df_watch = transform_watch_events(df_raw)
        df_fork = transform_fork_events(df_raw)
        write_to_bq(df_watch, table_name="source_watch_events")
        write_to_bq(df_fork, table_name="source_fork_events")
    finally:
        try:
            spark.stop()
        except Exception as exc:
            print(f"Spark stop warning: {exc}")


def main():
    spark_create_source_tbl()

if __name__ == "__main__":
    main()