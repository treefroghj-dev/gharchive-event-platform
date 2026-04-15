from __future__ import annotations

import argparse
import os
from datetime import date, datetime, timedelta, timezone
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "gharchive-events-platform-raw-dev")
GCS_RAW_PREFIX = os.getenv("GCS_RAW_PREFIX", "gharchive/raw")
GCS_PROCESSED_PREFIX = os.getenv("GCS_PROCESSED_PREFIX", "gharchive/processed")
HOUR_FILE_LAG_HOURS = int(os.getenv("GHARCHIVE_HOUR_FILE_LAG_HOURS", "3"))

def current_available_utc_date() -> date:
    dt = datetime.now(timezone.utc) - timedelta(hours=HOUR_FILE_LAG_HOURS)
    return dt.date()


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid date '{value}'. Expected YYYY-MM-DD format.") from exc


def generate_date_range(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def parse_args() -> argparse.Namespace:
    end_default = current_available_utc_date()
    start_default = end_default - timedelta(days=13)

    parser = argparse.ArgumentParser(
        description="Transform GH Archive raw files from GCS and write processed parquet."
    )
    parser.add_argument("--start-date", default=start_default.isoformat())
    parser.add_argument("--end-date", default=end_default.isoformat())
    parser.add_argument(
        "--skip-if-processed-exists",
        action="store_true",
        help="Skip if both processed watch and fork outputs already exist.",
    )
    return parser.parse_args()


def build_spark(*, app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def raw_gcs_glob_for_date(target_date: str) -> str:
    year, month, day = target_date.split("-")
    return (
        f"gs://{GCS_BUCKET_NAME}/{GCS_RAW_PREFIX.strip('/')}/"
        f"year={year}/month={month}/day={day}/hour=*/*.json.gz"
    )


def processed_gcs_path_for_date(target_date: str, event_type: str) -> str:
    prefix = GCS_PROCESSED_PREFIX.strip("/")
    return f"gs://{GCS_BUCKET_NAME}/{prefix}/{event_type}/event_date={target_date}/"


def build_watch_df(df_raw: DataFrame) -> DataFrame:
    return (
        df_raw.filter(F.col("type") == "WatchEvent")
        .withColumn("event_ts", F.to_timestamp("created_at"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            "event_ts",
            "event_date",
            "event_hour",
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


def build_fork_df(df_raw: DataFrame) -> DataFrame:
    return (
        df_raw.filter(F.col("type") == "ForkEvent")
        .withColumn("event_ts", F.to_timestamp("created_at"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            "event_ts",
            "event_date",
            "event_hour",
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


def gcs_path_exists(spark: SparkSession, gcs_path: str) -> bool:
    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(gcs_path)
    fs = path.getFileSystem(hadoop_conf)
    return fs.exists(path)


def processed_outputs_exist_for_date(spark: SparkSession, target_date: str) -> bool:
    watch_output = processed_gcs_path_for_date(target_date, "watch")
    fork_output = processed_gcs_path_for_date(target_date, "fork")
    return gcs_path_exists(spark, watch_output) and gcs_path_exists(spark, fork_output)


def transform_one_date(
    spark: SparkSession,
    target_date: str,
    skip_if_processed_exists: bool = False,
) -> None:
    print(f"Starting transform for date: {target_date}")

    if skip_if_processed_exists and processed_outputs_exist_for_date(spark, target_date):
        print(f"Skipping {target_date}: processed outputs already exist.")
        return

    input_path = raw_gcs_glob_for_date(target_date)
    watch_output_path = processed_gcs_path_for_date(target_date, "watch")
    fork_output_path = processed_gcs_path_for_date(target_date, "fork")

    print(f"Reading raw input from: {input_path}")
    df_raw = spark.read.json(input_path)

    df_relevant = df_raw.filter(F.col("type").isin("WatchEvent", "ForkEvent"))

    df_watch = build_watch_df(df_relevant)
    df_fork = build_fork_df(df_relevant)

    print(f"Writing watch output to: {watch_output_path}")
    (
        df_watch.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(watch_output_path)
    )

    print(f"Writing fork output to: {fork_output_path}")
    (
        df_fork.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(fork_output_path)
    )

    print(f"Finished transform for date: {target_date}")


def transform_raw_to_processed_gcs_range(
    start_date: str,
    end_date: str,
    skip_if_processed_exists: bool = False,
) -> None:
    start_dt = parse_iso_date(start_date)
    end_dt = parse_iso_date(end_date)

    if end_dt < start_dt:
        raise ValueError("--end-date must be greater than or equal to --start-date")

    spark = build_spark(app_name="gharchive_events_transform")

    try:
        for dt in generate_date_range(start_dt, end_dt):
            transform_one_date(
                spark=spark,
                target_date=dt.isoformat(),
                skip_if_processed_exists=skip_if_processed_exists,
            )
    finally:
        spark.stop()


def main() -> None:
    args = parse_args()
    transform_raw_to_processed_gcs_range(
        start_date=args.start_date,
        end_date=args.end_date,
        skip_if_processed_exists=args.skip_if_processed_exists,
    )


if __name__ == "__main__":
    main()