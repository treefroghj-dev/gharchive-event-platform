from __future__ import annotations

from datetime import datetime, timedelta, UTC
import argparse 

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from gharchive_events.utils.config import (
    GCP_PROJECT_ID,
    GCS_BUCKET_NAME,
    GCS_PREFIX,
    BQ_DATASET,
    BQ_TEMP_GCS_BUCKET,
    GOOGLE_APPLICATION_CREDENTIALS
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


def build_spark(include_bq: bool = True) -> SparkSession:
    packages = [
        "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11",
        "com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.1",
    ]

    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("gharchive_events_transform")
        .set("spark.jars.packages", ",".join(packages))
        .set(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .set(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
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
    df.write.format("bigquery") \
            .option("table", f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}") \
            .option("temporaryGcsBucket", BQ_TEMP_GCS_BUCKET) \
            .mode("overwrite") \
            .save()
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
    
    spark = build_spark()

    try: 
        df_raw = spark.read.json(input_paths)

        df_watch = transform_watch_events(df_raw)
        df_fork = transform_fork_events(df_raw)    
        write_to_bq(df_watch, table_name="source_watch_events",)
        write_to_bq(df_fork, table_name="source_fork_events",
        )
    finally:
        spark.stop()

def main():
    spark_create_source_tbl()

if __name__ == "__main__":
    main()