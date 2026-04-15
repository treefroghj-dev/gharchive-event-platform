import os


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


GCP_PROJECT_ID = require_env("GHARCHIVE_GCP_PROJECT_ID")
GCP_REGION = os.getenv("GHARCHIVE_GCP_REGION", "us-central1")
GCS_BUCKET_NAME = require_env("GHARCHIVE_GCS_BUCKET_NAME")
GCS_RAW_PREFIX = os.getenv("GHARCHIVE_GCS_RAW_PREFIX", "gharchive/raw")
GCS_PROCESSED_PREFIX = os.getenv("GHARCHIVE_GCS_PROCESSED_PREFIX", "gharchive/processed")
BQ_DATASET = os.getenv("GHARCHIVE_BQ_DATASET", "gharchive")
GOOGLE_APPLICATION_CREDENTIALS = require_env("GOOGLE_APPLICATION_CREDENTIALS")

# Dataproc batch (Airflow DAG). Optional at DAG-parse time; must be set for batches to succeed.
DATAPROC_SERVICE_ACCOUNT = os.getenv("GHARCHIVE_DATAPROC_SERVICE_ACCOUNT", "")
# Days to subtract from logical ds when building partition dates (ingest / transform lag).
PIPELINE_DATA_DATE_LAG_DAYS = int(os.getenv("GHARCHIVE_PIPELINE_DATA_DATE_LAG_DAYS", "1"))