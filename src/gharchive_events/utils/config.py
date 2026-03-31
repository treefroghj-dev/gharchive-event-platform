import os


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


GCP_PROJECT_ID = require_env("GHARCHIVE_GCP_PROJECT_ID")
GCS_BUCKET_NAME = require_env("GHARCHIVE_GCS_BUCKET_NAME")
GCS_PREFIX = os.getenv("GHARCHIVE_GCS_PREFIX", "gharchive/raw")
BQ_DATASET = os.getenv("GHARCHIVE_BQ_DATASET", "gharchive")
BQ_TEMP_GCS_BUCKET = os.getenv("GHARCHIVE_BQ_TEMP_GCS_BUCKET")
LOCAL_TMP_DIR = os.getenv("LOCAL_TMP_DIR", "/tmp/gharchive")
GOOGLE_APPLICATION_CREDENTIALS = require_env("GOOGLE_APPLICATION_CREDENTIALS")