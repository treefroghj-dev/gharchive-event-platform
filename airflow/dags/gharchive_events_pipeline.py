from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from google.cloud import storage

from gharchive_events.utils.config import (
    DATAPROC_SERVICE_ACCOUNT,
    GCP_PROJECT_ID,
    GCP_REGION,
    GCS_BUCKET_NAME,
    GCS_PROCESSED_PREFIX,
    GCS_RAW_PREFIX,
    PIPELINE_DATA_DATE_LAG_DAYS,
)


def data_partition_date(ds: str) -> str:
    day = datetime.strptime(ds, "%Y-%m-%d").date()
    return (day - timedelta(days=PIPELINE_DATA_DATE_LAG_DAYS)).isoformat()


def upload_transform_script_to_gcs() -> None:
    local_path = "/opt/app/src/gharchive_events/transform/gharchive_events_transform.py"
    blob_name = "jobs/gharchive_events_transform.py"

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

    print(f"Uploaded {local_path} to gs://{GCS_BUCKET_NAME}/{blob_name}")


PROJECT_ID = GCP_PROJECT_ID
REGION = GCP_REGION
TRANSFORM_SCRIPT_GCS_URI = f"gs://{GCS_BUCKET_NAME}/jobs/gharchive_events_transform.py"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}


with DAG(
    dag_id="gharchive_events_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"data_partition_date": data_partition_date},
) as dag:

    ingest_task = BashOperator(
        task_id="ingest_gharchive_to_gcs",
        bash_command=(
            "cd /opt/app && "
            "PYTHONPATH=/opt/app/src:/opt/lib:/opt/venv/lib/python3.11/site-packages "
            "python -m gharchive_events.ingestion.gharchive_to_gcs "
            "--skip-if-exists"
        ),
    )

    upload_transform_script = PythonOperator(
        task_id="upload_transform_script_to_gcs",
        python_callable=upload_transform_script_to_gcs,
    )

    transform_tasks = []

    for days_back in range(13, -1, -1):
        target_date_tmpl = f"{{{{ data_partition_date(macros.ds_add(ds, -{days_back})) }}}}"

        transform_task = DataprocCreateBatchOperator(
            task_id=f"transform_d_minus_{days_back}",
            project_id=PROJECT_ID,
            region=REGION,
            batch_id=(
                f"gharchive-transform-d{days_back}-"
                + "{{ ts_nodash | lower }}-{{ ti.try_number }}"
            ),
            batch={
                "pyspark_batch": {
                    "main_python_file_uri": TRANSFORM_SCRIPT_GCS_URI,
                    "args": [
                        "--start-date",
                        target_date_tmpl,
                        "--end-date",
                        target_date_tmpl,
                        "--gcs-bucket-name",
                        GCS_BUCKET_NAME,
                        "--gcs-raw-prefix",
                        GCS_RAW_PREFIX,
                        "--gcs-processed-prefix",
                        GCS_PROCESSED_PREFIX,
                        "--skip-if-processed-exists",
                    ],
                },
                "environment_config": {
                    "execution_config": {
                        "service_account": DATAPROC_SERVICE_ACCOUNT,
                    },
                },
                "runtime_config": {
                    "version": "2.2",
                },
                "labels": {
                    "pipeline": "gharchive",
                    "day_offset": str(days_back),
                },
            },
            deferrable=True,
        )

        transform_tasks.append(transform_task)

    load_watch_events = BashOperator(
        task_id="load_watch_to_bq",
        bash_command=(
            "cd /opt/app && "
            "python /opt/app/src/gharchive_events/load/load_processed_to_bq.py "
            "--event-type watch "
            "--start-date {{ data_partition_date(macros.ds_add(ds, -13)) }} "
            "--end-date {{ data_partition_date(ds) }} "
            "--write-disposition WRITE_TRUNCATE"
        ),
    )

    load_fork_events = BashOperator(
        task_id="load_fork_to_bq",
        bash_command=(
            "cd /opt/app && "
            "python /opt/app/src/gharchive_events/load/load_processed_to_bq.py "
            "--event-type fork "
            "--start-date {{ data_partition_date(macros.ds_add(ds, -13)) }} "
            "--end-date {{ data_partition_date(ds) }} "
            "--write-disposition WRITE_TRUNCATE"
        ),
    )

    _dbt_vars_cli = "--vars '{\"anchor_date\": \"{{ ds }}\"}'"

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=(
            "set -eu\n"
            "cd /opt/app/gharchive_dbt || exit 1\n"
            "dbt test --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select source:gharchive_events\n"
            "dbt run --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_staging\n"
            "dbt test --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_staging\n"
        ),
    )

    dbt_run_fct = BashOperator(
        task_id="dbt_run_fct",
        bash_command=(
            "set -eu\n"
            "cd /opt/app/gharchive_dbt || exit 1\n"
            "dbt run --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_fct\n"
        ),
    )

    dbt_test_fct = BashOperator(
        task_id="dbt_test_fct",
        bash_command=(
            "set -eu\n"
            "cd /opt/app/gharchive_dbt || exit 1\n"
            "dbt test --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_fct\n"
        ),
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "set -eu\n"
            "cd /opt/app/gharchive_dbt || exit 1\n"
            "dbt run --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_marts\n"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            "set -eu\n"
            "cd /opt/app/gharchive_dbt || exit 1\n"
            "dbt test --profiles-dir /opt/app/gharchive_dbt "
            + _dbt_vars_cli
            + " --select tag:layer_marts\n"
        ),
    )

    ingest_task >> upload_transform_script >> transform_tasks[0]

    for i in range(len(transform_tasks) - 1):
        transform_tasks[i] >> transform_tasks[i + 1]

    transform_tasks[-1] >> load_watch_events >> load_fork_events

    (
        load_fork_events
        >> dbt_test_staging
        >> dbt_run_fct
        >> dbt_test_fct
        >> dbt_run_marts
        >> dbt_test_marts
    )