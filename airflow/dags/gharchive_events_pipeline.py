from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

from gharchive_events.ingestion.gharchive_to_gcs import ingest_gharchive_to_gcs
from gharchive_events.transform.gharchive_events_transform import spark_create_source_tbl


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
}

# set the schedule as None to only run the dag manually, if run the day daily
# base, set the schedule to schedule="@daily"

with DAG(
    dag_id="gharchive_events_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    default_args=default_args,
    catchup=False,
) as dag:
    ingest_task = BashOperator(
        task_id="ingest_gharchive_to_gcs",
        bash_command=(
            "cd /opt/app && "
            "PYTHONPATH=. python -m gharchive_events.ingestion.gharchive_to_gcs "
            "--skip-if-exists"
        ),
    )

    transform_task = BashOperator(
        task_id="transform_raw_to_bq_source",
        bash_command=(
            "cd /opt/app/src && "
            "PYTHONPATH=. python -m gharchive_events.transform.gharchive_events_transform "
            "--execution-date {{ ds }}"
        ),
    )

    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/app/gharchive_dbt && dbt run --profiles-dir /opt/app/gharchive_dbt",
    )

    #if only test ingest_task, run
    ingest_task >> transform_task >> dbt_run_task
    