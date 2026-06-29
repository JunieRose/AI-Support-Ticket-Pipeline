"""
DAG: support_ticket_pipeline

End-to-end support ticket pipeline:
  1. generate_raw_data   — Generates synthetic ticket CSV and upload to OCI
  2. validate_raw_data   — Creates validated and quarantine CSV
  3. process_ai_enrichment — Enriches tickets via Gemini / TextBlob
  4. load_to_lakehouse   — Merges enriched data into Oracle DB
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.pipelines.generate_raw_data import main as generate_raw_data_task
from src.pipelines.validate_raw_data import main as validate_raw_data_task
from src.pipelines.process_ai_enrichment import main as process_ai_enrichment_task
from src.pipelines.load_to_lakehouse import main as load_to_lakehouse_task


default_args = {
    "owner": "junie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def validate_wrapper(ti, **kwargs):

    timestamp = ti.xcom_pull(
        task_ids="generate_raw_data"
    )

    validate_raw_data_task(
        pipeline_timestamp=timestamp
    )


def enrich_wrapper(ti, **kwargs):

    timestamp = ti.xcom_pull(
        task_ids="generate_raw_data"
    )

    process_ai_enrichment_task(
        pipeline_timestamp=timestamp
    )


def lakehouse_wrapper(ti, **kwargs):

    timestamp = ti.xcom_pull(
        task_ids="generate_raw_data"
    )

    load_to_lakehouse_task(
        pipeline_timestamp=timestamp
    )


with DAG(
    dag_id="support_ticket_pipeline",
    default_args=default_args,
    description="End-to-end support ticket pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["portfolio", "oci", "ai"],
) as dag:

    generate_raw_data = PythonOperator(
        task_id="generate_raw_data",
        python_callable=generate_raw_data_task
    )

    validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_wrapper
    )

    process_ai_enrichment = PythonOperator(
        task_id="process_ai_enrichment",
        python_callable=enrich_wrapper
    )

    load_to_lakehouse = PythonOperator(
        task_id="load_to_lakehouse",
        python_callable=lakehouse_wrapper
    )

    (
        generate_raw_data
        >> validate_raw_data
        >> process_ai_enrichment
        >> load_to_lakehouse
    )