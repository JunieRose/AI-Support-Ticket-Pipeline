import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.generate_raw_data import main as generate_raw_data_task
from src.upload_to_staging import main as upload_to_staging_task
from src.process_ai_enrichment import main as process_ai_enrichment_task
from src.load_to_lakehouse import main as load_to_lakehouse_task


default_args = {
    "owner": "junie",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


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

    load_to_staging = PythonOperator(
        task_id="upload_to_staging",
        python_callable=upload_to_staging_task
    )

    process_ai_enrichment = PythonOperator(
        task_id="process_ai_enrichment",
        python_callable=process_ai_enrichment_task
    )

    load_to_lakehouse = PythonOperator(
        task_id="load_to_lakehouse",
        python_callable=load_to_lakehouse_task
    )

    (
        generate_raw_data
        >> load_to_staging
        >> process_ai_enrichment
        >> load_to_lakehouse
    )