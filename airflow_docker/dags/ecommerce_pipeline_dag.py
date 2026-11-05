from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import os
from pyspark.sql import SparkSession

# Import your modules
from ecomm_pipeline.extractor import MonthlyExtractor
from ecomm_pipeline.producer import send_to_kafka

# Constants
DATA_FOLDER = "/opt/data"

# dbt project mounted from ../dbt; artifacts go to /tmp so the mount stays clean
DBT = (
    "cd /opt/airflow/dbt && /opt/airflow/dbt_venv/bin/dbt"
)
DBT_FLAGS = "--profiles-dir . --target prod --target-path /tmp/dbt_target --log-path /tmp/dbt_logs"

# Default DAG args
default_args = {
    'owner': 'ecommerce_analytics',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_extraction(**context):
    extractor = MonthlyExtractor()
    result = extractor.extract()
    if result is None:
        raise ValueError("Extraction failed or no data extracted.")
    # Push output path and month info to XCom for downstream tasks
    context['ti'].xcom_push(key='output_path', value=result['output_path'])
    context['ti'].xcom_push(key='processed_month', value=result['processed_month'])

def send_data_to_kafka(**context):
    output_path = context['ti'].xcom_pull(key='output_path', task_ids='extract_monthly_data')
    if output_path is None or not os.path.exists(output_path):
        raise FileNotFoundError(f"Extracted data file not found: {output_path}")

    spark = SparkSession.builder.appName("KafkaProducerLoader").getOrCreate()
    df = spark.read.parquet(output_path)
    if df.count() == 0:
        print("⚠️ No records found in extracted parquet to send to Kafka.")
        return

    send_to_kafka(df)
    spark.stop()

with DAG(
    dag_id='ecommerce_monthly_extraction_and_kafka',
    description='Monthly extraction of ecommerce events and sending to Kafka',
    schedule_interval='0 */3 * * *',  # Adjust schedule as needed
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['ecommerce', 'spark', 'kafka', 'dbt'],
) as dag:

    extract_monthly_data = PythonOperator(
        task_id='extract_monthly_data',
        python_callable=run_extraction,
        provide_context=True,
    )

    send_to_kafka_task = PythonOperator(
        task_id='send_to_kafka',
        python_callable=send_data_to_kafka,
        provide_context=True,
    )

    # Fails if the Kafka sink has not landed new events recently
    dbt_source_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=f"{DBT} source freshness {DBT_FLAGS}",
    )

    # Builds staging and reporting models and runs every data test in DAG order:
    # bad source data stops the run before it reaches the dashboard tables
    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command=f"{DBT} build {DBT_FLAGS}",
    )

    # Set dependencies

    extract_monthly_data >> send_to_kafka_task >> dbt_source_freshness >> dbt_build
