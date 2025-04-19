from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import os
from pyspark.sql import SparkSession

# Import your modules
from ecomm_pipeline.extractor import MonthlyExtractor
from ecomm_pipeline.producer import send_to_kafka
from ecomm_pipeline.snowflake import validate_data, materialize_views

# Constants
DATA_FOLDER = "/opt/data"

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
    tags=['ecommerce', 'spark', 'kafka'],
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

    validate = PythonOperator(
        task_id='validate_snowflake_table',
        python_callable=validate_data,
    )

    refresh = PythonOperator(
        task_id='materialize_views',
        python_callable=materialize_views,
    )
    
    # Set dependencies

    extract_monthly_data >> send_to_kafka_task >> validate >> refresh
