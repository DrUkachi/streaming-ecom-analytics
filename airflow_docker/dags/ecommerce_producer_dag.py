from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from ecomm_pipeline.snowflake_validate import validate_data, materialize_views

default_args = {
    'owner': 'ecommerce_analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ecommerce_full_pipeline',
    default_args=default_args,
    description='Run Kafka producer, then validate and refresh Snowflake views',
    schedule_interval="0 */2 * * *",  # Every 2 hours
    catchup=False,
    tags=['ecommerce', 'snowflake', 'kafka'],
) as dag:

    run_producer = BashOperator(
        task_id='run_ecomm_kafka_producer',
        bash_command='spark-submit /opt/airflow/dags/ecomm_pipeline/ecomm_producer.py',
    )

    validate = PythonOperator(
        task_id='validate_snowflake_view_data',
        python_callable=validate_data,
    )

    refresh = PythonOperator(
        task_id='materialize_views',
        python_callable=materialize_views,
    )

    # Set dependencies
    run_producer >> validate >> refresh
