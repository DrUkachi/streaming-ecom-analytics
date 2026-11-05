FROM apache/airflow:2.10.5

USER root

# Update package lists and install Java
RUN apt-get update && apt-get install -y default-jre-headless

# Switch to airflow user to install Python packages
USER airflow

# Install PySpark
RUN pip install pyspark

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's
RUN python -m venv /opt/airflow/dbt_venv \
    && /opt/airflow/dbt_venv/bin/pip install --no-cache-dir "dbt-core~=1.9.0" "dbt-snowflake~=1.9.0"
