FROM apache/airflow:2.10.5

USER root

# Update package lists and install Java
RUN apt-get update && apt-get install -y default-jre-headless

# Switch to airflow user to install Python packages
USER airflow

# Install PySpark
RUN pip install pyspark
