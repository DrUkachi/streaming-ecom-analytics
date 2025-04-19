# config.py
import os

class Config:
    # Kafka Configuration
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "kafka_ecommerce_stream")
    BOOTSTRAP_SERVER = os.getenv("e_com_server")
    CONFLUENT_API_KEY = os.getenv("e_com_key")
    CONFLUENT_API_SECRET = os.getenv("e_com_secret")

    # Spark Configuration
    SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "KafkaPush")

    # Data Path
    PARQUET_PATH = os.getenv("PARQUET_PATH", "")

    # Snowflake Configuration
    SNOWFLAKE_USERNAME = os.getenv("SNOWFLAKE_USERNAME")
    SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    SNOWFLAKE_VIEW_NAME = os.getenv("SNOWFLAKE_VIEW_NAME", "ecommerce_view")
    SNOWFLAKE_TABLE_NAME = os.getenv("SNOWFLAKE_TABLE_NAME", "ecommerce_table")
    SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ecommerce_role")

    # Metabase Configuration
    
    METABASE_USERNAME = os.getenv("METABASE_USERNAME", "your_username")
    METABASE_PASSWORD = os.getenv("METABASE_PASSWORD", "your_password")
    
    
    