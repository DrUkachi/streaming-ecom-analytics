import snowflake.connector
from utils.config import Config
from utils.snowflake_refresh import REFRESH_QUERIES

def connect_to_snowflake():
    return snowflake.connector.connect(
        user=Config.SNOWFLAKE_USERNAME,
        password=Config.SNOWFLAKE_PASSWORD,
        account=Config.SNOWFLAKE_ACCOUNT,
        warehouse=Config.SNOWFLAKE_WAREHOUSE,
        database=Config.SNOWFLAKE_DATABASE,
        schema=Config.SNOWFLAKE_SCHEMA
    )

def validate_data():
    conn = connect_to_snowflake()
    try:
        # Record Count Validation
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {Config.SNOWFLAKE_VIEW_NAME}")
        count = cursor.fetchone()[0]
        print(f"✅ Record count: {count}")
        cursor.close()

        # Null Validation
        cursor = conn.cursor()
        null_query = f"""
            SELECT COUNT(*) FROM {Config.SNOWFLAKE_VIEW_NAME}
            WHERE product_id IS NULL 
              OR category_id IS NULL 
              OR event_time IS NULL 
              OR user_id IS NULL
        """
        cursor.execute(null_query)
        null_count = cursor.fetchone()[0]
        print(f"✅ Null check: {null_count} nulls found")
        cursor.close()

        # Latest Date Check
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(create_time::DATE) FROM {Config.SNOWFLAKE_VIEW_NAME}")
        latest_date = cursor.fetchone()[0]
        print(f"✅ Latest create time: {latest_date}")
        cursor.close()

    finally:
        conn.close()

def materialize_views():
    conn = connect_to_snowflake()
    try:
        for table_name, sql in REFRESH_QUERIES.items():
            print(f"🔄 Materializing: {table_name}")
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                print(f"✅ Successfully refreshed: {table_name}")
            except Exception as e:
                print(f"❌ Error refreshing {table_name}: {e}")
            finally:
                cursor.close()
    finally:
        conn.close()
