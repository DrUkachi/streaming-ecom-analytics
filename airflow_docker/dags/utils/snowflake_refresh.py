# snowflake_refresh.py
from utils.config import Config


REFRESH_QUERIES = {
    "DAILY_REVENUE": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.DAILY_REVENUE;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.DAILY_REVENUE
        SELECT
          DATE_TRUNC('DAY', event_time) AS event_date,
          ROUND(SUM(price), 2) AS total_revenue
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        WHERE event_type = 'purchase'
        GROUP BY event_date;
    """,

    "TOP_SELLING_BRANDS": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.TOP_SELLING_BRANDS;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.TOP_SELLING_BRANDS
        SELECT
          brand,
          ROUND(SUM(price), 2) AS revenue
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        WHERE event_type = 'purchase'
        GROUP BY brand
        ORDER BY revenue DESC
        LIMIT 10;
    """,

    "CONVERSION_FUNNEL": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.CONVERSION_FUNNEL;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.CONVERSION_FUNNEL
        SELECT
          event_type,
          COUNT(*) AS event_count
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        WHERE event_type IN ('view', 'cart', 'purchase')
        GROUP BY event_type;
    """,

    "ABANDONED_CART_USERS": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.ABANDONED_CART_USERS;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.ABANDONED_CART_USERS
        SELECT
          user_id,
          COUNT(*) AS cart_events
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        WHERE event_type = 'cart'
          AND user_id NOT IN (
            SELECT DISTINCT user_id
            FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
            WHERE event_type = 'purchase'
          )
        GROUP BY user_id
        ORDER BY cart_events DESC;
    """,

    "AVERAGE_ORDER_VALUE_DAILY": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.AVERAGE_ORDER_VALUE_DAILY;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.AVERAGE_ORDER_VALUE_DAILY
        SELECT
          DATE_TRUNC('DAY', event_time) AS event_date,
          ROUND(SUM(price) / COUNT(DISTINCT user_session), 2) AS average_order_value
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        WHERE event_type = 'purchase'
        GROUP BY event_date;
    """,

    "DAILY_ACTIVE_USERS": f"""
        TRUNCATE TABLE {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.DAILY_ACTIVE_USERS_BY_EVENT_TYPE;
        INSERT INTO {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.DAILY_ACTIVE_USERS_BY_EVENT_TYPE
        SELECT
          DATE_TRUNC('DAY', event_time) AS event_date,
          event_type,
          COUNT(DISTINCT user_session) AS daily_active_users
        FROM {Config.SNOWFLAKE_DATABASE}.{Config.SNOWFLAKE_SCHEMA}.VIEW_KAFKA_ECOMMERCE_STREAM
        GROUP BY event_date, event_type
        ORDER BY event_date DESC;
    """,
}
