{{ config(alias='DAILY_REVENUE') }}

select
    event_date,
    round(sum(price), 2)               as total_revenue,
    count(*)                           as purchase_events,
    count(distinct user_session)       as purchasing_sessions,
    count(distinct user_id)            as purchasing_users
from {{ ref('stg_kafka__ecommerce_events') }}
where event_type = 'purchase'
group by event_date
