{{ config(alias='AVERAGE_ORDER_VALUE_DAILY') }}

-- An "order" is a session with at least one purchase, as in the original job.

select
    event_date,
    round({{ safe_divide('sum(price)', 'count(distinct user_session)') }}, 2) as average_order_value,
    count(distinct user_session)                                              as orders
from {{ ref('stg_kafka__ecommerce_events') }}
where event_type = 'purchase'
group by event_date
