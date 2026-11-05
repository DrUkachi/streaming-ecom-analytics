-- One row per browsing session, with how far it got down the funnel.

select
    user_session,
    min(user_id)                                                      as user_id,
    min(event_at)                                                     as session_started_at,
    cast(min(event_at) as date)                                       as session_date,
    count(*)                                                          as events,
    max(case when event_type = 'view' then 1 else 0 end)              as has_view,
    max(case when event_type = 'cart' then 1 else 0 end)              as has_cart,
    max(case when event_type = 'purchase' then 1 else 0 end)          as has_purchase,
    sum(case when event_type = 'purchase' then price else 0 end)      as purchase_revenue
from {{ ref('stg_kafka__ecommerce_events') }}
group by user_session
