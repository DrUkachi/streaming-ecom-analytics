{{ config(alias='ABANDONED_CART_USERS') }}

-- Users who added to cart but never purchased.
-- Uses NOT EXISTS rather than NOT IN: with NOT IN, a single null user_id in
-- the purchase subquery makes the whole filter return no rows.

with cart_events as (

    select user_id, event_at
    from {{ ref('stg_kafka__ecommerce_events') }}
    where event_type = 'cart'

),

purchasers as (

    select distinct user_id
    from {{ ref('stg_kafka__ecommerce_events') }}
    where event_type = 'purchase'

)

select
    c.user_id,
    count(*)            as cart_events,
    max(c.event_at)     as last_cart_at
from cart_events c
where not exists (
    select 1 from purchasers p where p.user_id = c.user_id
)
group by c.user_id
