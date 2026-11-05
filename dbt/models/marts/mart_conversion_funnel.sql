{{ config(alias='CONVERSION_FUNNEL') }}

-- Keeps the original event_type / event_count columns the dashboard uses,
-- and adds session-level reach and conversion against viewing sessions.

with event_counts as (

    select
        event_type,
        count(*) as event_count
    from {{ ref('stg_kafka__ecommerce_events') }}
    where event_type in ('view', 'cart', 'purchase')
    group by event_type

),

session_reach as (

    select 'view'     as event_type, sum(has_view)     as sessions from {{ ref('int_sessions') }}
    union all
    select 'cart'     as event_type, sum(has_cart)     as sessions from {{ ref('int_sessions') }}
    union all
    select 'purchase' as event_type, sum(has_purchase) as sessions from {{ ref('int_sessions') }}

),

view_sessions as (

    select sessions as view_sessions from session_reach where event_type = 'view'

)

select
    e.event_type,
    case e.event_type when 'view' then 1 when 'cart' then 2 when 'purchase' then 3 end as funnel_step,
    e.event_count,
    s.sessions,
    round({{ safe_divide('s.sessions', 'v.view_sessions') }}, 4)                         as pct_of_view_sessions
from event_counts e
join session_reach s on s.event_type = e.event_type
cross join view_sessions v
