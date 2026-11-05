-- Total revenue in the reporting table must match total purchase value in
-- the cleaned events, so the dashboard never silently drops or double counts.

with reporting as (
    select coalesce(sum(total_revenue), 0) as revenue
    from {{ ref('fct_daily_revenue') }}
),

events as (
    select coalesce(round(sum(price), 2), 0) as revenue
    from {{ ref('stg_kafka__ecommerce_events') }}
    where event_type = 'purchase'
)

select reporting.revenue as reporting_revenue, events.revenue as event_revenue
from reporting
cross join events
where abs(reporting.revenue - events.revenue) > 0.05 * (select count(*) from {{ ref('fct_daily_revenue') }})
