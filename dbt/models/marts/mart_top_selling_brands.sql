{{ config(alias='TOP_SELLING_BRANDS') }}

with brand_revenue as (

    select
        coalesce(brand, 'unknown')     as brand,
        round(sum(price), 2)           as revenue,
        count(*)                       as purchase_events
    from {{ ref('stg_kafka__ecommerce_events') }}
    where event_type = 'purchase'
    group by coalesce(brand, 'unknown')

),

ranked as (

    select
        *,
        row_number() over (order by revenue desc, brand) as revenue_rank
    from brand_revenue

)

select brand, revenue, purchase_events, revenue_rank
from ranked
where revenue_rank <= {{ var('top_brands_limit', 10) }}
