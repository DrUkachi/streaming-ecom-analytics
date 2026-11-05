{{ config(materialized='view') }}

-- One clean row per event. Types are cast once here so every mart
-- downstream shares the same definitions.

with source as (

    select * from {{ source('kafka', 'ecommerce_events') }}

),

renamed as (

    select
        cast(event_time as timestamp)                    as event_at,
        cast(event_time as date)                         as event_date,
        lower(trim(event_type))                          as event_type,
        cast(product_id as varchar)                      as product_id,
        cast(category_id as varchar)                     as category_id,
        nullif(trim(category_code), '')                  as category_code,
        nullif(lower(trim(brand)), '')                   as brand,
        cast(coalesce(price, 0) as double)               as price,
        cast(user_id as varchar)                         as user_id,
        cast(user_session as varchar)                    as user_session,
        cast(create_time as timestamp)                   as loaded_at
    from source

),

deduplicated as (

    -- The producer can resend a batch on retry. Keep one copy of each event.
    select *
    from renamed
    qualify row_number() over (
        partition by event_at, event_type, product_id, user_id, user_session
        order by loaded_at desc
    ) = 1

)

select
    md5(
        cast(event_at as varchar) || '|' || event_type || '|' || product_id
        || '|' || user_id || '|' || user_session
    )                                                    as event_id,
    *
from deduplicated
