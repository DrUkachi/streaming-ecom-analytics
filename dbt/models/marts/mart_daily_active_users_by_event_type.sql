{{ config(alias='DAILY_ACTIVE_USERS_BY_EVENT_TYPE') }}

-- The original job counted distinct sessions and called them users.
-- Users and sessions are now reported separately.

select
    event_date,
    event_type,
    count(distinct user_id)          as daily_active_users,
    count(distinct user_session)     as daily_sessions
from {{ ref('stg_kafka__ecommerce_events') }}
group by event_date, event_type
