-- No funnel step can reach more sessions than there are sessions in total.

select f.event_type, f.sessions
from {{ ref('mart_conversion_funnel') }} f
where f.sessions > (select count(*) from {{ ref('int_sessions') }})
