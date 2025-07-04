with session_engaged_events as (
SELECT
     ep.ga_session_id,
     params.value.string_value as session_engaged_value,
FROM `data-analytics-mate.DA.event_params` ep, unnest(event_params) as params
where params.key = 'session_engaged'
)
select
    device,
    count(case when session_engaged_value = '1' then se.ga_session_id end) /
    count(case when session_engaged_value is not null then se.ga_session_id end) * 100 as engaged_sessions_share
from session_engaged_events se
join `DA.session_params` sp
on se.ga_session_id = sp.ga_session_id
group by device
order by 2 desc
