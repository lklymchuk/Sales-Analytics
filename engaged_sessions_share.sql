-- Common Table Expression (CTE) to extract session engaged events
with session_engaged_events as (
    select
        ep.ga_session_id,
        params.value.string_value as session_engaged_value
    from data-analytics-mate.DA.event_params ep,
         unnest(event_params) as params
    where params.key = 'session_engaged'  -- Filter only 'session_engaged' parameter
)

-- Calculate the share of engaged sessions by device type
select
    device,
    -- Calculate percentage of sessions where session_engaged_value = '1' among all sessions with non-null session_engaged_value
    count(case when session_engaged_value = '1' then se.ga_session_id end) /
    count(case when session_engaged_value is not null then se.ga_session_id end) * 100 as engaged_sessions_share
from session_engaged_events se
join DA.session_params sp
  on se.ga_session_id = sp.ga_session_id  -- Join to get device info
group by device
order by engaged_sessions_share desc;  -- Order by highest engagement share
