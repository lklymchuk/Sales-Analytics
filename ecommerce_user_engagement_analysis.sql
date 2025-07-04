-- Step 1: Aggregate unique accounts by date, country, send interval, verification, and unsubscribed status
with account_metrics as (
    select
         s.date,
         sp.country,
         ac.send_interval,
         ac.is_verified,
         ac.is_unsubscribed,
         count(distinct ac.id) as account_cnt
    from data-analytics-mate.DA.account_session acs
    join DA.account ac
      on acs.account_id = ac.id
    join DA.session s
      on acs.ga_session_id = s.ga_session_id
    join DA.session_params sp
      on acs.ga_session_id = sp.ga_session_id
    group by s.date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed
),

-- Step 2: Calculate email metrics (sent, opened, visited) grouped by same parameters
email_metrics as (
    select
         date_add(s.date, interval es.sent_date day) as sent_date,
         sp.country,
         ac.send_interval,
         ac.is_verified,
         ac.is_unsubscribed,
         count(distinct es.id_message) as sent_msg,
         count(distinct eo.id_message) as open_msg,
         count(distinct ev.id_message) as visit_msg
    from DA.email_sent es
    join DA.account ac
      on es.id_account = ac.id
    left join DA.email_open eo
      on es.id_message = eo.id_message
    left join DA.email_visit ev
      on es.id_message = ev.id_message
    join DA.account_session acs
      on es.id_account = acs.account_id
    join DA.session s
      on acs.ga_session_id = s.ga_session_id
    join DA.session_params sp
      on acs.ga_session_id = sp.ga_session_id
    group by sent_date, sp.country, ac.send_interval, ac.is_verified, ac.is_unsubscribed
),

-- Step 3: Combine account and email metrics with UNION ALL, filling missing columns with zeros to align schema
union_data as (
    select
         date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         account_cnt,
         0 as sent_msg,
         0 as open_msg,
         0 as visit_msg
    from account_metrics
    union all
    select
         sent_date as date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         0 as account_cnt,
         sent_msg,
         open_msg,
         visit_msg
    from email_metrics
),

-- Step 4: Aggregate sums by grouping parameters to get final combined metrics
final as (
    select
         date,
         country,
         send_interval,
         is_verified,
         is_unsubscribed,
         sum(account_cnt) as account_cnt,
         sum(sent_msg) as sent_msg,
         sum(open_msg) as open_msg,
         sum(visit_msg) as visit_msg
    from union_data
    group by date, country, send_interval, is_verified, is_unsubscribed
),

-- Step 5: Calculate ranks of countries based on total accounts and total sent messages
rank_part as (
    select
         *,
         dense_rank() over (order by total_country_account_cnt desc) as rank_total_country_account_cnt,
         dense_rank() over (order by total_country_sent_cnt desc) as rank_total_country_sent_cnt
    from (
        select
            *,
            sum(account_cnt) over (partition by country) as total_country_account_cnt,
            sum(sent_msg) over (partition by country) as total_country_sent_cnt
        from final
    ) as total_part
)

-- Final select: output only records where rank is within top 10 by accounts or sent messages
select *
from rank_part
where rank_total_country_account_cnt <= 10 or rank_total_country_sent_cnt <= 10;
