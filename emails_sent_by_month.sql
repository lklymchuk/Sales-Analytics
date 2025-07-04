select distinct
     sent_month,
     id_account,
     sent_msg_percent_from_this_month,
     first_sent_date,
     last_sent_date
from(
select
     sent_month,
     id_account,
     sum(email_cnt) over (partition by sent_month order by id_account) /
     sum(email_cnt) over (partition by sent_month) * 100
     as sent_msg_percent_from_this_month,
     min(sent_date) over (partition by sent_month, id_account) as first_sent_date,
     max(sent_date) over (partition by sent_month, id_account) as last_sent_date
from(
select
     date_add (s.date, interval es.sent_date DAY) as sent_date,
     date(
     extract(year from date_add (s.date, interval es.sent_date DAY)),
     extract(month from date_add (s.date, interval es.sent_date DAY)),
     1) as sent_month,
     id_account,
     count(distinct es.id_message) as email_cnt
from `data-analytics-mate.DA.email_sent` es
join `data-analytics-mate.DA.account_session` acs
on es.id_account = acs.account_id
join `data-analytics-mate.DA.session` s
on acs.ga_session_id = s.ga_session_id
group by s.date, es.sent_date, id_account
) date_snt
) sent_msg_percent
order by 1,2

