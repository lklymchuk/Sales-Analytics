--створення окремого CTE для визначення кількості унікальних аккаунтів в розрізі параметрів date, country, send_interval, is_verified, is_unsubscribed


with account_metrics as (
select
     s.date,
     sp.country,
     ac.send_interval,
     ac.is_verified,
     ac.is_unsubscribed,
     count(distinct ac.id) as account_cnt,
from `data-analytics-mate.DA.account_session` acs
join `DA.account` ac
on acs.account_id = ac.id
join `DA.session` s
on acs.ga_session_id = s.ga_session_id
join `DA.session_params` sp
on acs.ga_session_id = sp.ga_session_id
group by 1, 2, 3, 4, 5
),


--визначення емейл-метрик, таких як: дата відправки, кількість відправлених, відкритих та відвіданих повідомлень, в розрізі параметрів date, country, send_interval, is_verified, is_unsubscribed


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
from `DA.email_sent` es
join `DA.account` ac
on es.id_account = ac.id
left join `DA.email_open` eo
on es.id_message = eo.id_message
left join `DA.email_visit` ev
on es.id_message = ev.id_message
join `DA.account_session` acs
on es.id_account = acs.account_id
join `DA.session` s
on acs.ga_session_id = s.ga_session_id
join `DA.session_params` sp
on acs.ga_session_id = sp.ga_session_id
group by 1, 2, 3, 4, 5
),


--обʼєднання account_metrics та email_metrics за допомогою union all та присвоєння значення 0 колонкам, яких бракує, для узгодження порядку та типів даних


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


--групування даних в розрізі параметрів date, country, send_interval, is_verified, is_unsubscribed, обчислюючи загальну кількість аккаунтів, надісланих, відкритих та відвіданих повідомлень


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
group by 1, 2, 3, 4, 5
),


--визначення рейтингу країн за кількістю підписників та відправлених листів за допомогою віконних функцій


rank_part as (
select
     *,
     dense_rank() over (order by total_country_account_cnt desc) as rank_total_country_account_cnt,
     dense_rank() over (order by total_country_sent_cnt desc) as rank_total_country_sent_cnt
from (
  select *,
         sum(account_cnt) over (partition by country) as total_country_account_cnt,
         sum(sent_msg) over (partition by country) as total_country_sent_cnt,
  from final
) as total_part
)


--виведення лише тих даних, де ранк менше або дорівнює 10


select *
from rank_part
where rank_total_country_account_cnt <= 10 or rank_total_country_sent_cnt <= 10


