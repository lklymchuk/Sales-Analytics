-- Select distinct month, account ID, percentage of messages sent in the month,
-- first and last send dates per account
select distinct
     sent_month,
     id_account,
     sent_msg_percent_from_this_month,  -- Percentage of emails sent by this account in the month
     first_sent_date,                   -- First date this account sent an email in the month
     last_sent_date                    -- Last date this account sent an email in the month
from (
    select
         sent_month,
         id_account,
         -- Calculate cumulative sum of email counts for accounts within the month,
         -- divided by total email count for the month, multiplied by 100 to get percent
         sum(email_cnt) over (partition by sent_month order by id_account) /
         sum(email_cnt) over (partition by sent_month) * 100
         as sent_msg_percent_from_this_month,

         -- Minimum send date per account per month
         min(sent_date) over (partition by sent_month, id_account) as first_sent_date,

         -- Maximum send date per account per month
         max(sent_date) over (partition by sent_month, id_account) as last_sent_date
    from (
        select
             -- Calculate actual send date by adding sent_date offset days to session date
             date_add(s.date, interval es.sent_date DAY) as sent_date,

             -- Extract year and month from calculated send date, set day=1 to get month start
             date(
                 extract(year from date_add(s.date, interval es.sent_date DAY)),
                 extract(month from date_add(s.date, interval es.sent_date DAY)),
                 1
             ) as sent_month,

             id_account,

             -- Count distinct email messages sent by account on that date
             count(distinct es.id_message) as email_cnt
        from data-analytics-mate.DA.email_sent es
        join data-analytics-mate.DA.account_session acs
          on es.id_account = acs.account_id
        join data-analytics-mate.DA.session s
          on acs.ga_session_id = s.ga_session_id
        group by s.date, es.sent_date, id_account
    ) date_snt
) sent_msg_percent
order by sent_month, id_account;
