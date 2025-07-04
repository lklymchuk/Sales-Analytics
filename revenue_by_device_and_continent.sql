with revenue_usd as (
select
     sp.continent,
     sum(p.price) as revenue,
     sum(case when device = 'mobile' then p.price end) as revenue_from_mobile,
     sum(case when device = 'desktop' then p.price end) as revenue_from_desktop,
from `data-analytics-mate.DA.order` o
join `DA.product` p
on o.item_id = p.item_id
join `DA.session_params` sp
on o.ga_session_id = sp.ga_session_id
group by sp.continent
),

registration as (
select
     sp.continent,
     count(acs.account_id) as account_count,
     count(case when is_verified = 1 then id end) as verified_account,
     count(sp.ga_session_id) as session_count
from `DA.session_params` sp
left join `DA.account_session` acs
on sp.ga_session_id = acs.ga_session_id
left join `DA.account` ac
on acs.account_id = ac.id
group by sp.continent
)

select
     registration.continent,

     revenue_usd.revenue,
     revenue_usd.revenue_from_mobile,
     revenue_usd.revenue_from_desktop,
     (revenue / SUM(revenue) OVER ()) * 100 AS revenue_percent_from_total,

     registration.account_count,
     registration.verified_account,
     registration.session_count 
from registration
left join revenue_usd
on registration.continent = revenue_usd.continent
