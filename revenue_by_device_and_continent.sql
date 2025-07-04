-- Calculate revenue and revenue breakdown by device for each continent
with revenue_usd as (
    select
         sp.continent,
         sum(p.price) as revenue,  -- Total revenue per continent
         sum(case when device = 'mobile' then p.price else 0 end) as revenue_from_mobile,  -- Revenue from mobile devices
         sum(case when device = 'desktop' then p.price else 0 end) as revenue_from_desktop  -- Revenue from desktop devices
    from data-analytics-mate.DA.order o
    join DA.product p
      on o.item_id = p.item_id
    join DA.session_params sp
      on o.ga_session_id = sp.ga_session_id
    group by sp.continent
),

-- Calculate number of accounts, verified accounts, and sessions per continent
registration as (
    select
         sp.continent,
         count(acs.account_id) as account_count,  -- Total accounts per continent
         count(case when is_verified = 1 then id end) as verified_account,  -- Verified accounts count
         count(sp.ga_session_id) as session_count  -- Total sessions per continent
    from DA.session_params sp
    left join DA.account_session acs
      on sp.ga_session_id = acs.ga_session_id
    left join DA.account ac
      on acs.account_id = ac.id
    group by sp.continent
)

-- Combine revenue and registration data, and calculate revenue share percentage
select
     registration.continent,
     revenue_usd.revenue,
     revenue_usd.revenue_from_mobile,
     revenue_usd.revenue_from_desktop,
     (revenue_usd.revenue / SUM(revenue_usd.revenue) OVER ()) * 100 AS revenue_percent_from_total,  -- Percent of total revenue by continent
     registration.account_count,
     registration.verified_account,
     registration.session_count 
from registration
left join revenue_usd
  on registration.continent = revenue_usd.continent;
