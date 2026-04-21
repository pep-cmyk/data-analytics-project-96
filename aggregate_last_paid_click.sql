with cte as (
    select
        s.visitor_id,
        s.visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        row_number() over (
            partition by s.visitor_id
            order by
                case
                    when s.medium in (
                        'cpc',
                        'cpm',
                        'cpa',
                        'youtube',
                        'cpp',
                        'tg',
                        'social'
                    ) then 1
                    else 0
                end desc,
                s.visit_date desc
        ) as rn
    from sessions as s
),

last_paid_click as (
    select
        c.visitor_id,
        c.visit_date::date as visit_date,
        c.utm_source,
        c.utm_medium,
        c.utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    from cte as c
    left join leads as l
        on
            c.visitor_id = l.visitor_id
            and c.visit_date <= l.created_at
    where
        c.rn = 1
        and c.utm_medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

visitors_agg as (
    select
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        count(visitor_id) as visitors_count
    from last_paid_click
    group by 1, 2, 3, 4
),

leads_agg as (
    select
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        count(lead_id) as leads_count,
        count(
            case
                when
                    closing_reason = 'Успешно реализовано'
                    or status_id = 142
                    then 1
            end
        ) as purchases_count,
        sum(
            case
                when
                    closing_reason = 'Успешно реализовано'
                    or status_id = 142
                    then amount
            end
        ) as revenue
    from last_paid_click
    group by 1, 2, 3, 4
),

ads_agg as (
    select
        campaign_date::date as visit_date,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) as total_cost
    from (
        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from vk_ads

        union all

        select
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        from ya_ads
    ) as t
    group by 1, 2, 3, 4
)

select
    v.visit_date,
    v.visitors_count,
    v.utm_source,
    v.utm_medium,
    v.utm_campaign,
    a.total_cost,
    l.leads_count,
    l.purchases_count,
    l.revenue
from visitors_agg as v
left join leads_agg as l
    on
        v.visit_date = l.visit_date
        and v.utm_source = l.utm_source
        and v.utm_medium = l.utm_medium
        and v.utm_campaign = l.utm_campaign
left join ads_agg as a
    on
        v.visit_date = a.visit_date
        and v.utm_source = a.utm_source
        and v.utm_medium = a.utm_medium
        and v.utm_campaign = a.utm_campaign
order by
    9 desc nulls last,
    1 asc,
    2 desc,
    3 asc,
    4 asc,
    5 asc
limit 15;
