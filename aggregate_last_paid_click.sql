with last_paid_click as (
    select
        s.visitor_id,
        s.visit_date::date as visit_date,
        s.source as utm_source,
        s.medium as utm_medium,
        s.campaign as utm_campaign,
        row_number() over (
            partition by s.visitor_id
            order by
                case
                    when
                        s.medium in (
                            'cpc',
                            'cpm',
                            'cpa',
                            'youtube',
                            'cpp',
                            'tg',
                            'social'
                        )
                        then 1
                    else 0
                end desc,
                s.visit_date desc
        ) as rn
    from sessions as s
),

attributed_visits as (
    select
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    from last_paid_click
    where
        rn = 1
        and utm_medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

ads_cost as (
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
),

aggregated as (
    select
        av.visit_date,
        av.utm_source,
        av.utm_medium,
        av.utm_campaign,
        count(*) as visitors_count,
        count(l.lead_id) as leads_count,
        sum(
            case
                when
                    l.closing_reason = 'Успешно реализовано'
                    then 1
                else 0
            end
        ) as purchases_count,
        sum(
            case
                when
                    l.closing_reason = 'Успешно реализовано'
                    then l.amount
                else 0
            end
        ) as revenue
    from attributed_visits as av
    left join leads as l
        on
            av.visitor_id = l.visitor_id
            and l.created_at::date >= av.visit_date
    group by 1, 3, 4, 5
)

select
    a.visit_date,
    a.visitors_count,
    a.utm_source,
    a.utm_medium,
    a.utm_campaign,
    c.total_cost,
    a.leads_count,
    a.purchases_count,
    a.revenue
from aggregated as a
left join ads_cost as c
    on
        a.visit_date = c.visit_date
        and a.utm_source = c.utm_source
        and a.utm_medium = c.utm_medium
        and a.utm_campaign = c.utm_campaign
order by
    9 desc nulls last,
    1 asc,
    2 desc,
    3 asc,
    4 asc,
    5 asc
limit 15