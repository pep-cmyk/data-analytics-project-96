with cte as (
    select
        coalesce(s.source, vk.utm_source, ya.utm_source) as utm_source,
        coalesce(s.medium, vk.utm_medium, ya.utm_medium) as utm_medium,
        coalesce(s.campaign, vk.utm_campaign, ya.utm_campaign) as utm_campaign,
        case
            when
                coalesce(s.medium, vk.utm_medium, ya.utm_medium) = 'organic'
                then 1
            else 2
        end as source,
        row_number()
            over (
                partition by s.visitor_id
                order by
                    s.visit_date desc,
                    coalesce(s.medium, vk.utm_medium, ya.utm_medium) desc
            )
            as rn
    from sessions as s
    left join vk_ads as vk
        on
            s.source = vk.utm_source
            and s.campaign = vk.utm_campaign
            and s.content = vk.utm_content
    left join ya_ads as ya
        on
            s.source = ya.utm_source
            and s.campaign = ya.utm_campaign
            and s.content = ya.utm_content
),

utm_paid as (
    select distinct
        utm_source,
        utm_medium,
        utm_campaign
    from cte
    where source = 2 and rn = 1
),

i_hate_sqlfluff as (
    select
        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        date_trunc('day', s.visit_date) as visit_date,
        sum(l.amount) as revenue,
        count(s.visitor_id) as visitors_count,
        sum(coalesce(vk.daily_spent, ya.daily_spent)) as total_cost,
        count(l.lead_id) as leads_count,
        count(case
            when l.closing_reason = 'Успешно реализовано' then 1
            else 0
        end) as purchases_count
    from sessions as s
    left join utm_paid as u
        on
            s.source = u.utm_source
            and s.medium = u.utm_medium
            and s.campaign = u.utm_campaign
    left join vk_ads as vk
        on
            s.source = vk.utm_source
            and s.campaign = vk.utm_campaign
            and s.content = vk.utm_content
    left join ya_ads as ya
        on
            s.source = ya.utm_source
            and s.campaign = ya.utm_campaign
            and s.content = ya.utm_content
    left join leads as l
        on s.visitor_id = l.visitor_id
    group by 1, 3, 4, 5
    order by
        9 desc nulls last,
        1 asc,
        2 desc,
        3 asc, 4 asc, 5 asc
    limit 15
)

select
    visit_date,
    visitors_count,
    utm_source,
    utm_medium,
    utm_campaign,
    total_cost,
    leads_count,
    purchases_count,
    revenue
from i_hate_sqlfluff