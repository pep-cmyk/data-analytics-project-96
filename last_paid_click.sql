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

last_paid_click as (
    select
        visitor_id,
        visit_date,
        utm_source,
        utm_medium,
        utm_campaign
    from cte
    where
        rn = 1
        and utm_medium in (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
)

select
    lpc.visitor_id,
    lpc.visit_date,
    lpc.utm_source,
    lpc.utm_medium,
    lpc.utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
from last_paid_click as lpc
left join leads as l
    on
        lpc.visitor_id = l.visitor_id
        and lpc.visit_date <= l.created_at
order by
    l.amount desc nulls last,
    lpc.visit_date asc,
    lpc.utm_source asc,
    lpc.utm_medium asc,
    lpc.utm_campaign asc
limit 10;
