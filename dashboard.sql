--сквозная аналитика, с которой я собрал половину графиков и презентации
-------------------------------------------------------------------
--считаю посетителей
WITH visitors_count AS (
    SELECT
        visit_date::date AS visit_day,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        count(DISTINCT visitor_id) AS visitors
    FROM sessions
    GROUP BY 1, 2, 3, 4
),

--считаю рекламные расходы
ad_spent AS (
    SELECT
        campaign_date::date AS visit_day,
        utm_source,
        utm_medium,
        utm_campaign,
        sum(daily_spent) AS spent
    FROM (
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            utm_medium,
            utm_campaign,
            daily_spent
        FROM ya_ads
    ) AS t
    GROUP BY 1, 2, 3, 4
),

--атрибуция посетителей last paid click для лидов
--тут нужные мне id под rn = 1
visitors_atr AS (
    SELECT
        visit_date::date AS visit_day,
        visitor_id,
        source AS utm_source,
        medium AS utm_medium,
        campaign AS utm_campaign,
        row_number() OVER (
            PARTITION BY visitor_id
            ORDER BY
                CASE
                    WHEN medium IN (
                        'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
                    ) THEN 2
                    ELSE 1
                END DESC,
                visit_date DESC
        ) AS rn
    FROM sessions
),

--считаю лиды, оплативших и заработок
leads_pu_count AS (
    SELECT
        l.created_at::date AS visit_day,
        v_a.utm_source,
        v_a.utm_medium,
        v_a.utm_campaign,
        count(l.lead_id) AS leads,
        sum(l.amount) AS revenue,
        sum(
            CASE
                WHEN l.amount != 0 THEN 1
                ELSE 0
            END
        ) AS pu_count
    FROM visitors_atr AS v_a
    INNER JOIN leads AS l
        ON v_a.visitor_id = l.visitor_id
    WHERE v_a.rn = 1
    GROUP BY 1, 2, 3, 4
),

--собираю скелет для будущей кучи
skeleton AS (
    SELECT
        visit_day,
        utm_source,
        utm_medium,
        utm_campaign
    FROM visitors_count
    UNION
    SELECT
        visit_day,
        utm_source,
        utm_medium,
        utm_campaign
    FROM ad_spent
    UNION
    SELECT
        visit_day,
        utm_source,
        utm_medium,
        utm_campaign
    FROM leads_pu_count
)--,
--собираю все в одну кучу
--fin AS(

SELECT
    s.utm_source,
    s.utm_medium,
    s.utm_campaign,
    to_char(s.visit_day, 'DD-MM-YYYY') AS visit_day,
    coalesce(vc.visitors, 0) AS visitors,
    coalesce(a_s.spent, 0) AS spent,
    coalesce(lc.leads, 0) AS leads,
    coalesce(lc.pu_count, 0) AS pu_count,
    coalesce(lc.revenue, 0) AS revenue
FROM skeleton AS s
LEFT JOIN visitors_count AS vc
    ON
        s.visit_day = vc.visit_day
        AND s.utm_source = vc.utm_source
        AND s.utm_medium = vc.utm_medium
        AND s.utm_campaign = vc.utm_campaign
LEFT JOIN ad_spent AS a_s
    ON
        s.visit_day = a_s.visit_day
        AND s.utm_source = a_s.utm_source
        AND s.utm_medium = a_s.utm_medium
        AND s.utm_campaign = a_s.utm_campaign
LEFT JOIN leads_pu_count AS lc
    ON
        s.visit_day = lc.visit_day
        AND s.utm_source = lc.utm_source
        AND s.utm_medium = lc.utm_medium
        AND s.utm_campaign = lc.utm_campaign
WHERE a_s.spent != 0;
/*)
я превращал поледний этап запроса в СТЕ и вот тут вот проводил потом все
вычисления для подсчета cpu, cpl, cppu и тд.
запросов было оч много, поэтому сюда сохранил в таком вот виде*/
-------------------------------------------------------------------
--считаю сколько дней с момента перехода по рекламе закрывается 90% лидов
WITH visitors_total AS (
    SELECT
        l.lead_id,
        s.visitor_id,
        l.created_at::date AS lead_day,
        s.visit_date::date AS visit_day,
        row_number()
            OVER (PARTITION BY l.lead_id ORDER BY s.visit_date ASC)
            AS rn
    FROM leads AS l
    INNER JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
    WHERE s.visit_date <= l.created_at
),

percentiles AS (
    SELECT
        lead_day - visit_day AS day_dif,
        ntile(100) OVER (ORDER BY lead_day - visit_day ASC) AS percentile
    FROM visitors_total
    WHERE rn = 1
)

SELECT round(avg(day_dif))
FROM percentiles
WHERE percentile = 90;
-------------------------------------------------------------------
--Считаю корреляцию между рекламой и органикой
--рекламные расходы
WITH ads AS (
    SELECT
        campaign_date::date AS cam_date,
        utm_source,
        sum(daily_spent) AS spent
    FROM (
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            campaign_date,
            utm_source,
            daily_spent
        FROM ya_ads
    ) AS t
    GROUP BY 1, 2
),

--разложил на vk и ya
ads_pivot AS (
    SELECT
        cam_date,
        sum(CASE WHEN utm_source = 'vk' THEN spent ELSE 0 END) AS vk_spent,
        sum(CASE WHEN utm_source = 'yandex' THEN spent ELSE 0 END) AS ya_spent,
        sum(spent) AS total_spent
    FROM ads
    GROUP BY 1
),

--органика
organic AS (
    SELECT
        visit_date::date AS cam_date,
        count(DISTINCT visitor_id) AS organic_sessions
    FROM sessions
    WHERE medium = 'organic'
    GROUP BY 1
)

--собрал все в кучу и взял лаг
SELECT
    o.cam_date,
    o.organic_sessions,
    coalesce(a.vk_spent, 0) AS vk_spent,
    coalesce(a.ya_spent, 0) AS ya_spent,
    lag(vk_spent, 1) OVER (ORDER BY a.cam_date ASC) AS vk_lag_1,
    lag(ya_spent, 1) OVER (ORDER BY a.cam_date ASC) AS ya_lag_1,
    lag(vk_spent, 2) OVER (ORDER BY a.cam_date ASC) AS vk_lag_2,
    lag(ya_spent, 2) OVER (ORDER BY a.cam_date ASC) AS ya_lag_2
FROM organic AS o
LEFT JOIN ads_pivot AS a
    ON o.cam_date = a.cam_date
ORDER BY 1;
/*дальше в гугл таблицах посчитал корреляцию, вот значения:
corr_vk:		-0.173
corr_ya:		0.286
corr_vk_lag_1:	-0.082
corr_ya_lag_1:	0
corr_vk_lag_2:	0.047
corr_ya_lag_2:	-0.125
таким образом, только на яндексе в тот же день есть хоть что-то.
и то, значение 0.286 больше похоже на шум, насколько я знаю*/
-------------------------------------------------------------------
--для графика с кумулятивными лидами
WITH visitors_total AS (
    SELECT
        l.lead_id,
        s.visitor_id,
        l.created_at::date AS lead_day,
        s.visit_date::date AS visit_day,
        row_number()
            OVER (PARTITION BY l.lead_id ORDER BY s.visit_date ASC)
            AS rn
    FROM leads AS l
    INNER JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
    WHERE s.visit_date <= l.created_at
)

SELECT
    lead_day - visit_day AS day_dif,
    sum(count(lead_id))
        OVER (ORDER BY lead_day - visit_day ASC)
        AS leads_cumulative
FROM visitors_total
WHERE rn = 1
GROUP BY 1;
