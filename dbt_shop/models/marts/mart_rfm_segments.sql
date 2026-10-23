{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(segment, monetary)'
) }}

WITH user_stats AS (
    SELECT
        user_id,
        maxIf(event_time, event_type = 'purchase') AS last_purchase_at,
        countIf(event_type = 'purchase')           AS frequency,
        round(sumIf(price, event_type = 'purchase'), 2) AS monetary
    FROM {{ ref('stg_events') }}
    GROUP BY user_id
    HAVING frequency > 0
),
scored AS (
    SELECT
        user_id,
        last_purchase_at,
        dateDiff('day', last_purchase_at, toDateTime('2019-10-31 23:59:59')) AS recency_days,
        frequency,
        monetary,
        ntile(5) OVER (ORDER BY last_purchase_at DESC)  AS r_score,
        ntile(5) OVER (ORDER BY frequency ASC)          AS f_score,
        ntile(5) OVER (ORDER BY monetary ASC)           AS m_score
    FROM user_stats
)
SELECT
    user_id,
    last_purchase_at,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score AS rfm_total,
    multiIf(
        r_score >= 4 AND f_score >= 4 AND m_score >= 4, 'champions',
        r_score >= 3 AND f_score >= 3 AND m_score >= 3, 'loyal',
        r_score >= 4 AND f_score <= 2 AND m_score <= 2, 'new',
        r_score <= 2 AND f_score >= 4 AND m_score >= 4, 'cant_lose',
        r_score <= 2 AND f_score >= 3 AND m_score >= 3, 'at_risk',
        r_score <= 2 AND f_score <= 2 AND m_score <= 2, 'lost',
        r_score >= 3 AND f_score <= 2 AND m_score <= 2, 'promising',
        'others'
    ) AS segment
FROM scored