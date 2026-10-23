{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='event_date',
    partition_by='toYYYYMM(event_date)'
) }}

SELECT
    event_date,
    countIf(event_type = 'view')     AS views,
    countIf(event_type = 'cart')     AS carts,
    countIf(event_type = 'purchase') AS purchases,
    uniqExactIf(user_id, event_type = 'purchase') AS unique_buyers,
    round(sumIf(price, event_type = 'purchase'), 2) AS revenue,
    round(sumIf(price, event_type = 'purchase')
          / nullIf(countIf(event_type = 'purchase'), 0), 2) AS avg_order_value,
    round(countIf(event_type = 'purchase') * 100.0
          / nullIf(countIf(event_type = 'view'), 0), 4) AS conversion_rate_pct
FROM {{ ref('stg_events') }}
GROUP BY event_date
ORDER BY event_date