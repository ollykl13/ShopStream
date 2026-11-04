{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='event_hour',
    engine='MergeTree',
    order_by='event_hour',
    partition_by='toYYYYMM(event_hour)'
) }}

SELECT
    toStartOfHour(event_time) AS event_hour,
    countIf(event_type = 'view')     AS views,
    countIf(event_type = 'cart')     AS carts,
    countIf(event_type = 'purchase') AS purchases,
    round(countIf(event_type = 'cart') * 100.0
          / nullIf(countIf(event_type = 'view'), 0), 2) AS view_to_cart_pct,
    round(countIf(event_type = 'purchase') * 100.0
          / nullIf(countIf(event_type = 'cart'), 0), 2) AS cart_to_purchase_pct,
    round(countIf(event_type = 'purchase') * 100.0
          / nullIf(countIf(event_type = 'view'), 0), 2) AS view_to_purchase_pct
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
WHERE event_hour >= toStartOfHour(now() - INTERVAL 2 HOUR)
{% endif %}
GROUP BY event_hour
ORDER BY event_hour