{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(purchases, revenue)'
) }}

SELECT
    product_id,
    any(brand)         AS brand,
    any(category_code) AS category_code,
    countIf(event_type = 'view')     AS views,
    countIf(event_type = 'cart')     AS carts,
    countIf(event_type = 'purchase') AS purchases,
    uniqExactIf(user_id, event_type = 'purchase') AS unique_buyers,
    round(sumIf(price, event_type = 'purchase'), 2) AS revenue,
    round(sumIf(price, event_type = 'purchase')
          / nullIf(countIf(event_type = 'purchase'), 0), 2) AS avg_price
FROM {{ ref('stg_events') }}
GROUP BY product_id
HAVING purchases > 0
ORDER BY revenue DESC