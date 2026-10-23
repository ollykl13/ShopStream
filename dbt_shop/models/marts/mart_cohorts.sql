{{ config(
    materialized='table',
    engine='MergeTree',
    order_by='(cohort_week, week_number)'
) }}

WITH user_first_purchase AS (
    SELECT
        user_id,
        toStartOfWeek(min(event_time)) AS cohort_week
    FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase'
    GROUP BY user_id
),
user_purchases AS (
    SELECT DISTINCT
        e.user_id,
        ufp.cohort_week,
        toStartOfWeek(e.event_time) AS purchase_week
    FROM {{ ref('stg_events') }} e
    INNER JOIN user_first_purchase ufp ON e.user_id = ufp.user_id
    WHERE e.event_type = 'purchase'
),
cohort_sizes AS (
    SELECT
        cohort_week,
        count() AS cohort_size
    FROM user_first_purchase
    GROUP BY cohort_week
)
SELECT
    up.cohort_week,
    dateDiff('week', up.cohort_week, up.purchase_week) AS week_number,
    count(DISTINCT up.user_id) AS users,
    cs.cohort_size,
    round(count(DISTINCT up.user_id) * 100.0 / cs.cohort_size, 2) AS retention_pct
FROM user_purchases up
INNER JOIN cohort_sizes cs ON up.cohort_week = cs.cohort_week
GROUP BY up.cohort_week, week_number, cs.cohort_size
ORDER BY up.cohort_week, week_number