{{ config(materialized='view') }}

SELECT
    event_time,
    toDate(event_time)   AS event_date,
    toHour(event_time)   AS event_hour,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session,
    _ingested_at
FROM shop.raw_events
WHERE event_time IS NOT NULL
  AND user_id > 0
  AND product_id > 0