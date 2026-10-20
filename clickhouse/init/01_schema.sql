-- ============================================================
-- ShopStream: схема ClickHouse, слой RAW
-- ============================================================

-- 1. Таблица постоянного хранения событий
CREATE TABLE IF NOT EXISTS shop.raw_events
(
    event_time    DateTime,
    event_type    LowCardinality(String),
    product_id    UInt64,
    category_id   UInt64,
    category_code String,
    brand         LowCardinality(String),
    price         Float64,
    user_id       UInt64,
    user_session  String,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, user_id, product_id);

-- 2. Kafka Engine: читает сообщения из Redpanda
CREATE TABLE IF NOT EXISTS shop.kafka_events
(
    event_time    DateTime,
    event_type    String,
    product_id    UInt64,
    category_id   UInt64,
    category_code String,
    brand         String,
    price         Float64,
    user_id       UInt64,
    user_session  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'redpanda:9092',
    kafka_topic_list    = 'shop_events',
    kafka_group_name    = 'clickhouse_consumer',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 2;

-- 3. Materialized View: Kafka -> raw_events
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.mv_kafka_to_raw
TO shop.raw_events
AS
SELECT
    event_time,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session
FROM shop.kafka_events;