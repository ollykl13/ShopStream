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

-- 2. Карантин для ошибок парсинга
CREATE TABLE IF NOT EXISTS shop.kafka_events_errors
(
    _topic        String,
    _partition    UInt64,
    _offset       UInt64,
    _raw_message  String,
    _error        String,
    _ingested_at  DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY _ingested_at;

-- 3. Kafka Engine
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
    kafka_group_name    = 'clickhouse_consumer_v2',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 2,
    kafka_auto_offset_reset = 'earliest',
    kafka_handle_error_mode = 'stream';

-- 4. MV: валидные события -> raw_events
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
FROM shop.kafka_events
WHERE length(_error) = 0;

-- 5. MV: битые события -> карантин
CREATE MATERIALIZED VIEW IF NOT EXISTS shop.mv_kafka_errors
TO shop.kafka_events_errors
AS
SELECT
    _topic,
    _partition,
    _offset,
    _raw_message,
    _error
FROM shop.kafka_events
WHERE length(_error) > 0;