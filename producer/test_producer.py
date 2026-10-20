"""
Тестовый продюсер: отправляет N событий в топик shop_events.
Используется для проверки end-to-end пайплайна Kafka -> ClickHouse.
"""
import json
import sys
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "shop_events"
NUM_EVENTS = 10


def make_event(i: int) -> dict:
    """Простое тестовое событие. Формат должен совпадать с kafka_events."""
    return {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "event_type": ["view", "cart", "purchase"][i % 3],
        "product_id": 100000 + i,
        "category_id": 100 + i,
        "category_code": "electronics.smartphone",
        "brand": ["apple", "samsung", "xiaomi"][i % 3],
        "price": round(100.0 + i * 10.5, 2),
        "user_id": 500000 + i,
        "user_session": f"test-session-{i:04d}",
    }


def main() -> int:
    print(f"Connecting to {BOOTSTRAP_SERVERS} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except NoBrokersAvailable:
        print("ERROR: No brokers available. Is Redpanda running on localhost:9092?")
        print("Hint: check 'docker compose -f infra/docker-compose.yml ps'")
        return 1

    print(f"Sending {NUM_EVENTS} events to topic '{TOPIC}' ...")
    for i in range(NUM_EVENTS):
        event = make_event(i)
        future = producer.send(TOPIC, event)
        metadata = future.get(timeout=10)
        print(
            f"  [{i+1:02d}/{NUM_EVENTS}] "
            f"partition={metadata.partition} offset={metadata.offset} "
            f"type={event['event_type']} price={event['price']}"
        )

    producer.flush()
    producer.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
