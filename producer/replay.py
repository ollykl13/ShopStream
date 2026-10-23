"""
Реплей датасета REES46 в топик shop_events.
Читает CSV и отправляет события в Kafka с заданной скоростью.
"""
import argparse
import csv
import json
import sys
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "shop_events"


def parse_event_time(raw: str) -> str:
    """REES46: '2019-10-01 00:00:00 UTC' -> '2019-10-01 00:00:00'."""
    return raw.replace(" UTC", "").strip()


def row_to_event(row: dict) -> dict:
    return {
        "event_time":   parse_event_time(row["event_time"]),
        "event_type":   row["event_type"],
        "product_id":   int(row["product_id"]),
        "category_id":  int(row["category_id"]),
        "category_code": row["category_code"] or "",
        "brand":        row["brand"] or "",
        "price":        float(row["price"]),
        "user_id":      int(row["user_id"]),
        "user_session": row["user_session"],
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Replay REES46 CSV into Kafka")
    p.add_argument("--file", required=True)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--rate", type=int, default=0)
    p.add_argument("--skip-errors", action="store_true")
    p.add_argument("--log-every", type=int, default=10000)
    args = p.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        return 1

    print(f"Connecting to {BOOTSTRAP_SERVERS} ...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=10,
            batch_size=65536,
        )
    except NoBrokersAvailable:
        print("ERROR: No brokers available.")
        return 1

    print(f"Replaying {path} -> topic '{TOPIC}'")
    print(f"  limit={args.limit or 'all'}  rate={args.rate or 'unlimited'} ev/s")

    start = time.time()
    sent = 0
    skipped = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if args.limit and sent >= args.limit:
                break
            try:
                event = row_to_event(row)
            except (ValueError, KeyError) as e:
                if args.skip_errors:
                    skipped += 1
                    continue
                print(f"ERROR at row {sent + skipped + 2}: {e}")
                print(f"  row = {row}")
                return 1

            producer.send(TOPIC, event)
            sent += 1

            if interval:
                time.sleep(interval)

            if sent % args.log_every == 0:
                elapsed = time.time() - start
                rate = sent / elapsed if elapsed else 0
                print(f"  sent={sent:,}  skipped={skipped:,}  "
                      f"elapsed={elapsed:.1f}s  rate={rate:,.0f} ev/s")

    producer.flush()
    producer.close()

    elapsed = time.time() - start
    rate = sent / elapsed if elapsed else 0
    print(f"\nDone. sent={sent:,}  skipped={skipped:,}  "
          f"elapsed={elapsed:.1f}s  avg_rate={rate:,.0f} ev/s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
