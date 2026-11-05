"""
Реплей датасета REES46 в топик shop_events.
Поддерживает resume: сохраняет, на какой строке CSV остановился.
"""
import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "shop_events"


def parse_event_time(raw: str) -> str:
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


def load_state(state_file: Path) -> int:
    """Читает, с какой строки CSV продолжать."""
    if not state_file.exists():
        return 0
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return int(data.get("rows_processed", 0))
    except Exception as e:
        print(f"WARN: could not read state file {state_file}: {e}")
        return 0


def save_state(state_file: Path, rows_processed: int):
    """Сохраняет прогресс на диск (атомарно через .tmp)."""
    try:
        state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_file.with_suffix(state_file.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({
                "rows_processed": rows_processed,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }, f)
        tmp.replace(state_file)
    except Exception as e:
        print(f"WARN: could not save state: {e}")


def main() -> int:
    p = argparse.ArgumentParser(description="Replay REES46 CSV into Kafka")
    p.add_argument("--file", required=True)
    p.add_argument("--limit", type=int, default=0,
                   help="max rows to send this run (0 = all)")
    p.add_argument("--rate", type=int, default=0,
                   help="events per second (0 = unlimited)")
    p.add_argument("--skip-errors", action="store_true")
    p.add_argument("--log-every", type=int, default=10000)
    p.add_argument("--skip", type=int, default=0,
                   help="skip first N data rows")
    p.add_argument("--state-file", default=None,
                   help="path to state file (default: <csv>.state)")
    p.add_argument("--resume", action="store_true",
                   help="resume from state file")
    args = p.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"ERROR: file not found: {path}")
        return 1

    state_file = Path(args.state_file) if args.state_file else path.with_suffix(path.suffix + ".state")

    skip = args.skip
    if args.resume and not args.skip:
        skip = load_state(state_file)
        if skip:
            print(f"Resuming from row {skip:,} (state file: {state_file})")

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
    print(f"  limit={args.limit or 'all'}  rate={args.rate or 'unlimited'} ev/s  skip={skip:,}")

    start = time.time()
    sent = 0
    skipped = 0
    processed = skip
    interval = 1.0 / args.rate if args.rate > 0 else 0.0

    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                if i < skip:
                    processed = i + 1
                    continue

                if args.limit and sent >= args.limit:
                    break

                try:
                    event = row_to_event(row)
                except (ValueError, KeyError) as e:
                    if args.skip_errors:
                        skipped += 1
                        processed = i + 1
                        continue
                    print(f"ERROR at CSV row {i + 2}: {e}")
                    print(f"  row = {row}")
                    save_state(state_file, processed)
                    return 1

                producer.send(TOPIC, event)
                sent += 1
                processed = i + 1

                if interval:
                    time.sleep(interval)

                if sent % args.log_every == 0:
                    save_state(state_file, processed)
                    elapsed = time.time() - start
                    rate = sent / elapsed if elapsed else 0
                    print(f"  sent={sent:,}  skipped={skipped:,}  "
                          f"processed={processed:,}  "
                          f"elapsed={elapsed:.1f}s  rate={rate:,.0f} ev/s")

        producer.flush()
        producer.close()
        save_state(state_file, processed)

        elapsed = time.time() - start
        rate = sent / elapsed if elapsed else 0
        print(f"\nDone. sent={sent:,}  skipped={skipped:,}  "
              f"processed={processed:,}  "
              f"elapsed={elapsed:.1f}s  avg_rate={rate:,.0f} ev/s")
        return 0

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Saving state ...")
        producer.flush()
        producer.close()
        save_state(state_file, processed)
        print(f"Saved at CSV row {processed:,}.")
        print(f"To resume: python producer/replay.py --file {path} --resume")
        return 130


if __name__ == "__main__":
    sys.exit(main())