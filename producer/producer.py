# producer/producer.py
# Entry point — replays the dataset into the churn.raw Kafka topic.
# Implements data_contract.md v2.3, sections 6.1 – 6.5.
#
# Usage:
#   python producer.py                          # 10 rec/s, clean only
#   python producer.py --rate 100               # faster replay
#   python producer.py --inject-errors          # interleave fault records
#   python producer.py --rate 100 --inject-errors

import argparse
import datetime
import json
import logging
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Schema-guided JSON serialization (data_contract.md §6.2) ─────────────────

def serialize_field(name: str, raw_value: str):
    """
    Convert a raw CSV string token to the correct JSON type.

    Rules (from the contract §6.2):
    - Empty string in a nullable field → None (JSON null).
    - Empty string in a required field → keep as "" so NiFi Rule 2 can reject it.
    - Integer fields → int when the token is a whole number; keep as string otherwise
      (wrong type intentionally left for NiFi Rule 1 to catch).
    - Float/number fields → float when parseable; keep as string otherwise.
    - String fields → remain strings.
    - Values outside allowed ranges are kept as numbers (NiFi Rule 3 catches them).
    """
    # Nullable: empty string → null
    if raw_value == "" and name in config.NULLABLE_FIELDS:
        return None

    # Integer field
    if name in config.INTEGER_FIELDS:
        if raw_value == "":
            return None   # only reaches here if it's also nullable
        try:
            f = float(raw_value)
            if f == int(f):
                return int(f)
            # Non-whole number in an integer field: keep as string so Rule 1 fires
            return raw_value
        except ValueError:
            return raw_value  # unparseable → keep as string, Rule 1 fires

    # Float / number field
    if name in config.FLOAT_FIELDS:
        if raw_value == "" and name in config.NULLABLE_FIELDS:
            return None
        try:
            return float(raw_value)
        except ValueError:
            return raw_value  # unparseable → keep as string, Rule 1 fires

    # String field — return as-is (including empty strings for required fields)
    return raw_value


def build_message(row: dict) -> dict:
    """
    Build one Kafka message from a CSV row dict.
    - Drops the 6 excluded columns (§6.1).
    - Applies schema-guided serialization (§6.2).
    - Adds event_timestamp as ISO 8601 UTC (§6.3).
    """
    msg = {}
    for name, raw_value in row.items():
        if name in config.EXCLUDED_COLUMNS:
            continue
        msg[name] = serialize_field(name, raw_value)

    # event_timestamp = real wall-clock UTC time of publication (§6.3)
    msg["event_timestamp"] = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return msg


# ── Kafka producer ────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    """Create and return a configured KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        # Kafka key = customer_id bytes; null key when customer_id is empty (§6.4)
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
        retries=3,
    )


# ── Record loading ────────────────────────────────────────────────────────────

def load_records():
    """
    Load records from the CSV via dataset_loader.py (Phase 2).
    Falls back to 3 stub records if dataset_loader is not ready yet.
    """
    try:
        from dataset_loader import load_records as _load
        return _load()
    except ImportError:
        log.warning("dataset_loader.py not found — using 3 stub records for testing.")
        return [
            {"customer_id": "C000000001", "gender": "Female", "age": "43",
             "under_30": "No", "senior_citizen": "No", "married": "No",
             "dependents": "No", "number_of_dependents": "0", "city": "Pasadena",
             "referred_a_friend": "No", "number_of_referrals": "0",
             "tenure_in_months": "34", "offer": "", "phone_service": "Yes",
             "avg_monthly_long_distance_charges": "32.02", "multiple_lines": "Yes",
             "internet_service": "Yes", "internet_type": "DSL",
             "avg_monthly_gb_download": "3", "premium_tech_support": "No",
             "streaming_tv": "No", "streaming_movies": "No", "streaming_music": "No",
             "unlimited_data": "Yes", "contract": "Month-to-Month",
             "paperless_billing": "Yes", "payment_method": "Bank Withdrawal",
             "monthly_charge": "58.19", "total_charges": "1978.46",
             "total_refunds": "0", "total_extra_data_charges": "0",
             "total_long_distance_charges": "1088.68", "total_revenue": "3067.14",
             "satisfaction_score": "5", "customer_status": "Stayed",
             "churn_label": "No", "churn_score": "43", "cltv": "4454",
             "churn_category": "", "churn_reason": "",
             "customer_service_calls_1m": "1", "customer_service_calls_3m": "2",
             "income": "3192", "churn_date": "",
             "calls_1m": "133", "calls_3m": "431", "sms_1m": "86", "sms_3m": "272",
             "purchases_1m": "0", "purchases_3m": "4",
             "data_usage_gb_1m": "2.77", "data_usage_gb_3m": "9"},
        ]


def interleave_faults(clean_records: list) -> list:
    """
    Scatter fault records evenly among clean records (§6.5).
    Silently skips if fault_injector.py is not ready yet.
    """
    try:
        from fault_injector import get_fault_records
        faults = get_fault_records()
    except ImportError:
        log.warning("fault_injector.py not found — skipping fault injection.")
        return clean_records

    if not faults:
        return clean_records

    combined = list(clean_records)
    step = max(len(combined) // len(faults), 1)
    for i, fault in enumerate(faults):
        insert_at = min(i * step + step // 2, len(combined))
        combined.insert(insert_at, fault)

    log.info("Injected %d fault records into the stream.", len(faults))
    return combined


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay churn_raw.csv into Kafka topic 'churn.raw'."
    )
    parser.add_argument(
        "--rate", type=float, default=config.DEFAULT_RATE, metavar="N",
        help="Records to publish per second (default: %(default)s). "
             "10 rec/s ≈ 5.6 h, 100 rec/s ≈ 33 min, 1000 rec/s ≈ 3.3 min.",
    )
    parser.add_argument(
        "--inject-errors", action="store_true",
        help="Interleave fault records (requires fault_injector.py).",
    )
    return parser.parse_args()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    delay = 1.0 / args.rate

    log.info("Connecting to Kafka at %s …", config.KAFKA_BOOTSTRAP)
    try:
        producer = build_producer()
    except KafkaError as exc:
        log.error("Could not connect to Kafka: %s", exc)
        sys.exit(1)

    log.info("Loading records …")
    records = list(load_records())

    if args.inject_errors:
        records = interleave_faults(records)

    total = len(records)
    log.info("Publishing %d records to '%s' at %.1f rec/s …",
             total, config.TOPIC_RAW, args.rate)

    published = 0
    start = time.perf_counter()

    for raw_row in records:
        # Build the JSON message (type-serialize + add event_timestamp)
        message = build_message(raw_row)

        # Kafka key = customer_id if non-empty, else null (§6.4)
        kafka_key = message.get("customer_id") or None

        try:
            producer.send(config.TOPIC_RAW, key=kafka_key, value=message)
            published += 1

            if published % 1000 == 0:
                elapsed = time.perf_counter() - start
                log.info("  %d / %d sent  (%.1f rec/s actual)",
                         published, total, published / elapsed)

        except KafkaError as exc:
            log.error("Failed to send record %s: %s", kafka_key, exc)

        time.sleep(delay)

    producer.flush()
    elapsed = time.perf_counter() - start
    log.info("Done. %d records published in %.1f s (%.1f rec/s).",
             published, elapsed, published / elapsed)


if __name__ == "__main__":
    main()
