"""Kafka Avro producer supporting static replay and synthetic e-commerce traffic."""

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import random
import sys
import time
from typing import Iterator, TypedDict

from confluent_kafka.serialization import MessageField, SerializationContext
import yaml

from projects.common.kafka_avro import (
    create_avro_producer,
    kafka_delivery_report,
    load_avro_schema,
)
from projects.common.logger import get_logger

logger = get_logger("EcommerceAvroProducer")

SYNTHETIC_USERS = [
    "user_101",
    "user_102",
    "user_103",
    "user_104",
    "user_105",
    "user_106",
    "user_107",
    "user_108",
]

SYNTHETIC_PRODUCTS = [
    ("prod_01", "electronics", 1200.00, "/products/laptop"),
    ("prod_02", "apparel", 85.00, "/products/shoes"),
    ("prod_03", "home", 45.00, "/products/lamp"),
    ("prod_04", "electronics", 800.00, "/products/phone"),
    ("prod_05", "home", 150.00, "/products/desk"),
]


class EcommerceEvent(TypedDict):
    """Typed schema representation for a single e-commerce activity event."""

    user_id: str
    url: str
    event_type: str
    event_time: str
    product_id: str | None
    category: str | None
    price: float | None
    ip_address: str | None
    user_agent: str | None


def generate_synthetic_events(
    count: int,
    late_event_ratio: float = 0.10,
    anomaly_ratio: float = 0.05,
    seed: int | None = None,
) -> Iterator[EcommerceEvent]:
    """Generate realistic e-commerce funnel events with controlled late arrivals and anomalies.

    Produces coherent user journeys across ``view``, ``add_to_cart``, and
    ``purchase`` stages referencing the OLTP customer and product keys. Controlled
    late-arriving events exercise Spark's event-time watermark, while controlled
    domain anomalies (negative price or invalid ``event_type``) pass Avro wire
    serialization to exercise PyDeequ checks and the Silver DLQ quarantine table.

    Args:
        count: Total number of events to generate.
        late_event_ratio: Probability in ``[0, 1]`` of shifting ``event_time``
            backward by 5–25 seconds to simulate out-of-order late arrivals.
        anomaly_ratio: Probability in ``[0, 1]`` of emitting a domain-violating
            event for DLQ testing.
        seed: Optional random seed for deterministic test generation.

    Yields:
        ``EcommerceEvent`` dictionaries conforming to ``ecommerce_event.avsc``.
    """
    rng = random.Random(seed)
    base_time = datetime.now(timezone.utc)
    funnel_weights = [("view", 0.55), ("add_to_cart", 0.25), ("purchase", 0.20)]
    event_choices = [e for e, _ in funnel_weights]
    weights = [w for _, w in funnel_weights]

    for idx in range(count):
        user_id = rng.choice(SYNTHETIC_USERS)
        product_id, category, price, prod_url = rng.choice(SYNTHETIC_PRODUCTS)
        event_type = rng.choices(event_choices, weights=weights, k=1)[0]

        event_dt = base_time + timedelta(seconds=idx * 0.2)
        if rng.random() < late_event_ratio:
            event_dt = event_dt - timedelta(seconds=rng.randint(5, 25))

        url = "/checkout" if event_type == "purchase" else prod_url
        if event_type == "view" and rng.random() < 0.15:
            url = "/home"
            product_id, category, price = None, None, None

        # Inject domain-invalid anomalies that stay Avro-schema-valid for DLQ testing.
        if rng.random() < anomaly_ratio:
            if rng.random() < 0.5:
                price = -abs(price or 50.0)
            else:
                event_type = "refund_err"

        yield {
            "user_id": user_id,
            "url": url,
            "event_type": event_type,
            "event_time": event_dt.strftime("%Y-%m-%dT%H:%M:%S"),
            "product_id": product_id,
            "category": category,
            "price": price,
            "ip_address": f"192.168.1.{10 + (SYNTHETIC_USERS.index(user_id) % 50)}",
            "user_agent": "Mozilla/5.0",
        }


def _iter_file_events(input_path: str) -> Iterator[EcommerceEvent]:
    """Yield events line-by-line from a JSON-lines input file."""
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line.strip())


def produce_events(
    config_path: str,
    schema_path: str,
    input_path: str,
    delay_seconds: float = 0.2,
    mode: str = "file",
    count: int = 50,
    continuous: bool = False,
    rate: float = 5.0,
    late_event_ratio: float = 0.10,
    anomaly_ratio: float = 0.05,
    seed: int | None = None,
) -> int:
    """Stream e-commerce events to Kafka/Redpanda with Avro serialization.

    Args:
        config_path: Path to the YAML configuration file containing Kafka and
            Schema Registry defaults.
        schema_path: Path to the ``.avsc`` Avro schema file.
        input_path: Path to the JSON-lines input dataset (used in ``"file"`` mode).
        delay_seconds: Sleep interval between messages in ``"file"`` mode.
        mode: Event source mode (``"file"`` or ``"synthetic"``).
        count: Number of events per batch when ``mode == "synthetic"``.
        continuous: When ``True`` in synthetic mode, streams indefinitely until
            interrupted.
        rate: Target events per second in synthetic mode.
        late_event_ratio: Ratio of out-of-order late timestamps in synthetic mode.
        anomaly_ratio: Ratio of domain-invalid anomaly events in synthetic mode.
        seed: Optional random seed for deterministic synthetic generation.

    Returns:
        Total number of produced events.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    kafka_cfg, sr_cfg = (
        config.get("kafka_config", {}),
        config.get("schema_registry", {}),
    )
    bootstrap = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", kafka_cfg.get("bootstrap_servers", "localhost:9092")
    )
    topic = os.getenv("KAFKA_TOPIC", kafka_cfg.get("topic", "ecommerce_events"))
    sr_url = os.getenv(
        "SCHEMA_REGISTRY_URL", sr_cfg.get("url", "http://localhost:8081")
    )

    schema_str = load_avro_schema(schema_path)
    producer, serializer = create_avro_producer(bootstrap, sr_url, schema_str)
    ctx = SerializationContext(topic, MessageField.VALUE)

    sleep_interval = (
        (1.0 / rate) if (mode == "synthetic" and rate > 0) else delay_seconds
    )
    total_sent = 0

    try:
        while True:
            events_iter = (
                generate_synthetic_events(
                    count=count,
                    late_event_ratio=late_event_ratio,
                    anomaly_ratio=anomaly_ratio,
                    seed=seed,
                )
                if mode == "synthetic"
                else _iter_file_events(input_path)
            )
            if mode == "file":
                logger.info("Reading e-commerce events from: %s", input_path)
            else:
                logger.info(
                    "Producing synthetic events (count=%d, rate=%.1f/s, late=%.2f, anomaly=%.2f)...",
                    count,
                    rate,
                    late_event_ratio,
                    anomaly_ratio,
                )

            for event_data in events_iter:
                serialized = serializer(event_data, ctx)
                key = str(event_data.get("user_id", "unknown")).encode("utf-8")

                producer.produce(
                    topic=topic,
                    key=key,
                    value=serialized,
                    callback=lambda err, msg: kafka_delivery_report(err, msg, logger),
                )
                # Non-blocking poll(0) services librdkafka delivery callbacks without stalling.
                producer.poll(0)
                total_sent += 1
                if sleep_interval > 0:
                    time.sleep(sleep_interval)

            if not (mode == "synthetic" and continuous):
                break
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user.")
    finally:
        # Block until all buffered messages in librdkafka are delivered before exiting.
        producer.flush()

    logger.info(
        "Successfully produced %d e-commerce events to '%s'.", total_sent, topic
    )
    return total_sent


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description="Produce Avro e-commerce events to Kafka/Redpanda."
    )
    parser.add_argument(
        "--config", default=os.path.join(script_dir, "config/ecommerce_config.yml")
    )
    parser.add_argument(
        "--schema", default=os.path.join(script_dir, "schemas/ecommerce_event.avsc")
    )
    parser.add_argument(
        "--input", default=os.path.join(script_dir, "input_data/ecommerce_events.json")
    )
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument(
        "--mode",
        choices=["file", "synthetic"],
        default="file",
        help="Replay static JSONL file or generate synthetic traffic.",
    )
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--rate", type=float, default=5.0)
    parser.add_argument("--late-event-ratio", type=float, default=0.10)
    parser.add_argument("--anomaly-ratio", type=float, default=0.05)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    try:
        produce_events(
            args.config,
            args.schema,
            args.input,
            delay_seconds=args.delay,
            mode=args.mode,
            count=args.count,
            continuous=args.continuous,
            rate=args.rate,
            late_event_ratio=args.late_event_ratio,
            anomaly_ratio=args.anomaly_ratio,
            seed=args.seed,
        )
    except Exception as exc:
        logger.error("Producer error: %s", exc)
        sys.exit(1)
