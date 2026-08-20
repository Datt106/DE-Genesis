"""Sinh service log có định danh và gửi bền vững vào Kafka."""

from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka import KafkaException, Producer


DEFAULT_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DEFAULT_TOPIC = os.getenv("WEEK6_LOG_TOPIC", "week6-service-logs")


def make_log_event(
    *,
    rng: random.Random | None = None,
    event_time: datetime | None = None,
    event_id: str | None = None,
) -> dict:
    """Tạo event hợp lệ, cho phép cố định RNG/thời gian khi kiểm thử."""

    random_source = rng or random.Random()
    timestamp = (event_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
    service = random_source.choice(("catalog", "checkout", "payment"))
    status_code = random_source.choices(
        (200, 201, 400, 404, 429, 500, 502),
        weights=(56, 12, 7, 7, 5, 8, 5),
        k=1,
    )[0]
    return {
        "event_id": event_id or str(uuid.uuid4()),
        "event_time": timestamp.isoformat(),
        "service": service,
        "method": random_source.choice(("GET", "POST", "PUT", "DELETE")),
        "path": random_source.choice(
            ("/products", "/orders", "/payments", "/health")
        ),
        "status_code": status_code,
        "latency_ms": random_source.randint(5, 1500),
        "host": f"{service}-{random_source.randint(1, 3)}",
    }


def send_events(
    producer: Producer,
    *,
    topic: str,
    count: int,
    interval_seconds: float,
    rng: random.Random,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if count < 0:
        raise ValueError("count phải lớn hơn hoặc bằng 0")
    if interval_seconds < 0:
        raise ValueError("interval_seconds không được âm")
    sent = 0
    while count == 0 or sent < count:
        event = make_log_event(rng=rng)
        delivery: dict[str, object] = {}

        def on_delivery(error, message) -> None:
            delivery["error"] = error
            delivery["message"] = message

        producer.produce(
            topic,
            key=event["event_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=on_delivery,
        )
        producer.poll(0)
        if producer.flush(15) != 0:
            raise KafkaException("Kafka không xác nhận event trong 15 giây")
        if delivery.get("error") is not None:
            raise KafkaException(delivery["error"])
        metadata = delivery["message"]
        sent += 1
        print(
            json.dumps(
                {
                    "event_id": event["event_id"],
                    "partition": metadata.partition(),
                    "offset": metadata.offset(),
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        if count == 0 or sent < count:
            sleep(interval_seconds)
    return sent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gửi service log Tuần 6 vào Kafka")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--count", type=int, default=int(os.getenv("WEEK6_LOG_EVENT_COUNT", "0"))
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("WEEK6_LOG_EVENT_INTERVAL_SECONDS", "1")),
    )
    parser.add_argument("--seed", type=int, default=None)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "message.send.max.retries": 10,
            "max.in.flight.requests.per.connection": 5,
            "linger.ms": 20,
        }
    )
    try:
        send_events(
            producer,
            topic=args.topic,
            count=args.count,
            interval_seconds=args.interval,
            rng=random.Random(args.seed),
        )
        return 0
    except KeyboardInterrupt:
        return 130
    finally:
        producer.flush(15)


if __name__ == "__main__":
    raise SystemExit(main())
