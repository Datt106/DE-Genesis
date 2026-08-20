"""Sinh log dịch vụ mẫu và gửi vào Kafka cho bài thực hành tuần 4."""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "service-logs")


def configure_utf8_console() -> None:
    """Giữ thông báo tiếng Việt đọc được trên PowerShell dùng code page cũ."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8", errors="replace")


def make_event(
    rng: Optional[random.Random] = None,
    event_time: Optional[datetime] = None,
) -> dict:
    """Tạo một sự kiện hợp lệ; cho phép truyền RNG để kiểm thử tái lập."""

    random_source = rng or random
    timestamp = event_time or datetime.now(timezone.utc)
    service = random_source.choice(["catalog", "checkout", "payment"])
    status_code = random_source.choices(
        [200, 201, 400, 404, 500, 502],
        weights=[50, 15, 8, 8, 10, 9],
        k=1,
    )[0]
    return {
        "ts": timestamp.astimezone(timezone.utc).isoformat(),
        "service": service,
        "method": random_source.choice(["GET", "POST"]),
        "path": random_source.choice(
            ["/products", "/orders", "/payments", "/health"]
        ),
        "status_code": status_code,
        "latency_ms": random_source.randint(20, 1200),
    }


def send_events(
    producer: KafkaProducer,
    topic: str,
    count: int = 0,
    interval_seconds: float = 1.0,
    delivery_timeout_seconds: float = 10.0,
    invalid_every: int = 0,
    rng: Optional[random.Random] = None,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Gửi sự kiện và đợi Kafka xác nhận từng bản ghi.

    ``count=0`` giữ chế độ chạy liên tục để tương thích với hướng dẫn ban đầu.
    Hàm trả về số sự kiện đã được broker xác nhận.
    """

    if count < 0:
        raise ValueError("count phải lớn hơn hoặc bằng 0")
    if interval_seconds < 0:
        raise ValueError("interval_seconds phải lớn hơn hoặc bằng 0")
    if delivery_timeout_seconds <= 0:
        raise ValueError("delivery_timeout_seconds phải lớn hơn 0")
    if invalid_every < 0:
        raise ValueError("invalid_every phải lớn hơn hoặc bằng 0")

    sent = 0
    while count == 0 or sent < count:
        event = make_event(rng=rng)
        if invalid_every and (sent + 1) % invalid_every == 0:
            event = {**event, "latency_ms": -1}
        metadata = producer.send(topic, value=event).get(
            timeout=delivery_timeout_seconds
        )
        sent += 1
        print(
            json.dumps(
                {
                    "event_number": sent,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                    "event": event,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )
        if count == 0 or sent < count:
            sleep(interval_seconds)

    return sent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sinh log dịch vụ mẫu và gửi vào Kafka."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=BOOTSTRAP_SERVERS,
        help=f"Danh sách Kafka broker (mặc định: {BOOTSTRAP_SERVERS}).",
    )
    parser.add_argument(
        "--topic",
        default=TOPIC,
        help=f"Topic nhận dữ liệu (mặc định: {TOPIC}).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.getenv("KAFKA_EVENT_COUNT", "0")),
        help="Số sự kiện cần gửi; 0 nghĩa là chạy đến khi nhấn Ctrl+C.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.getenv("KAFKA_EVENT_INTERVAL_SECONDS", "1")),
        help="Khoảng nghỉ giữa hai sự kiện, tính bằng giây.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Seed để tái lập chuỗi dữ liệu khi kiểm thử.",
    )
    parser.add_argument(
        "--delivery-timeout",
        type=float,
        default=10.0,
        help="Số giây tối đa chờ Kafka xác nhận một sự kiện.",
    )
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=0,
        help=(
            "Cứ N sự kiện tạo một latency âm để kiểm thử quarantine; "
            "0 nghĩa là chỉ sinh dữ liệu hợp lệ."
        ),
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    configure_utf8_console()
    args = build_parser().parse_args(argv)
    if args.count < 0:
        print("Lỗi: --count phải lớn hơn hoặc bằng 0.", file=sys.stderr)
        return 2
    if args.interval < 0:
        print("Lỗi: --interval phải lớn hơn hoặc bằng 0.", file=sys.stderr)
        return 2
    if args.delivery_timeout <= 0:
        print("Lỗi: --delivery-timeout phải lớn hơn 0.", file=sys.stderr)
        return 2
    if args.invalid_every < 0:
        print("Lỗi: --invalid-every phải lớn hơn hoặc bằng 0.", file=sys.stderr)
        return 2

    producer: Optional[KafkaProducer] = None
    rng = random.Random(args.seed)
    try:
        producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=5,
            request_timeout_ms=max(1000, int(args.delivery_timeout * 1000)),
        )
        mode = "liên tục" if args.count == 0 else f"{args.count} sự kiện"
        print(
            f"Gửi {mode} vào topic {args.topic} qua "
            f"{args.bootstrap_servers}. Nhấn Ctrl+C để dừng.",
            flush=True,
        )
        sent = send_events(
            producer=producer,
            topic=args.topic,
            count=args.count,
            interval_seconds=args.interval,
            delivery_timeout_seconds=args.delivery_timeout,
            invalid_every=args.invalid_every,
            rng=rng,
        )
        print(f"Đã gửi và nhận xác nhận cho {sent} sự kiện.", flush=True)
        return 0
    except KeyboardInterrupt:
        print("Đã dừng producer theo yêu cầu.", flush=True)
        return 130
    except (KafkaError, OSError) as exc:
        print(f"Không thể gửi dữ liệu tới Kafka: {exc}", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.flush(timeout=args.delivery_timeout)
            producer.close(timeout=args.delivery_timeout)


if __name__ == "__main__":
    raise SystemExit(main())
