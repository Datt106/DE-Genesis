from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_kafka_init_converges_existing_topics_to_minimum_partition_count() -> None:
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")

    assert "ensure_topic()" in compose
    assert '--create --if-not-exists' in compose
    assert '--alter' in compose
    assert 'if [ "$$current" -lt "$$required" ]' in compose
    assert 'if [ -z "$$final" ] || [ "$$final" -lt "$$required" ]' in compose
    assert 'ensure_topic "$$SERVICE_LOG_TOPIC" "$$SERVICE_LOG_PARTITIONS"' in compose
    assert 'ensure_topic "$$WEEK6_LOG_TOPIC" "$$WEEK6_LOG_PARTITIONS"' in compose


def test_kafka_partition_defaults_match_roadmap() -> None:
    env = (ROOT / ".env.example").read_text(encoding="utf-8")

    assert "SERVICE_LOG_PARTITIONS=3" in env
    assert "WEEK6_LOG_PARTITIONS=3" in env
