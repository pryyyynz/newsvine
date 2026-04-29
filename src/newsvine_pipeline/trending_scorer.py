import json
import logging
import math
import os
import time
from datetime import datetime
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
import redis

from newsvine_api.config import get_settings

LOGGER = logging.getLogger("newsvine.trending_scorer")

EVENT_WEIGHTS: dict[str, float] = {
    "click": 1.0,
    "like": 2.0,
    "bookmark": 1.8,
    "search": 1.5,
}


def _parse_timestamp(raw: str | None) -> float:
    if not raw:
        return time.time()
    cleaned = raw.strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(cleaned).timestamp()
    except ValueError:
        return time.time()


def _event_weight(event_type: str) -> float:
    return EVENT_WEIGHTS.get(event_type, 1.0)


def _update_score(
    client: redis.Redis,
    *,
    sorted_set_key: str,
    last_ts_hash_key: str,
    article_id: str,
    event_timestamp: float,
    signal: float,
    decay_window_seconds: float,
) -> float:
    old_score = client.zscore(sorted_set_key, article_id) or 0.0
    old_ts_raw = client.hget(last_ts_hash_key, article_id)
    old_ts = float(old_ts_raw) if old_ts_raw else event_timestamp

    elapsed = max(0.0, event_timestamp - old_ts)
    decay_multiplier = math.exp(-elapsed / max(1.0, decay_window_seconds))
    new_score = (old_score * decay_multiplier) + signal

    client.zadd(sorted_set_key, {article_id: new_score})
    client.hset(last_ts_hash_key, article_id, event_timestamp)
    return float(new_score)


def process_event(
    event: dict[str, Any],
    *,
    client: redis.Redis,
    decay_window_seconds: float,
) -> bool:
    article_id = str(event.get("article_id") or "").strip()
    if not article_id:
        return False

    event_type = str(event.get("event_type") or "click")
    country = str(event.get("country") or "global").lower()
    event_ts = _parse_timestamp(str(event.get("timestamp") or ""))
    signal = _event_weight(event_type)

    _update_score(
        client,
        sorted_set_key="trending:global",
        last_ts_hash_key="trending:last_ts:global",
        article_id=article_id,
        event_timestamp=event_ts,
        signal=signal,
        decay_window_seconds=decay_window_seconds,
    )

    _update_score(
        client,
        sorted_set_key=f"trending:country:{country}",
        last_ts_hash_key=f"trending:last_ts:country:{country}",
        article_id=article_id,
        event_timestamp=event_ts,
        signal=signal,
        decay_window_seconds=decay_window_seconds,
    )

    return True


def _publish_top_global(
    *,
    client: redis.Redis,
    producer: KafkaProducer,
    topic: str,
    top_n: int,
) -> None:
    article_ids = client.zrevrange("trending:global", 0, max(0, top_n - 1))
    payload = {
        "timestamp": datetime.utcnow().isoformat(),
        "article_ids": article_ids,
    }
    producer.send(topic, payload)


def consume_once(max_messages: int = 50, timeout_seconds: int = 20) -> int:
    settings = get_settings()
    decay_window_seconds = float(os.getenv("TRENDING_DECAY_WINDOW_SECONDS", "3600"))
    top_n = int(os.getenv("TRENDING_TOP_N", "50"))

    client = redis.from_url(settings.redis_url, decode_responses=True)
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"trending-scorer-once-{int(time.time() * 1000)}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    deadline = time.time() + timeout_seconds
    processed = 0

    try:
        while time.time() < deadline and processed < max_messages:
            records = consumer.poll(timeout_ms=1000, max_records=max_messages)
            if not records:
                continue

            for partition_records in records.values():
                for message in partition_records:
                    if process_event(
                        message.value,
                        client=client,
                        decay_window_seconds=decay_window_seconds,
                    ):
                        processed += 1
    finally:
        _publish_top_global(
            client=client,
            producer=producer,
            topic=settings.trending_updates_topic,
            top_n=top_n,
        )
        producer.flush(timeout=10)
        producer.close()
        consumer.close()

    return processed


def run_forever() -> None:
    settings = get_settings()
    decay_window_seconds = float(os.getenv("TRENDING_DECAY_WINDOW_SECONDS", "3600"))
    top_n = int(os.getenv("TRENDING_TOP_N", "50"))
    publish_interval_seconds = float(os.getenv("TRENDING_PUBLISH_INTERVAL_SECONDS", "5"))

    client = redis.from_url(settings.redis_url, decode_responses=True)
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=os.getenv("TRENDING_GROUP_ID", "trending-scorer"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    LOGGER.info("Starting trending scorer on topic %s", settings.user_interactions_topic)
    next_publish = time.time() + publish_interval_seconds

    try:
        for message in consumer:
            process_event(
                message.value,
                client=client,
                decay_window_seconds=decay_window_seconds,
            )

            now = time.time()
            if now >= next_publish:
                _publish_top_global(
                    client=client,
                    producer=producer,
                    topic=settings.trending_updates_topic,
                    top_n=top_n,
                )
                next_publish = now + publish_interval_seconds
    finally:
        producer.flush(timeout=10)
        producer.close()
        consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("TRENDING_LOG_LEVEL", "INFO"))
    run_forever()


if __name__ == "__main__":
    main()
