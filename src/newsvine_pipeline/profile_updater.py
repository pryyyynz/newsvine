import json
import logging
import os
import time
from typing import Any

from kafka import KafkaConsumer
import redis

from newsvine_api.config import get_settings
from newsvine_api.recommendation_utils import (
    deserialize_sparse_vector,
    l2_normalize,
    serialize_sparse_vector,
    trim_sparse_vector,
)

LOGGER = logging.getLogger("newsvine.profile_updater")

SIGNALS: dict[str, float] = {
    "click": 1.0,
    "like": 2.0,
    "search": 1.5,
    "bookmark": 1.8,
}


def _signal_for_event(event_type: str) -> float:
    return SIGNALS.get(event_type, 1.0)


def _embedding_alpha() -> float:
    raw = os.getenv("PROFILE_EMBEDDING_ALPHA", "0.1")
    try:
        parsed = float(raw)
    except ValueError:
        return 0.1
    return min(1.0, max(0.01, parsed))


def _embedding_term_limit() -> int:
    raw = os.getenv("PROFILE_MAX_EMBEDDING_TERMS", "3000")
    try:
        parsed = int(raw)
    except ValueError:
        return 3000
    return max(100, parsed)


def _update_user_embedding(
    *,
    client: redis.Redis,
    user_id: str,
    article_id: str,
    signal: float,
) -> None:
    article_embedding_raw = client.get(f"article:{article_id}:embedding")
    article_embedding = deserialize_sparse_vector(article_embedding_raw)
    if not article_embedding:
        return

    user_embedding_key = f"user:{user_id}:embedding"
    existing_embedding = deserialize_sparse_vector(client.get(user_embedding_key))

    alpha = _embedding_alpha()
    retention = 1.0 - alpha

    merged: dict[str, float] = {}
    scaled_signal = max(0.0, signal)
    for term in set(existing_embedding) | set(article_embedding):
        old_value = existing_embedding.get(term, 0.0)
        article_value = article_embedding.get(term, 0.0) * scaled_signal
        updated = (retention * old_value) + (alpha * article_value)
        if updated != 0.0:
            merged[term] = updated

    normalized = l2_normalize(trim_sparse_vector(merged, _embedding_term_limit()))
    client.set(user_embedding_key, serialize_sparse_vector(normalized))


def process_event(event: dict[str, Any], *, client: redis.Redis) -> bool:
    user_id = str(event.get("user_id") or "").strip()
    article_id = str(event.get("article_id") or "").strip()
    event_type = str(event.get("event_type") or "click")
    topic = str(event.get("topic") or "general").lower()

    metadata = event.get("metadata") or {}
    if not user_id and isinstance(metadata, dict):
        user_id = str(metadata.get("user_id") or "").strip()
    if not topic and isinstance(metadata, dict):
        topic = str(metadata.get("topic") or "general").lower()

    if not user_id:
        return False

    signal = _signal_for_event(event_type)
    vector_key = f"user:{user_id}:vector"
    old_raw = client.hget(vector_key, topic)
    old_value = float(old_raw) if old_raw is not None else 0.0
    new_value = (0.9 * old_value) + (0.1 * signal)

    client.hset(vector_key, topic, new_value)

    if article_id:
        _update_user_embedding(client=client, user_id=user_id, article_id=article_id, signal=signal)
        history_key = f"user:{user_id}:history"
        client.lpush(history_key, article_id)
        client.ltrim(history_key, 0, 499)

    return True


def consume_once(max_messages: int = 50, timeout_seconds: int = 20) -> int:
    settings = get_settings()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"profile-updater-once-{int(time.time() * 1000)}",
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
                    if process_event(message.value, client=client):
                        processed += 1
    finally:
        consumer.close()

    return processed


def run_forever() -> None:
    settings = get_settings()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=os.getenv("PROFILE_GROUP_ID", "profile-updater"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    LOGGER.info("Starting profile updater on topic %s", settings.user_interactions_topic)

    try:
        for message in consumer:
            process_event(message.value, client=client)
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("PROFILE_LOG_LEVEL", "INFO"))
    run_forever()


if __name__ == "__main__":
    main()
