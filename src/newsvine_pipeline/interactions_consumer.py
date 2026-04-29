import json
import logging
import os
import time
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

from kafka import KafkaConsumer
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import init_db

LOGGER = logging.getLogger("newsvine.interactions_consumer")


def _ensure_schema() -> None:
    engine = init_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS interaction_events_raw (
                    event_id VARCHAR(64) PRIMARY KEY,
                    event_type VARCHAR(32) NOT NULL,
                    article_id VARCHAR(128) NOT NULL,
                    user_id VARCHAR(128) NOT NULL,
                    country VARCHAR(40) NOT NULL,
                    topic VARCHAR(80) NOT NULL,
                    query TEXT NULL,
                    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
                    event_ts TIMESTAMP NOT NULL,
                    ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS ix_interaction_events_raw_event_ts
                ON interaction_events_raw (event_ts DESC)
                """
            )
        )


def _parse_event_ts(raw: str | None) -> datetime:
    if not raw:
        return datetime.utcnow()

    cleaned = raw.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return datetime.utcnow()

    # Persist naive UTC timestamps to match the rest of local schema usage.
    if parsed.tzinfo is not None:
        return parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed


def _stable_event_id(event: dict[str, Any]) -> str:
    parts = [
        str(event.get("event_type") or "click"),
        str(event.get("article_id") or ""),
        str(event.get("user_id") or ""),
        str(event.get("timestamp") or ""),
    ]
    return sha256("|".join(parts).encode("utf-8")).hexdigest()[:64]


def _normalize_event(event: dict[str, Any]) -> dict[str, Any] | None:
    article_id = str(event.get("article_id") or "").strip()
    user_id = str(event.get("user_id") or "").strip()
    if not article_id or not user_id:
        return None

    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    event_id = str(event.get("event_id") or "").strip() or _stable_event_id(event)
    event_type = str(event.get("event_type") or "click").strip().lower()
    country = str(event.get("country") or metadata.get("country") or "global").strip().lower()
    topic = str(event.get("topic") or metadata.get("topic") or "general").strip().lower()
    query = event.get("query")

    return {
        "event_id": event_id,
        "event_type": event_type,
        "article_id": article_id,
        "user_id": user_id,
        "country": country or "global",
        "topic": topic or "general",
        "query": str(query) if query is not None else None,
        "metadata": json.dumps(metadata),
        "event_ts": _parse_event_ts(str(event.get("timestamp") or "")),
    }


def process_event_record(event: dict[str, Any]) -> bool:
    normalized = _normalize_event(event)
    if normalized is None:
        return False

    engine = init_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO interaction_events_raw (
                    event_id,
                    event_type,
                    article_id,
                    user_id,
                    country,
                    topic,
                    query,
                    metadata,
                    event_ts,
                    ingested_at
                )
                VALUES (
                    :event_id,
                    :event_type,
                    :article_id,
                    :user_id,
                    :country,
                    :topic,
                    :query,
                    CAST(:metadata AS JSONB),
                    :event_ts,
                    NOW()
                )
                ON CONFLICT (event_id) DO UPDATE
                SET event_type = EXCLUDED.event_type,
                    article_id = EXCLUDED.article_id,
                    user_id = EXCLUDED.user_id,
                    country = EXCLUDED.country,
                    topic = EXCLUDED.topic,
                    query = EXCLUDED.query,
                    metadata = EXCLUDED.metadata,
                    event_ts = EXCLUDED.event_ts,
                    ingested_at = NOW()
                """
            ),
            normalized,
        )

    return True


def consume_once(max_messages: int = 200, timeout_seconds: int = 20) -> int:
    _ensure_schema()
    settings = get_settings()
    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"interactions-consumer-once-{int(time.time() * 1000)}",
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
                    if process_event_record(message.value):
                        processed += 1
    finally:
        consumer.close()

    return processed


def run_forever() -> None:
    _ensure_schema()
    settings = get_settings()

    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=os.getenv("INTERACTIONS_GROUP_ID", "interactions-consumer"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    LOGGER.info("Starting interactions consumer on topic %s", settings.user_interactions_topic)

    try:
        for message in consumer:
            process_event_record(message.value)
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("INTERACTIONS_LOG_LEVEL", "INFO"))
    run_forever()


if __name__ == "__main__":
    main()
