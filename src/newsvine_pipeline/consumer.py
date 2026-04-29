import json
import logging
import os
import re
import time
from datetime import datetime
from functools import lru_cache
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
import redis
import requests
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import Base, init_db
from newsvine_api import models as _models  # noqa: F401
from newsvine_pipeline.recommendation_embeddings import TfidfEmbeddingIndexer, get_embedding_indexer

LOGGER = logging.getLogger("newsvine.consumer")
REQUIRED_KEYS = {"id", "title", "content", "category", "timestamp", "source", "country", "url"}
OPTIONAL_KEYS = {"image_url"}
ALLOWED_KEYS = REQUIRED_KEYS | OPTIONAL_KEYS

# ── Keyword-based article classifier ────────────────────────────────────
_CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "technology": [
        "artificial intelligence", "software", "cyber", "cyberattack",
        "robot", "startup", "google", "microsoft", "apple inc",
        "openai", "chatgpt", "semiconductor", "quantum computing",
        "blockchain", "cryptocurrency", "bitcoin", "smartphone", "5g",
        "cloud computing", "machine learning", "data breach", "social media",
        "tesla", "spacex", "nvidia", "algorithm", "tech giant", "tech firm",
        "silicon valley", "deepfake", "autonomous vehicle", "ai model",
    ],
    "business": [
        "economy", "economic", "stock market", "trade war", "tariff", "gdp",
        "inflation", "interest rate", "central bank", "federal reserve", "imf",
        "world bank", "investment", "investor", "ipo", "merger", "acquisition",
        "revenue", "profit", "bankruptcy", "recession", "oil price", "opec",
        "wall street", "nasdaq", "dow jones", "ftse", "financial",
        "tax cut", "budget deficit", "debt crisis", "bond market", "currency",
        "supply chain", "manufacturing", "sanctions", "embargo",
        "fuel price", "commodity",
    ],
    "science": [
        "nasa", "asteroid", "planet", "mars mission", "moon landing", "orbit",
        "telescope", "physics", "biology", "chemistry", "fossil", "dinosaur",
        "evolution", "genome", "dna", "study finds", "experiment",
        "scientific", "scientist", "discovery", "species", "earthquake",
        "volcano", "climate change", "climate crisis", "carbon emission",
        "renewable energy", "archaeology", "paleontology", "artemis",
        "satellite launch", "space station", "rover", "comet", "glacier",
    ],
    "health": [
        "medical", "hospital", "patient", "disease",
        "virus", "vaccine", "covid", "pandemic", "cancer", "treatment",
        "pharmaceutical", "mental health", "surgery", "diagnosis",
        "outbreak", "infection", "world health", "clinical trial",
        "therapy", "nutrition", "obesity", "diabetes", "public health",
        "healthcare", "malaria", "cholera", "ebola", "tuberculosis",
    ],
    "sports": [
        "football", "soccer", "cricket", "tennis", "basketball", "nba",
        "nfl", "fifa", "olympic", "championship", "tournament", "league",
        "world cup", "premier league", "champions league", "rugby", "golf",
        "formula 1", "boxing", "ufc", "athlete", "stadium", "referee",
        "goalkeeper", "striker", "midfielder", "la liga", "serie a",
        "bundesliga", "eredivisie", "real madrid", "derby win", "maradona",
    ],
    "entertainment": [
        "movie", "film", "actor", "actress", "celebrity", "album",
        "concert", "grammy", "oscar", "emmy", "netflix", "disney", "tv show",
        "streaming", "hollywood", "bollywood", "pop star", "singer",
        "box office", "festival", "k-pop", "red carpet", "musician",
    ],
}


def _classify_article(title: str, content: str, existing_category: str) -> str:
    """Return a specific category based on title/content keywords, or existing_category."""
    if existing_category and existing_category != "general":
        return existing_category

    text_lower = f"{title} {content[:1000]}".lower()
    scores: dict[str, int] = {}
    for category, keywords in _CATEGORY_KEYWORDS.items():
        score = sum(1 for kw in keywords if kw in text_lower)
        if score >= 1:
            scores[category] = score

    if not scores:
        return "world"  # default for news articles with no specific category match

    return max(scores, key=scores.get)


def _ensure_schema() -> None:
    engine = init_db()
    Base.metadata.create_all(bind=engine)


def _to_datetime(raw: str) -> datetime:
    cleaned = raw.strip()

    match = re.match(
        r"^(?P<base>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(?P<fraction>\d+))?(?P<tz>Z|[+-]\d{2}:\d{2})?$",
        cleaned,
    )
    if match:
        base = match.group("base")
        fraction_raw = match.group("fraction") or ""
        fraction = f".{fraction_raw[:6].ljust(6, '0')}" if fraction_raw else ""
        tz = match.group("tz") or "+00:00"
        if tz == "Z":
            tz = "+00:00"
        cleaned = f"{base}{fraction}{tz}"

    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    parsed = datetime.fromisoformat(cleaned)
    return parsed.replace(tzinfo=None)


def _validate_and_trim(payload: dict[str, Any]) -> dict[str, str]:
    article = {key: payload.get(key) for key in ALLOWED_KEYS}
    missing = [key for key in REQUIRED_KEYS if not article.get(key)]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(sorted(missing))}")
    return {
        key: ("" if value is None else str(value))
        for key, value in article.items()
    }


@lru_cache(maxsize=1)
def _get_embedding_indexer() -> TfidfEmbeddingIndexer:
    return get_embedding_indexer()


def process_article_record(article: dict[str, Any], *, elasticsearch_url: str) -> None:
    clean = _validate_and_trim(article)

    enriched_category = _classify_article(
        clean["title"], clean["content"], clean["category"],
    )

    document = {
        "id": clean["id"],
        "title": clean["title"],
        "content": clean["content"],
        "content_snippet": clean["content"][:500],
        "category": enriched_category,
        "timestamp": clean["timestamp"],
        "source": clean["source"],
        "country": clean["country"],
        "url": clean["url"],
        "image_url": clean.get("image_url", ""),
    }

    response = requests.put(
        f"{elasticsearch_url}/articles/_doc/{clean['id']}",
        json=document,
        timeout=10,
    )
    response.raise_for_status()

    engine = init_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO news_raw_articles (id, source, country, category, url, published_at, payload, ingested_at)
                VALUES (:id, :source, :country, :category, :url, :published_at, CAST(:payload AS JSONB), :ingested_at)
                ON CONFLICT (id) DO UPDATE
                SET source = EXCLUDED.source,
                    country = EXCLUDED.country,
                    category = EXCLUDED.category,
                    url = EXCLUDED.url,
                    published_at = EXCLUDED.published_at,
                    payload = EXCLUDED.payload,
                    ingested_at = EXCLUDED.ingested_at
                """
            ),
            {
                "id": clean["id"],
                "source": clean["source"],
                "country": clean["country"],
                "category": enriched_category,
                "url": clean["url"],
                "published_at": _to_datetime(clean["timestamp"]),
                "payload": json.dumps(clean),
                "ingested_at": datetime.utcnow(),
            },
        )

    try:
        _get_embedding_indexer().index_article(
            article_id=clean["id"],
            title=clean["title"],
            content=clean["content"],
            category=clean["category"],
            country=clean["country"],
            timestamp=clean["timestamp"],
        )
    except redis.RedisError:
        LOGGER.exception("Failed to store recommendation embedding for article_id=%s", clean["id"])


def consume_once(max_messages: int = 10, timeout_seconds: int = 20) -> int:
    _ensure_schema()
    settings = get_settings()
    group_id = f"news-consumer-once-{int(time.time() * 1000)}"

    consumer = KafkaConsumer(
        settings.news_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    deadline = time.time() + timeout_seconds
    processed = 0

    try:
        while time.time() < deadline and processed < max_messages:
            records = consumer.poll(timeout_ms=1000, max_records=max_messages)
            if not records:
                continue

            for topic_partition in records.values():
                for message in topic_partition:
                    try:
                        process_article_record(
                            message.value,
                            elasticsearch_url=settings.elasticsearch_url,
                        )
                        processed += 1
                    except Exception as exc:
                        producer.send(
                            settings.news_dlq_topic,
                            {
                                "error": str(exc),
                                "payload": message.value,
                                "timestamp": datetime.utcnow().isoformat(),
                            },
                        )
    finally:
        producer.flush(timeout=10)
        producer.close()
        consumer.close()

    return processed


def run_forever() -> None:
    _ensure_schema()
    settings = get_settings()
    group_id = os.getenv("CONSUMER_GROUP_ID", "news-article-consumer")

    consumer = KafkaConsumer(
        settings.news_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=group_id,
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    LOGGER.info("Starting consumer for topic %s", settings.news_topic)

    try:
        for message in consumer:
            try:
                process_article_record(message.value, elasticsearch_url=settings.elasticsearch_url)
            except Exception as exc:
                producer.send(
                    settings.news_dlq_topic,
                    {
                        "error": str(exc),
                        "payload": message.value,
                        "timestamp": datetime.utcnow().isoformat(),
                    },
                )
    finally:
        producer.flush(timeout=10)
        producer.close()
        consumer.close()


def main() -> None:
    logging.basicConfig(level=os.getenv("CONSUMER_LOG_LEVEL", "INFO"))
    run_forever()


if __name__ == "__main__":
    main()
