import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import redis
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import init_db

LOGGER = logging.getLogger("newsvine.phase5.refresh_article_embeddings")


def _to_epoch(value: datetime) -> float:
    if value.tzinfo is None:
        return value.timestamp()
    return value.astimezone(timezone.utc).timestamp()


def run() -> tuple[int, int]:
    settings = get_settings()
    top_n = int(os.getenv("PHASE5_REDIS_CATEGORY_TOP_N", "500"))

    engine = init_db()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                    e.article_id,
                    e.category,
                    e.embedding_json,
                    e.source_published_at,
                    COALESCE(a.country, 'global') AS country
                FROM news_features.article_embeddings e
                LEFT JOIN news_raw_articles a
                  ON a.id = e.article_id
                ORDER BY e.category, e.source_published_at DESC
                """
            )
        ).mappings().all()

    by_category: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        category = str(row["category"] or "general").lower().strip() or "general"
        if len(by_category[category]) >= top_n:
            continue
        by_category[category].append(dict(row))

    article_count = 0
    category_count = 0

    for category, items in by_category.items():
        category_key = f"reco:category:{category}:recent"
        pipeline = client.pipeline()
        pipeline.delete(category_key)

        for item in items:
            article_id = str(item["article_id"])
            country = str(item["country"] or "global").lower().strip() or "global"
            published_at = item["source_published_at"]
            score = _to_epoch(published_at) if isinstance(published_at, datetime) else 0.0

            pipeline.set(f"article:{article_id}:embedding", str(item["embedding_json"]))
            pipeline.hset(
                f"article:{article_id}:meta",
                mapping={
                    "category": category,
                    "country": country,
                    "timestamp": str(published_at),
                },
            )
            pipeline.zadd(category_key, {article_id: score})
            article_count += 1

        pipeline.execute()
        category_count += 1

    # Remove old TF-IDF vocabulary after switching to phase 5 embeddings.
    client.delete("reco:tfidf:v1:vocabulary")
    client.set("phase5:last_refresh:article_embeddings", str(article_count))

    LOGGER.info(
        "Refreshed %s article embeddings across %s categories",
        article_count,
        category_count,
    )
    return article_count, category_count


def main() -> None:
    logging.basicConfig(level=os.getenv("PHASE5_REDIS_REFRESH_LOG_LEVEL", "INFO"))
    run()


if __name__ == "__main__":
    main()
