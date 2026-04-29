import logging
import os
from collections import defaultdict

import redis
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import init_db

LOGGER = logging.getLogger("newsvine.phase5.refresh_als_scores")


def run() -> int:
    settings = get_settings()
    top_n = int(os.getenv("PHASE5_ALS_TOP_N", "200"))

    engine = init_db()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT user_id, article_id, score
                FROM news_features.als_user_recommendations
                ORDER BY user_id, score DESC
                """
            )
        ).mappings().all()

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        user_id = str(row["user_id"])
        if len(grouped[user_id]) >= top_n:
            continue
        grouped[user_id].append(dict(row))

    updated = 0
    for user_id, recs in grouped.items():
        key = f"user:{user_id}:als"
        payload = {str(item["article_id"]): float(item["score"]) for item in recs}

        pipeline = client.pipeline()
        pipeline.delete(key)
        if payload:
            pipeline.zadd(key, payload)
        pipeline.execute()
        updated += 1

    client.set("phase5:last_refresh:als_scores", str(updated))
    LOGGER.info("Refreshed ALS recommendation sets for %s users", updated)
    return updated


def main() -> None:
    logging.basicConfig(level=os.getenv("PHASE5_REDIS_REFRESH_LOG_LEVEL", "INFO"))
    run()


if __name__ == "__main__":
    main()
