import logging
import os
from collections import defaultdict

import redis
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import init_db

LOGGER = logging.getLogger("newsvine.phase5.refresh_user_vectors")


def run() -> int:
    settings = get_settings()
    engine = init_db()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT user_id, category, normalized_topic_weight
                FROM marts.fct_user_profiles
                WHERE normalized_topic_weight > 0
                ORDER BY user_id, category
                """
            )
        ).mappings()

        vectors: dict[str, dict[str, float]] = defaultdict(dict)
        for row in rows:
            user_id = str(row["user_id"])
            category = str(row["category"]).lower().strip()
            weight = float(row["normalized_topic_weight"])
            vectors[user_id][category] = weight

    updated = 0
    for user_id, mapping in vectors.items():
        target_key = f"user:{user_id}:vector"
        temp_key = f"{target_key}:phase5_tmp"

        pipeline = client.pipeline()
        pipeline.delete(temp_key)
        if mapping:
            payload = {key: str(value) for key, value in mapping.items()}
            pipeline.hset(temp_key, mapping=payload)
        else:
            pipeline.hset(temp_key, mapping={"general": "0.0"})
        pipeline.rename(temp_key, target_key)
        pipeline.execute()
        updated += 1

    client.set("phase5:last_refresh:user_vectors", str(updated))
    LOGGER.info("Refreshed %s user vectors", updated)
    return updated


def main() -> None:
    logging.basicConfig(level=os.getenv("PHASE5_REDIS_REFRESH_LOG_LEVEL", "INFO"))
    run()


if __name__ == "__main__":
    main()
