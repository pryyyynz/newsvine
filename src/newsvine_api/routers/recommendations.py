import json
import logging
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
import redis
import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from newsvine_api.config import Settings, get_settings
from newsvine_api.db import get_db
from newsvine_api.models import UserPreference
from newsvine_api.recommendation_utils import (
    SparseVector,
    cosine_similarity,
    deserialize_sparse_vector,
)
from newsvine_api.schemas import NewsArticle, RecommendationItem, RecommendationListResponse
from newsvine_api.security import decode_token

router = APIRouter(tags=["recommendations"])
LOGGER = logging.getLogger("newsvine.recommendations")


@dataclass
class _TrendingCacheEntry:
    created_at: float
    items: list[RecommendationItem]


_TRENDING_CACHE: _TrendingCacheEntry | None = None


@lru_cache(maxsize=1)
def _get_redis_client() -> redis.Redis:
    settings = get_settings()
    return redis.from_url(settings.redis_url, decode_responses=True)


def _subject_from_authorization(authorization: str | None) -> str | None:
    if not authorization:
        return None

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    token = parts[1].strip()
    if not token:
        return None

    try:
        payload = decode_token(token)
    except Exception:
        return None

    if payload.get("type") != "access":
        return None

    subject = payload.get("sub")
    return str(subject) if subject else None


def _log_fallback(
    reason: str,
    *,
    user_id: str | None,
    limit: int,
    cache_age_seconds: float | None = None,
) -> None:
    payload: dict[str, Any] = {
        "event": "recommendation_fallback",
        "reason": reason,
        "user_id": user_id or "anonymous",
        "limit": limit,
    }
    if cache_age_seconds is not None:
        payload["cache_age_seconds"] = round(cache_age_seconds, 3)

    LOGGER.info(json.dumps(payload, separators=(",", ":")))


def _fetch_articles(article_ids: list[str]) -> dict[str, dict[str, Any]]:
    if not article_ids:
        return {}

    settings = get_settings()
    try:
        response = requests.post(
            f"{settings.elasticsearch_url}/articles/_mget",
            json={"ids": article_ids},
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Search backend unavailable",
        ) from exc

    docs = response.json().get("docs", [])
    result: dict[str, dict[str, Any]] = {}
    for doc in docs:
        if not doc.get("found"):
            continue
        source = doc.get("_source", {})
        article_id = str(doc.get("_id", ""))
        result[article_id] = {"id": article_id, **source}
    return result


def _load_user_embedding(client: redis.Redis, user_id: str | None) -> SparseVector:
    if not user_id:
        return {}
    return deserialize_sparse_vector(client.get(f"user:{user_id}:embedding"))


def _load_user_topic_vector(client: redis.Redis, user_id: str | None) -> dict[str, float]:
    if not user_id:
        return {}

    raw = client.hgetall(f"user:{user_id}:vector")
    parsed: dict[str, float] = {}
    for topic, value in raw.items():
        try:
            parsed[topic.lower()] = float(value)
        except (TypeError, ValueError):
            continue
    return parsed


def _candidate_article_ids(
    *,
    client: redis.Redis,
    user_id: str | None,
    settings: Settings,
    limit: int,
) -> list[str]:
    candidate_limit = max(limit, settings.recommendation_candidate_limit)
    topic_limit = max(1, settings.recommendation_topic_candidate_limit)
    per_topic_limit = max(20, candidate_limit // topic_limit)

    topic_vector = _load_user_topic_vector(client, user_id)
    ranked_topics = sorted(topic_vector.items(), key=lambda item: item[1], reverse=True)[:topic_limit]

    seen: set[str] = set()
    candidates: list[str] = []

    for topic, _score in ranked_topics:
        category_key = f"reco:category:{topic}:recent"
        article_ids = client.zrevrange(category_key, 0, per_topic_limit - 1)
        for article_id in article_ids:
            if article_id in seen:
                continue
            seen.add(article_id)
            candidates.append(article_id)

    trending_ids = client.zrevrange("trending:global", 0, candidate_limit - 1)
    for article_id in trending_ids:
        if article_id in seen:
            continue
        seen.add(article_id)
        candidates.append(article_id)

    return candidates


def _scale_to_unit_interval(raw_scores: dict[str, float]) -> dict[str, float]:
    if not raw_scores:
        return {}

    values = list(raw_scores.values())
    low = min(values)
    high = max(values)
    if high == low:
        return {article_id: 1.0 if score > 0 else 0.0 for article_id, score in raw_scores.items()}

    return {
        article_id: (score - low) / (high - low)
        for article_id, score in raw_scores.items()
    }


def _rank_personalized(
    *,
    client: redis.Redis,
    user_id: str | None,
    user_embedding: SparseVector,
    candidate_ids: list[str],
    limit: int,
    settings: Settings,
) -> list[RecommendationItem]:
    if not candidate_ids or not user_embedding:
        return []

    embedding_keys = [f"article:{article_id}:embedding" for article_id in candidate_ids]
    embedding_values = client.mget(embedding_keys)

    article_embeddings: dict[str, SparseVector] = {}
    for article_id, raw_embedding in zip(candidate_ids, embedding_values, strict=False):
        parsed = deserialize_sparse_vector(raw_embedding)
        if parsed:
            article_embeddings[article_id] = parsed

    if not article_embeddings:
        return []

    article_ids = list(article_embeddings.keys())
    pipeline = client.pipeline()
    for article_id in article_ids:
        pipeline.zscore("trending:global", article_id)

    als_key = f"user:{user_id}:als" if user_id else ""
    if als_key:
        for article_id in article_ids:
            pipeline.zscore(als_key, article_id)

    combined_raw_values = pipeline.execute()
    split_index = len(article_ids)
    trending_raw_values = combined_raw_values[:split_index]
    collaborative_raw_values = combined_raw_values[split_index:] if als_key else []

    trending_scores: dict[str, float] = {}
    for article_id, raw_score in zip(article_ids, trending_raw_values, strict=False):
        trending_scores[article_id] = float(raw_score) if raw_score is not None else 0.0

    trending_boosts = _scale_to_unit_interval(trending_scores)

    collaborative_scores: dict[str, float] = {}
    if collaborative_raw_values:
        for article_id, raw_score in zip(article_ids, collaborative_raw_values, strict=False):
            collaborative_scores[article_id] = float(raw_score) if raw_score is not None else 0.0
    collaborative_boosts = _scale_to_unit_interval(collaborative_scores)

    ranked_scores: list[tuple[str, float]] = []
    for article_id, article_embedding in article_embeddings.items():
        content_similarity = max(0.0, cosine_similarity(user_embedding, article_embedding))
        trending_boost = trending_boosts.get(article_id, 0.0)
        collaborative = collaborative_boosts.get(article_id, 0.0)
        final_score = (
            (settings.recommendation_content_weight * content_similarity)
            + (settings.recommendation_trending_weight * trending_boost)
            + (settings.recommendation_collaborative_weight * collaborative)
        )
        ranked_scores.append((article_id, final_score))

    ranked_scores.sort(key=lambda item: item[1], reverse=True)
    top_ranked = ranked_scores[:limit]
    article_lookup = _fetch_articles([article_id for article_id, _score in top_ranked])

    items: list[RecommendationItem] = []
    for article_id, score in top_ranked:
        payload = article_lookup.get(article_id)
        if not payload:
            continue
        items.append(RecommendationItem(score=float(score), article=NewsArticle(**payload)))

    return items


def _global_trending_items(client: redis.Redis, limit: int) -> list[RecommendationItem]:
    ranked = client.zrevrange("trending:global", 0, max(0, limit - 1), withscores=True)
    article_ids = [article_id for article_id, _score in ranked]
    article_lookup = _fetch_articles(article_ids)

    raw_scores = {article_id: float(score) for article_id, score in ranked}
    normalized_scores = _scale_to_unit_interval(raw_scores)

    items: list[RecommendationItem] = []
    for article_id, _score in ranked:
        payload = article_lookup.get(article_id)
        if not payload:
            continue
        items.append(
            RecommendationItem(
                score=normalized_scores.get(article_id, 0.0),
                article=NewsArticle(**payload),
            )
        )
    return items


def _parse_preference_topics(raw_value: str) -> list[str]:
    value = raw_value.strip()
    if not value:
        return []

    try:
        parsed: Any = json.loads(value)
    except json.JSONDecodeError:
        parsed = value

    if isinstance(parsed, list):
        values = parsed
    elif isinstance(parsed, str):
        values = [part.strip() for part in parsed.split(",") if part.strip()]
    else:
        values = []

    topics: list[str] = []
    for item in values:
        if not isinstance(item, str):
            continue
        normalized = item.strip().lower()
        if normalized:
            topics.append(normalized)
    return topics


def _preference_topics(db: Session, user_id: str | None) -> list[str]:
    if not user_id:
        return []

    try:
        user_pk = int(user_id)
    except ValueError:
        return []

    rows = db.scalars(select(UserPreference).where(UserPreference.user_id == user_pk)).all()
    topics: list[str] = []
    for row in rows:
        key = row.preference_key.lower()
        if "topic" not in key and "category" not in key:
            continue
        topics.extend(_parse_preference_topics(row.preference_value))

    deduped: list[str] = []
    seen: set[str] = set()
    for topic in topics:
        if topic in seen:
            continue
        seen.add(topic)
        deduped.append(topic)

    return deduped


def _topic_filtered_trending_items(
    *,
    client: redis.Redis,
    settings: Settings,
    preference_topics: list[str],
    limit: int,
) -> list[RecommendationItem]:
    if not preference_topics:
        return []

    candidate_limit = max(limit, settings.recommendation_candidate_limit)
    ranked = client.zrevrange("trending:global", 0, candidate_limit - 1, withscores=True)
    if not ranked:
        return []

    pipeline = client.pipeline()
    for article_id, _score in ranked:
        pipeline.hget(f"article:{article_id}:meta", "category")
    categories = pipeline.execute()

    allowed_topics = set(preference_topics)
    filtered_ranked: list[tuple[str, float]] = []
    for (article_id, score), category in zip(ranked, categories, strict=False):
        category_name = str(category or "").lower().strip()
        if category_name and category_name in allowed_topics:
            filtered_ranked.append((article_id, float(score)))

    if not filtered_ranked:
        return []

    filtered_ranked.sort(key=lambda item: item[1], reverse=True)
    top_ranked = filtered_ranked[:limit]
    article_lookup = _fetch_articles([article_id for article_id, _score in top_ranked])

    raw_scores = {article_id: score for article_id, score in top_ranked}
    normalized_scores = _scale_to_unit_interval(raw_scores)

    items: list[RecommendationItem] = []
    for article_id, _score in top_ranked:
        payload = article_lookup.get(article_id)
        if not payload:
            continue
        items.append(
            RecommendationItem(
                score=normalized_scores.get(article_id, 0.0),
                article=NewsArticle(**payload),
            )
        )

    return items


def _store_trending_cache(items: list[RecommendationItem]) -> None:
    global _TRENDING_CACHE
    _TRENDING_CACHE = _TrendingCacheEntry(
        created_at=time.time(),
        items=[item.model_copy(deep=True) for item in items],
    )


def _read_trending_cache(
    *,
    limit: int,
    ttl_seconds: int,
) -> tuple[list[RecommendationItem], float] | None:
    if _TRENDING_CACHE is None:
        return None

    cache_age_seconds = time.time() - _TRENDING_CACHE.created_at
    if cache_age_seconds > ttl_seconds:
        return None

    cached_items = [item.model_copy(deep=True) for item in _TRENDING_CACHE.items[:limit]]
    return cached_items, cache_age_seconds


@router.get("/recommendations", response_model=RecommendationListResponse)
def get_recommendations(
    limit: int = Query(default=20, ge=1, le=100),
    user_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> RecommendationListResponse:
    settings = get_settings()

    resolved_user_id = _subject_from_authorization(authorization)
    if not resolved_user_id and user_id:
        resolved_user_id = user_id.strip() or None

    try:
        client = _get_redis_client()

        user_embedding = _load_user_embedding(client, resolved_user_id)
        if user_embedding:
            candidate_ids = _candidate_article_ids(
                client=client,
                user_id=resolved_user_id,
                settings=settings,
                limit=limit,
            )
            personalized = _rank_personalized(
                client=client,
                user_id=resolved_user_id,
                user_embedding=user_embedding,
                candidate_ids=candidate_ids,
                limit=limit,
                settings=settings,
            )
            if personalized:
                return RecommendationListResponse(total=len(personalized), items=personalized)

        preference_topics = _preference_topics(db, resolved_user_id)
        if preference_topics:
            filtered_items = _topic_filtered_trending_items(
                client=client,
                settings=settings,
                preference_topics=preference_topics,
                limit=limit,
            )
            if filtered_items:
                _store_trending_cache(filtered_items)
                _log_fallback(
                    "empty_user_vector_preference_filtered_trending",
                    user_id=resolved_user_id,
                    limit=limit,
                )
                return RecommendationListResponse(total=len(filtered_items), items=filtered_items)

        global_items = _global_trending_items(client, limit)
        _store_trending_cache(global_items)
        if resolved_user_id:
            _log_fallback("empty_user_vector_global_trending", user_id=resolved_user_id, limit=limit)
        else:
            _log_fallback("anonymous_global_trending", user_id=None, limit=limit)
        return RecommendationListResponse(total=len(global_items), items=global_items)
    except redis.RedisError as exc:
        cached = _read_trending_cache(
            limit=limit,
            ttl_seconds=settings.recommendation_fallback_ttl_seconds,
        )
        if cached is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Recommendation cache unavailable",
            ) from exc

        cached_items, cache_age_seconds = cached
        _log_fallback(
            "redis_unavailable_in_memory_cache",
            user_id=resolved_user_id,
            limit=limit,
            cache_age_seconds=cache_age_seconds,
        )
        return RecommendationListResponse(total=len(cached_items), items=cached_items)
