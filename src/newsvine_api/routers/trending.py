from datetime import datetime, timezone
from functools import lru_cache

from fastapi import APIRouter, HTTPException, Query, status
import redis
import requests

from newsvine_api.config import get_settings
from newsvine_api.schemas import NewsArticle, TrendingItem, TrendingListResponse

router = APIRouter(prefix="/trending", tags=["trending"])


@lru_cache(maxsize=1)
def _get_redis_client() -> redis.Redis:
    settings = get_settings()
    return redis.from_url(settings.redis_url, decode_responses=True)


def _fetch_articles(article_ids: list[str]) -> dict[str, dict]:
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
    result: dict[str, dict] = {}
    for doc in docs:
        if not doc.get("found"):
            continue
        source = doc.get("_source", {})
        result[doc.get("_id", "")] = {"id": doc.get("_id", ""), **source}
    return result


def _parse_timestamp(raw: str | None) -> datetime | None:
    if raw is None:
        return None

    cleaned = raw.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _freshness_multiplier(raw: str | None) -> float:
    settings = get_settings()
    published_at = _parse_timestamp(raw)
    if published_at is None:
        return 1.0

    age_days = max(0.0, (datetime.now(timezone.utc) - published_at).total_seconds() / 86400.0)
    decay_after_days = max(0.0, settings.trending_freshness_decay_after_days)
    max_age_days = max(decay_after_days, settings.trending_freshness_max_age_days)

    if age_days <= decay_after_days:
        return 1.0
    if max_age_days == decay_after_days or age_days >= max_age_days:
        return 0.0

    remaining_window = max_age_days - decay_after_days
    return max(0.0, min(1.0, (max_age_days - age_days) / remaining_window))


def _read_trending_set(key: str, limit: int) -> TrendingListResponse:
    client = _get_redis_client()
    try:
        candidate_limit = min(1000, max(limit * 5, 50))
        ranked = client.zrevrange(key, 0, max(0, candidate_limit - 1), withscores=True)
    except redis.RedisError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Trending cache unavailable",
        ) from exc

    article_ids = [member for member, _score in ranked]
    articles = _fetch_articles(article_ids)

    adjusted_ranked: list[tuple[float, dict]] = []
    for member, score in ranked:
        payload = articles.get(member)
        if not payload:
            continue

        freshness = _freshness_multiplier(payload.get("timestamp"))
        if freshness <= 0.0:
            continue

        adjusted_ranked.append((float(score) * freshness, payload))

    adjusted_ranked.sort(key=lambda item: item[0], reverse=True)

    items: list[TrendingItem] = []
    for adjusted_score, payload in adjusted_ranked[:limit]:
        items.append(TrendingItem(score=adjusted_score, article=NewsArticle(**payload)))

    return TrendingListResponse(total=len(items), items=items)


@router.get("/global", response_model=TrendingListResponse)
def get_global_trending(
    limit: int = Query(default=50, ge=1, le=200),
) -> TrendingListResponse:
    return _read_trending_set("trending:global", limit)


@router.get("/regional", response_model=TrendingListResponse)
def get_regional_trending(
    user_country: str = Query(min_length=2, max_length=8),
    limit: int = Query(default=50, ge=1, le=200),
) -> TrendingListResponse:
    country = user_country.lower()
    return _read_trending_set(f"trending:country:{country}", limit)
