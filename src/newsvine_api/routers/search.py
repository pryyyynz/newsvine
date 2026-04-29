from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query, status
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests

from newsvine_api.auth_context import subject_from_authorization
from newsvine_api.config import get_settings
from newsvine_api.schemas import NewsArticle, SearchResponse, SearchResultItem

router = APIRouter(tags=["search"])


@lru_cache(maxsize=1)
def _get_kafka_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def _search_query(*, q: str, country: str | None, limit: int, offset: int) -> dict[str, Any]:
    filters = []
    if country:
        filters.append({"term": {"country": country.lower().strip()}})

    return {
        "from": offset,
        "size": limit,
        "query": {
            "function_score": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "multi_match": {
                                    "query": q,
                                    "fields": ["title^2", "content", "content_snippet"],
                                    "type": "best_fields",
                                    "operator": "and",
                                }
                            }
                        ],
                        "filter": filters,
                    }
                },
                "functions": [
                    {
                        "gauss": {
                            "timestamp": {
                                "origin": "now",
                                "scale": "7d",
                                "offset": "0d",
                                "decay": 0.5,
                            }
                        }
                    }
                ],
                "score_mode": "multiply",
                "boost_mode": "sum",
            }
        },
    }


def _publish_search_event(
    *,
    query: str,
    country: str,
    user_id: str,
    topic: str,
    article_id: str,
) -> None:
    settings = get_settings()
    event = {
        "event_id": hashlib.sha256(
            f"search|{user_id}|{query}|{datetime.now(timezone.utc).isoformat()}".encode("utf-8")
        ).hexdigest(),
        "event_type": "search",
        "article_id": article_id,
        "query": query,
        "user_id": user_id,
        "country": country,
        "topic": topic,
        "metadata": {
            "source": "search_endpoint",
            "country": country,
            "topic": topic,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    producer = _get_kafka_producer()
    producer.send(settings.user_interactions_topic, event)


@router.get("/search", response_model=SearchResponse)
def search(
    q: str = Query(min_length=2, max_length=256),
    country: str | None = Query(default=None, min_length=2, max_length=8),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    user_id: str | None = Query(default=None),
    authorization: str | None = Header(default=None),
) -> SearchResponse:
    settings = get_settings()
    query = _search_query(q=q.strip(), country=country, limit=limit, offset=offset)

    try:
        response = requests.post(
            f"{settings.elasticsearch_url}/articles/_search",
            json=query,
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Search backend unavailable",
        ) from exc

    payload = response.json()
    hits = payload.get("hits", {})
    total_raw = hits.get("total", 0)
    total = total_raw.get("value", 0) if isinstance(total_raw, dict) else int(total_raw)

    items: list[SearchResultItem] = []
    for hit in hits.get("hits", []):
        source = hit.get("_source", {})
        article_payload = {"id": hit.get("_id", ""), **source}
        score = float(hit.get("_score") or 0.0)
        items.append(
            SearchResultItem(
                relevance_score=score,
                article=NewsArticle(**article_payload),
            )
        )

    resolved_user = subject_from_authorization(authorization) or (user_id or "anonymous").strip()
    resolved_country = (country or "global").lower().strip() or "global"
    resolved_topic = items[0].article.category.lower().strip() if items else "general"
    fallback_article_id = f"search-{hashlib.sha256(q.encode('utf-8')).hexdigest()[:24]}"
    event_article_id = items[0].article.id if items else fallback_article_id

    try:
        _publish_search_event(
            query=q,
            country=resolved_country,
            user_id=resolved_user,
            topic=resolved_topic,
            article_id=event_article_id,
        )
    except KafkaError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event pipeline unavailable",
        ) from exc

    return SearchResponse(total=total, items=items)
