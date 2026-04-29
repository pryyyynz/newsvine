from datetime import datetime, timezone
from functools import lru_cache
import json

from fastapi import APIRouter, Header, HTTPException, Query, status
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests
from sqlalchemy import text

from newsvine_api.auth_context import subject_from_authorization
from newsvine_api.config import get_settings
from newsvine_api.db import init_db
from newsvine_api.schemas import NewsArticle, NewsListResponse

router = APIRouter(prefix="/news", tags=["news"])


@lru_cache(maxsize=1)
def _get_kafka_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def _es_search(category: str | None, country: str | None, limit: int, offset: int) -> dict:
    settings = get_settings()

    filters = []
    if category:
        filters.append({"term": {"category": category}})
    if country:
        filters.append({"term": {"country": country}})

    query: dict[str, object] = {
        "from": offset,
        "size": limit,
        "sort": [{"timestamp": {"order": "desc"}}],
        "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
    }

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

    return response.json()


def _es_get(article_id: str) -> dict:
    settings = get_settings()
    try:
        response = requests.get(
            f"{settings.elasticsearch_url}/articles/_doc/{article_id}",
            timeout=10,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Article not found")
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Search backend unavailable",
        ) from exc
    payload = response.json()
    source = payload.get("_source")
    if source is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Article not found")
    return {"id": payload.get("_id", article_id), **source}


def es_mget(article_ids: list[str]) -> dict[str, dict]:
    """Batch-fetch multiple articles from ES. Returns {id: doc_dict}."""
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
    except requests.RequestException:
        return {}
    result = {}
    for doc in response.json().get("docs", []):
        if doc.get("found"):
            source = doc.get("_source", {})
            result[doc["_id"]] = {"id": doc["_id"], **source}
    return result


def _emit_article_viewed(article_id: str) -> None:
    settings = get_settings()
    event = {
        "event_type": "click",
        "article_id": article_id,
        "user_id": "anonymous",
        "country": "global",
        "topic": "general",
        "metadata": {"source": "news_detail"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer = _get_kafka_producer()
        producer.send(settings.user_interactions_topic, event).get(timeout=10)
    except KafkaError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event pipeline unavailable",
        ) from exc


def _record_reading_history(user_id: str | None, article_id: str) -> None:
    if not user_id:
        return

    try:
        user_pk = int(user_id)
    except ValueError:
        return

    engine = init_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO reading_history (user_id, article_id, read_at)
                VALUES (:user_id, :article_id, NOW())
                """
            ),
            {"user_id": user_pk, "article_id": article_id},
        )


@router.get("", response_model=NewsListResponse)
def list_news(
    category: str | None = Query(default=None),
    country: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> NewsListResponse:
    payload = _es_search(category=category, country=country, limit=limit, offset=offset)
    hits = payload.get("hits", {})
    total_raw = hits.get("total", 0)
    total = total_raw.get("value", 0) if isinstance(total_raw, dict) else int(total_raw)

    items: list[NewsArticle] = []
    for hit in hits.get("hits", []):
        doc = hit.get("_source", {})
        if "id" not in doc:
            doc = {"id": hit.get("_id", ""), **doc}
        items.append(NewsArticle(**doc))

    return NewsListResponse(total=total, items=items)


@router.get("/{article_id}", response_model=NewsArticle)
def get_news(
    article_id: str,
    authorization: str | None = Header(default=None),
    track: bool = Query(default=True),
) -> NewsArticle:
    article = _es_get(article_id)
    if track:
        _emit_article_viewed(article_id)
        _record_reading_history(subject_from_authorization(authorization), article_id)
    return NewsArticle(**article)
