import json
import socket
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from httpx import ASGITransport, AsyncClient
from kafka import KafkaConsumer
import pytest
import redis
import requests

from newsvine_api.config import get_settings
from newsvine_api.main import app
from newsvine_pipeline.profile_updater import consume_once as profile_consume_once
from newsvine_pipeline.trending_scorer import consume_once as trending_consume_once


def _can_connect(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False


def _bootstrap_host_port(bootstrap_servers: str) -> tuple[str, int]:
    first = bootstrap_servers.split(",")[0].strip()
    if ":" in first:
        host, port = first.rsplit(":", 1)
        try:
            return host, int(port)
        except ValueError:
            return host, 9092
    return first, 9092


def _url_host_port(url: str, default_port: int) -> tuple[str, int]:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    return host, port


@pytest.fixture(autouse=True)
def clean_trending_state() -> None:
    settings = get_settings()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    client.delete(
        "trending:global",
        "trending:country:us",
        "trending:last_ts:global",
        "trending:last_ts:country:us",
        "user:test-user-1:vector",
        "user:test-user-1:history",
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_events_drive_trending_and_profile_updates() -> None:
    settings = get_settings()

    kafka_host, kafka_port = _bootstrap_host_port(settings.kafka_bootstrap_servers)
    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)
    redis_host, redis_port = _url_host_port(settings.redis_url, 6379)

    if (
        not _can_connect(kafka_host, kafka_port)
        or not _can_connect(es_host, es_port)
        or not _can_connect(redis_host, redis_port)
    ):
        pytest.skip("Kafka, Elasticsearch, or Redis is not reachable")

    now = datetime.now(timezone.utc).isoformat()
    article_a = f"trending-a-{int(time.time())}"
    article_b = f"trending-b-{int(time.time())}"

    for article_id in (article_a, article_b):
        resp = requests.put(
            f"{settings.elasticsearch_url}/articles/_doc/{article_id}",
            json={
                "id": article_id,
                "title": f"Article {article_id}",
                "content": "Phase 3 integration content",
                "content_snippet": "Phase 3 integration content",
                "category": "general",
                "timestamp": now,
                "source": "fixture",
                "country": "us",
                "url": f"https://example.com/{article_id}",
            },
            timeout=10,
        )
        resp.raise_for_status()

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        event_a = await client.post(
            "/events",
            json={
                "event_type": "like",
                "article_id": article_a,
                "metadata": {"user_id": "test-user-1", "country": "us", "topic": "tech"},
            },
        )
        event_b = await client.post(
            "/events",
            json={
                "event_type": "click",
                "article_id": article_b,
                "metadata": {"user_id": "test-user-1", "country": "us", "topic": "tech"},
            },
        )

        assert event_a.status_code == 202
        assert event_b.status_code == 202

    trending_processed = trending_consume_once(max_messages=50, timeout_seconds=20)
    profile_processed = profile_consume_once(max_messages=50, timeout_seconds=20)

    assert trending_processed >= 2
    assert profile_processed >= 2

    cache = redis.from_url(settings.redis_url, decode_responses=True)
    global_ids = cache.zrevrange("trending:global", 0, 9)
    regional_ids = cache.zrevrange("trending:country:us", 0, 9)

    assert article_a in global_ids
    assert article_a in regional_ids

    vector_value = cache.hget("user:test-user-1:vector", "tech")
    assert vector_value is not None
    assert float(vector_value) > 0

    history = cache.lrange("user:test-user-1:history", 0, 10)
    assert article_a in history or article_b in history

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        global_resp = await client.get("/trending/global", params={"limit": 10})
        regional_resp = await client.get("/trending/regional", params={"user_country": "us", "limit": 10})

    assert global_resp.status_code == 200
    assert regional_resp.status_code == 200
    assert any(item["article"]["id"] == article_a for item in global_resp.json()["items"])
    assert any(item["article"]["id"] == article_a for item in regional_resp.json()["items"])

    consumer = KafkaConsumer(
        settings.trending_updates_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-trending-updates-{int(time.time())}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=4000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    messages = [record.value for record in consumer]
    consumer.close()

    assert any(isinstance(msg.get("article_ids"), list) for msg in messages)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_global_trending_filters_out_articles_past_max_age() -> None:
    settings = get_settings()

    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)
    redis_host, redis_port = _url_host_port(settings.redis_url, 6379)
    if not _can_connect(es_host, es_port) or not _can_connect(redis_host, redis_port):
        pytest.skip("Elasticsearch or Redis is not reachable")

    now = datetime.now(timezone.utc)
    fresh_article_id = f"trending-fresh-{int(time.time())}"
    stale_article_id = f"trending-stale-{int(time.time())}"

    article_payloads = [
        (fresh_article_id, now.isoformat()),
        (stale_article_id, (now - timedelta(days=12)).isoformat()),
    ]

    for article_id, timestamp in article_payloads:
        response = requests.put(
            f"{settings.elasticsearch_url}/articles/_doc/{article_id}",
            json={
                "id": article_id,
                "title": f"Article {article_id}",
                "content": "Trending freshness integration content",
                "content_snippet": "Trending freshness integration content",
                "category": "general",
                "timestamp": timestamp,
                "source": "fixture",
                "country": "us",
                "url": f"https://example.com/{article_id}",
            },
            timeout=10,
        )
        response.raise_for_status()

    cache = redis.from_url(settings.redis_url, decode_responses=True)
    cache.zadd("trending:global", {fresh_article_id: 10.0, stale_article_id: 100.0})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/trending/global", params={"limit": 10})

    assert response.status_code == 200
    article_ids = [item["article"]["id"] for item in response.json()["items"]]
    assert fresh_article_id in article_ids
    assert stale_article_id not in article_ids
