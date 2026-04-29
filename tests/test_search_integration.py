import asyncio
import socket
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from httpx import ASGITransport, AsyncClient
import pytest
import redis
import requests

from newsvine_api.config import get_settings
from newsvine_api.main import app
from newsvine_pipeline.profile_updater import consume_once as profile_consume_once


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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_search_results_emit_event_and_update_user_vector() -> None:
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

    article_id = f"search-tech-{int(time.time())}"
    response = requests.put(
        f"{settings.elasticsearch_url}/articles/_doc/{article_id}?refresh=wait_for",
        json={
            "id": article_id,
            "title": "Quantum AI Breakthrough",
            "content": "Quantum AI search relevance integration fixture",
            "content_snippet": "Quantum AI search relevance integration fixture",
            "category": "tech",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "fixture",
            "country": "us",
            "url": f"https://example.com/{article_id}",
        },
        timeout=10,
    )
    response.raise_for_status()

    user_id = "search-user-1"
    cache = redis.from_url(settings.redis_url, decode_responses=True)
    cache.delete(f"user:{user_id}:vector")

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        search_resp = await client.get(
            "/search",
            params={
                "q": "Quantum AI",
                "user_id": user_id,
                "country": "us",
                "limit": 5,
            },
        )

    assert search_resp.status_code == 200
    payload = search_resp.json()
    assert payload["items"]
    assert any(item["article"]["id"] == article_id for item in payload["items"])
    assert isinstance(payload["items"][0]["relevance_score"], float)

    deadline = time.time() + 90
    updated = False
    while time.time() < deadline:
        profile_consume_once(max_messages=250, timeout_seconds=8)
        value = cache.hget(f"user:{user_id}:vector", "tech")
        if value is not None and float(value) > 0:
            updated = True
            break
        await asyncio.sleep(1)

    assert updated
