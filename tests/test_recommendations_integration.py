import socket
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from httpx import ASGITransport, AsyncClient
import pytest
import redis
import requests
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import Base, init_db
from newsvine_api.main import app
from newsvine_api.recommendation_utils import serialize_sparse_vector
from newsvine_api.routers import recommendations as recommendations_router


def _can_connect(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(1)
        try:
            sock.connect((host, port))
            return True
        except OSError:
            return False


def _url_host_port(url: str, default_port: int) -> tuple[str, int]:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    return host, port


def _index_article(article_id: str, *, category: str) -> None:
    settings = get_settings()
    response = requests.put(
        f"{settings.elasticsearch_url}/articles/_doc/{article_id}",
        json={
            "id": article_id,
            "title": f"Article {article_id}",
            "content": "Recommendation integration content",
            "content_snippet": "Recommendation integration content",
            "category": category,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "fixture",
            "country": "us",
            "url": f"https://example.com/{article_id}",
        },
        timeout=10,
    )
    response.raise_for_status()


@pytest.fixture(scope="session", autouse=True)
def setup_schema() -> None:
    engine = init_db()
    Base.metadata.create_all(bind=engine)


@pytest.fixture(autouse=True)
def clean_state() -> None:
    settings = get_settings()

    engine = init_db()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE news_raw_articles RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE refresh_tokens RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE bookmarks RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE reading_history RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE user_preferences RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))

    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    patterns = [
        "article:*:embedding",
        "article:*:meta",
        "reco:category:*:recent",
        "reco:tfidf:v1:vocabulary",
        "trending:*",
        "user:*:vector",
        "user:*:embedding",
        "user:*:history",
    ]
    for pattern in patterns:
        keys = list(redis_client.scan_iter(match=pattern))
        if keys:
            redis_client.delete(*keys)

    recommendations_router._TRENDING_CACHE = None
    recommendations_router._get_redis_client.cache_clear()

    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)
    if _can_connect(es_host, es_port):
        requests.post(
            f"{settings.elasticsearch_url}/articles/_delete_by_query",
            json={"query": {"match_all": {}}},
            timeout=10,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recommendations_personalized_ranking() -> None:
    settings = get_settings()
    redis_host, redis_port = _url_host_port(settings.redis_url, 6379)
    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)

    if not _can_connect(redis_host, redis_port) or not _can_connect(es_host, es_port):
        pytest.skip("Redis or Elasticsearch is not reachable")

    article_a = f"reco-a-{int(time.time())}"
    article_b = f"reco-b-{int(time.time())}"
    _index_article(article_a, category="tech")
    _index_article(article_b, category="tech")

    cache = redis.from_url(settings.redis_url, decode_responses=True)
    now_score = datetime.now(timezone.utc).timestamp()

    cache.set(f"article:{article_a}:embedding", serialize_sparse_vector({"1": 1.0}))
    cache.set(f"article:{article_b}:embedding", serialize_sparse_vector({"2": 1.0}))
    cache.hset(f"article:{article_a}:meta", mapping={"category": "tech", "country": "us"})
    cache.hset(f"article:{article_b}:meta", mapping={"category": "tech", "country": "us"})
    cache.zadd("reco:category:tech:recent", {article_a: now_score, article_b: now_score - 1})
    cache.zadd("trending:global", {article_a: 10.0, article_b: 10.0})
    cache.hset("user:test-user-1:vector", "tech", 2.0)
    cache.set("user:test-user-1:embedding", serialize_sparse_vector({"1": 1.0}))

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/recommendations",
            params={"user_id": "test-user-1", "limit": 10},
        )

    assert response.status_code == 200
    items = response.json()["items"]
    ids = [item["article"]["id"] for item in items]

    assert article_a in ids
    assert article_b in ids
    assert ids.index(article_a) < ids.index(article_b)
    assert isinstance(items[0]["score"], float)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recommendations_cold_start_uses_preference_topics() -> None:
    settings = get_settings()
    redis_host, redis_port = _url_host_port(settings.redis_url, 6379)
    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)

    if not _can_connect(redis_host, redis_port) or not _can_connect(es_host, es_port):
        pytest.skip("Redis or Elasticsearch is not reachable")

    article_tech = f"reco-tech-{int(time.time())}"
    article_sports = f"reco-sports-{int(time.time())}"
    _index_article(article_tech, category="tech")
    _index_article(article_sports, category="sports")

    engine = init_db()
    with engine.begin() as conn:
        user_id = conn.execute(
            text(
                """
                INSERT INTO users (email, password_hash, created_at)
                VALUES (:email, :password_hash, NOW())
                RETURNING id
                """
            ),
            {
                "email": f"reco-user-{int(time.time())}@example.com",
                "password_hash": "fixture-password-hash",
            },
        ).scalar_one()
        conn.execute(
            text(
                """
                INSERT INTO user_preferences (user_id, preference_key, preference_value)
                VALUES (:user_id, :preference_key, :preference_value)
                """
            ),
            {
                "user_id": int(user_id),
                "preference_key": "topics",
                "preference_value": "tech",
            },
        )

    cache = redis.from_url(settings.redis_url, decode_responses=True)
    cache.hset(f"article:{article_tech}:meta", mapping={"category": "tech", "country": "us"})
    cache.hset(f"article:{article_sports}:meta", mapping={"category": "sports", "country": "us"})
    cache.zadd("trending:global", {article_tech: 12.0, article_sports: 13.0})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/recommendations",
            params={"user_id": str(user_id), "limit": 10},
        )

    assert response.status_code == 200
    items = response.json()["items"]

    assert items
    assert all(item["article"]["category"] == "tech" for item in items)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_recommendations_fallback_to_in_memory_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = get_settings()
    redis_host, redis_port = _url_host_port(settings.redis_url, 6379)
    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)

    if not _can_connect(redis_host, redis_port) or not _can_connect(es_host, es_port):
        pytest.skip("Redis or Elasticsearch is not reachable")

    article_id = f"reco-fallback-{int(time.time())}"
    _index_article(article_id, category="general")

    cache = redis.from_url(settings.redis_url, decode_responses=True)
    cache.hset(f"article:{article_id}:meta", mapping={"category": "general", "country": "us"})
    cache.zadd("trending:global", {article_id: 8.0})

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        warm_response = await client.get("/recommendations", params={"limit": 10})
        assert warm_response.status_code == 200

        def _raise_redis_error() -> redis.Redis:
            raise redis.RedisError("simulated redis outage")

        monkeypatch.setattr(recommendations_router, "_get_redis_client", _raise_redis_error)

        fallback_response = await client.get("/recommendations", params={"limit": 10})

    assert fallback_response.status_code == 200
    items = fallback_response.json()["items"]
    assert any(item["article"]["id"] == article_id for item in items)
