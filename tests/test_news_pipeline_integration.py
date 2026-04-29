import hashlib
import json
import socket
import time
import asyncio
from datetime import datetime, timezone
from urllib.parse import urlparse

from httpx import ASGITransport, AsyncClient
from kafka import KafkaConsumer, KafkaProducer
import pytest
import requests
from sqlalchemy import text

from newsvine_api.config import get_settings
from newsvine_api.db import Base, init_db
from newsvine_api.main import app
from newsvine_pipeline.consumer import consume_once


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


def _article_payload(article_id: str) -> dict[str, str]:
    return {
        "id": article_id,
        "title": "Fixture Story",
        "content": "This is a fixture article body for phase 2 integration testing.",
        "category": "general",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "fixture",
        "country": "global",
        "url": f"https://example.com/articles/{article_id}",
    }


@pytest.fixture(scope="session", autouse=True)
def setup_schema() -> None:
    engine = init_db()
    Base.metadata.create_all(bind=engine)


@pytest.fixture(autouse=True)
def clean_state() -> None:
    settings = get_settings()
    engine = init_db()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE news_raw_articles CASCADE"))

    if _can_connect("localhost", 9200):
        import requests

        requests.post(
            f"{settings.elasticsearch_url}/articles/_delete_by_query",
            json={"query": {"match_all": {}}},
            timeout=10,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ingest_fixture_article_then_get_news() -> None:
    settings = get_settings()

    kafka_host, kafka_port = _bootstrap_host_port(settings.kafka_bootstrap_servers)
    es_host, es_port = _url_host_port(settings.elasticsearch_url, 9200)

    if not _can_connect(kafka_host, kafka_port) or not _can_connect(es_host, es_port):
        pytest.skip("Kafka or Elasticsearch is not reachable locally")

    article_id = hashlib.sha256(str(time.time()).encode("utf-8")).hexdigest()
    fixture = _article_payload(article_id)

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    producer.send(settings.news_topic, fixture).get(timeout=10)
    producer.flush(timeout=10)
    producer.close()

    consumed_any = False
    indexed = False
    consume_deadline = time.time() + 90
    while time.time() < consume_deadline:
        processed = consume_once(max_messages=250, timeout_seconds=8)
        if processed > 0:
            consumed_any = True

        lookup = requests.get(
            f"{settings.elasticsearch_url}/articles/_doc/{article_id}",
            timeout=10,
        )
        if lookup.status_code == 200 and lookup.json().get("found"):
            indexed = True
            break

        await asyncio.sleep(1)

    assert consumed_any
    assert indexed

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        started = time.time()
        found = False
        while time.time() - started < 60:
            response = await client.get("/news", params={"limit": 25, "offset": 0})
            assert response.status_code == 200
            items = response.json()["items"]
            if any(item["id"] == article_id for item in items):
                found = True
                break
            await asyncio.sleep(1)

        assert found

        detail = await client.get(f"/news/{article_id}")
        assert detail.status_code == 200
        assert detail.json()["id"] == article_id

    consumer = KafkaConsumer(
        settings.user_interactions_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=f"test-user-interactions-{article_id[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=4000,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    events = [record.value for record in consumer]
    consumer.close()

    assert any(event.get("article_id") == article_id for event in events)
