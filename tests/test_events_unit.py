import pytest
from httpx import ASGITransport, AsyncClient

from newsvine_api.main import app
from newsvine_api.routers import events as events_router


class _FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []

    def send(self, topic: str, value: dict):
        self.sent.append((topic, value))
        return None


@pytest.mark.asyncio
async def test_events_publish_returns_accepted(monkeypatch: pytest.MonkeyPatch) -> None:
    producer = _FakeProducer()
    events_router._get_kafka_producer.cache_clear()
    monkeypatch.setattr(events_router, "_get_kafka_producer", lambda: producer)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/events",
            json={
                "event_type": "click",
                "article_id": "abc123",
                "metadata": {"user_id": "u-1", "country": "us", "topic": "tech"},
            },
        )

    assert response.status_code == 202
    payload = response.json()
    assert payload["status"] == "accepted"
    assert payload["event_id"]

    assert len(producer.sent) == 1
    topic, event = producer.sent[0]
    assert topic == "user-interactions"
    assert event["event_type"] == "click"
    assert event["article_id"] == "abc123"
    assert event["user_id"] == "u-1"
    assert event["country"] == "us"
    assert event["topic"] == "tech"


@pytest.mark.asyncio
async def test_events_rejects_invalid_event_type() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/events",
            json={
                "event_type": "share",
                "article_id": "abc123",
                "metadata": {"user_id": "u-1"},
            },
        )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_search_event_requires_query() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/events",
            json={
                "event_type": "search",
                "article_id": "abc123",
                "metadata": {"user_id": "u-1"},
            },
        )

    assert response.status_code == 422
