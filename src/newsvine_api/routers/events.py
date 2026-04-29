from datetime import datetime, timezone
from functools import lru_cache
import json
from uuid import uuid4

from fastapi import APIRouter, Header, HTTPException, status
from kafka import KafkaProducer
from kafka.errors import KafkaError

from newsvine_api.config import get_settings
from newsvine_api.schemas import EventAcceptedResponse, EventRequest
from newsvine_api.security import decode_token

router = APIRouter(prefix="/events", tags=["events"])


@lru_cache(maxsize=1)
def _get_kafka_producer() -> KafkaProducer:
    settings = get_settings()
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


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


@router.post("", response_model=EventAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
def publish_event(
    payload: EventRequest,
    authorization: str | None = Header(default=None),
) -> EventAcceptedResponse:
    settings = get_settings()

    user_id = _subject_from_authorization(authorization) or payload.metadata.user_id
    if not user_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="metadata.user_id or Bearer access token is required",
        )

    country = (payload.metadata.country or "global").lower()
    topic = (payload.metadata.topic or "general").lower()

    event = {
        "event_id": str(uuid4()),
        "event_type": payload.event_type,
        "article_id": payload.article_id,
        "query": payload.query,
        "user_id": str(user_id),
        "country": country,
        "topic": topic,
        "metadata": payload.metadata.model_dump(exclude_none=True),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        producer = _get_kafka_producer()
        producer.send(settings.user_interactions_topic, event)
    except KafkaError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event pipeline unavailable",
        ) from exc

    return EventAcceptedResponse(status="accepted", event_id=event["event_id"])
