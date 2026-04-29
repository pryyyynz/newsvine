import json
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from kafka import KafkaProducer
from kafka.errors import KafkaError
import redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from newsvine_api.auth_context import subject_from_authorization
from newsvine_api.config import get_settings
from newsvine_api.db import get_db
from newsvine_api.models import Bookmark, ReadingHistory, User, UserPreference
from newsvine_api.routers.news import es_mget
from newsvine_api.schemas import (
    NewsArticle,
    UpdateUserProfileRequest,
    UserBookmarksResponse,
    UserBookmarkItem,
    UserHistoryItem,
    UserHistoryResponse,
    UserProfileResponse,
)

router = APIRouter(prefix="/users", tags=["users"])


def _current_user(authorization: str | None, db: Session) -> User:
    subject = subject_from_authorization(authorization)
    if not subject:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")

    try:
        user_id = int(subject)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized") from exc

    user = db.scalar(select(User).where(User.id == user_id))
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    return user


def _parse_preference(value: str) -> Any:
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _serialize_preference(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))


def _user_preferences(db: Session, user_id: int) -> tuple[str, dict[str, Any]]:
    rows = db.scalars(select(UserPreference).where(UserPreference.user_id == user_id)).all()

    preferences: dict[str, Any] = {}
    country = "global"

    for row in rows:
        key = row.preference_key.strip().lower()
        parsed = _parse_preference(row.preference_value)
        if key == "country":
            country = str(parsed).lower().strip() or "global"
        else:
            preferences[key] = parsed

    return country, preferences


def _set_user_preference(db: Session, user_id: int, key: str, value: Any) -> None:
    normalized_key = key.strip().lower()
    if not normalized_key:
        return

    existing = db.scalar(
        select(UserPreference).where(
            UserPreference.user_id == user_id,
            UserPreference.preference_key == normalized_key,
        )
    )

    serialized = _serialize_preference(value)
    if existing is None:
        db.add(
            UserPreference(
                user_id=user_id,
                preference_key=normalized_key,
                preference_value=serialized,
            )
        )
    else:
        existing.preference_value = serialized


def _invalidate_user_cache(user_id: int) -> None:
    settings = get_settings()
    client = redis.from_url(settings.redis_url, decode_responses=True)

    keys = [
        f"user:{user_id}:vector",
        f"user:{user_id}:embedding",
        f"user:{user_id}:als",
    ]
    client.delete(*keys)


@router.get("/me", response_model=UserProfileResponse)
def get_me(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> UserProfileResponse:
    user = _current_user(authorization, db)
    country, preferences = _user_preferences(db, user.id)
    return UserProfileResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        country=country,
        preferences=preferences,
    )


@router.put("/me", response_model=UserProfileResponse)
def update_me(
    payload: UpdateUserProfileRequest,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> UserProfileResponse:
    user = _current_user(authorization, db)

    if payload.country is not None:
        _set_user_preference(db, user.id, "country", payload.country.lower().strip())

    if payload.name is not None:
        user.name = payload.name.strip() or None

    for key, value in payload.preferences.items():
        _set_user_preference(db, user.id, key, value)

    db.commit()

    try:
        _invalidate_user_cache(user.id)
    except redis.RedisError:
        # Cache invalidation failure should not block profile updates.
        pass

    country, preferences = _user_preferences(db, user.id)
    return UserProfileResponse(
        id=user.id,
        email=user.email,
        name=user.name,
        country=country,
        preferences=preferences,
    )


@router.get("/me/interactions/{article_id}")
def get_article_interactions(
    article_id: str,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    user = _current_user(authorization, db)
    liked = db.scalar(
        select(func.count()).select_from(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "like",
        )
    ) or 0
    bookmarked = db.scalar(
        select(func.count()).select_from(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "bookmark",
        )
    ) or 0
    return {"liked": liked > 0, "bookmarked": bookmarked > 0}


@router.post("/me/likes/{article_id}", status_code=status.HTTP_201_CREATED)
def add_like(
    article_id: str,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    user = _current_user(authorization, db)
    existing = db.scalar(
        select(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "like",
        )
    )
    if not existing:
        db.add(Bookmark(user_id=user.id, article_id=article_id, bookmark_type="like"))
        db.commit()
        # Publish like event to Kafka so recommendations pipeline picks it up
        try:
            settings = get_settings()
            producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            producer.send(settings.user_interactions_topic, {
                "event_id": str(uuid4()),
                "event_type": "like",
                "article_id": article_id,
                "user_id": str(user.id),
                "country": "global",
                "topic": "general",
                "metadata": {"user_id": str(user.id)},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            producer.close(timeout=2)
        except KafkaError:
            pass  # Don't block the like on Kafka failure
    return {"status": "liked"}


@router.delete("/me/likes/{article_id}")
def remove_like(
    article_id: str,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    user = _current_user(authorization, db)
    row = db.scalar(
        select(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "like",
        )
    )
    if row:
        db.delete(row)
        db.commit()
    return {"status": "unliked"}


@router.post("/me/bookmarks/{article_id}", status_code=status.HTTP_201_CREATED)
def add_bookmark(
    article_id: str,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    user = _current_user(authorization, db)
    existing = db.scalar(
        select(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "bookmark",
        )
    )
    if not existing:
        db.add(Bookmark(user_id=user.id, article_id=article_id, bookmark_type="bookmark"))
        db.commit()
    return {"status": "bookmarked"}


@router.delete("/me/bookmarks/{article_id}")
def remove_bookmark(
    article_id: str,
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> dict:
    user = _current_user(authorization, db)
    row = db.scalar(
        select(Bookmark).where(
            Bookmark.user_id == user.id,
            Bookmark.article_id == article_id,
            Bookmark.bookmark_type == "bookmark",
        )
    )
    if row:
        db.delete(row)
        db.commit()
    return {"status": "unbookmarked"}

@router.get("/me/history", response_model=UserHistoryResponse)
def get_me_history(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> UserHistoryResponse:
    user = _current_user(authorization, db)

    total = int(
        db.scalar(select(func.count()).select_from(ReadingHistory).where(ReadingHistory.user_id == user.id))
        or 0
    )

    rows = db.scalars(
        select(ReadingHistory)
        .where(ReadingHistory.user_id == user.id)
        .order_by(ReadingHistory.read_at.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    # Batch-fetch all articles from ES in one request
    article_ids = list({row.article_id for row in rows})
    articles_map = es_mget(article_ids)

    items = []
    for row in rows:
        doc = articles_map.get(row.article_id)
        article = NewsArticle(**doc) if doc else None
        items.append(
            UserHistoryItem(article_id=row.article_id, read_at=row.read_at.isoformat(), article=article)
        )

    return UserHistoryResponse(total=total, items=items)


@router.get("/me/bookmarks", response_model=UserBookmarksResponse)
def get_me_bookmarks(
    limit: int = Query(default=20, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
) -> UserBookmarksResponse:
    user = _current_user(authorization, db)

    total = int(
        db.scalar(
            select(func.count()).select_from(Bookmark).where(
                Bookmark.user_id == user.id,
                Bookmark.bookmark_type == "bookmark",
            )
        ) or 0
    )

    rows = db.scalars(
        select(Bookmark)
        .where(Bookmark.user_id == user.id, Bookmark.bookmark_type == "bookmark")
        .order_by(Bookmark.created_at.desc())
        .limit(limit)
        .offset(offset)
    ).all()

    items = [
        UserBookmarkItem(article_id=row.article_id, created_at=row.created_at.isoformat())
        for row in rows
    ]

    return UserBookmarksResponse(total=total, items=items)
