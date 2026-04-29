import time

from httpx import ASGITransport, AsyncClient
import pytest
from sqlalchemy import text

from newsvine_api.db import init_db
from newsvine_api.main import app


@pytest.mark.integration
@pytest.mark.asyncio
async def test_users_me_update_history_and_bookmarks() -> None:
    email = f"phase6-user-{int(time.time())}@example.com"
    password = "StrongPass123"

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        register = await client.post(
            "/auth/register",
            json={"email": email, "password": password},
        )
        assert register.status_code == 201

        login = await client.post(
            "/auth/login",
            json={"email": email, "password": password},
        )
        assert login.status_code == 200
        token = login.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        me = await client.get("/users/me", headers=headers)
        assert me.status_code == 200
        assert me.json()["email"] == email
        assert me.json()["country"] == "global"

        update = await client.put(
            "/users/me",
            headers=headers,
            json={
                "country": "us",
                "preferences": {
                    "topics": ["tech", "science"],
                    "language": "en",
                },
            },
        )
        assert update.status_code == 200
        update_payload = update.json()
        assert update_payload["country"] == "us"
        assert update_payload["preferences"]["topics"] == ["tech", "science"]

        me_after = await client.get("/users/me", headers=headers)
        assert me_after.status_code == 200
        user_id = int(me_after.json()["id"])

    engine = init_db()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO reading_history (user_id, article_id, read_at)
                VALUES (:user_id, :article_id, NOW())
                """
            ),
            {"user_id": user_id, "article_id": "history-article-1"},
        )
        conn.execute(
            text(
                """
                INSERT INTO bookmarks (user_id, article_id, created_at)
                VALUES (:user_id, :article_id, NOW())
                """
            ),
            {"user_id": user_id, "article_id": "bookmark-article-1"},
        )

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        login = await client.post(
            "/auth/login",
            json={"email": email, "password": password},
        )
        token = login.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        history = await client.get("/users/me/history", headers=headers)
        assert history.status_code == 200
        assert history.json()["total"] >= 1
        assert any(item["article_id"] == "history-article-1" for item in history.json()["items"])

        bookmarks = await client.get("/users/me/bookmarks", headers=headers)
        assert bookmarks.status_code == 200
        assert bookmarks.json()["total"] >= 1
        assert any(item["article_id"] == "bookmark-article-1" for item in bookmarks.json()["items"])
