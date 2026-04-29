import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text

from newsvine_api.db import Base, init_db
from newsvine_api.main import app


@pytest.fixture(scope="session", autouse=True)
def setup_schema() -> None:
    engine = init_db()
    Base.metadata.create_all(bind=engine)


@pytest.fixture(autouse=True)
def clean_tables() -> None:
    engine = init_db()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE news_raw_articles RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE refresh_tokens RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE bookmarks RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE reading_history RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE user_preferences RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_register_login_and_refresh_flow() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        register = await client.post(
            "/auth/register", json={"email": "alice@example.com", "password": "StrongPass123"}
        )
        assert register.status_code == 201

        login = await client.post(
            "/auth/login", json={"email": "alice@example.com", "password": "StrongPass123"}
        )
        assert login.status_code == 200
        payload = login.json()
        assert "access_token" in payload
        assert "refresh_token" in payload

        refresh = await client.post(
            "/auth/refresh", json={"refresh_token": payload["refresh_token"]}
        )
        assert refresh.status_code == 200
        rotated = refresh.json()
        assert rotated["refresh_token"] != payload["refresh_token"]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reuse_old_refresh_token_fails() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post(
            "/auth/register", json={"email": "bob@example.com", "password": "StrongPass123"}
        )
        login = await client.post(
            "/auth/login", json={"email": "bob@example.com", "password": "StrongPass123"}
        )
        first_refresh = login.json()["refresh_token"]

        first_rotation = await client.post("/auth/refresh", json={"refresh_token": first_refresh})
        assert first_rotation.status_code == 200

        reuse_attempt = await client.post("/auth/refresh", json={"refresh_token": first_refresh})
        assert reuse_attempt.status_code == 401
