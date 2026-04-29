from datetime import datetime, timedelta, timezone
import hashlib
import secrets

import bcrypt
import jwt

from newsvine_api.config import get_settings

def hash_password(password: str) -> str:
    hashed = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def _token_expiry(minutes: int = 0, days: int = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=minutes, days=days)


def create_access_token(subject: str) -> str:
    settings = get_settings()
    payload = {
        "sub": subject,
        "type": "access",
        "exp": _token_expiry(minutes=settings.access_token_minutes),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def create_refresh_token(subject: str) -> tuple[str, str, datetime]:
    settings = get_settings()
    raw_jti = secrets.token_hex(16)
    jti = hashlib.sha256(raw_jti.encode("utf-8")).hexdigest()
    exp = _token_expiry(days=settings.refresh_token_days)
    payload = {
        "sub": subject,
        "type": "refresh",
        "jti": jti,
        "exp": exp,
    }
    token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return token, jti, exp


def decode_token(token: str) -> dict:
    settings = get_settings()
    return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
