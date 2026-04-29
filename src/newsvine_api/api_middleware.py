from __future__ import annotations

from dataclasses import dataclass
import logging
import time
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import redis

from newsvine_api.auth_context import subject_from_authorization
from newsvine_api.config import get_settings

LOGGER = logging.getLogger("newsvine.api")

try:
    import structlog

    STRUCT_LOGGER = structlog.get_logger("newsvine.api")
except ImportError:
    STRUCT_LOGGER = None


@dataclass(frozen=True)
class BucketRule:
    capacity: int
    refill_per_second: float


class _TokenBucketLimiter:
    _SCRIPT = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_rate = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local tokens = tonumber(redis.call('HGET', key, 'tokens'))
local last_refill = tonumber(redis.call('HGET', key, 'last_refill'))

if tokens == nil then
  tokens = capacity
end

if last_refill == nil then
  last_refill = now
end

local elapsed = now - last_refill
if elapsed < 0 then
  elapsed = 0
end

tokens = math.min(capacity, tokens + (elapsed * refill_rate))

local allowed = 0
if tokens >= 1 then
  allowed = 1
  tokens = tokens - 1
end

redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
redis.call('EXPIRE', key, 180)

return {allowed, tokens}
"""

    def __init__(self, client: redis.Redis):
        self.client = client

    def allow(self, *, key: str, rule: BucketRule) -> bool:
        now = time.time()
        try:
            result = self.client.eval(
                self._SCRIPT,
                1,
                key,
                str(rule.capacity),
                str(rule.refill_per_second),
                str(now),
            )
        except redis.RedisError:
            # Fail open if redis is unavailable so API remains usable.
            return True

        if not isinstance(result, list) or not result:
            return True

        return int(result[0]) == 1


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",", 1)[0].strip() or "unknown"

    client = request.client
    if client is None or not client.host:
        return "unknown"
    return client.host


def register_middlewares(app: FastAPI) -> None:
    settings = get_settings()
    redis_client = redis.from_url(settings.redis_url, decode_responses=True)
    limiter = _TokenBucketLimiter(redis_client)

    user_capacity = max(1, settings.request_rate_limit_per_user_per_minute)
    ip_capacity = max(1, settings.request_rate_limit_per_ip_per_minute)
    user_rule = BucketRule(capacity=user_capacity, refill_per_second=user_capacity / 60.0)
    ip_rule = BucketRule(capacity=ip_capacity, refill_per_second=ip_capacity / 60.0)

    ignored_paths = {"/health", "/ready", "/metrics", "/openapi.json", "/docs", "/redoc"}

    @app.middleware("http")
    async def phase6_runtime_middleware(request: Request, call_next):
        started = time.perf_counter()
        request_id = request.headers.get("X-Request-ID", "").strip() or str(uuid4())
        request.state.request_id = request_id

        path = request.url.path
        method = request.method
        user_id = subject_from_authorization(request.headers.get("Authorization"))
        ip = _client_ip(request)

        if path not in ignored_paths:
            if not limiter.allow(key=f"rate:ip:{ip}", rule=ip_rule):
                response = JSONResponse(
                    status_code=429,
                    content={
                        "error": "rate_limited",
                        "message": "Too many requests from this IP",
                        "code": "RATE_LIMIT_IP",
                    },
                )
                response.headers["X-Request-ID"] = request_id
                return response

            if user_id and not limiter.allow(key=f"rate:user:{user_id}", rule=user_rule):
                response = JSONResponse(
                    status_code=429,
                    content={
                        "error": "rate_limited",
                        "message": "Too many requests for this user",
                        "code": "RATE_LIMIT_USER",
                    },
                )
                response.headers["X-Request-ID"] = request_id
                return response

        response = await call_next(request)
        response.headers["X-Request-ID"] = request_id

        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        payload = {
            "request_id": request_id,
            "method": method,
            "path": path,
            "status_code": response.status_code,
            "duration_ms": elapsed_ms,
            "user_id": user_id or "anonymous",
            "ip": ip,
        }
        if STRUCT_LOGGER is not None:
            STRUCT_LOGGER.info("request_complete", **payload)
        else:
            LOGGER.info("request_complete %s", payload)

        return response
