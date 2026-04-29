from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from sqlalchemy import text

from newsvine_api.api_middleware import register_middlewares
from newsvine_api.config import get_settings
from newsvine_api.db import Base, init_db
from newsvine_api.logging_config import configure_logging
from newsvine_api.routers.auth import router as auth_router
from newsvine_api.routers.events import router as events_router
from newsvine_api.routers.news import router as news_router
from newsvine_api.routers.recommendations import router as recommendations_router
from newsvine_api.routers.search import router as search_router
from newsvine_api.routers.trending import router as trending_router
from newsvine_api.routers.users import router as users_router
from newsvine_api.telemetry import configure_telemetry

LOGGER = logging.getLogger("newsvine.main")
configure_logging()


@asynccontextmanager
async def lifespan(_: FastAPI):
    settings = get_settings()
    engine = init_db()
    if settings.auto_create_schema:
        Base.metadata.create_all(bind=engine)
    yield


app = FastAPI(title="newsvine-api", lifespan=lifespan)
settings = get_settings()
cors_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins or ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-ID"],
    allow_credentials=False,
)
register_middlewares(app)
app.include_router(auth_router)
app.include_router(events_router)
app.include_router(news_router)
app.include_router(trending_router)
app.include_router(recommendations_router)
app.include_router(search_router)
app.include_router(users_router)

try:
    from prometheus_fastapi_instrumentator import Instrumentator

    Instrumentator().instrument(app).expose(app)
except ImportError:
    LOGGER.warning("prometheus-fastapi-instrumentator not installed; /metrics endpoint disabled")

configure_telemetry(app)


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
    message = str(exc.detail)
    payload = {
        "error": "http_error",
        "message": message,
        "code": f"HTTP_{exc.status_code}",
    }

    response = JSONResponse(status_code=exc.status_code, content=payload)
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers["X-Request-ID"] = str(request_id)
    return response


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    errors = exc.errors()
    message = "Validation failed"
    if errors:
        first = errors[0]
        detail = first.get("msg")
        if isinstance(detail, str) and detail.strip():
            message = detail

    payload = {
        "error": "validation_error",
        "message": message,
        "code": "VALIDATION_ERROR",
    }

    response = JSONResponse(status_code=422, content=payload)
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers["X-Request-ID"] = str(request_id)
    return response


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, _exc: Exception) -> JSONResponse:
    payload = {
        "error": "internal_error",
        "message": "Internal server error",
        "code": "INTERNAL_ERROR",
    }
    response = JSONResponse(status_code=500, content=payload)
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        response.headers["X-Request-ID"] = str(request_id)
    return response


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    engine = init_db()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return {"status": "ready"}
