from newsvine_api.routers.auth import router as auth_router
from newsvine_api.routers.events import router as events_router
from newsvine_api.routers.news import router as news_router
from newsvine_api.routers.recommendations import router as recommendations_router
from newsvine_api.routers.trending import router as trending_router

__all__ = ["auth_router", "events_router", "news_router", "recommendations_router", "trending_router"]
