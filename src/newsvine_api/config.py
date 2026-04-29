from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "newsvine-api"
    app_env: str = "local"
    database_url: str = "postgresql+psycopg://newsvine:newsvine@localhost:5432/users"
    elasticsearch_url: str = "http://localhost:9200"
    redis_url: str = "redis://localhost:6379/0"
    kafka_bootstrap_servers: str = "localhost:9092"
    news_topic: str = "news-articles"
    user_interactions_topic: str = "user-interactions"
    trending_updates_topic: str = "trending-updates"
    news_dlq_topic: str = "news-articles-dlq"
    jwt_secret: str = "change-this-secret"
    jwt_algorithm: str = "HS256"
    access_token_minutes: int = 15
    refresh_token_days: int = 7
    auto_create_schema: bool = False
    recommendation_content_weight: float = 0.6
    recommendation_trending_weight: float = 0.3
    recommendation_collaborative_weight: float = 0.1
    recommendation_candidate_limit: int = 300
    recommendation_fallback_ttl_seconds: int = 60
    recommendation_tfidf_bootstrap_docs: int = 10000
    recommendation_category_embedding_cap: int = 500
    recommendation_topic_candidate_limit: int = 5
    trending_freshness_decay_after_days: float = 5.0
    trending_freshness_max_age_days: float = 10.0
    request_rate_limit_per_user_per_minute: int = 100
    request_rate_limit_per_ip_per_minute: int = 1000
    cors_allow_origins: str = "*"
    otel_enabled: bool = False
    otel_service_name: str = "newsvine-api"
    otel_exporter_otlp_endpoint: str = "http://localhost:4318/v1/traces"

    model_config = SettingsConfigDict(env_prefix="APP_", case_sensitive=False)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
