\connect users;

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_preferences (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    preference_key VARCHAR(100) NOT NULL,
    preference_value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reading_history (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    article_id VARCHAR(128) NOT NULL,
    read_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS bookmarks (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    article_id VARCHAR(128) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    jti VARCHAR(64) UNIQUE NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_raw_articles (
    id VARCHAR(128) PRIMARY KEY,
    source VARCHAR(120) NOT NULL,
    country VARCHAR(40) NOT NULL,
    category VARCHAR(80) NOT NULL,
    url TEXT NOT NULL,
    published_at TIMESTAMP NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS interaction_events_raw (
    event_id VARCHAR(64) PRIMARY KEY,
    event_type VARCHAR(32) NOT NULL,
    article_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(128) NOT NULL,
    country VARCHAR(40) NOT NULL,
    topic VARCHAR(80) NOT NULL,
    query TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_ts TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_interaction_events_raw_event_ts
    ON interaction_events_raw (event_ts DESC);

CREATE INDEX IF NOT EXISTS ix_interaction_events_raw_user_id
    ON interaction_events_raw (user_id);

CREATE INDEX IF NOT EXISTS ix_interaction_events_raw_article_id
    ON interaction_events_raw (article_id);

CREATE SCHEMA IF NOT EXISTS news_features;

CREATE TABLE IF NOT EXISTS news_features.article_embeddings (
    article_id VARCHAR(128) PRIMARY KEY,
    category VARCHAR(80) NOT NULL,
    model_name VARCHAR(120) NOT NULL,
    embedding_json TEXT NOT NULL,
    embedding_dim INTEGER NOT NULL,
    source_published_at TIMESTAMP NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_article_embeddings_category
    ON news_features.article_embeddings (category);

CREATE INDEX IF NOT EXISTS ix_article_embeddings_refreshed_at
    ON news_features.article_embeddings (refreshed_at DESC);

CREATE TABLE IF NOT EXISTS news_features.als_user_recommendations (
    user_id VARCHAR(128) NOT NULL,
    article_id VARCHAR(128) NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    model_run_id VARCHAR(64) NOT NULL,
    refreshed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, article_id)
);

CREATE INDEX IF NOT EXISTS ix_als_user_recommendations_user
    ON news_features.als_user_recommendations (user_id);
