import json
import logging
import time
from datetime import datetime
from functools import lru_cache

import redis
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import text
from sqlalchemy.engine import Engine

from newsvine_api.config import get_settings
from newsvine_api.db import init_db
from newsvine_api.recommendation_utils import serialize_sparse_vector, trim_sparse_vector

LOGGER = logging.getLogger("newsvine.recommendation_embeddings")
VOCABULARY_KEY = "reco:tfidf:v1:vocabulary"


class TfidfEmbeddingIndexer:
    def __init__(
        self,
        *,
        redis_client: redis.Redis,
        engine: Engine,
        bootstrap_doc_limit: int,
        category_cap: int,
    ) -> None:
        self.redis_client = redis_client
        self.engine = engine
        self.bootstrap_doc_limit = max(1, bootstrap_doc_limit)
        self.category_cap = max(1, category_cap)
        self._vectorizer: TfidfVectorizer | None = None

    def _bootstrap_corpus(self) -> list[str]:
        stmt = text(
            """
            SELECT payload->>'title' AS title, payload->>'content' AS content
            FROM news_raw_articles
            ORDER BY ingested_at ASC
            LIMIT :limit
            """
        )

        with self.engine.connect() as conn:
            rows = conn.execute(stmt, {"limit": self.bootstrap_doc_limit}).mappings().all()

        corpus: list[str] = []
        for row in rows:
            title = str(row.get("title") or "").strip()
            content = str(row.get("content") or "").strip()
            merged = " ".join(part for part in (title, content) if part)
            if merged:
                corpus.append(merged)

        return corpus

    def _load_cached_vocabulary(self) -> dict[str, int] | None:
        raw = self.redis_client.get(VOCABULARY_KEY)
        if not raw:
            return None

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None

        if not isinstance(payload, dict):
            return None

        vocabulary: dict[str, int] = {}
        for token, index in payload.items():
            if not isinstance(token, str):
                continue
            try:
                vocabulary[token] = int(index)
            except (TypeError, ValueError):
                continue

        return vocabulary or None

    def _store_vocabulary(self, vocabulary: dict[str, int]) -> None:
        serializable_vocabulary = {
            str(token): int(index)
            for token, index in vocabulary.items()
        }
        self.redis_client.set(
            VOCABULARY_KEY,
            json.dumps(serializable_vocabulary, separators=(",", ":")),
        )

    def _build_vectorizer(self, seed_documents: list[str]) -> TfidfVectorizer | None:
        corpus = self._bootstrap_corpus()
        corpus.extend(doc for doc in seed_documents if doc)
        if not corpus:
            return None

        vocabulary = self._load_cached_vocabulary()
        if vocabulary is None:
            bootstrap = TfidfVectorizer(stop_words="english", max_features=20000, norm="l2")
            bootstrap.fit(corpus)
            if not bootstrap.vocabulary_:
                return None
            vocabulary = dict(bootstrap.vocabulary_)
            self._store_vocabulary(vocabulary)

        vectorizer = TfidfVectorizer(stop_words="english", vocabulary=vocabulary, norm="l2")
        vectorizer.fit(corpus)
        return vectorizer

    def _ensure_vectorizer(self, seed_documents: list[str]) -> TfidfVectorizer | None:
        if self._vectorizer is not None:
            return self._vectorizer

        self._vectorizer = self._build_vectorizer(seed_documents)
        return self._vectorizer

    @staticmethod
    def _event_score(timestamp_raw: str | None) -> float:
        if not timestamp_raw:
            return time.time()

        cleaned = timestamp_raw.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned).timestamp()
        except ValueError:
            return time.time()

    def _evict_overflow(self, category_key: str) -> None:
        total = int(self.redis_client.zcard(category_key) or 0)
        overflow = total - self.category_cap
        if overflow <= 0:
            return

        stale_article_ids = self.redis_client.zrange(category_key, 0, overflow - 1)
        if not stale_article_ids:
            return

        pipeline = self.redis_client.pipeline()
        for stale_article_id in stale_article_ids:
            pipeline.zrem(category_key, stale_article_id)
            pipeline.delete(f"article:{stale_article_id}:embedding")
            pipeline.delete(f"article:{stale_article_id}:meta")
        pipeline.execute()

    def index_article(
        self,
        *,
        article_id: str,
        title: str,
        content: str,
        category: str,
        country: str,
        timestamp: str,
    ) -> None:
        document = " ".join(part for part in (title.strip(), content.strip()) if part)
        if not document:
            return

        vectorizer = self._ensure_vectorizer([document])
        if vectorizer is None:
            LOGGER.warning("Skipping embedding index because vectorizer could not be built")
            return

        row = vectorizer.transform([document]).getrow(0)
        embedding = {
            str(index): float(value)
            for index, value in zip(row.indices.tolist(), row.data.tolist(), strict=False)
            if value > 0.0
        }
        embedding = trim_sparse_vector(embedding, max_terms=3000)

        category_clean = category.lower().strip() or "general"
        country_clean = country.lower().strip() or "global"
        category_key = f"reco:category:{category_clean}:recent"

        pipeline = self.redis_client.pipeline()
        pipeline.set(f"article:{article_id}:embedding", serialize_sparse_vector(embedding))
        pipeline.hset(
            f"article:{article_id}:meta",
            mapping={
                "category": category_clean,
                "country": country_clean,
                "timestamp": timestamp,
            },
        )
        pipeline.zadd(category_key, {article_id: self._event_score(timestamp)})
        pipeline.execute()

        self._evict_overflow(category_key)


@lru_cache(maxsize=1)
def get_embedding_indexer() -> TfidfEmbeddingIndexer:
    settings = get_settings()
    return TfidfEmbeddingIndexer(
        redis_client=redis.from_url(settings.redis_url, decode_responses=True),
        engine=init_db(),
        bootstrap_doc_limit=settings.recommendation_tfidf_bootstrap_docs,
        category_cap=settings.recommendation_category_embedding_cap,
    )
