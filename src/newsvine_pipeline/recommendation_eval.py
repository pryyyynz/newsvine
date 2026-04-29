from __future__ import annotations

import argparse
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path

import mlflow

from newsvine_api.recommendation_utils import SparseVector, cosine_similarity, l2_normalize


@dataclass(frozen=True)
class ArticleRecord:
    article_id: str
    category: str
    embedding: SparseVector
    trending_score: float


@dataclass(frozen=True)
class UserRecord:
    user_id: str
    preferred_category: str
    embedding: SparseVector


def _dcg_at_k(relevances: list[float], k: int) -> float:
    total = 0.0
    for rank, relevance in enumerate(relevances[:k], start=1):
        denominator = math.log2(rank + 1)
        total += (2.0**relevance - 1.0) / denominator
    return total


def ndcg_at_k(ranked_article_ids: list[str], relevance_map: dict[str, float], k: int) -> float:
    ranked_relevances = [relevance_map.get(article_id, 0.0) for article_id in ranked_article_ids]
    best_relevances = sorted(relevance_map.values(), reverse=True)

    dcg = _dcg_at_k(ranked_relevances, k)
    idcg = _dcg_at_k(best_relevances, k)
    if idcg == 0.0:
        return 0.0
    return dcg / idcg


def _build_article_embedding(category_index: int, *, rng: random.Random) -> SparseVector:
    vector: SparseVector = {str(category_index): 1.0}
    for _ in range(3):
        noise_dim = str(rng.randint(0, 63))
        vector[noise_dim] = vector.get(noise_dim, 0.0) + rng.uniform(0.01, 0.08)
    return l2_normalize(vector)


def _build_user_embedding(category_index: int, *, rng: random.Random) -> SparseVector:
    vector: SparseVector = {str(category_index): 1.0}
    for _ in range(2):
        noise_dim = str(rng.randint(0, 63))
        vector[noise_dim] = vector.get(noise_dim, 0.0) + rng.uniform(0.01, 0.05)
    return l2_normalize(vector)


def generate_synthetic_dataset(
    *,
    seed: int,
    user_count: int,
    article_count: int,
) -> tuple[list[UserRecord], list[ArticleRecord], list[dict[str, str]]]:
    rng = random.Random(seed)
    categories = ["tech", "business", "sports", "health", "science", "politics"]

    users: list[UserRecord] = []
    for user_idx in range(user_count):
        preferred_category = categories[user_idx % len(categories)]
        category_index = categories.index(preferred_category)
        users.append(
            UserRecord(
                user_id=f"user-{user_idx}",
                preferred_category=preferred_category,
                embedding=_build_user_embedding(category_index, rng=rng),
            )
        )

    articles: list[ArticleRecord] = []
    for article_idx in range(article_count):
        category = categories[article_idx % len(categories)]
        category_index = categories.index(category)
        articles.append(
            ArticleRecord(
                article_id=f"article-{article_idx}",
                category=category,
                embedding=_build_article_embedding(category_index, rng=rng),
                trending_score=rng.uniform(0.0, 100.0),
            )
        )

    interactions: list[dict[str, str]] = []
    start_ts = datetime.now(timezone.utc) - timedelta(days=30)
    for user in users:
        preferred_articles = [article for article in articles if article.category == user.preferred_category]
        sampled = rng.sample(preferred_articles, k=min(12, len(preferred_articles)))
        for offset, article in enumerate(sampled):
            interactions.append(
                {
                    "user_id": user.user_id,
                    "article_id": article.article_id,
                    "event_type": "click",
                    "timestamp": (start_ts + timedelta(hours=offset)).isoformat(),
                }
            )

    return users, articles, interactions


def _min_max_scale(value: float, low: float, high: float) -> float:
    if high == low:
        return 1.0 if value > 0 else 0.0
    return (value - low) / (high - low)


def rank_content_based(
    *,
    user: UserRecord,
    articles: list[ArticleRecord],
    content_weight: float,
    trending_weight: float,
    collaborative_weight: float,
) -> list[str]:
    trending_values = [article.trending_score for article in articles]
    low = min(trending_values)
    high = max(trending_values)

    scored: list[tuple[str, float]] = []
    for article in articles:
        content_similarity = max(0.0, cosine_similarity(user.embedding, article.embedding))
        trending_boost = _min_max_scale(article.trending_score, low, high)
        collaborative = 0.0
        total_score = (
            (content_weight * content_similarity)
            + (trending_weight * trending_boost)
            + (collaborative_weight * collaborative)
        )
        scored.append((article.article_id, total_score))

    scored.sort(key=lambda item: item[1], reverse=True)
    return [article_id for article_id, _score in scored]


def rank_random(*, articles: list[ArticleRecord], rng: random.Random) -> list[str]:
    article_ids = [article.article_id for article in articles]
    rng.shuffle(article_ids)
    return article_ids


def evaluate_ndcg(
    *,
    users: list[UserRecord],
    articles: list[ArticleRecord],
    rng: random.Random,
    k: int,
    content_weight: float,
    trending_weight: float,
    collaborative_weight: float,
) -> tuple[float, float]:
    model_scores: list[float] = []
    random_scores: list[float] = []

    for user in users:
        relevance_map = {
            article.article_id: 3.0 if article.category == user.preferred_category else 0.0
            for article in articles
        }

        model_ranked = rank_content_based(
            user=user,
            articles=articles,
            content_weight=content_weight,
            trending_weight=trending_weight,
            collaborative_weight=collaborative_weight,
        )
        random_ranked = rank_random(articles=articles, rng=rng)

        model_scores.append(ndcg_at_k(model_ranked, relevance_map, k))
        random_scores.append(ndcg_at_k(random_ranked, relevance_map, k))

    mean_model = sum(model_scores) / max(1, len(model_scores))
    mean_random = sum(random_scores) / max(1, len(random_scores))
    return mean_model, mean_random


def log_to_mlflow(*, ndcg_model: float, ndcg_random: float, uplift: float, k: int) -> None:
    mlflow.set_experiment("recommendation-eval")
    with mlflow.start_run(run_name="phase4-content-based-v1"):
        mlflow.log_param("k", k)
        mlflow.log_param("content_weight", 0.6)
        mlflow.log_param("trending_weight", 0.3)
        mlflow.log_param("collaborative_weight", 0.1)
        mlflow.log_metric("ndcg_random_at_20", ndcg_random)
        mlflow.log_metric("ndcg_content_at_20", ndcg_model)
        mlflow.log_metric("uplift_vs_random", uplift)


def _write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row))
            handle.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run offline recommendation evaluation with NDCG@20")
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--users", type=int, default=120)
    parser.add_argument("--articles", type=int, default=600)
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument("--write-interactions", type=str, default="")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    users, articles, interactions = generate_synthetic_dataset(
        seed=args.seed,
        user_count=max(10, args.users),
        article_count=max(100, args.articles),
    )

    ndcg_model, ndcg_random = evaluate_ndcg(
        users=users,
        articles=articles,
        rng=rng,
        k=max(1, args.k),
        content_weight=0.6,
        trending_weight=0.3,
        collaborative_weight=0.1,
    )

    uplift = 0.0
    if ndcg_random > 0:
        uplift = (ndcg_model - ndcg_random) / ndcg_random

    if args.write_interactions:
        _write_jsonl(Path(args.write_interactions), interactions)

    log_to_mlflow(
        ndcg_model=ndcg_model,
        ndcg_random=ndcg_random,
        uplift=uplift,
        k=max(1, args.k),
    )

    print(f"NDCG@20 random baseline: {ndcg_random:.4f}")
    print(f"NDCG@20 content model: {ndcg_model:.4f}")
    print(f"Relative uplift: {uplift:.2%}")

    required = ndcg_random * 1.30
    if ndcg_model <= required:
        raise AssertionError(
            "Content model NDCG@20 did not exceed random baseline by at least 30%. "
            f"required>{required:.4f}, actual={ndcg_model:.4f}"
        )


if __name__ == "__main__":
    main()
