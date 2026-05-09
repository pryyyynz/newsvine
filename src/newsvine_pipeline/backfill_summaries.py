"""Backfill LLM summaries for already-indexed Elasticsearch articles."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import requests

from newsvine_api.config import get_settings
from newsvine_pipeline.nvidia_kimi import summarize_article_for_display

LOGGER = logging.getLogger("newsvine.backfill_summaries")


def _latest_articles(limit: int, scan_size: int) -> list[dict[str, Any]]:
    settings = get_settings()
    response = requests.post(
        f"{settings.elasticsearch_url}/articles/_search",
        json={
            "size": scan_size,
            "sort": [{"timestamp": {"order": "desc"}}],
            "query": {"match_all": {}},
        },
        timeout=20,
    )
    response.raise_for_status()

    articles: list[dict[str, Any]] = []
    for hit in response.json().get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        if str(source.get("ai_summary") or "").strip():
            continue
        content = str(source.get("content") or "").strip()
        title = str(source.get("title") or "").strip()
        if not title or len(content) < 80:
            continue
        articles.append({"id": hit.get("_id"), **source})
        if len(articles) >= limit:
            break
    return articles


def backfill(limit: int, scan_size: int) -> int:
    settings = get_settings()
    updated = 0

    for article in _latest_articles(limit=limit, scan_size=scan_size):
        article_id = article["id"]
        summary = summarize_article_for_display(article["title"], article["content"])
        if not summary:
            LOGGER.info("No summary generated for article_id=%s", article_id)
            continue

        update_doc = {
            "ai_summary": summary.get("ai_summary", ""),
            "key_points": summary.get("key_points", ""),
        }
        if update_doc["ai_summary"]:
            update_doc["content_snippet"] = update_doc["ai_summary"][:500]

        response = requests.post(
            f"{settings.elasticsearch_url}/articles/_update/{article_id}",
            json={"doc": update_doc},
            timeout=20,
        )
        response.raise_for_status()
        updated += 1
        LOGGER.info("Backfilled summary for article_id=%s", article_id)

    return updated


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--scan-size", type=int, default=50)
    args = parser.parse_args()
    updated = backfill(limit=max(1, args.limit), scan_size=max(args.limit, args.scan_size))
    LOGGER.info("Updated %s articles", updated)


if __name__ == "__main__":
    main()
