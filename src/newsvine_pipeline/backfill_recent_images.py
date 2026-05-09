"""Backfill image_url for the newest Elasticsearch articles that still have no image."""

from __future__ import annotations

import argparse
import logging
from typing import Any

import requests

from newsvine_api.config import get_settings
from newsvine_pipeline.ingestor import _extract_og_image_from_html, _is_social_card_image_url

LOGGER = logging.getLogger("newsvine.backfill_recent_images")


def _articles_missing_images(limit: int, scan_size: int) -> list[dict[str, Any]]:
    settings = get_settings()
    response = requests.post(
        f"{settings.elasticsearch_url}/articles/_search",
        json={
            "size": scan_size,
            "sort": [{"timestamp": {"order": "desc"}}],
            "_source": ["title", "url", "timestamp", "source", "image_url"],
            "query": {
                "bool": {
                    "should": [
                        {"term": {"image_url.keyword": ""}},
                        {"bool": {"must_not": [{"exists": {"field": "image_url"}}]}},
                    ],
                    "minimum_should_match": 1,
                }
            },
        },
        timeout=20,
    )
    response.raise_for_status()

    articles: list[dict[str, Any]] = []
    for hit in response.json().get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        if not source.get("url"):
            continue
        articles.append({"id": hit["_id"], **source})
        if len(articles) >= limit:
            break
    return articles


def _image_from_page(url: str) -> str:
    try:
        response = requests.get(
            url,
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsvineBot/1.0)"},
            allow_redirects=True,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.info("Could not fetch %s: %s", url, exc)
        return ""

    image_url = _extract_og_image_from_html(response.text[:200_000])
    if not image_url or not image_url.lower().startswith("http"):
        return ""
    if _is_social_card_image_url(image_url):
        return ""
    return image_url


def backfill(limit: int, scan_size: int) -> int:
    settings = get_settings()
    updated = 0

    for article in _articles_missing_images(limit=limit, scan_size=scan_size):
        image_url = _image_from_page(str(article["url"]))
        if not image_url:
            LOGGER.info("No image found for article_id=%s title=%r", article["id"], article.get("title"))
            continue

        response = requests.post(
            f"{settings.elasticsearch_url}/articles/_update/{article['id']}",
            json={"doc": {"image_url": image_url}},
            timeout=20,
        )
        response.raise_for_status()
        updated += 1
        LOGGER.info("Updated image for article_id=%s image_url=%s", article["id"], image_url)

    return updated


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--scan-size", type=int, default=40)
    args = parser.parse_args()
    updated = backfill(limit=max(1, args.limit), scan_size=max(args.limit, args.scan_size))
    LOGGER.info("Updated %s image URLs", updated)


if __name__ == "__main__":
    main()
