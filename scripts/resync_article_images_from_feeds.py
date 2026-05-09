"""Refresh image_url in Elasticsearch from current RSS/NewsAPI items (ingestor rules).

Run after tightening image extraction so cards use feed thumbnails / inline RSS art again
instead of small social preview URLs that were stored earlier.

  ELASTICSEARCH_URL=http://localhost:9200 python scripts/resync_article_images_from_feeds.py

Requires the same env as the ingestor for NewsAPI (INGEST_NEWSAPI_KEY) if that source is enabled.
"""
from __future__ import annotations

import json
import os
import sys

import requests

from newsvine_pipeline.ingestor import _article_id, _build_sources, _extract_image_url, _fetch_articles

ES = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200").rstrip("/")


def main() -> None:
    resolved: dict[str, str] = {}
    for source in _build_sources():
        try:
            items = _fetch_articles(source)
        except Exception as exc:
            print(f"skip source {source['name']}: {exc}", file=sys.stderr)
            continue
        for item in items:
            url = str(item.get("url") or item.get("link") or "").strip()
            if not url:
                continue
            img = _extract_image_url(item)
            if not img:
                continue
            resolved[_article_id(url)] = img

    print(f"Resolved {len(resolved)} image URLs from live feeds")

    if not resolved:
        print("Nothing to write")
        return

    bulk_lines: list[str] = []
    for doc_id, img_url in resolved.items():
        bulk_lines.append(json.dumps({"update": {"_index": "articles", "_id": doc_id}}))
        bulk_lines.append(json.dumps({"doc": {"image_url": img_url}}))

    body = "\n".join(bulk_lines) + "\n"
    r = requests.post(
        f"{ES}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=120,
    )
    r.raise_for_status()
    result = r.json()
    errors = sum(1 for item in result["items"] if item.get("update", {}).get("error"))
    print(f"Bulk updated {len(resolved)} documents, errors: {errors}")


if __name__ == "__main__":
    main()
