import hashlib
import html
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import feedparser
from kafka import KafkaProducer
import redis
import requests

from newsvine_api.config import get_settings

LOGGER = logging.getLogger("newsvine.ingestor")
REQUIRED_FIELDS = ("id", "title", "content", "category", "timestamp", "source", "country", "url")


def _extract_image_url(item: dict[str, Any]) -> str:
    """Extract the best image URL from an RSS entry or API article."""
    # feedparser: media_thumbnail
    thumbnails = item.get("media_thumbnail") or []
    if thumbnails and isinstance(thumbnails, list):
        url = thumbnails[0].get("url", "")
        if url:
            return url
    # feedparser: media_content
    media = item.get("media_content") or []
    if media and isinstance(media, list):
        for m in media:
            if "image" in (m.get("type") or m.get("medium") or ""):
                url = m.get("url", "")
                if url:
                    return url
            url = m.get("url", "")
            if url:
                return url
    # feedparser: enclosures / links
    links = item.get("links") or []
    for link in links:
        if "image" in (link.get("type") or ""):
            return link.get("href", "")
    enclosures = item.get("enclosures") or []
    for enc in enclosures:
        if "image" in (enc.get("type") or ""):
            return enc.get("url") or enc.get("href", "")
    # NewsAPI: urlToImage
    url_to_image = item.get("urlToImage") or ""
    if url_to_image:
        return url_to_image
    # image field directly
    return item.get("image_url") or item.get("image") or ""


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _strip_html(content: str) -> str:
    text = re.sub(r"<[^>]+>", " ", content)
    text = html.unescape(text)
    # Remove common RSS junk patterns (social buttons, sharing text, bylines)
    junk_patterns = [
        r"\bx\s+whatsapp-stroke\s+copylink\b.*?(?=\b[A-Z][a-z])",
        r"\bShare\s+on\s+(?:Facebook|Twitter|WhatsApp|LinkedIn)\b",
        r"\bAdd\s+\w+\s+on\s+Google\s+info\b",
        r"\bContinue reading\.\.\.\b",
    ]
    for pat in junk_patterns:
        text = re.sub(pat, "", text, flags=re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_timestamp(raw_ts: str | None) -> str:
    if not raw_ts:
        return datetime.now(timezone.utc).isoformat()

    cleaned = raw_ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned.replace("Z", "+00:00")

    for parser in (datetime.fromisoformat, parsedate_to_datetime):
        try:
            parsed = parser(cleaned)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            continue

    return datetime.now(timezone.utc).isoformat()


def _article_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def _normalize_article(item: dict[str, Any], source_name: str, category: str, country: str) -> dict[str, str]:
    url = str(item.get("url") or item.get("link") or "").strip()
    title = str(item.get("title") or "").strip()
    content_raw = str(item.get("content") or item.get("description") or item.get("summary") or "")
    content = _strip_html(content_raw)
    timestamp = _normalize_timestamp(str(item.get("timestamp") or item.get("publishedAt") or item.get("published") or ""))
    image_url = _extract_image_url(item)

    if not url or not title or not content:
        raise ValueError("Missing required fields for article")

    payload = {
        "id": _article_id(url),
        "title": title,
        "content": content,
        "category": category or "general",
        "timestamp": timestamp,
        "source": source_name,
        "country": country or "global",
        "url": url,
        "image_url": image_url,
    }

    for field in REQUIRED_FIELDS:
        if not payload.get(field):
            raise ValueError(f"Invalid article schema: missing {field}")

    return payload


def _build_sources() -> list[dict[str, str]]:
    sources = [
        # ── World / General ──
        {
            "kind": "rss",
            "name": "aljazeera_all_rss",
            "url": "https://www.aljazeera.com/xml/rss/all.xml",
            "category": "general",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "bbc_world_rss",
            "url": "http://feeds.bbci.co.uk/news/world/rss.xml",
            "category": "general",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "bbc_africa_rss",
            "url": "http://feeds.bbci.co.uk/news/world/africa/rss.xml",
            "category": "general",
            "country": "africa",
        },
        {
            "kind": "rss",
            "name": "guardian_world_rss",
            "url": "https://www.theguardian.com/world/rss",
            "category": "general",
            "country": "global",
        },
        # ── Business ──
        {
            "kind": "rss",
            "name": "bbc_business_rss",
            "url": "http://feeds.bbci.co.uk/news/business/rss.xml",
            "category": "business",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "guardian_business_rss",
            "url": "https://www.theguardian.com/uk/business/rss",
            "category": "business",
            "country": "global",
        },
        # ── Technology ──
        {
            "kind": "rss",
            "name": "bbc_tech_rss",
            "url": "http://feeds.bbci.co.uk/news/technology/rss.xml",
            "category": "technology",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "guardian_tech_rss",
            "url": "https://www.theguardian.com/uk/technology/rss",
            "category": "technology",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "ars_technica_rss",
            "url": "https://feeds.arstechnica.com/arstechnica/index",
            "category": "technology",
            "country": "global",
        },
        # ── Science ──
        {
            "kind": "rss",
            "name": "bbc_science_rss",
            "url": "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
            "category": "science",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "guardian_science_rss",
            "url": "https://www.theguardian.com/science/rss",
            "category": "science",
            "country": "global",
        },
        # ── Health ──
        {
            "kind": "rss",
            "name": "bbc_health_rss",
            "url": "http://feeds.bbci.co.uk/news/health/rss.xml",
            "category": "health",
            "country": "global",
        },
        # ── Entertainment ──
        {
            "kind": "rss",
            "name": "bbc_entertainment_rss",
            "url": "http://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
            "category": "entertainment",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "guardian_culture_rss",
            "url": "https://www.theguardian.com/uk/culture/rss",
            "category": "entertainment",
            "country": "global",
        },
        # ── Sports ──
        {
            "kind": "rss",
            "name": "bbc_sport_rss",
            "url": "http://feeds.bbci.co.uk/sport/rss.xml",
            "category": "sports",
            "country": "global",
        },
        {
            "kind": "rss",
            "name": "guardian_sport_rss",
            "url": "https://www.theguardian.com/uk/sport/rss",
            "category": "sports",
            "country": "global",
        },
    ]

    newsapi_key = os.getenv("INGEST_NEWSAPI_KEY", "").strip()
    if newsapi_key:
        sources.append(
            {
                "kind": "newsapi",
                "name": "newsapi_headlines",
                "url": "https://newsapi.org/v2/top-headlines",
                "category": os.getenv("INGEST_NEWSAPI_CATEGORY", "general"),
                "country": os.getenv("INGEST_NEWSAPI_COUNTRY", "us"),
            }
        )
    else:
        LOGGER.warning("INGEST_NEWSAPI_KEY not set, skipping NewsAPI source")

    return sources


def _fetch_full_text(url: str) -> str:
    """Fetch the full article page and extract paragraph text."""
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsvineBot/1.0)"},
        )
        resp.raise_for_status()
        html = resp.text
        # Remove script/style/nav/footer blocks
        html = re.sub(r"<(script|style|nav|footer|header|aside)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        # Extract text from <p> tags
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, flags=re.DOTALL | re.IGNORECASE)
        texts = [_strip_html(p).strip() for p in paragraphs]
        # Keep paragraphs that look like real sentences (>40 chars)
        texts = [t for t in texts if len(t) > 40]
        full = "\n".join(texts)
        return full if len(full) > 200 else ""
    except Exception as exc:
        LOGGER.debug("Full-text fetch failed for %s: %s", url, exc)
        return ""


def _fetch_rss(source: dict[str, str]) -> list[dict[str, Any]]:
    feed = feedparser.parse(source["url"])
    entries = []
    for entry in feed.entries:
        content = ""
        if hasattr(entry, "content") and entry.content:
            content = entry.content[0].value
        summary = content or entry.get("summary") or ""
        link = entry.get("link") or ""

        # Try to fetch full article text
        full_text = _fetch_full_text(link) if link else ""

        item: dict[str, Any] = {
            "url": link,
            "title": entry.get("title"),
            "content": full_text or summary,
            "published": entry.get("published") or entry.get("updated"),
        }
        # Pass through image-related fields from feedparser
        if hasattr(entry, "media_thumbnail"):
            item["media_thumbnail"] = entry.media_thumbnail
        if hasattr(entry, "media_content"):
            item["media_content"] = entry.media_content
        if hasattr(entry, "links"):
            item["links"] = entry.links
        if hasattr(entry, "enclosures"):
            item["enclosures"] = entry.enclosures

        entries.append(item)
    return entries


def _fetch_newsapi(source: dict[str, str]) -> list[dict[str, Any]]:
    api_key = os.getenv("INGEST_NEWSAPI_KEY", "").strip()
    if not api_key:
        return []

    response = requests.get(
        source["url"],
        params={
            "apiKey": api_key,
            "country": source["country"],
            "category": source["category"],
            "pageSize": 50,
        },
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    if data.get("status") != "ok":
        raise ValueError(f"NewsAPI error: {data}")
    return data.get("articles", [])


def _ensure_bloom(client: redis.Redis, key: str) -> bool:
    try:
        client.execute_command("BF.RESERVE", key, 0.01, 1000000)
        return True
    except redis.ResponseError as exc:
        message = str(exc).lower()
        if "item exists" in message or "exists" in message:
            return True
        if "unknown command" in message:
            LOGGER.warning("RedisBloom module unavailable, using set fallback for dedup")
            return False
        raise


def _is_duplicate(client: redis.Redis, bloom_enabled: bool, key: str, article_url: str) -> bool:
    token = hashlib.sha256(article_url.encode("utf-8")).hexdigest()

    if bloom_enabled:
        try:
            added = int(client.execute_command("BF.ADD", key, token))
            return added == 0
        except redis.ResponseError as exc:
            if "unknown command" not in str(exc).lower():
                raise

    set_key = f"{key}:fallback"
    return client.sadd(set_key, token) == 0


def _fetch_articles(source: dict[str, str]) -> list[dict[str, Any]]:
    if source["kind"] == "newsapi":
        return _fetch_newsapi(source)
    if source["kind"] == "rss":
        return _fetch_rss(source)
    raise ValueError(f"Unsupported source kind: {source['kind']}")


def _publish_dlq(producer: KafkaProducer, dlq_topic: str, source: str, payload: Any, error: str) -> None:
    message = {
        "source": source,
        "error": error,
        "payload": payload,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(dlq_topic, message)


def run_once() -> int:
    settings = get_settings()
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    redis_client = redis.from_url(os.getenv("INGEST_REDIS_URL", "redis://localhost:6379/0"))

    bloom_key = os.getenv("INGEST_BLOOM_KEY", "news:urlhash:bloom")
    bloom_enabled = _ensure_bloom(redis_client, bloom_key)

    sources = _build_sources()
    published = 0

    for source in sources:
        source_name = source["name"]
        try:
            items = _fetch_articles(source)
        except Exception as exc:
            LOGGER.exception("Failed to fetch source %s", source_name)
            _publish_dlq(producer, settings.news_dlq_topic, source_name, {"source": source}, str(exc))
            continue

        for item in items:
            try:
                article = _normalize_article(
                    item=item,
                    source_name=source_name,
                    category=source["category"],
                    country=source["country"],
                )
                if _is_duplicate(redis_client, bloom_enabled, bloom_key, article["url"]):
                    continue
                producer.send(settings.news_topic, article)
                published += 1
            except Exception as exc:
                _publish_dlq(producer, settings.news_dlq_topic, source_name, item, str(exc))

    producer.flush(timeout=10)
    producer.close()
    return published


def main() -> None:
    logging.basicConfig(level=os.getenv("INGEST_LOG_LEVEL", "INFO"))
    poll_seconds = _get_int("INGEST_POLL_SECONDS", 300)

    while True:
        try:
            count = run_once()
            LOGGER.info("Published %s articles in this cycle", count)
        except Exception:
            LOGGER.exception("Ingestion cycle failed")
        time.sleep(max(5, poll_seconds))


if __name__ == "__main__":
    main()
