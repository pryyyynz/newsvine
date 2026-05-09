"""Deterministic article body extraction (no LLM). Prefer Trafilatura from the canonical URL."""

from __future__ import annotations

import html
import logging
import os
import re

import requests

LOGGER = logging.getLogger("newsvine.article_extraction")


def _strip_paragraph_html(content: str) -> str:
    text = re.sub(r"<[^>]+>", " ", content)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _legacy_paragraph_extract(url: str) -> str:
    """Original heuristic: fetch HTML and join long <p> blocks (fragile but a fallback)."""
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsvineBot/1.0)"},
        )
        resp.raise_for_status()
        raw = resp.text
        raw = re.sub(
            r"<(script|style|nav|footer|header|aside)[^>]*>.*?</\1>",
            "",
            raw,
            flags=re.DOTALL | re.IGNORECASE,
        )
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", raw, flags=re.DOTALL | re.IGNORECASE)
        texts = [_strip_paragraph_html(p).strip() for p in paragraphs]
        texts = [t for t in texts if len(t) > 40]
        full = "\n".join(texts)
        return full if len(full) > 200 else ""
    except Exception as exc:
        LOGGER.debug("Legacy extract failed for %s: %s", url, exc)
        return ""


def fetch_article_plain_text(url: str) -> str:
    """
    Fetch main article text from the page URL.

    Uses Trafilatura when enabled, else legacy paragraph parsing. Falls back to the legacy
    extractor when Trafilatura returns little or no text.
    """
    if not url.strip():
        return ""
    use_trafilatura = os.getenv("INGEST_USE_TRAFILATURA", "1").lower() not in ("0", "false", "no")
    min_chars = int(os.getenv("INGEST_TRAFILATURA_MIN_CHARS", "200"))

    if use_trafilatura:
        try:
            import trafilatura

            downloaded = trafilatura.fetch_url(url)
            if downloaded:
                text = trafilatura.extract(
                    downloaded,
                    include_comments=False,
                    include_tables=False,
                    favor_recall=True,
                )
                out = (text or "").strip()
                if len(out) >= min_chars:
                    return out
                if out:
                    legacy = _legacy_paragraph_extract(url)
                    return legacy if len(legacy) >= min_chars else (legacy or out)
        except Exception as exc:
            LOGGER.debug("Trafilatura extract failed for %s: %s", url, exc)

    return _legacy_paragraph_extract(url)
