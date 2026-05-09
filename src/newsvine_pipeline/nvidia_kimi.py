"""Summaries and structured fields via NVIDIA-hosted Kimi (OpenAI-compatible chat API)."""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

LOGGER = logging.getLogger("newsvine.nvidia_kimi")

_DEFAULT_BASE = "https://integrate.api.nvidia.com/v1"
_DEFAULT_MODEL = "moonshotai/kimi-k2.6"
_MAX_INPUT_CHARS = 18_000


def _parse_json_object(raw: str) -> dict[str, Any]:
    text = raw.strip()
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    if fence:
        text = fence.group(1).strip()
    return json.loads(text)


def summarize_article_for_display(title: str, article_body: str) -> dict[str, str]:
    """
    Return ai_summary and key_points (multiline bullets) for storage and API.

    Empty dict if disabled, on error, or missing API key.
    """
    if os.getenv("NVIDIA_LLM_ENABLE", "1").lower() in ("0", "false", "no"):
        return {}
    api_key = (os.getenv("NVIDIA_API_KEY") or "").strip()
    if not api_key:
        return {}

    body = (article_body or "").strip()
    if len(body) < 80:
        return {}

    clipped = body[:_MAX_INPUT_CHARS]
    model = os.getenv("NVIDIA_MODEL", _DEFAULT_MODEL)
    base = os.getenv("NVIDIA_BASE_URL", _DEFAULT_BASE).rstrip("/")
    timeout = float(os.getenv("NVIDIA_HTTP_TIMEOUT", "120"))

    prompt = (
        "You help a news reader app. Given the article title and body text, produce:\n"
        "- A concise neutral summary (2-4 sentences).\n"
        "- Exactly 3-5 short key points (no numbering in strings; we format later).\n"
        "Rules: use only information from the article; no URLs; no speculation; "
        "output valid JSON only.\n\n"
        f'Title: {title}\n\nArticle text:\n"""\n{clipped}\n"""\n\n'
        'Respond with JSON only in this shape:\n'
        '{"summary":"<string>","key_points":["...","..."]}'
    )

    try:
        from openai import OpenAI

        client = OpenAI(base_url=base, api_key=api_key, timeout=timeout)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.25,
            max_tokens=1024,
        )
        msg = resp.choices[0].message
        content = (msg.content or "").strip()
        if not content:
            return {}
        data = _parse_json_object(content)
        summary = str(data.get("summary", "")).strip()
        points_raw = data.get("key_points")
        if not isinstance(points_raw, list):
            points_raw = []
        points = [str(p).strip() for p in points_raw if str(p).strip()]
        points = points[:8]
        if not summary and not points:
            return {}
        key_points_text = "\n".join(f"- {p}" for p in points) if points else ""
        return {"ai_summary": summary, "key_points": key_points_text}
    except Exception:
        LOGGER.exception("NVIDIA Kimi enrichment failed for title=%r", title[:80])
        return {}
