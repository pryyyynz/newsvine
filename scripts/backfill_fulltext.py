"""Backfill full article text for existing articles with short content."""
import json
import re
import sys
import requests

ES = "http://localhost:9200"
MIN_CONTENT_LEN = 300  # articles with less than this will be re-fetched


def _strip_html(content: str) -> str:
    text = re.sub(r"<[^>]+>", " ", content)
    return re.sub(r"\s+", " ", text).strip()


def _fetch_full_text(url: str) -> str:
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsvineBot/1.0)"},
        )
        resp.raise_for_status()
        html = resp.text
        html = re.sub(r"<(script|style|nav|footer|header|aside)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        paragraphs = re.findall(r"<p[^>]*>(.*?)</p>", html, flags=re.DOTALL | re.IGNORECASE)
        texts = [_strip_html(p).strip() for p in paragraphs]
        texts = [t for t in texts if len(t) > 40]
        full = "\n".join(texts)
        return full if len(full) > 200 else ""
    except Exception:
        return ""


# Fetch all short-content articles
resp = requests.post(
    f"{ES}/articles/_search",
    json={
        "size": 2000,
        "query": {"match_all": {}},
        "_source": ["url", "content"],
    },
    timeout=30,
).json()

hits = resp["hits"]["hits"]
short = [h for h in hits if len(h["_source"].get("content", "")) < MIN_CONTENT_LEN]
print(f"Total articles: {len(hits)}, short content (<{MIN_CONTENT_LEN} chars): {len(short)}")

bulk_lines = []
updated = 0
for i, hit in enumerate(short):
    url = hit["_source"].get("url", "")
    if not url:
        continue
    full_text = _fetch_full_text(url)
    if full_text and len(full_text) > len(hit["_source"].get("content", "")):
        updated += 1
        bulk_lines.append(json.dumps({"update": {"_index": "articles", "_id": hit["_id"]}}))
        bulk_lines.append(json.dumps({"doc": {"content": full_text, "content_snippet": full_text[:500]}}))
    if (i + 1) % 20 == 0:
        print(f"  processed {i + 1}/{len(short)}...", file=sys.stderr)

if bulk_lines:
    body = "\n".join(bulk_lines) + "\n"
    r = requests.post(f"{ES}/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"}, timeout=60)
    result = r.json()
    errors = sum(1 for item in result["items"] if item.get("update", {}).get("error"))
    print(f"Updated {updated} articles with full text, errors: {errors}")
else:
    print("No articles need updating")
