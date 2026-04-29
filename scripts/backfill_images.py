"""Backfill image_url for existing articles by extracting og:image from their URLs."""
import json
import re
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

ES = "http://localhost:9200"


def _extract_og_image(url: str) -> str:
    """Fetch a page and extract the og:image meta tag."""
    try:
        resp = requests.get(
            url,
            timeout=15,
            headers={"User-Agent": "Mozilla/5.0 (compatible; NewsvineBot/1.0)"},
            allow_redirects=True,
        )
        resp.raise_for_status()
        html = resp.text[:50_000]  # only scan first 50KB
        # Try og:image first
        match = re.search(
            r'<meta\s+(?:property|name)=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']og:image["\']',
                html,
                re.IGNORECASE,
            )
        if match:
            return match.group(1).strip()
        # Fallback: twitter:image
        match = re.search(
            r'<meta\s+(?:property|name)=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
            html,
            re.IGNORECASE,
        )
        if not match:
            match = re.search(
                r'<meta\s+content=["\']([^"\']+)["\']\s+(?:property|name)=["\']twitter:image["\']',
                html,
                re.IGNORECASE,
            )
        if match:
            return match.group(1).strip()
        return ""
    except Exception:
        return ""


# Fetch all articles missing image_url
print("Fetching articles from Elasticsearch...")
resp = requests.post(
    f"{ES}/articles/_search",
    json={
        "size": 5000,
        "query": {"match_all": {}},
        "_source": ["url", "image_url"],
    },
    timeout=30,
).json()

hits = resp["hits"]["hits"]
need_image = [
    h for h in hits
    if not h["_source"].get("image_url") and h["_source"].get("url")
]
print(f"Total articles: {len(hits)}, missing image_url: {len(need_image)}")

if not need_image:
    print("All articles already have images!")
    sys.exit(0)

# Extract og:image in parallel
results = {}
with ThreadPoolExecutor(max_workers=10) as pool:
    futures = {
        pool.submit(_extract_og_image, h["_source"]["url"]): h["_id"]
        for h in need_image
    }
    for i, future in enumerate(as_completed(futures)):
        doc_id = futures[future]
        img = future.result()
        if img:
            results[doc_id] = img
        if (i + 1) % 50 == 0:
            print(f"  processed {i + 1}/{len(need_image)}...", file=sys.stderr)

print(f"Found images for {len(results)}/{len(need_image)} articles")

# Bulk update
if results:
    bulk_lines = []
    for doc_id, img_url in results.items():
        bulk_lines.append(json.dumps({"update": {"_index": "articles", "_id": doc_id}}))
        bulk_lines.append(json.dumps({"doc": {"image_url": img_url}}))

    body = "\n".join(bulk_lines) + "\n"
    r = requests.post(
        f"{ES}/_bulk",
        data=body,
        headers={"Content-Type": "application/x-ndjson"},
        timeout=60,
    )
    result = r.json()
    errors = sum(1 for item in result["items"] if item.get("update", {}).get("error"))
    print(f"Updated {len(results)} articles with image_url, errors: {errors}")
else:
    print("No images found to update")
