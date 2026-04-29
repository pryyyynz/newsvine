"""Backfill: decode HTML entities in existing articles (title, content)."""
import html
import re
import requests

ES_URL = "http://localhost:9200"

JUNK_PATTERNS = [
    r"\bx\s+whatsapp-stroke\s+copylink\b.*?(?=\b[A-Z][a-z])",
    r"\bShare\s+on\s+(?:Facebook|Twitter|WhatsApp|LinkedIn)\b",
    r"\bAdd\s+\w+\s+on\s+Google\s+info\b",
    r"\bContinue reading\.\.\.\b",
]


def clean_text(text: str) -> str:
    decoded = html.unescape(text)
    for pat in JUNK_PATTERNS:
        decoded = re.sub(pat, "", decoded, flags=re.IGNORECASE | re.DOTALL)
    return re.sub(r"\s+", " ", decoded).strip()


def main():
    scroll = requests.post(
        f"{ES_URL}/articles/_search?scroll=2m",
        json={"size": 200, "query": {"match_all": {}}},
        timeout=30,
    ).json()

    scroll_id = scroll["_scroll_id"]
    hits = scroll["hits"]["hits"]
    updated = 0
    total = 0

    while hits:
        bulk_lines = []
        for hit in hits:
            doc = hit["_source"]
            doc_id = hit["_id"]
            total += 1

            new_title = clean_text(doc.get("title", ""))
            new_content = clean_text(doc.get("content", ""))

            if new_title != doc.get("title") or new_content != doc.get("content"):
                bulk_lines.append(f'{{"update":{{"_id":"{doc_id}","_index":"articles"}}}}')
                import json
                bulk_lines.append(json.dumps({"doc": {"title": new_title, "content": new_content}}))
                updated += 1

        if bulk_lines:
            body = "\n".join(bulk_lines) + "\n"
            resp = requests.post(f"{ES_URL}/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"}, timeout=30)
            if resp.status_code != 200:
                print(f"Bulk error: {resp.text[:200]}")

        scroll = requests.post(
            f"{ES_URL}/_search/scroll",
            json={"scroll": "2m", "scroll_id": scroll_id},
            timeout=30,
        ).json()
        scroll_id = scroll.get("_scroll_id", scroll_id)
        hits = scroll.get("hits", {}).get("hits", [])

    print(f"Scanned {total} articles, updated {updated} with decoded entities.")


if __name__ == "__main__":
    main()
