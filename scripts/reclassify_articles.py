"""One-off script to reclassify existing articles in Elasticsearch."""
import json
import requests
from newsvine_pipeline.consumer import _classify_article

ES = "http://localhost:9200"

resp = requests.post(
    f"{ES}/articles/_search",
    json={"size": 2000, "query": {"match_all": {}}, "_source": ["title", "content", "category"]},
    timeout=10,
).json()

hits = resp["hits"]["hits"]
print(f"Total articles to reclassify: {len(hits)}")

bulk_lines = []
updated = 0
for hit in hits:
    src = hit["_source"]
    # Pass "general" to force re-classification with updated keywords
    new_cat = _classify_article(src.get("title", ""), src.get("content", ""), "general")
    old_cat = src.get("category", "")
    if new_cat != old_cat:
        updated += 1
        bulk_lines.append(json.dumps({"update": {"_index": "articles", "_id": hit["_id"]}}))
        bulk_lines.append(json.dumps({"doc": {"category": new_cat}}))

if bulk_lines:
    body = "\n".join(bulk_lines) + "\n"
    r = requests.post(f"{ES}/_bulk", data=body, headers={"Content-Type": "application/x-ndjson"}, timeout=30)
    result = r.json()
    errors = sum(1 for item in result["items"] if item.get("update", {}).get("error"))
    print(f"Updated {updated} articles, errors: {errors}")
else:
    print("No articles need updating")

# Show new distribution
agg = requests.post(
    f"{ES}/articles/_search",
    json={"size": 0, "aggs": {"cats": {"terms": {"field": "category", "size": 30}}}},
    timeout=10,
).json()
print("\nCategory distribution:")
for b in agg["aggregations"]["cats"]["buckets"]:
    print(f"  {b['key']}: {b['doc_count']}")
