from newsvine_api.schemas import NewsArticle


def _article_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "id": "article-1",
        "title": "Fixture title",
        "content": "Fixture content",
        "category": "general",
        "timestamp": "2026-05-09T00:00:00+00:00",
        "source": "fixture",
        "country": "us",
        "url": "https://example.com/article-1",
    }
    payload.update(overrides)
    return payload


def test_news_article_normalizes_llm_key_points_list() -> None:
    article = NewsArticle(
        **_article_payload(
            ai_summary=["First sentence.", "Second sentence."],
            key_points=["One", "Two", ""],
        )
    )

    assert article.ai_summary == "First sentence. Second sentence."
    assert article.key_points == "- One\n- Two"

