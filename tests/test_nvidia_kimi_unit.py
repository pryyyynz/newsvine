"""Unit tests for NVIDIA Kimi JSON parsing helpers."""

from newsvine_pipeline.nvidia_kimi import summarize_article_for_display


def test_parse_json_object_with_markdown_fence(monkeypatch: object) -> None:
    import newsvine_pipeline.nvidia_kimi as nk

    raw = '```json\n{"summary":"Hello world.","key_points":["a","b"]}\n```'
    data = nk._parse_json_object(raw)
    assert data["summary"] == "Hello world."
    assert data["key_points"] == ["a", "b"]


def test_summarize_returns_empty_without_api_key(monkeypatch: object) -> None:
    monkeypatch.delenv("NVIDIA_API_KEY", raising=False)
    assert summarize_article_for_display("Title", "x" * 200) == {}
