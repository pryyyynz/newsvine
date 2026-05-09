"""Unit tests for RSS/API image URL extraction (no social card assets)."""

from newsvine_pipeline import ingestor


def test_prefers_sharper_when_thumbnail_and_inline_both_present() -> None:
    item = {
        "media_thumbnail": [{"url": "https://ichef.bbci.co.uk/news/240/foo.jpg"}],
        "rss_description_html": '<p><img src="https://ichef.bbci.co.uk/news/976/bar.jpg"/></p>',
    }
    assert ingestor._extract_image_url(item) == "https://ichef.bbci.co.uk/news/976/bar.jpg"


def test_prefers_thumbnail_when_it_hints_larger_than_inline() -> None:
    item = {
        "media_thumbnail": [{"url": "https://ichef.bbci.co.uk/news/976/foo.jpg"}],
        "rss_description_html": '<p><img src="https://ichef.bbci.co.uk/news/240/bar.jpg"/></p>',
    }
    assert ingestor._extract_image_url(item) == "https://ichef.bbci.co.uk/news/976/foo.jpg"


def test_inline_rss_image_when_no_thumbnail() -> None:
    item = {
        "rss_description_html": (
            '<div><img src="https://ichef.bbci.co.uk/live-experience/cps/480/cpsprodpb/x.jpg"/></div>'
        ),
    }
    assert ingestor._extract_image_url(item).endswith("x.jpg")


def test_skips_twitter_cdn_in_media_content() -> None:
    item = {
        "media_content": [
            {"url": "https://pbs.twimg.com/media/abc.jpg", "medium": "image"},
            {"url": "https://example.com/editorial.jpg", "medium": "image"},
        ],
    }
    assert ingestor._extract_image_url(item) == "https://example.com/editorial.jpg"


def test_skips_url_to_image_when_social_cdn() -> None:
    item = {"urlToImage": "https://pbs.twimg.com/media/card.png"}
    assert ingestor._extract_image_url(item) == ""


def test_guardian_style_media_picks_largest_width_query() -> None:
    item = {
        "media_content": [
            {"url": "https://i.guim.co.uk/img/media/x/0_0_100_100/master/1.jpg?width=140&quality=85", "type": None, "medium": None},
            {"url": "https://i.guim.co.uk/img/media/x/0_0_100_100/master/1.jpg?width=700&quality=85", "type": None, "medium": None},
        ],
    }
    out = ingestor._extract_image_url(item)
    assert "width=1200" in out
    assert "width=140" not in out


def test_bbci_thumbnail_standard_segment_upscaled() -> None:
    item = {
        "media_thumbnail": [
            {"url": "https://ichef.bbci.co.uk/ace/standard/240/cpsprodpb/live/x.jpg"},
        ],
    }
    out = ingestor._extract_image_url(item)
    assert "/standard/976/" in out


def test_extract_og_image_unescapes_html_entities() -> None:
    markup = '<meta property="og:image" content="https://example.com/a.jpg?width=1200&amp;quality=85">'
    assert ingestor._extract_og_image_from_html(markup) == "https://example.com/a.jpg?width=1200&quality=85"
