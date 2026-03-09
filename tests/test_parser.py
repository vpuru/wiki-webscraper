import pytest

from wiki_scraper.parser import ContentParser

SAMPLE_HTML = """
<!DOCTYPE html>
<html>
<head><title>Python - Wikipedia</title></head>
<body>
<h1 id="firstHeading">Python (programming language)</h1>
<div id="mw-content-text">
  <p>Python is a <a href="/wiki/Programming_language">programming language</a>.</p>
  <p>It was created by <a href="/wiki/Guido_van_Rossum">Guido van Rossum</a>.</p>
  <p>See also <a href="/wiki/Java_(programming_language)">Java</a>.</p>
  <p>External: <a href="https://python.org">python.org</a></p>
  <p>Special: <a href="/wiki/Special:Search">search</a></p>
  <p>Talk: <a href="/wiki/Talk:Python">talk page</a></p>
  <p>Duplicate: <a href="/wiki/Programming_language">programming language</a></p>
  <p>Fragment: <a href="/wiki/Guido_van_Rossum#Early_life">early life</a></p>
</div>
</body>
</html>
"""


@pytest.fixture
def parser():
    return ContentParser(allowed_host="en.wikipedia.org")


def test_extracts_title(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert result.title == "Python (programming language)"


def test_extracts_wiki_links(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert "https://en.wikipedia.org/wiki/Programming_language" in result.outgoing_links
    assert "https://en.wikipedia.org/wiki/Guido_van_Rossum" in result.outgoing_links
    assert "https://en.wikipedia.org/wiki/Java_(programming_language)" in result.outgoing_links


def test_filters_external_links(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert not any("python.org" in link for link in result.outgoing_links)


def test_filters_excluded_namespaces(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert not any("Special:" in link for link in result.outgoing_links)
    assert not any("Talk:" in link for link in result.outgoing_links)


def test_deduplicates_links(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert result.outgoing_links.count("https://en.wikipedia.org/wiki/Programming_language") == 1


def test_strips_fragments_from_links(parser: ContentParser):
    result = parser.parse(SAMPLE_HTML, "https://en.wikipedia.org/wiki/Python")
    assert "https://en.wikipedia.org/wiki/Guido_van_Rossum" in result.outgoing_links
    assert not any("#" in link for link in result.outgoing_links)


def test_missing_title():
    p = ContentParser(allowed_host="en.wikipedia.org")
    html = '<html><body><div id="mw-content-text"></div></body></html>'
    result = p.parse(html, "https://en.wikipedia.org/wiki/Empty")
    assert result.title == ""
