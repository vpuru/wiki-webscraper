from wiki_scraper.url_utils import normalize_url, is_allowed_host, is_wiki_article


class TestNormalizeUrl:
    def test_strips_fragment(self):
        result = normalize_url("https://en.wikipedia.org/wiki/Python#History")
        assert result == "https://en.wikipedia.org/wiki/Python"

    def test_strips_query_params(self):
        result = normalize_url("https://en.wikipedia.org/wiki/Python?action=edit")
        assert result == "https://en.wikipedia.org/wiki/Python"

    def test_enforces_https(self):
        result = normalize_url("http://en.wikipedia.org/wiki/Python")
        assert result == "https://en.wikipedia.org/wiki/Python"

    def test_lowercases_host(self):
        result = normalize_url("https://EN.WIKIPEDIA.ORG/wiki/Python")
        assert result == "https://en.wikipedia.org/wiki/Python"

    def test_relative_url(self):
        result = normalize_url("/wiki/Java", base_url="https://en.wikipedia.org/wiki/Python")
        assert result == "https://en.wikipedia.org/wiki/Java"

    def test_returns_none_for_non_http(self):
        assert normalize_url("ftp://example.com/file") is None
        assert normalize_url("mailto:user@example.com") is None

    def test_preserves_path_case(self):
        result = normalize_url("https://en.wikipedia.org/wiki/Python_(programming_language)")
        assert result == "https://en.wikipedia.org/wiki/Python_(programming_language)"


class TestIsAllowedHost:
    def test_same_host(self):
        assert is_allowed_host("https://en.wikipedia.org/wiki/Python", "en.wikipedia.org")

    def test_different_host(self):
        assert not is_allowed_host("https://fr.wikipedia.org/wiki/Python", "en.wikipedia.org")

    def test_case_insensitive(self):
        assert is_allowed_host("https://EN.WIKIPEDIA.ORG/wiki/Python", "en.wikipedia.org")


class TestIsWikiArticle:
    def test_valid_article(self):
        assert is_wiki_article("https://en.wikipedia.org/wiki/Python")

    def test_non_wiki_path(self):
        assert not is_wiki_article("https://en.wikipedia.org/w/index.php")

    def test_excluded_namespaces(self):
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Special:Search")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Talk:Python")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/User:Example")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/File:Example.jpg")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Template:Infobox")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Help:Contents")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Draft:Article")
        assert not is_wiki_article("https://en.wikipedia.org/wiki/Category:Science")

    def test_article_with_namespace_like_name(self):
        # "Special" as part of a title, not a namespace prefix
        assert is_wiki_article("https://en.wikipedia.org/wiki/Special_relativity")
