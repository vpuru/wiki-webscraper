from urllib.parse import urlparse, urlunparse, urljoin

EXCLUDED_NAMESPACES = frozenset({
    "Special:", "Talk:", "User:", "User_talk:", "Wikipedia:", "Wikipedia_talk:",
    "File:", "File_talk:", "MediaWiki:", "MediaWiki_talk:", "Template:",
    "Template_talk:", "Help:", "Help_talk:", "Category:", "Category_talk:",
    "Portal:", "Portal_talk:", "Draft:", "Draft_talk:", "TimedText:",
    "TimedText_talk:", "Module:", "Module_talk:",
})


def normalize_url(url: str, base_url: str | None = None) -> str | None:
    if base_url and not url.startswith(("http://", "https://", "//")):
        url = urljoin(base_url, url)

    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return None

    host = parsed.hostname
    if host is None:
        return None
    host = host.lower()

    path = parsed.path or "/"

    return urlunparse(("https", host, path, "", "", ""))


def is_allowed_host(url: str, allowed_host: str) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname
    if host is None:
        return False
    return host.lower() == allowed_host.lower()


def is_wiki_article(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path
    if not path.startswith("/wiki/"):
        return False
    title = path[len("/wiki/"):]
    return not any(title.startswith(ns) for ns in EXCLUDED_NAMESPACES)
