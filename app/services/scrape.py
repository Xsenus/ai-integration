# app/services/scrape.py
from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

from app.config import settings

log = logging.getLogger("services.scrape")


class FetchError(RuntimeError):
    pass


def _looks_like_redirect_placeholder(html: str) -> bool:
    snippet = (html or "")[:1024].lower()
    if "moved permanently" not in snippet:
        return False
    # типичная заглушка IBM_HTTP_Server: "The document has moved here."
    return "document has moved" in snippet or "http server" in snippet


def normalize_whitespace(text: str) -> str:
    text = re.sub(r"\r", "\n", text)
    text = re.sub(r"[ \t\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def hard_split(text: str, chunk_size: int) -> list[str]:
    if not text:
        return []
    size = int(chunk_size)
    return [text[i : i + size] for i in range(0, len(text), size)]


def to_home_url(domain_or_url: str) -> str:
    # поддержим и "uniconf.ru", и "https://uniconf.ru", и "www.uniconf.ru/"
    d = domain_or_url.strip().rstrip("/")
    if not d:
        raise ValueError("Empty domain")
    if "://" not in d:
        d = "https://" + d
    parsed = urlparse(d)
    host = parsed.netloc.replace("www.", "")
    return f"https://{host}/"


def _extract_html_redirect_target(html: str, base_url: str) -> str | None:
    if not html:
        return None

    snippet = html[:4096]
    soup = BeautifulSoup(snippet, "html.parser")

    meta = soup.find(
        "meta",
        attrs={"http-equiv": lambda value: isinstance(value, str) and value.lower() == "refresh"},
    )
    if meta and meta.get("content"):
        parts = [part.strip() for part in meta["content"].split(";") if part.strip()]
        for part in parts:
            if part.lower().startswith("url="):
                target = part.split("=", 1)[1].strip(" \"'")
                if target:
                    return urljoin(base_url, target)

    if _looks_like_redirect_placeholder(html):
        anchor = soup.find("a", href=True)
        if anchor and anchor["href"]:
            return urljoin(base_url, anchor["href"])

    return None


async def fetch_home_via_scraperapi(
    domain_or_url: str,
    *,
    retries: int = 3,
    max_redirects: int | None = None,
) -> tuple[str, str]:
    if not settings.SCRAPERAPI_KEY:
        raise FetchError("SCRAPERAPI_KEY is not configured")

    current_url = to_home_url(domain_or_url)
    params_base = {"api_key": settings.SCRAPERAPI_KEY}
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/122 Safari/537.36"}

    redirects_followed = 0
    redirect_limit = settings.PARSE_MAX_REDIRECTS if max_redirects is None else max_redirects
    if redirect_limit < 0:
        redirect_limit = 0

    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        while True:
            params = dict(params_base, url=current_url)
            backoff = 1.0
            redirect_followed = False

            for attempt in range(1, retries + 1):
                try:
                    log.info("HTTP: ScraperAPI → %s (try %s/%s)", current_url, attempt, retries)
                    r = await client.get(
                        "https://api.scraperapi.com/",
                        params=params,
                        headers=headers,
                        follow_redirects=True,
                        max_redirects=redirect_limit,
                    )
                    if r.status_code != 200:
                        body = (r.text or "")[:400]
                        raise FetchError(f"HTTP {r.status_code}: {body}")
                    html = r.text or ""
                    if len(html) < settings.PARSE_MIN_HTML_LEN:
                        raise FetchError("Ответ слишком короткий")

                    redirect_target = _extract_html_redirect_target(html, current_url)
                    if redirect_target:
                        redirects_followed += 1
                        log.info(
                            "HTTP: HTML redirect обнаружен (%s/%s): %s → %s",
                            redirects_followed,
                            redirect_limit,
                            current_url,
                            redirect_target,
                        )
                        if redirects_followed > redirect_limit:
                            raise FetchError("Превышено число HTML-редиректов")
                        current_url = redirect_target
                        redirect_followed = True
                        break

                    log.info("HTTP: OK, %s символов HTML", len(html))
                    return html, current_url
                except Exception as e:  # noqa: BLE001
                    if attempt >= retries:
                        raise FetchError(f"Fetch failed after {attempt} tries: {e}") from e
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)

            if redirect_followed:
                continue

            raise FetchError("Не удалось получить страницу после повторов")

    # не должно сюда дойти
    raise FetchError("Unreachable")


def html_to_full_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "canvas"]):
        tag.decompose()
    raw_text = soup.get_text(separator="\n")
    return normalize_whitespace(raw_text)


async def fetch_and_chunk(domain_or_url: str) -> tuple[str, list[str], str]:
    """
    Возвращает: (home_url, chunks, normalized_domain)

    home_url — https://host/
    normalized_domain — host без www
    """
    html, final_url = await fetch_home_via_scraperapi(domain_or_url)
    home_url = to_home_url(final_url)
    normalized_domain = urlparse(home_url).netloc.replace("www.", "")
    text = html_to_full_text(html)
    chunks = hard_split(text, settings.PARSE_MAX_CHUNK_SIZE)
    log.info(
        "scrape: домен %s → текст %s символов, чанков %s",
        normalized_domain,
        len(text),
        len(chunks),
    )
    return home_url, chunks, normalized_domain
