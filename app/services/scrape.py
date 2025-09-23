# app/services/scrape.py
from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.config import settings

log = logging.getLogger("services.scrape")


class FetchError(RuntimeError):
    pass


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


async def fetch_home_via_scraperapi(domain_or_url: str, *, retries: int = 5) -> str:
    if not settings.SCRAPERAPI_KEY:
        raise FetchError("SCRAPERAPI_KEY is not configured")

    target_url = to_home_url(domain_or_url)
    params = {"api_key": settings.SCRAPERAPI_KEY, "url": target_url}
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/122 Safari/537.36"}

    backoff = 1.0
    async with httpx.AsyncClient(timeout=60) as client:
        for attempt in range(1, retries + 1):
            try:
                log.info("HTTP: ScraperAPI → %s (try %s/%s)", target_url, attempt, retries)
                r = await client.get("https://api.scraperapi.com/", params=params, headers=headers)
                if r.status_code != 200:
                    body = (r.text or "")[:400]
                    raise FetchError(f"HTTP {r.status_code}: {body}")
                html = r.text or ""
                if len(html) < settings.PARSE_MIN_HTML_LEN:
                    raise FetchError("Ответ слишком короткий")
                log.info("HTTP: OK, %s символов HTML", len(html))
                return html
            except Exception as e:  # noqa: BLE001
                if attempt >= retries:
                    raise FetchError(f"Fetch failed after {attempt} tries: {e}") from e
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
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
    home_url = to_home_url(domain_or_url)
    normalized_domain = urlparse(home_url).netloc.replace("www.", "")
    html = await fetch_home_via_scraperapi(normalized_domain)
    text = html_to_full_text(html)
    chunks = hard_split(text, settings.PARSE_MAX_CHUNK_SIZE)
    return home_url, chunks, normalized_domain
