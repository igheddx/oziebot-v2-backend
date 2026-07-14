"""Bounded link fetching for TeacherAssist generation context."""

from __future__ import annotations

import ipaddress
import re
import socket
from html.parser import HTMLParser
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx

LINK_CONTEXT_ITEM_LIMIT = 1600
LINK_CONTEXT_TOTAL_LIMIT = 6000
LINK_FETCH_TIMEOUT_SECONDS = 6.0
LINK_FETCH_MAX_BYTES = 500_000


class _ReadableHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title_parts: list[str] = []
        self.meta_description: str | None = None
        self.body_parts: list[str] = []
        self._in_title = False
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "noscript", "svg"}:
            self._skip_depth += 1
        if normalized == "title":
            self._in_title = True
        if normalized == "meta":
            attr_map = {key.lower(): value for key, value in attrs if value is not None}
            name = (attr_map.get("name") or attr_map.get("property") or "").lower()
            if name in {"description", "og:description", "twitter:description"}:
                self.meta_description = attr_map.get("content")

    def handle_endtag(self, tag: str) -> None:
        normalized = tag.lower()
        if normalized in {"script", "style", "noscript", "svg"} and self._skip_depth:
            self._skip_depth -= 1
        if normalized == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        text = _normalize_text(data)
        if not text:
            return
        if self._in_title:
            self.title_parts.append(text)
        elif self._skip_depth == 0:
            self.body_parts.append(text)


def _normalize_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _host_is_public(hostname: str) -> bool:
    if hostname in {"localhost", "127.0.0.1", "::1"}:
        return False
    try:
        addresses = socket.getaddrinfo(hostname, None, proto=socket.IPPROTO_TCP)
    except socket.gaierror:
        return False
    for address in addresses:
        ip_value = address[4][0]
        try:
            parsed = ipaddress.ip_address(ip_value)
        except ValueError:
            return False
        if (
            parsed.is_private
            or parsed.is_loopback
            or parsed.is_link_local
            or parsed.is_multicast
            or parsed.is_reserved
        ):
            return False
    return True


def _validate_fetchable_url(url: str) -> tuple[bool, str | None]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False, "Only http and https links can be fetched."
    if not _host_is_public(parsed.hostname):
        return False, "Link host is not publicly fetchable."
    return True, None


def _youtube_video_id(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host.endswith("youtu.be"):
        candidate = parsed.path.strip("/").split("/", 1)[0]
        return candidate or None
    if "youtube.com" in host:
        query = parse_qs(parsed.query)
        if query.get("v"):
            return query["v"][0]
        if parsed.path.startswith("/shorts/") or parsed.path.startswith("/embed/"):
            return (
                parsed.path.strip("/").split("/", 1)[1] if "/" in parsed.path.strip("/") else None
            )
    return None


def _fetch_url_text(url: str) -> tuple[str | None, str | None]:
    valid, reason = _validate_fetchable_url(url)
    if not valid:
        return None, reason
    try:
        with httpx.Client(
            follow_redirects=True,
            timeout=LINK_FETCH_TIMEOUT_SECONDS,
            headers={"User-Agent": "OziebotTeacherAssist/1.0"},
        ) as client:
            response = client.get(url)
            response.raise_for_status()
            content = response.content[:LINK_FETCH_MAX_BYTES]
            content_type = response.headers.get("content-type", "")
    except Exception as exc:
        return None, str(exc)

    if "text/html" not in content_type and "text/plain" not in content_type:
        return None, f"Unsupported link content type: {content_type or 'unknown'}"
    text = content.decode(response.encoding or "utf-8", errors="ignore")
    if "text/plain" in content_type:
        return _normalize_text(text), None

    parser = _ReadableHtmlParser()
    parser.feed(text)
    title = _normalize_text(" ".join(parser.title_parts))
    description = _normalize_text(parser.meta_description)
    body = _normalize_text(" ".join(parser.body_parts))
    parts = [part for part in (title, description, body) if part]
    return _normalize_text(" ".join(parts)), None


def _link_label(material: dict[str, Any]) -> str:
    return str(
        material.get("title")
        or material.get("display_name")
        or material.get("external_url")
        or "Reference link"
    )


def build_link_prompt_context(
    materials: list[dict[str, Any]],
    *,
    source_label: str,
    total_char_limit: int = LINK_CONTEXT_TOTAL_LIMIT,
    item_char_limit: int = LINK_CONTEXT_ITEM_LIMIT,
) -> dict[str, Any]:
    used: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    remaining = total_char_limit
    for material in materials:
        url = str(material.get("external_url") or material.get("url") or "").strip()
        entry = {
            "id": str(material.get("id")) if material.get("id") is not None else None,
            "title": _link_label(material),
            "external_url": url or None,
            "resource_type": material.get("resource_type"),
            "source_label": source_label,
            "youtube_video_id": _youtube_video_id(url) if url else None,
        }
        if not url:
            skipped.append({**entry, "reason": "No URL was available for this link."})
            continue
        text, error = _fetch_url_text(url)
        if not text:
            skipped.append({**entry, "reason": error or "No readable link text was available."})
            continue
        excerpt = text[: min(item_char_limit, remaining)].strip()
        if not excerpt:
            skipped.append(
                {**entry, "reason": "Prompt text budget was exhausted before this link."}
            )
            continue
        used.append({**entry, "included_characters": len(excerpt), "excerpt": excerpt})
        remaining -= len(excerpt)
        if remaining <= 0:
            break
    return {
        "used_links": used,
        "skipped_links": skipped,
        "used_count": len(used),
        "skipped_count": len(skipped),
        "remaining_char_budget": max(0, remaining),
    }
