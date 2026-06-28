"""Pixabay image search and local storage pipeline for student lesson slide decks."""

from __future__ import annotations

import logging
import mimetypes
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

import httpx
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.teacher_assist_v2_slide_visual_asset import TeacherAssistV2SlideVisualAsset
from oziebot_api.services.teacher_assist.storage import save_teacher_assist_bytes

logger = logging.getLogger(__name__)

PIXABAY_API_URL = "https://pixabay.com/api/"
_SEARCH_TIMEOUT = 10.0
_DOWNLOAD_TIMEOUT = 20.0
_MAX_IMAGE_BYTES = 8 * 1024 * 1024  # 8 MB ceiling for slide images


def _search_pixabay(
    *,
    query: str,
    api_key: str,
    preferred_image_type: str = "photo",
) -> dict[str, Any] | None:
    """Try one Pixabay search query; return first hit or None."""
    image_type = preferred_image_type if preferred_image_type in {"photo", "illustration", "vector"} else "photo"
    try:
        response = httpx.get(
            PIXABAY_API_URL,
            params={
                "key": api_key,
                "q": query,
                "image_type": image_type,
                "safesearch": "true",
                "per_page": 5,
                "min_width": 400,
                "min_height": 300,
            },
            timeout=_SEARCH_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        hits = data.get("hits") or []
        return hits[0] if hits else None
    except Exception:
        logger.exception("Pixabay search failed for query %r", query)
        return None


def _download_image(url: str) -> tuple[bytes, str] | None:
    """Download image bytes from URL; return (bytes, mime_type) or None."""
    try:
        response = httpx.get(url, timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True)
        response.raise_for_status()
        content = response.content
        if not content or len(content) > _MAX_IMAGE_BYTES:
            logger.warning("Pixabay image rejected: %d bytes from %s", len(content), url)
            return None
        content_type = response.headers.get("content-type", "").split(";")[0].strip()
        if not content_type.startswith("image/"):
            guessed = mimetypes.guess_type(url)[0]
            content_type = guessed if guessed else "image/jpeg"
        return content, content_type
    except Exception:
        logger.exception("Failed to download image from %s", url)
        return None


def _attribution(hit: dict[str, Any]) -> str:
    user = hit.get("user") or "Unknown"
    return f"Photo by {user} on Pixabay (CC0)"


def _filename(url: str, mime_type: str) -> str:
    path = urlparse(url).path
    name = path.rsplit("/", 1)[-1] if "/" in path else ""
    if not name or "." not in name:
        ext = mimetypes.guess_extension(mime_type) or ".jpg"
        name = f"slide_image{ext}"
    return name


def fetch_slide_images_for_artifact(
    db: Session,
    *,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    settings: Settings,
    api_key: str,
) -> None:
    """
    Walk every slide in the artifact that has visual.type == "image_search",
    fetch a Pixabay image for it, store the file, and update the DB asset row.

    Safe to call even when the API key is missing — callers must guard that.
    Call AFTER persist_package_artifact() so asset rows already exist.
    """
    content = artifact.content_json or {}
    slides = content.get("slides") or []
    if not isinstance(slides, list):
        return

    tenant_id = uuid.UUID(str(artifact.tenant_id))
    now = datetime.now(UTC)

    for slide in slides:
        if not isinstance(slide, dict):
            continue

        slide_id = str(slide.get("id") or "")
        if not slide_id:
            continue

        visual = slide.get("visual")
        if not isinstance(visual, dict) or visual.get("type") != "image_search":
            continue

        img_search = visual.get("image_search") or {}
        search_terms = [t for t in (img_search.get("search_terms") or []) if isinstance(t, str) and t.strip()]
        if not search_terms:
            continue

        preferred_type = str(img_search.get("preferred_image_type") or "photo")
        grade_band = str(img_search.get("target_grade_band") or "elementary")

        # Fallback chain: provided terms → grade-band generic → universal fallback
        fallback_chain = list(search_terms) + [
            f"{grade_band} students classroom learning",
            "students learning classroom education",
        ]

        hit: dict[str, Any] | None = None
        for term in fallback_chain:
            hit = _search_pixabay(query=term, api_key=api_key, preferred_image_type=preferred_type)
            if hit:
                break

        if not hit:
            logger.info(
                "artifact=%s slide=%s: no Pixabay image found after %d attempts",
                artifact.id, slide_id, len(fallback_chain),
            )
            continue

        image_url = str(hit.get("largeImageURL") or hit.get("webformatURL") or "")
        if not image_url:
            continue

        result = _download_image(image_url)
        if not result:
            continue
        image_bytes, mime_type = result

        try:
            stored = save_teacher_assist_bytes(
                settings,
                tenant_id=tenant_id,
                area="resources",
                original_filename=_filename(image_url, mime_type),
                contents=image_bytes,
                mime_type=mime_type,
            )
        except Exception:
            logger.exception("artifact=%s slide=%s: storage failed", artifact.id, slide_id)
            continue

        attribution = _attribution(hit)

        asset_row: TeacherAssistV2SlideVisualAsset | None = (
            db.query(TeacherAssistV2SlideVisualAsset)
            .filter_by(artifact_id=artifact.id, slide_id=slide_id)
            .first()
        )

        if asset_row is not None:
            asset_row.source_url = image_url
            asset_row.local_asset_key = stored.storage_key
            asset_row.attribution = attribution
            asset_row.visual_generation_status = "fetched"
            asset_row.updated_at = now
        else:
            db.add(
                TeacherAssistV2SlideVisualAsset(
                    artifact_id=artifact.id,
                    slide_id=slide_id,
                    visual_type="image",
                    title=f"{slide.get('title') or slide_id} — Image",
                    description=str(img_search.get("image_alt_text") or ""),
                    source_type="pixabay",
                    source_url=image_url,
                    attribution=attribution,
                    local_asset_key=stored.storage_key,
                    prompt_hint=str(img_search.get("image_alt_text") or ""),
                    educational_purpose=str(img_search.get("educational_purpose") or ""),
                    suggested_placement=str(visual.get("placement") or "right"),
                    layout_template=str(slide.get("layout") or ""),
                    visual_generation_status="fetched",
                    search_terms_json=search_terms,
                    suggested_sources_json=["Pixabay"],
                    created_at=now,
                    updated_at=now,
                )
            )

        logger.info(
            "artifact=%s slide=%s: stored image key=%s attribution=%r",
            artifact.id, slide_id, stored.storage_key, attribution,
        )

    db.flush()


def create_pending_image_assets(
    db: Session,
    *,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
) -> None:
    """
    Create 'pending' asset rows for every image_search slide that has no asset row yet.

    Call this unconditionally after persist_package_artifact() so the frontend always
    has a layout placeholder even when the Pixabay key is not configured yet.
    """
    content = artifact.content_json or {}
    slides = content.get("slides") or []
    if not isinstance(slides, list):
        return

    now = datetime.now(UTC)

    for slide in slides:
        if not isinstance(slide, dict):
            continue

        slide_id = str(slide.get("id") or "")
        if not slide_id:
            continue

        visual = slide.get("visual")
        if not isinstance(visual, dict) or visual.get("type") != "image_search":
            continue

        existing: TeacherAssistV2SlideVisualAsset | None = (
            db.query(TeacherAssistV2SlideVisualAsset)
            .filter_by(artifact_id=artifact.id, slide_id=slide_id)
            .first()
        )
        if existing is not None:
            continue

        img_search = visual.get("image_search") or {}
        search_terms = [t for t in (img_search.get("search_terms") or []) if isinstance(t, str) and t.strip()]

        db.add(
            TeacherAssistV2SlideVisualAsset(
                artifact_id=artifact.id,
                slide_id=slide_id,
                visual_type="image",
                title=f"{slide.get('title') or slide_id} — Image",
                description=str(img_search.get("image_alt_text") or ""),
                source_type="pixabay",
                source_url=None,
                attribution=None,
                local_asset_key=None,
                prompt_hint=str(img_search.get("image_alt_text") or ""),
                educational_purpose=str(img_search.get("educational_purpose") or ""),
                suggested_placement=str(visual.get("placement") or "right"),
                layout_template=str(slide.get("layout") or ""),
                visual_generation_status="pending",
                search_terms_json=search_terms,
                suggested_sources_json=["Pixabay"],
                created_at=now,
                updated_at=now,
            )
        )
        logger.info("artifact=%s slide=%s: created pending image asset", artifact.id, slide_id)

    db.flush()
