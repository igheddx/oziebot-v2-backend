"""Pixabay image search and local storage pipeline for student lesson slide decks."""

from __future__ import annotations

import logging
import mimetypes
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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
_PIXABAY_MAX_WORKERS = 12  # pure HTTP — no API rate limit concern

# Keywords that indicate a search term already has age/grade context.
# If none are present, we inject a grade-level prefix so Pixabay doesn't
# return adults, office stock, or abstract diagrams.
_AGE_CONTEXT_KEYWORDS = frozenset([
    "children", "child", "kids", "kid", "boy", "girl",
    "elementary", "fifth grade", "5th grade", "middle school", "high school",
    "student", "pupils", "classroom",
])


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


def _has_age_context(term: str) -> bool:
    lower = term.lower()
    return any(kw in lower for kw in _AGE_CONTEXT_KEYWORDS)


# Per-grade (K, 1–12): (inject_prefix, fallback_terms, last_resort)
# inject_prefix  → appended to abstract terms that have no age context
# fallback_terms → tried after all AI terms fail
# last_resort    → absolute final fallback
_GRADE_IMAGE_PROFILE: dict[str, tuple[str, list[str], str]] = {
    "K":  ("kindergarten children",
           ["kindergarten children playing learning classroom", "young kids reading books school", "5 year old children classroom teacher"],
           "young children kindergarten classroom"),
    "1":  ("first grade children",
           ["first grade children reading classroom", "6 year old kids learning elementary", "young elementary students teacher"],
           "young children elementary school"),
    "2":  ("second grade children",
           ["second grade children reading books classroom", "7 year old kids elementary school", "young elementary students learning"],
           "young children elementary school"),
    "3":  ("third grade elementary students",
           ["third grade students reading books classroom", "8 year old children elementary school", "elementary kids learning classroom"],
           "elementary school children reading"),
    "4":  ("fourth grade elementary students",
           ["fourth grade students reading books classroom", "9 year old children elementary school", "elementary students learning classroom"],
           "elementary school students reading books"),
    "5":  ("fifth grade elementary students",
           ["fifth grade students reading books classroom", "10 year old children elementary school", "elementary school kids learning"],
           "elementary school students reading books"),
    "6":  ("sixth grade middle school students",
           ["sixth grade middle school students classroom", "11 year old students learning school", "middle school kids reading classroom"],
           "middle school students learning"),
    "7":  ("seventh grade middle school students",
           ["seventh grade middle school students learning", "12 year old tweens school classroom", "middle school students reading books"],
           "middle school students classroom"),
    "8":  ("eighth grade middle school students",
           ["eighth grade middle school students classroom", "13 year old students learning school", "middle school students studying"],
           "middle school students learning"),
    "9":  ("ninth grade high school students",
           ["ninth grade freshman high school students classroom", "14 year old teenagers school learning", "high school freshmen students studying"],
           "high school students classroom"),
    "10": ("tenth grade high school students",
           ["tenth grade high school students learning classroom", "15 year old teenagers school", "high school sophomore students studying"],
           "high school students learning"),
    "11": ("eleventh grade high school students",
           ["eleventh grade high school students classroom", "16 year old teenagers school learning", "high school junior students studying"],
           "high school students classroom"),
    "12": ("twelfth grade high school students",
           ["twelfth grade senior high school students", "17 year old teenagers school graduation", "high school senior students studying"],
           "high school students learning"),
}

# Tier fallbacks when a specific grade code is unknown
_TIER_FALLBACK = {
    "elementary": ("elementary school children",
                   ["elementary school children reading books classroom", "kids learning classroom authentic"],
                   "children reading books elementary school"),
    "middle":     ("middle school students",
                   ["middle school students learning classroom", "tweens reading studying books"],
                   "middle school students classroom"),
    "high":       ("high school students",
                   ["high school students learning classroom", "teenagers studying reading books"],
                   "high school students classroom"),
}


def _grade_profile(grade_code: str | None, grade_band: str) -> tuple[str, list[str], str]:
    """Resolve (inject_prefix, fallback_terms, last_resort) from a grade code or band."""
    if grade_code and grade_code in _GRADE_IMAGE_PROFILE:
        return _GRADE_IMAGE_PROFILE[grade_code]
    if "elementary" in grade_band or "k_5" in grade_band:
        return _TIER_FALLBACK["elementary"]
    if "middle" in grade_band or "6_8" in grade_band:
        return _TIER_FALLBACK["middle"]
    return _TIER_FALLBACK["high"]


def _build_fallback_chain(
    search_terms: list[str],
    preferred_type: str,
    grade_band: str,
    grade_code: str | None = None,
) -> tuple[list[str], str]:
    """Return (fallback_chain, preferred_type) for a slide's image search.

    Every term is guaranteed to carry age/grade context so Pixabay returns the
    right age group — elementary kids, middle schoolers, or high school teens —
    never adults, offices, or abstract diagrams.
    """
    inject_prefix, fallback_terms, last_resort = _grade_profile(grade_code, grade_band)

    # Inject grade context into any AI term that lacks it.
    enhanced: list[str] = []
    for term in search_terms:
        if _has_age_context(term):
            enhanced.append(term)
        else:
            enhanced.append(f"{term} {inject_prefix}")

    # Full chain: enhanced AI terms → original AI terms (backup) → grade fallbacks → last resort
    seen: set[str] = set()
    chain: list[str] = []
    for t in enhanced + list(search_terms) + fallback_terms + [last_resort]:
        if t not in seen:
            seen.add(t)
            chain.append(t)

    return chain, preferred_type


def _fetch_one_slide_image(
    *,
    slide_id: str,
    slide: dict[str, Any],
    fallback_chain: list[str],
    preferred_type: str,
    api_key: str,
    artifact_id: Any,
    settings: Settings,
    tenant_id: uuid.UUID,
) -> dict[str, Any] | None:
    """Pure I/O: search Pixabay, download, store. No DB access — safe to run in threads."""
    hit: dict[str, Any] | None = None
    for term in fallback_chain:
        hit = _search_pixabay(query=term, api_key=api_key, preferred_image_type=preferred_type)
        if hit:
            break

    if not hit:
        logger.info(
            "artifact=%s slide=%s: no Pixabay image found after %d attempts",
            artifact_id, slide_id, len(fallback_chain),
        )
        return None

    image_url = str(hit.get("largeImageURL") or hit.get("webformatURL") or "")
    if not image_url:
        return None

    result = _download_image(image_url)
    if not result:
        return None
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
        logger.exception("artifact=%s slide=%s: storage failed", artifact_id, slide_id)
        return None

    return {
        "slide_id": slide_id,
        "slide": slide,
        "image_url": image_url,
        "attribution": _attribution(hit),
        "stored_key": stored.storage_key,
    }


def fetch_slide_images_for_artifact(
    db: Session,
    *,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    settings: Settings,
    api_key: str,
    grade_code: str | None = None,
) -> None:
    """
    Fetch Pixabay images for every image_search slide in the artifact, storing
    the files and updating the DB asset rows.

    Phase 1: collect tasks (no I/O).
    Phase 2: parallel HTTP — search + download + store (_PIXABAY_MAX_WORKERS threads).
    Phase 3: sequential DB upserts, then flush.

    Safe to call even when the API key is missing — callers must guard that.
    Call AFTER persist_package_artifact() so asset rows already exist.
    """
    content = artifact.content_json or {}
    slides = content.get("slides") or []
    if not isinstance(slides, list):
        return

    tenant_id = uuid.UUID(str(artifact.tenant_id))

    # --- Phase 1: collect tasks -------------------------------------------------
    tasks: list[dict[str, Any]] = []
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
        fallback_chain, ptype = _build_fallback_chain(search_terms, preferred_type, grade_band, grade_code=grade_code)
        tasks.append({
            "slide_id": slide_id,
            "slide": slide,
            "img_search": img_search,
            "visual": visual,
            "search_terms": search_terms,
            "fallback_chain": fallback_chain,
            "preferred_type": ptype,
        })

    if not tasks:
        return

    # --- Phase 2: parallel Pixabay fetch ----------------------------------------
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=_PIXABAY_MAX_WORKERS) as pool:
        future_to_slide = {
            pool.submit(
                _fetch_one_slide_image,
                slide_id=t["slide_id"],
                slide=t["slide"],
                fallback_chain=t["fallback_chain"],
                preferred_type=t["preferred_type"],
                api_key=api_key,
                artifact_id=artifact.id,
                settings=settings,
                tenant_id=tenant_id,
            ): t["slide_id"]
            for t in tasks
        }
        for future in as_completed(future_to_slide):
            sid = future_to_slide[future]
            try:
                res = future.result()
            except Exception:
                logger.exception("artifact=%s slide=%s: fetch worker raised", artifact.id, sid)
                res = None
            if res is not None:
                results[sid] = res

    logger.info(
        "artifact=%s: fetched %d/%d slide images in parallel",
        artifact.id, len(results), len(tasks),
    )

    # --- Phase 3: sequential DB upserts -----------------------------------------
    now = datetime.now(UTC)
    task_by_id = {t["slide_id"]: t for t in tasks}

    for sid, res in results.items():
        t = task_by_id[sid]
        img_search = t["img_search"]
        visual = t["visual"]
        search_terms = t["search_terms"]
        slide = res["slide"]
        image_url = res["image_url"]
        attribution = res["attribution"]
        stored_key = res["stored_key"]

        asset_row: TeacherAssistV2SlideVisualAsset | None = (
            db.query(TeacherAssistV2SlideVisualAsset)
            .filter_by(artifact_id=artifact.id, slide_id=sid)
            .first()
        )

        if asset_row is not None:
            asset_row.source_url = image_url
            asset_row.local_asset_key = stored_key
            asset_row.attribution = attribution
            asset_row.visual_generation_status = "fetched"
            asset_row.updated_at = now
        else:
            db.add(
                TeacherAssistV2SlideVisualAsset(
                    artifact_id=artifact.id,
                    slide_id=sid,
                    visual_type="image",
                    title=f"{slide.get('title') or sid} — Image",
                    description=str(img_search.get("image_alt_text") or ""),
                    source_type="pixabay",
                    source_url=image_url,
                    attribution=attribution,
                    local_asset_key=stored_key,
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
            artifact.id, sid, stored_key, attribution,
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
