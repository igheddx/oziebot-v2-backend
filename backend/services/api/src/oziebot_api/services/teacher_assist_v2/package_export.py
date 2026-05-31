"""HTML export helpers for v2 instructional package artifacts."""

from __future__ import annotations

import html
from typing import Any

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackageArtifact
from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url, save_teacher_assist_bytes
from oziebot_api.services.teacher_assist_v2.planning_constants import PACKAGE_ARTIFACT_GROUPS


def _esc(value: Any) -> str:
    return html.escape(str(value)) if value is not None else ""


def render_daily_lesson_plan_html(content: dict[str, Any]) -> str:
    blocks = []
    for subject in content.get("subjects") or []:
        blocks.append(
            f"<section><h2>{_esc(subject.get('subject_name'))}</h2>"
            f"<p><strong>Objective:</strong> {_esc(subject.get('objective'))}</p>"
            f"<p><strong>Mini lesson:</strong> {_esc(subject.get('mini_lesson'))}</p>"
            f"<p><strong>Teacher actions:</strong></p><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in subject.get("teacher_actions") or [])
            + "</ul><p><strong>Student activity:</strong></p><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in subject.get("student_activity") or [])
            + "</ul><p><strong>Materials:</strong></p><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in subject.get("materials") or [])
            + "</ul><p><strong>Assessment:</strong> {_esc(subject.get('assessment'))}</p>"
            f"<p><strong>Notes:</strong> {_esc(subject.get('notes'))}</p></section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto;line-height:1.5}"
        "section{border:1px solid #e2e8f0;border-radius:12px;padding:1rem;margin:1rem 0}</style></head><body>"
        f"<h1>{_esc(content.get('title'))}</h1>"
        f"<p>{_esc(content.get('summary'))}</p>"
        + "".join(blocks)
        + "</body></html>"
    )


def render_slide_deck_html(content: dict[str, Any]) -> str:
    slides = content.get("slides") or []
    slide_html = []
    for index, slide in enumerate(slides, start=1):
        bullets = slide.get("bullets") or []
        slide_html.append(
            f"<section class='slide'><p class='num'>Slide {index}</p>"
            f"<h2>{_esc(slide.get('title'))}</h2><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in bullets)
            + "</ul></section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:900px;margin:2rem auto}"
        ".slide{border:1px solid #cbd5e1;border-radius:16px;padding:1.25rem;margin:1rem 0;background:#f8fafc}"
        ".num{font-size:12px;color:#64748b;text-transform:uppercase}</style></head><body>"
        f"<h1>{_esc(content.get('title'))}</h1>"
        + "".join(slide_html)
        + "</body></html>"
    )


def render_generic_document_html(content: dict[str, Any]) -> str:
    sections = content.get("sections") or []
    body = "".join(
        f"<section><h2>{_esc(section.get('heading'))}</h2>"
        f"<p>{_esc(section.get('body'))}</p>"
        + (
            "<ul>" + "".join(f"<li>{_esc(item)}</li>" for item in section.get("bullets") or []) + "</ul>"
            if section.get("bullets")
            else ""
        )
        + "</section>"
        for section in sections
    )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        "<style>body{font-family:system-ui,sans-serif;max-width:800px;margin:2rem auto;line-height:1.5}"
        "section{margin:1.25rem 0}</style></head><body>"
        f"<h1>{_esc(content.get('title'))}</h1><p>{_esc(content.get('summary'))}</p>{body}</body></html>"
    )


def render_artifact_preview_html(*, artifact_type: str, content: dict[str, Any]) -> str:
    if artifact_type == "daily_lesson_plan":
        return render_daily_lesson_plan_html(content)
    if artifact_type == "subject_slide_deck":
        return render_slide_deck_html(content)
    return render_generic_document_html(content)


def save_artifact_export(
    *,
    settings: Settings,
    tenant_id,
    artifact_id,
    artifact_type: str,
    content: dict[str, Any],
    export_format: str = "html",
) -> tuple[str, str]:
    html_body = render_artifact_preview_html(artifact_type=artifact_type, content=content)
    filename = f"v2-package-{artifact_id}.{export_format}"
    stored = save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="exports",
        original_filename=filename,
        contents=html_body.encode("utf-8"),
        mime_type="text/html",
    )
    return stored.storage_key, export_format


def artifact_download_url(
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    *,
    settings: Settings | None,
) -> str | None:
    if not artifact.storage_key or settings is None:
        return None
    return get_teacher_assist_download_url(
        settings,
        storage_key=artifact.storage_key,
        original_filename=f"{artifact.title}.html",
        mime_type="text/html",
    )


def group_artifacts(artifacts: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {key: [] for key in PACKAGE_ARTIFACT_GROUPS}
    for artifact in artifacts:
        artifact_type = artifact["artifact_type"]
        for group_name, types in PACKAGE_ARTIFACT_GROUPS.items():
            if artifact_type in types:
                grouped[group_name].append(artifact)
                break
    return grouped
