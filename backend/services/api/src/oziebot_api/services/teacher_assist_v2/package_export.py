"""HTML export helpers for v2 instructional package artifacts."""

from __future__ import annotations

import html
from typing import Any

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_instructional_package import TeacherAssistV2InstructionalPackageArtifact
from oziebot_api.services.teacher_assist.storage import get_teacher_assist_download_url, save_teacher_assist_bytes
from oziebot_api.services.teacher_assist_v2.planning_constants import PACKAGE_ARTIFACT_GROUPS
from oziebot_api.services.teacher_assist_v2.slide_visuals import render_visual_svg

_BASE_STYLE = (
    "body{font-family:system-ui,sans-serif;max-width:8.5in;margin:1.25rem auto;line-height:1.55;color:#0f172a}"
    "h1,h2{color:#0f172a}section{border:1px solid #e2e8f0;border-radius:12px;padding:1rem;margin:1rem 0}"
    ".meta{font-size:0.9rem;color:#475569}.print-btn{margin:1rem 0;padding:.5rem 1rem;border:1px solid #cbd5e1;border-radius:8px;background:#fff;cursor:pointer}"
    ".helper{font-size:.85rem;color:#64748b;margin:.75rem 0}"
    "@page{margin:.75in}"
    "@media print{.print-btn,.no-print{display:none!important}body{margin:0;max-width:none}"
    "section,.slide{page-break-inside:avoid}h1,h2{page-break-after:avoid}}"
)


GOOGLE_FORMS_HELPER_TEXT = (
    "Use this package to manually create or later automate a Google Form. "
    "Direct Google Forms publishing is not enabled yet."
)


def _esc(value: Any) -> str:
    return html.escape(str(value)) if value is not None else ""


def _print_button() -> str:
    return "<button type='button' class='print-btn no-print' onclick='window.print()'>Print / Save as PDF</button>"


def _html_document(*, title: str, body: str) -> str:
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(title)}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>{body}</body></html>"
    )


def build_google_forms_package_payload(
    content: dict[str, Any],
    *,
    export_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.quiz_exports import build_google_forms_readonly_json

    return build_google_forms_readonly_json(content, export_context=export_context)


def build_answer_key_json_payload(content: dict[str, Any]) -> dict[str, Any]:
    mapping = normalize_objective_mapping(content)
    entries = content.get("answer_key") or []
    if not entries:
        entries = [
            {
                "number": question.get("number"),
                "answer": question.get("answer"),
                "explanation": question.get("explanation"),
                "points": question.get("points", 1),
            }
            for question in content.get("questions") or []
        ]
    return {
        "title": f"{content.get('title')} — Answer Key",
        "objective_mapping": mapping,
        "answers": [
            {
                "number": item.get("number"),
                "correct_answer": item.get("answer"),
                "explanation": item.get("explanation"),
                "points": item.get("points"),
                "objective_mapping": mapping,
            }
            for item in entries
        ],
    }


def render_answer_key_html(content: dict[str, Any]) -> str:
    entries = content.get("answer_key") or []
    if not entries:
        entries = [
            {
                "number": question.get("number"),
                "answer": question.get("answer"),
                "explanation": question.get("explanation"),
                "points": question.get("points", 1),
            }
            for question in content.get("questions") or []
        ]
    blocks = []
    for item in entries:
        block = (
            f"<section><h2>Question {_esc(item.get('number'))}</h2>"
            f"<p><strong>Correct answer:</strong> {_esc(item.get('answer'))}</p>"
        )
        if item.get("explanation"):
            block += f"<p><strong>Explanation:</strong> {_esc(item.get('explanation'))}</p>"
        if item.get("points") is not None:
            block += f"<p><strong>Points:</strong> {_esc(item.get('points'))}</p>"
        block += "</section>"
        blocks.append(block)
    title = f"{content.get('title')} — Answer Key"
    body = (
        f"{_print_button()}<h1>{_esc(title)}</h1>{_objective_block(content)}"
        + "".join(blocks)
    )
    return _html_document(title=title, body=body)


def normalize_objective_mapping(content: dict[str, Any]) -> dict[str, Any]:
    mapping = content.get("objective_mapping")
    if isinstance(mapping, dict):
        return mapping
    if isinstance(mapping, list):
        first = next((item for item in mapping if isinstance(item, dict)), None)
        return first or {}
    return {}


def objective_mapping_field(content: dict[str, Any], field: str) -> Any:
    mapping = normalize_objective_mapping(content)
    return mapping.get(field)


def _objective_summary(content: dict[str, Any]) -> tuple[str | None, str | None]:
    mapping = content.get("objective_mapping")
    if isinstance(mapping, dict):
        code = mapping.get("objective_code") or content.get("objective_code")
        text = (
            mapping.get("objective_text")
            or mapping.get("alignment_summary")
            or content.get("objective_text")
            or content.get("alignment_summary")
        )
        return (str(code) if code else None, str(text) if text else None)
    if isinstance(mapping, list):
        first = next((item for item in mapping if isinstance(item, dict)), None)
        code = (
            content.get("objective_code")
            or (first.get("objective_code") if first else None)
            or (first.get("objective_id") if first else None)
        )
        text = content.get("objective_text") or content.get("alignment_summary")
        return (str(code) if code else None, str(text) if text else None)
    code = content.get("objective_code")
    text = content.get("objective_text") or content.get("alignment_summary")
    return (str(code) if code else None, str(text) if text else None)


def _objective_block(content: dict[str, Any]) -> str:
    code, text = _objective_summary(content)
    if not code and not text:
        return ""
    return (
        f"<section class='meta'><h2>Objective</h2>"
        f"<p><strong>{_esc(code)}</strong> — {_esc(text)}</p></section>"
    )


def render_daily_lesson_plan_html(content: dict[str, Any]) -> str:
    blocks = []
    daily_topic = content.get("daily_topic")
    topic_block = ""
    if daily_topic:
        topic_block = f"<section class='meta'><h2>Today's focus</h2><p>{_esc(daily_topic)}</p></section>"
    for subject in content.get("subjects") or []:
        sections = [
            ("Today's topic", subject.get("daily_topic")),
            ("Objective", subject.get("objective")),
            ("Materials", subject.get("materials")),
            ("Mini lesson", subject.get("mini_lesson")),
            ("Teacher modeling", subject.get("teacher_modeling") or subject.get("teacher_actions")),
            ("Guided practice", subject.get("guided_practice")),
            ("Independent practice", subject.get("independent_practice")),
            ("Check for understanding", subject.get("check_for_understanding") or subject.get("assessment")),
            ("Closure", subject.get("closure")),
            ("Teacher notes", subject.get("notes")),
        ]
        body = ""
        for heading, value in sections:
            if isinstance(value, list):
                body += f"<p><strong>{_esc(heading)}:</strong></p><ul>" + "".join(
                    f"<li>{_esc(item)}</li>" for item in value
                ) + "</ul>"
            elif value:
                body += f"<p><strong>{_esc(heading)}:</strong> {_esc(value)}</p>"
        blocks.append(f"<section><h2>{_esc(subject.get('subject_name'))}</h2>{body}</section>")
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>"
        f"<p>{_esc(content.get('summary'))}</p>{topic_block}{_objective_block(content)}"
        + "".join(blocks)
        + "</body></html>"
    )


def render_slide_deck_html(content: dict[str, Any]) -> str:
    slides = content.get("slides") or []
    slide_html = []
    for index, slide in enumerate(slides, start=1):
        bullets = slide.get("bullets") or []
        visual = render_visual_svg(slide.get("visualType"), slide.get("visualData"))
        layout = slide.get("layout") or "text_only"
        body = f"<h2>{_esc(slide.get('title'))}</h2>"
        if slide.get("subtitle"):
            body += f"<p class='meta'>{_esc(slide.get('subtitle'))}</p>"
        if layout in {"text_left_visual_right", "visual_top_text_bottom", "two_column"} and visual:
            body += f"<div style='display:grid;grid-template-columns:1fr 1fr;gap:1rem;align-items:center'>{visual}<ul>"
            body += "".join(f"<li>{_esc(item)}</li>" for item in bullets) + "</ul></div>"
        else:
            if visual:
                body += visual
            if bullets:
                body += "<ul>" + "".join(f"<li>{_esc(item)}</li>" for item in bullets) + "</ul>"
        slide_html.append(
            f"<section class='slide' style='border:1px solid #cbd5e1;border-radius:16px;padding:1.25rem;margin:1rem 0;background:#f8fafc'>"
            f"<p style='font-size:12px;color:#64748b;text-transform:uppercase'>Slide {index} · {_esc(layout)}</p>{body}</section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}"
        + "".join(slide_html)
        + "</body></html>"
    )


def render_quiz_html(content: dict[str, Any], *, include_answers: bool = False) -> str:
    questions = content.get("questions") or []
    blocks = []
    intro = content.get("summary") or content.get("description")
    if intro and not include_answers:
        blocks.append(f"<section><h2>Instructions</h2><p>{_esc(intro)}</p></section>")
    if content.get("student_number_field") and not include_answers:
        blocks.append("<section><p><strong>Student Number:</strong> ________________________________</p></section>")
    for question in questions:
        points = question.get("points", 1)
        block = (
            f"<section><h2>Question {_esc(question.get('number'))}"
            f"{'' if include_answers else f' ({_esc(points)} pt)'}</h2>"
            f"<p>{_esc(question.get('prompt'))}</p>"
        )
        if question.get("type") == "multiple_choice":
            block += "<ol type='A'>" + "".join(
                f"<li>{_esc(choice)}</li>" for choice in question.get("choices") or []
            ) + "</ol>"
        else:
            block += "".join(
                "<div style='border-bottom:1px solid #cbd5e1;height:1.75rem;margin:.5rem 0'></div>"
                for _ in range(int(question.get("response_lines") or 4))
            )
        if include_answers:
            block += f"<p><strong>Correct answer:</strong> {_esc(question.get('answer'))}</p>"
            if question.get("explanation"):
                block += f"<p><strong>Explanation:</strong> {_esc(question.get('explanation'))}</p>"
            block += f"<p><strong>Points:</strong> {_esc(points)}</p>"
        block += "</section>"
        blocks.append(block)
    title = content.get("title") if not include_answers else f"{content.get('title')} — Answer Key"
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(title)}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(title)}</h1>{_objective_block(content)}"
        + "".join(blocks)
        + "</body></html>"
    )


def render_exit_ticket_html(content: dict[str, Any]) -> str:
    blocks = []
    for index, question in enumerate(content.get("questions") or [], start=1):
        lines = int(question.get("response_lines") or 3)
        blocks.append(
            f"<section><h2>Question {index}</h2><p>{_esc(question.get('prompt'))}</p>"
            + "".join("<div style='border-bottom:1px solid #cbd5e1;height:1.75rem;margin:.5rem 0'></div>" for _ in range(lines))
            + "</section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}"
        + "".join(blocks)
        + "</body></html>"
    )


def render_assignment_html(content: dict[str, Any]) -> str:
    instructions = content.get("student_instructions") or []
    criteria = content.get("success_criteria") or []
    lines = int(content.get("writing_lines") or 10)
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        "<style>" + _BASE_STYLE + " .passage{background:#f8fafc;padding:1rem;border-radius:12px}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}"
        f"<section><h2>Student Instructions</h2><ul>"
        + "".join(f"<li>{_esc(item)}</li>" for item in instructions)
        + "</ul></section>"
        "<section><h2>Success Criteria</h2><ul>"
        + "".join(f"<li>{_esc(item)}</li>" for item in criteria)
        + f"</ul><p><strong>Rubric:</strong> {_esc(content.get('rubric_reference'))}</p></section>"
        f"<section><h2>{_esc(content.get('passage_title'))}</h2><div class='passage'>{_esc(content.get('passage_text'))}</div></section>"
        f"<section><h2>Your Response</h2>"
        + "".join("<div style='border-bottom:1px solid #cbd5e1;height:1.75rem;margin:.5rem 0'></div>" for _ in range(lines))
        + "</section></body></html>"
    )


def render_writing_response_html(content: dict[str, Any]) -> str:
    lines = int(content.get("writing_lines_per_page") or 14)
    response_pages = max(1, int(content.get("response_pages") or 1))
    blocks = []
    if content.get("prompt"):
        blocks.append(f"<section><h2>Prompt</h2><p>{_esc(content.get('prompt'))}</p></section>")
    instructions = content.get("instructions") or []
    if instructions:
        blocks.append(
            "<section><h2>Instructions</h2><ul>"
            + "".join(f"<li>{_esc(item)}</li>" for item in instructions)
            + "</ul></section>"
        )
    for page_index in range(response_pages):
        blocks.append(
            f"<section><h2>Writing Response {page_index + 1}</h2>"
            + "".join("<div style='border-bottom:1px solid #cbd5e1;height:1.75rem;margin:.5rem 0'></div>" for _ in range(lines))
            + "</section>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}"
        + "".join(blocks)
        + "</body></html>"
    )


def render_rubric_html(content: dict[str, Any]) -> str:
    rows = []
    for criterion in content.get("criteria") or []:
        levels = criterion.get("levels") or []
        rows.append(
            f"<tr><td><strong>{_esc(criterion.get('name'))}</strong></td>"
            f"<td>{_esc(criterion.get('points'))} pts</td>"
            f"<td>{_esc(levels[0] if levels else '')}</td>"
            f"<td>{_esc(levels[1] if len(levels) > 1 else '')}</td>"
            f"<td>{_esc(levels[2] if len(levels) > 2 else '')}</td></tr>"
        )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE} table{{width:100%;border-collapse:collapse}} td,th{{border:1px solid #cbd5e1;padding:.5rem}}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>"
        f"<p>Total points: {_esc(content.get('total_points'))}</p>{_objective_block(content)}"
        f"<table><thead><tr><th>Criterion</th><th>Points</th><th>Strong</th><th>Partial</th><th>Limited</th></tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table></body></html>"
    )


def render_newsletter_html(content: dict[str, Any]) -> str:
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
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}{body}</body></html>"
    )


def render_vocabulary_html(content: dict[str, Any]) -> str:
    terms = content.get("terms") or []
    body = "".join(
        f"<section><h2>{_esc(item.get('term'))}</h2><p>{_esc(item.get('definition'))}</p></section>" for item in terms
    )
    return (
        f"<!DOCTYPE html><html><head><meta charset='utf-8'><title>{_esc(content.get('title'))}</title>"
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>{_objective_block(content)}{body}</body></html>"
    )


def render_generic_document_html(content: dict[str, Any]) -> str:
    if content.get("terms"):
        return render_vocabulary_html(content)
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
        f"<style>{_BASE_STYLE}</style></head><body>"
        f"{_print_button()}<h1>{_esc(content.get('title'))}</h1>"
        f"<p>{_esc(content.get('summary'))}</p>{_objective_block(content)}{body}</body></html>"
    )


def render_artifact_preview_html(*, artifact_type: str, content: dict[str, Any]) -> str:
    if artifact_type == "daily_lesson_plan":
        return render_daily_lesson_plan_html(content)
    if artifact_type == "subject_slide_deck":
        return render_slide_deck_html(content)
    if artifact_type == "quiz":
        return render_quiz_html(content)
    if artifact_type == "exit_ticket":
        return render_exit_ticket_html(content)
    if artifact_type == "assignment":
        return render_assignment_html(content)
    if artifact_type == "writing_response":
        return render_writing_response_html(content)
    if artifact_type == "rubric":
        return render_rubric_html(content)
    if artifact_type == "parent_newsletter_summary":
        return render_newsletter_html(content)
    if artifact_type == "vocabulary_list":
        return render_vocabulary_html(content)
    return render_generic_document_html(content)


def save_teacher_assist_export_bytes(
    *,
    settings: Settings,
    tenant_id,
    filename: str,
    contents: bytes,
    mime_type: str,
) -> str:
    stored = save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="exports",
        original_filename=filename,
        contents=contents,
        mime_type=mime_type,
    )
    return stored.storage_key


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
    storage_key = save_teacher_assist_export_bytes(
        settings=settings,
        tenant_id=tenant_id,
        filename=filename,
        contents=html_body.encode("utf-8"),
        mime_type="text/html",
    )
    return storage_key, export_format


def save_additional_artifact_exports(
    *,
    settings: Settings,
    tenant_id,
    artifact_id: str,
    artifact_type: str,
    content: dict[str, Any],
    export_context: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    _ = export_context
    _ = content
    _ = artifact_id
    _ = tenant_id
    _ = settings
    # Legacy entry point retained for callers that only pass quiz context without DB access.
    if artifact_type != "quiz":
        return []
    return []


def artifact_download_url(
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    *,
    settings: Settings | None,
    storage_key: str | None = None,
    filename: str | None = None,
    mime_type: str = "text/html",
) -> str | None:
    key = storage_key or artifact.storage_key
    if not key or settings is None:
        return None
    return get_teacher_assist_download_url(
        settings,
        storage_key=key,
        original_filename=filename or f"{artifact.title}.html",
        mime_type=mime_type,
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
