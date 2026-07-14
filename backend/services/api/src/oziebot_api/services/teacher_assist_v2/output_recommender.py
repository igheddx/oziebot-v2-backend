"""Rule-based engine to recommend instructional package output types from pacing guide context."""

from __future__ import annotations

from typing import Any


def recommend_outputs(
    *,
    subject_codes: set[str],
    objective_descriptions: list[str],
    total_guide_weeks: int,
) -> list[str]:
    """
    Return recommended optional output types based on pacing guide signals.

    Uses subject codes, TEKS objective text, and guide duration.
    Always returns a stable, ordered list — no AI call needed.
    """
    all_text = " ".join(objective_descriptions).lower()
    recommended: list[str] = []

    # Writing — ELA subject or writing-related TEKS keywords
    _writing_kws = {
        "write",
        "writing",
        "written",
        "compose",
        "draft",
        "narrative",
        "expository",
        "opinion",
        "argumentative",
        "informational",
        "literary",
        "revise",
        "edit",
        "paragraph",
        "essay",
        "response",
    }
    if "ELA" in subject_codes or _any_kw(all_text, _writing_kws):
        recommended.append("writing_response")
        recommended.append("rubric")

    # Assignment — reading, analysis, comprehension TEKS
    _analysis_kws = {
        "analyze",
        "analysis",
        "explain",
        "compare",
        "comprehension",
        "reading",
        "text",
        "passage",
        "evidence",
        "inference",
        "summarize",
        "summary",
        "central idea",
        "main idea",
        "theme",
        "author",
        "character",
        "plot",
    }
    if _any_kw(all_text, _analysis_kws):
        recommended.append("assignment")
        if "rubric" not in recommended:
            recommended.append("rubric")

    # Quiz — knowledge, recall, vocabulary TEKS
    _recall_kws = {
        "identify",
        "define",
        "describe",
        "recall",
        "vocabulary",
        "fluency",
        "recognize",
        "name",
        "list",
        "classify",
        "label",
        "match",
        "select",
        "choose",
    }
    if _any_kw(all_text, _recall_kws):
        recommended.append("quiz")

    # Vocabulary list — ELA or vocabulary-heavy TEKS
    _vocab_kws = {
        "vocabulary",
        "word",
        "term",
        "definition",
        "tier",
        "academic language",
        "glossary",
        "meaning",
        "context clue",
    }
    if "ELA" in subject_codes or _any_kw(all_text, _vocab_kws):
        recommended.append("vocabulary_list")

    # Exit ticket — always useful for checking daily TEKS mastery
    recommended.append("exit_ticket")

    # Bell ringer — multi-week guides benefit from review warm-ups
    if total_guide_weeks > 1:
        recommended.append("bell_ringer")

    # Study guide — longer guides or TEKS-heavy content
    _complex_kws = {
        "research",
        "inquiry",
        "multiple sources",
        "genre",
        "structure",
        "literary device",
        "figurative language",
    }
    if total_guide_weeks >= 3 or _any_kw(all_text, _complex_kws):
        recommended.append("study_guide")

    # Parent newsletter — always valuable
    recommended.append("parent_newsletter_summary")

    return _deduplicate(recommended)


def recommend_outputs_from_guide_context(
    subjects: list[dict[str, Any]],
    *,
    total_guide_weeks: int,
) -> list[str]:
    """Extract signals from _assignment_context subjects and call recommend_outputs."""
    subject_codes: set[str] = set()
    objective_descriptions: list[str] = []

    for subject in subjects:
        code = (subject.get("subject_code") or "").strip().upper()
        if code:
            subject_codes.add(code)

    return recommend_outputs(
        subject_codes=subject_codes,
        objective_descriptions=objective_descriptions,
        total_guide_weeks=total_guide_weeks,
    )


def recommend_outputs_from_guide_detail(
    guides: list[Any],
    *,
    total_guide_weeks: int,
    subject_codes: set[str],
) -> list[str]:
    """Extract objective descriptions from loaded guide detail objects and recommend."""
    objective_descriptions: list[str] = []
    seen: set[str] = set()

    for guide in guides:
        for period in getattr(guide, "periods", []):
            for mapped in getattr(period, "objectives", []):
                objective = getattr(mapped, "objective", None)
                description = getattr(objective, "description", None) or ""
                if description and description not in seen:
                    seen.add(description)
                    objective_descriptions.append(description)

    return recommend_outputs(
        subject_codes=subject_codes,
        objective_descriptions=objective_descriptions,
        total_guide_weeks=total_guide_weeks,
    )


def _any_kw(text: str, keywords: set[str]) -> bool:
    return any(kw in text for kw in keywords)


def _deduplicate(items: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
