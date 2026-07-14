"""Instructional resource bank — extracts and indexes district curriculum resources.

Extraction priority (highest to lowest):
  1. Structured pacing context fields: pacing_context.days[].materials_needed,
     pacing_context.attached_files[], pacing_context.reference_links[]
  2. Phase 0a AI-confirmed mentor_texts from curriculum_sequence_plan
  3. Regex patterns against full curriculum document text (last resort)

Resources are selected deterministically (week → day → strand → subject → artifact type →
instructional purpose → objective) and injected into each artifact's prompt payload.
The AI enriches instruction around the selected resource — it does not choose a different one.
"""

from __future__ import annotations

import re
import uuid
from typing import Any


# ---------------------------------------------------------------------------
# Resource type constants
# ---------------------------------------------------------------------------

RT_READ_ALOUD = "read_aloud"
RT_MENTOR_TEXT = "mentor_text"
RT_SHARED_READING = "shared_reading"
RT_INDEPENDENT_READING = "independent_reading"
RT_PASSAGE = "passage"
RT_POEM = "poem"
RT_ARTICLE = "article"
RT_GRAPHIC_ORGANIZER = "graphic_organizer"
RT_VIDEO = "video"
RT_ANCHOR_CHART = "anchor_chart"
RT_MANIPULATIVE = "manipulative"
RT_DIAGRAM = "diagram"
RT_OTHER = "other"

# Resource types that represent text/book selections (used for named_books_and_texts)
_TEXT_RESOURCE_TYPES: frozenset[str] = frozenset(
    {
        RT_READ_ALOUD,
        RT_MENTOR_TEXT,
        RT_SHARED_READING,
        RT_INDEPENDENT_READING,
        RT_PASSAGE,
        RT_POEM,
        RT_ARTICLE,
    }
)

# Instructional purpose → resource types that serve it best
_PURPOSE_PREFERRED_TYPES: dict[str, list[str]] = {
    "read_aloud": [RT_READ_ALOUD, RT_SHARED_READING, RT_PASSAGE, RT_MENTOR_TEXT],
    "guided_practice": [RT_MENTOR_TEXT, RT_PASSAGE, RT_SHARED_READING, RT_READ_ALOUD],
    "independent_practice": [RT_INDEPENDENT_READING, RT_PASSAGE, RT_ARTICLE, RT_MENTOR_TEXT],
    "writing_instruction": [RT_MENTOR_TEXT, RT_PASSAGE, RT_POEM],
    "word_study": [RT_POEM, RT_PASSAGE, RT_ANCHOR_CHART],
    "assessment": [RT_PASSAGE, RT_ARTICLE, RT_MENTOR_TEXT],
}

# Which resource types each artifact type prefers, in priority order
_ARTIFACT_PREFERRED_TYPES: dict[str, list[str]] = {
    "daily_lesson_plan": [
        RT_READ_ALOUD,
        RT_MENTOR_TEXT,
        RT_SHARED_READING,
        RT_PASSAGE,
        RT_POEM,
        RT_ARTICLE,
    ],
    "student_lesson_deck": [RT_READ_ALOUD, RT_PASSAGE, RT_MENTOR_TEXT, RT_SHARED_READING, RT_POEM],
    "subject_slide_deck": [RT_MENTOR_TEXT, RT_ANCHOR_CHART, RT_READ_ALOUD, RT_DIAGRAM],
    "bell_ringer": [RT_PASSAGE, RT_ARTICLE, RT_POEM, RT_READ_ALOUD],
    "exit_ticket": [RT_PASSAGE, RT_ARTICLE, RT_MENTOR_TEXT],
    "assignment": [RT_PASSAGE, RT_ARTICLE, RT_INDEPENDENT_READING, RT_MENTOR_TEXT],
    "quiz": [RT_PASSAGE, RT_ARTICLE, RT_MENTOR_TEXT],
    "writing_response": [RT_MENTOR_TEXT, RT_PASSAGE, RT_READ_ALOUD],
    "rubric": [RT_MENTOR_TEXT, RT_PASSAGE],
}
_DEFAULT_PREFERRED_TYPES: list[str] = [
    RT_READ_ALOUD,
    RT_MENTOR_TEXT,
    RT_SHARED_READING,
    RT_PASSAGE,
    RT_POEM,
    RT_ARTICLE,
    RT_ANCHOR_CHART,
    RT_GRAPHIC_ORGANIZER,
    RT_VIDEO,
    RT_DIAGRAM,
    RT_MANIPULATIVE,
    RT_INDEPENDENT_READING,
    RT_OTHER,
]

# Resource type → allowed classroom instructional activities.
# "allowed_uses" is injected into the prompt so the AI knows which activities are
# valid for this specific resource. It does NOT replace "instructional_purposes"
# (which is the purpose-matching scoring field); it is the AI behavioral constraint.
_ALLOWED_USES_FOR_TYPE: dict[str, list[str]] = {
    RT_READ_ALOUD: [
        "teacher_read_aloud",
        "think_aloud",
        "shared_reading_follow_along",
        "discussion",
        "turn_and_talk",
        "notebook_response",
    ],
    RT_MENTOR_TEXT: [
        "teacher_read_aloud",
        "shared_reading",
        "teacher_modeling",
        "guided_practice",
        "close_reading",
        "annotation",
        "discussion",
        "writing_response",
        "notebook_response",
    ],
    RT_SHARED_READING: [
        "shared_reading",
        "choral_reading",
        "echo_reading",
        "guided_practice",
        "annotation",
        "discussion",
        "close_reading",
    ],
    RT_INDEPENDENT_READING: [
        "independent_reading",
        "reading_response",
        "book_share",
    ],
    RT_PASSAGE: [
        "guided_practice",
        "close_reading",
        "annotation",
        "discussion",
        "assessment",
        "independent_practice",
    ],
    RT_POEM: [
        "teacher_read_aloud",
        "shared_reading",
        "choral_reading",
        "word_study",
        "discussion",
        "writing_response",
    ],
    RT_ARTICLE: [
        "guided_practice",
        "close_reading",
        "annotation",
        "discussion",
        "assessment",
        "independent_practice",
    ],
    RT_GRAPHIC_ORGANIZER: [
        "teacher_modeling",
        "guided_practice",
        "independent_practice",
    ],
    RT_VIDEO: [
        "whole_class_viewing",
        "discussion",
        "note_taking",
    ],
    RT_ANCHOR_CHART: [
        "teacher_modeling",
        "reference",
        "guided_practice",
    ],
    RT_MANIPULATIVE: [
        "hands_on",
        "guided_practice",
        "independent_practice",
    ],
    RT_DIAGRAM: [
        "teacher_modeling",
        "reference",
        "discussion",
    ],
}

# Strand name → resource types most relevant to that strand
_STRAND_PREFERRED_TYPES: dict[str, list[str]] = {
    "reading": [
        RT_READ_ALOUD,
        RT_SHARED_READING,
        RT_PASSAGE,
        RT_INDEPENDENT_READING,
        RT_ARTICLE,
        RT_POEM,
    ],
    "writing": [RT_MENTOR_TEXT, RT_PASSAGE, RT_POEM, RT_ARTICLE],
    "word study": [RT_POEM, RT_PASSAGE, RT_ANCHOR_CHART],
    "phonics": [RT_ANCHOR_CHART, RT_GRAPHIC_ORGANIZER, RT_MANIPULATIVE],
    "math": [RT_GRAPHIC_ORGANIZER, RT_DIAGRAM, RT_MANIPULATIVE, RT_ANCHOR_CHART],
}

# Extraction source → curriculum_grounding score (1.0 = definitive, 0.7 = inferred)
_SOURCE_GROUNDING: dict[str, float] = {
    "structured_materials_needed": 1.0,
    "structured_pacing_context": 0.9,
    "ai_sequence_plan": 0.8,
    "regex_document": 0.7,
}

_DAYS: list[str] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
_DAY_PATTERN = re.compile(r"\b(Monday|Tuesday|Wednesday|Thursday|Friday)\b", re.IGNORECASE)
_WEEK_PATTERN = re.compile(r"\bWeek\s+(\d+)\b", re.IGNORECASE)

# Regex patterns for resource type inference from topic/material strings
_TOPIC_TYPE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bread[\s\-]?aloud\b", re.IGNORECASE), RT_READ_ALOUD),
    (re.compile(r"\bshared[\s\-]?reading\b", re.IGNORECASE), RT_SHARED_READING),
    (re.compile(r"\bmentor[\s\-]?text\b", re.IGNORECASE), RT_MENTOR_TEXT),
    (re.compile(r"\bindependent[\s\-]?reading\b", re.IGNORECASE), RT_INDEPENDENT_READING),
    (re.compile(r"\bpoem\b|\bpoetry\b", re.IGNORECASE), RT_POEM),
    (re.compile(r"\barticle\b|\bnonfiction\b", re.IGNORECASE), RT_ARTICLE),
    (re.compile(r"\bpassage\b", re.IGNORECASE), RT_PASSAGE),
    (re.compile(r"\banchor[\s\-]?chart\b", re.IGNORECASE), RT_ANCHOR_CHART),
    (re.compile(r"\bgraphic[\s\-]?organizer\b", re.IGNORECASE), RT_GRAPHIC_ORGANIZER),
    (re.compile(r"\bvideo\b|\bfilm\b", re.IGNORECASE), RT_VIDEO),
]

# Regex document extraction patterns: (compiled, resource_type)
# Used only as a fallback when structured fields yield no results.
_EXTRACT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"read[\s\-]?aloud\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_READ_ALOUD),
    (re.compile(r"mentor[\s\-]?text\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_MENTOR_TEXT),
    (
        re.compile(r"shared[\s\-]?reading\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE),
        RT_SHARED_READING,
    ),
    (
        re.compile(r"independent[\s\-]?reading\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE),
        RT_INDEPENDENT_READING,
    ),
    (re.compile(r"passage\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_PASSAGE),
    (re.compile(r"\bpoem\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_POEM),
    (re.compile(r"\barticle\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_ARTICLE),
    (
        re.compile(r"graphic[\s\-]?organizer\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE),
        RT_GRAPHIC_ORGANIZER,
    ),
    (re.compile(r"anchor[\s\-]?chart\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_ANCHOR_CHART),
    (re.compile(r"\bvideo\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_VIDEO),
    (re.compile(r"manipulative\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_MANIPULATIVE),
    (re.compile(r"\bdiagram\s*[:\-]\s*([^\n;,]{3,80})", re.IGNORECASE), RT_DIAGRAM),
]

# Titles that are noise — pacing guide meta-labels, not actual resources
_NOISE_TITLES: frozenset[str] = frozenset(
    {
        "see above",
        "see below",
        "as needed",
        "teacher choice",
        "teacher selected",
        "district provided",
        "various",
        "multiple texts",
        "student selected",
        "tbd",
        "n/a",
        "none",
        "materials",
        "resources",
    }
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean_title(raw: str) -> str | None:
    """Strip whitespace, trailing punctuation, and reject noise titles."""
    title = raw.strip().rstrip(".,;:")
    title = re.sub(r"\s+", " ", title)
    if not title or len(title) < 3:
        return None
    if title.lower() in _NOISE_TITLES:
        return None
    if len(title) > 80:
        title = title[:80].rsplit(" ", 1)[0]
    if not any(c.isupper() for c in title):
        return None
    return title


def _infer_resource_type(text: str) -> str:
    """Infer resource_type from a topic or material label string."""
    for pattern, rt in _TOPIC_TYPE_PATTERNS:
        if pattern.search(text):
            return rt
    return RT_MENTOR_TEXT  # safe default for book-like materials


def _infer_strand_from_topic(topic: str) -> str | None:
    """Infer strand_name from a daily_topic string (e.g. 'Reading Workshop: ...')."""
    t = topic.lower()
    if "reading" in t:
        return "Reading"
    if "writing" in t:
        return "Writing"
    if "word study" in t or "phonics" in t or "spelling" in t:
        return "Word Study"
    return None


def _purposes_for_resource_type(resource_type: str) -> list[str]:
    """Default instructional purposes for a given resource type."""
    _MAP: dict[str, list[str]] = {
        RT_READ_ALOUD: ["read_aloud", "shared_reading", "guided_practice"],
        RT_SHARED_READING: ["shared_reading", "guided_practice", "read_aloud"],
        RT_MENTOR_TEXT: ["teacher_modeling", "guided_practice", "writing_instruction"],
        RT_INDEPENDENT_READING: ["independent_practice"],
        RT_PASSAGE: ["guided_practice", "independent_practice", "assessment"],
        RT_POEM: ["read_aloud", "word_study", "writing_instruction"],
        RT_ARTICLE: ["guided_practice", "independent_practice", "assessment"],
        RT_ANCHOR_CHART: ["teacher_modeling", "reference"],
        RT_GRAPHIC_ORGANIZER: ["guided_practice", "independent_practice"],
        RT_VIDEO: ["teacher_modeling", "engagement"],
        RT_DIAGRAM: ["teacher_modeling", "concept"],
        RT_MANIPULATIVE: ["hands_on", "guided_practice"],
    }
    return _MAP.get(resource_type, ["instruction"])


def _detect_week_near(text: str, pos: int, window: int = 600) -> int | None:
    """Return the closest Week N number within `window` chars before `pos`."""
    segment = text[max(0, pos - window) : pos]
    matches = list(_WEEK_PATTERN.finditer(segment))
    if not matches:
        return None
    return int(matches[-1].group(1))


def _detect_day_near(text: str, pos: int, window: int = 300) -> str | None:
    """Return a day name found within `window` chars before pos."""
    segment = text[max(0, pos - window) : pos]
    matches = list(_DAY_PATTERN.finditer(segment))
    if not matches:
        return None
    return matches[-1].group(1).capitalize()


def _excerpt_near(text: str, pos: int, radius: int = 120) -> str:
    return text[max(0, pos - radius) : min(len(text), pos + radius)].strip()


def _make_entry(
    *,
    title: str,
    resource_type: str,
    week: int | None,
    day: str | None,
    subject: str | None,
    strand_name: str | None,
    instructional_purposes: list[str],
    supported_objectives: list[str],
    extraction_source: str,
    source_document: str | None = None,
    source_excerpt: str | None = None,
    theme: str | None = None,
    topic: str | None = None,
    allowed_uses: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "resource_id": str(uuid.uuid4()),
        "title": title,
        "resource_type": resource_type,
        "week": week,
        "day": day,
        "subject": subject,
        "strand_name": strand_name,
        "theme": theme,
        "topic": topic,
        "instructional_purposes": instructional_purposes,
        "allowed_uses": allowed_uses
        if allowed_uses is not None
        else _ALLOWED_USES_FOR_TYPE.get(resource_type, ["instruction"]),
        "supported_objectives": supported_objectives,
        "extraction_source": extraction_source,
        "curriculum_grounding": _SOURCE_GROUNDING.get(extraction_source, 0.7),
        "source_document": source_document,
        "source_excerpt": source_excerpt,
        "is_curriculum_assigned": extraction_source != "regex_document",
    }


# ---------------------------------------------------------------------------
# Priority 1: Structured pacing context fields
# ---------------------------------------------------------------------------


def _extract_from_pacing_context(
    generation_context: dict[str, Any],
) -> list[dict[str, Any]]:
    """Extract resources from structured pacing context fields.

    Reads pacing_context.days[].materials_needed (per-day, highest specificity) and
    pacing_context.attached_files[] / reference_links[] (week-level). No regex — these
    are structured fields from the pacing guide database rows.
    """
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int | None, str | None]] = set()

    for week in generation_context.get("weeks") or []:
        week_num: int | None = week.get("sequence_number")
        for subj in week.get("subjects") or []:
            subj_name: str | None = subj.get("subject_name") or subj.get("subject")
            pacing = subj.get("pacing_context") or {}
            obj_codes = [
                str(o.get("objective_code") or o.get("description") or "")
                for o in (subj.get("objectives") or [])
                if o.get("objective_code") or o.get("description")
            ]

            # ── Per-day structured: materials_needed ─────────────────────────
            for day in pacing.get("days") or []:
                day_label: str | None = day.get("day_label") or day.get("day")
                materials_raw = day.get("materials_needed") or ""
                topic_raw = day.get("daily_topic") or ""

                if materials_raw and materials_raw.strip():
                    for raw_segment in re.split(r"[;|/]", materials_raw):
                        title = _clean_title(raw_segment)
                        if not title:
                            continue
                        rt = _infer_resource_type(materials_raw + " " + topic_raw)
                        strand = _infer_strand_from_topic(topic_raw)
                        key = (title.lower(), rt, week_num, day_label)
                        if key in seen:
                            continue
                        seen.add(key)
                        entries.append(
                            _make_entry(
                                title=title,
                                resource_type=rt,
                                week=week_num,
                                day=day_label,
                                subject=subj_name,
                                strand_name=strand,
                                instructional_purposes=_purposes_for_resource_type(rt),
                                supported_objectives=obj_codes,
                                extraction_source="structured_materials_needed",
                            )
                        )

            # ── Week-level structured: attached_files and reference_links ─────
            for bucket in ("attached_files", "reference_links"):
                for row in pacing.get(bucket) or []:
                    raw_title = (
                        row.get("title")
                        or row.get("display_name")
                        or row.get("original_filename")
                        or ""
                    )
                    title = _clean_title(raw_title)
                    if not title:
                        continue
                    rt = _infer_resource_type(
                        str(row.get("resource_type") or row.get("material_kind") or raw_title)
                    )
                    key = (title.lower(), rt, week_num, None)
                    if key in seen:
                        continue
                    seen.add(key)
                    entries.append(
                        _make_entry(
                            title=title,
                            resource_type=rt,
                            week=week_num,
                            day=None,
                            subject=subj_name,
                            strand_name=None,
                            instructional_purposes=_purposes_for_resource_type(rt),
                            supported_objectives=obj_codes,
                            extraction_source="structured_pacing_context",
                            source_document=raw_title,
                        )
                    )

    return entries


# ---------------------------------------------------------------------------
# Priority 2: Phase 0a AI-confirmed mentor_texts from curriculum_sequence_plan
# ---------------------------------------------------------------------------


def _extract_from_sequence_plan(
    curriculum_sequence_plan: dict[int, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Convert Phase 0a mentor_texts into bank entries."""
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int]] = set()

    for week_num, week_data in curriculum_sequence_plan.items():
        for title_raw in week_data.get("mentor_texts") or []:
            title = _clean_title(title_raw) if title_raw else None
            if not title:
                continue
            key = (title.lower(), RT_MENTOR_TEXT, week_num)
            if key in seen:
                continue
            seen.add(key)
            reading_focus = week_data.get("reading_focus")
            writing_focus = week_data.get("writing_focus")
            theme = week_data.get("theme") or None
            topic = reading_focus or writing_focus or None
            purposes = _purposes_for_resource_type(RT_MENTOR_TEXT)
            if reading_focus:
                purposes = ["read_aloud", "guided_practice"] + purposes
            entries.append(
                _make_entry(
                    title=title,
                    resource_type=RT_MENTOR_TEXT,
                    week=week_num,
                    day=None,
                    subject=None,
                    strand_name=None,
                    theme=theme,
                    topic=topic,
                    instructional_purposes=purposes,
                    supported_objectives=list(week_data.get("primary_teks") or []),
                    extraction_source="ai_sequence_plan",
                    source_document="curriculum_sequence_plan",
                )
            )

    return entries


# ---------------------------------------------------------------------------
# Priority 3: Regex extraction from full curriculum document text (fallback)
# ---------------------------------------------------------------------------


def _extract_from_document(doc_title: str, doc_text: str) -> list[dict[str, Any]]:
    """Parse extracted curriculum PDF text. Used only when structured fields yield nothing."""
    entries: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int | None]] = set()

    for pattern, resource_type in _EXTRACT_PATTERNS:
        for m in pattern.finditer(doc_text):
            raw_title = m.group(1)
            title = _clean_title(raw_title)
            if not title:
                continue
            week = _detect_week_near(doc_text, m.start())
            day = _detect_day_near(doc_text, m.start())
            key = (title.lower(), resource_type, week)
            if key in seen:
                continue
            seen.add(key)
            entries.append(
                _make_entry(
                    title=title,
                    resource_type=resource_type,
                    week=week,
                    day=day,
                    subject=None,
                    strand_name=None,
                    instructional_purposes=_purposes_for_resource_type(resource_type),
                    supported_objectives=[],
                    extraction_source="regex_document",
                    source_document=doc_title,
                    source_excerpt=_excerpt_near(doc_text, m.start()),
                )
            )

    return entries


# ---------------------------------------------------------------------------
# Public: build_instructional_resource_bank
# ---------------------------------------------------------------------------


def build_instructional_resource_bank(
    full_curriculum_docs: list[dict[str, Any]],
    curriculum_sequence_plan: dict[int, dict[str, Any]],
    *,
    generation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the instructional resource bank from all available curriculum data.

    Extraction priority:
      1. Structured pacing context (generation_context.weeks → pacing_context.days)
      2. Phase 0a AI-confirmed mentor_texts (curriculum_sequence_plan)
      3. Regex patterns against full curriculum PDF text (fallback)

    Called once after Phase 0a. No new AI calls. No new OCR.
    The returned bank must be persisted to instructional_resource_bank_json on the package
    so it survives regeneration.
    """
    source_documents: list[str] = []

    # Priority 1: structured pacing context (most reliable — day-specific, no inference)
    structured_entries = (
        _extract_from_pacing_context(generation_context) if generation_context else []
    )

    # Priority 2: AI sequence plan mentor_texts
    sequence_entries = _extract_from_sequence_plan(curriculum_sequence_plan)

    # Priority 3: regex document fallback — only run if structured + sequence yielded few entries
    regex_entries: list[dict[str, Any]] = []
    if (len(structured_entries) + len(sequence_entries)) < 3:
        for doc in full_curriculum_docs:
            doc_title = doc.get("title") or doc.get("filename") or "curriculum_document"
            doc_text = doc.get("text") or ""
            if doc_text:
                source_documents.append(doc_title)
                regex_entries.extend(_extract_from_document(doc_title, doc_text))

    # Merge with dedup: higher-priority sources win for same (title, type, week)
    seen_final: set[tuple[str, str, int | None]] = set()
    deduped: list[dict[str, Any]] = []
    for entry in structured_entries + sequence_entries + regex_entries:
        key = (entry["title"].lower(), entry["resource_type"], entry.get("week"))
        if key not in seen_final:
            seen_final.add(key)
            deduped.append(entry)

    return {
        "version": 2,
        "entries": deduped,
        "source_documents": source_documents,
        "total_entries": len(deduped),
        "extraction_summary": {
            "structured": len(structured_entries),
            "ai_sequence_plan": len(sequence_entries),
            "regex_document": len(regex_entries),
        },
    }


# ---------------------------------------------------------------------------
# Public: select_instructional_resource
# ---------------------------------------------------------------------------


def select_instructional_resource(
    bank: dict[str, Any],
    *,
    week: int | None,
    day: str | None,
    subject: str | None,
    artifact_type: str,
    objective_codes: list[str] | None = None,
    strand_name: str | None = None,
    instructional_purpose: str | None = None,
) -> dict[str, Any] | None:
    """Return the single best-matching resource for this artifact context.

    Selection is deterministic — same inputs always produce the same output.
    Scoring weights (in addition to week/day match):
      - strand alignment: highest weight when strand_name is provided
      - instructional purpose alignment
      - resource type preference for this artifact type
      - objective code overlap
      - extraction source quality (structured > AI > regex)

    Returns None when the bank is empty or no suitable entry exists.
    """
    entries = (bank or {}).get("entries") or []
    if not entries:
        return None

    preferred_types = _ARTIFACT_PREFERRED_TYPES.get(artifact_type, _DEFAULT_PREFERRED_TYPES)
    type_rank = {rt: i for i, rt in enumerate(preferred_types)}

    # Purpose-preferred types override artifact-preferred types when purpose is specified
    purpose_types: list[str] = []
    if instructional_purpose:
        purpose_types = _PURPOSE_PREFERRED_TYPES.get(instructional_purpose, [])

    # Strand-preferred types
    strand_types: list[str] = []
    if strand_name:
        strand_types = _STRAND_PREFERRED_TYPES.get(strand_name.lower(), [])

    obj_set = set(objective_codes or [])

    def _score(entry: dict[str, Any]) -> tuple[int, float, int]:
        s = 0

        # Week alignment
        e_week = entry.get("week")
        if e_week is None:
            s += 2
        elif e_week == week:
            s += 6
        else:
            s -= 10  # wrong week — nearly disqualified

        # Day alignment
        e_day = entry.get("day")
        if e_day and day and e_day.lower() == day.lower():
            s += 4  # increased from 3 — day specificity is high-value
        elif e_day and day and e_day.lower() != day.lower():
            s -= 1

        # Strand alignment — highest weight when strand is specified
        e_strand = (entry.get("strand_name") or "").lower()
        if strand_name and e_strand:
            if e_strand == strand_name.lower():
                s += 5
            else:
                s -= 2  # wrong strand is a meaningful penalty

        # Instructional purpose alignment
        if instructional_purpose and purpose_types:
            rt = entry.get("resource_type", RT_OTHER)
            if rt in purpose_types[:2]:
                s += 3
        if instructional_purpose:
            e_purposes = entry.get("instructional_purposes") or []
            if instructional_purpose in e_purposes:
                s += 2

        # Resource type preference (strand types override artifact types when both available)
        rt = entry.get("resource_type", RT_OTHER)
        if strand_types and rt in strand_types:
            strand_rank = strand_types.index(rt)
            s += max(0, len(strand_types) - strand_rank) + 1
        else:
            rt_rank = type_rank.get(rt)
            if rt_rank is not None:
                s += max(0, len(preferred_types) - rt_rank)
            else:
                s -= 2

        # Objective overlap
        e_objectives = set(entry.get("supported_objectives") or [])
        if obj_set and e_objectives:
            overlap = len(obj_set & e_objectives)
            s += min(overlap * 2, 4)

        # Extraction source quality
        grounding = entry.get("curriculum_grounding", 0.7)
        s += int(grounding * 3)  # structured (1.0) → +3; regex (0.7) → +2

        # Specificity: entries with day/strand specified beat week-level entries
        specificity = sum(
            1 for f in ("week", "day", "subject", "strand_name") if entry.get(f) is not None
        )

        return (s, grounding, specificity)

    candidates = [e for e in entries if _score(e)[0] > 0]
    if not candidates:
        # Relax: any curriculum-assigned entry from the right week
        candidates = [
            e
            for e in entries
            if e.get("is_curriculum_assigned") and (e.get("week") is None or e.get("week") == week)
        ]
    if not candidates:
        return None

    candidates.sort(key=_score, reverse=True)
    return candidates[0]


# ---------------------------------------------------------------------------
# Public: check_resource_sufficiency
# ---------------------------------------------------------------------------


def check_resource_sufficiency(
    bank: dict[str, Any],
    *,
    week: int | None,
    day: str | None,
    subject_name: str | None,
    artifact_type: str,
    strand_name: str | None = None,
    objective_codes: list[str] | None = None,
    instructional_purpose: str | None = None,
) -> dict[str, Any]:
    """Determine whether an appropriate curriculum resource exists for this artifact.

    Returns:
    {
      "sufficient": bool,
      "selected": dict | None,
      "adaptation_note": str | None,
    }

    Logic:
      - No resource in bank → sufficient=False → artifact flagged for teacher review
      - Resource found, type matches lesson needs → sufficient=True, no adaptation
      - Resource found, type mismatches → sufficient=True, adaptation_note explains how to adapt
        (use this resource creatively rather than substitute placeholder language)
    """
    selected = select_instructional_resource(
        bank,
        week=week,
        day=day,
        subject=subject_name,
        artifact_type=artifact_type,
        strand_name=strand_name,
        objective_codes=objective_codes,
        instructional_purpose=instructional_purpose,
    )

    if selected is None:
        return {"sufficient": False, "selected": None, "adaptation_note": None}

    preferred_types = _ARTIFACT_PREFERRED_TYPES.get(artifact_type, _DEFAULT_PREFERRED_TYPES)
    rt = selected.get("resource_type", RT_OTHER)
    is_good_type = rt in preferred_types[:4]

    if is_good_type:
        return {"sufficient": True, "selected": selected, "adaptation_note": None}

    # Type mismatch — resource available but may need adaptation
    adaptation_note = (
        f"The closest available curriculum resource is '{selected.get('title')}' "
        f"(type: {rt}). It may not be the ideal type for this artifact, but use it "
        f"as the instructional anchor rather than substituting generic placeholder language. "
        f"Adapt by drawing on relevant passages, examples, or themes within this resource."
    )
    return {"sufficient": True, "selected": selected, "adaptation_note": adaptation_note}


# ---------------------------------------------------------------------------
# Public: named_books_from_bank
# ---------------------------------------------------------------------------


def named_books_from_bank(
    bank: dict[str, Any],
    week: int | None,
    fallback: list[str],
) -> list[str]:
    """Return a deduplicated title list from the resource bank for prompt grounding.

    Prefers entries for the current week; falls back to all entries; if the bank
    is empty, returns the caller-supplied fallback list (pacing material filenames).
    Only includes text/book resource types (not charts, videos, manipulatives, etc.).
    """
    entries = (bank or {}).get("entries") or []
    week_titles: list[str] = []
    any_titles: list[str] = []
    seen: set[str] = set()

    for entry in entries:
        rt = entry.get("resource_type", RT_OTHER)
        if rt not in _TEXT_RESOURCE_TYPES:
            continue
        title = entry.get("title")
        if not title or title.lower() in seen:
            continue
        seen.add(title.lower())
        any_titles.append(title)
        if entry.get("week") is None or entry.get("week") == week:
            week_titles.append(title)

    if week_titles:
        return week_titles
    if any_titles:
        return any_titles
    return fallback


# ---------------------------------------------------------------------------
# Public: build_resource_alignment_map
# ---------------------------------------------------------------------------


def build_resource_alignment_map(
    bank: dict[str, Any],
    generation_context: dict[str, Any],
) -> dict[str, Any]:
    """Build a pre-committed resource alignment map for the entire package.

    Pre-commits one resource per (week, day, strand) combination so that all
    artifacts generated on the same day for the same strand use the SAME resource.
    This prevents drift where the lesson plan references "The Crane Girl" but the
    exit ticket falls back to generic placeholder language because selection ran
    independently.

    Returns:
    {
      "week_1": {
        "Monday": {
          "Reading": {resource_entry},
          "Writing": {resource_entry},
          "_default": {resource_entry},   # used when no strand-specific match
        },
        ...
      },
      ...
    }
    """
    alignment: dict[str, Any] = {}

    for week in generation_context.get("weeks") or []:
        week_num = week.get("sequence_number")
        week_key = f"week_{week_num}"
        week_map: dict[str, Any] = {}

        # Collect strand names in use this week
        for subj in week.get("subjects") or []:
            for obj in subj.get("objectives") or []:
                pass  # walk to get context

        # Build per-day alignment
        all_day_labels = set()
        for subj in week.get("subjects") or []:
            pacing = subj.get("pacing_context") or {}
            for day_plan in pacing.get("days") or []:
                all_day_labels.add(day_plan.get("day_label") or "")

        if not all_day_labels:
            all_day_labels = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday"}

        for day in sorted(all_day_labels):
            if not day:
                continue
            day_map: dict[str, Any] = {}

            # Per-strand selection
            for strand in ["Reading", "Writing", "Word Study", "Math"]:
                resource = select_instructional_resource(
                    bank,
                    week=week_num,
                    day=day,
                    subject=None,
                    artifact_type="daily_lesson_plan",
                    strand_name=strand,
                )
                if resource:
                    day_map[strand] = _alignment_entry(resource)

            # Default (no strand filter) — best resource for the day
            default_resource = select_instructional_resource(
                bank,
                week=week_num,
                day=day,
                subject=None,
                artifact_type="daily_lesson_plan",
            )
            if default_resource:
                day_map["_default"] = _alignment_entry(default_resource)

            if day_map:
                week_map[day] = day_map

        if week_map:
            alignment[week_key] = week_map

    return {
        "alignment": alignment,
        "total_weeks": len(alignment),
        "built_at": _alignment_timestamp(),
    }


def _alignment_entry(resource: dict[str, Any]) -> dict[str, Any]:
    """Trim a bank entry to the fields relevant for alignment display and prompt injection."""
    return {
        "title": resource.get("title"),
        "resource_type": resource.get("resource_type"),
        "theme": resource.get("theme"),
        "topic": resource.get("topic"),
        "allowed_uses": resource.get("allowed_uses") or [],
        "instructional_purposes": resource.get("instructional_purposes") or [],
        "supported_objectives": resource.get("supported_objectives") or [],
        "curriculum_grounding": resource.get("curriculum_grounding"),
        "extraction_source": resource.get("extraction_source"),
    }


def _alignment_timestamp() -> str:
    import datetime

    return datetime.datetime.now(datetime.timezone.utc).isoformat()
