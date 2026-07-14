"""Instructional Delivery Profile — classroom delivery constraint for TeacherAssist v2.

Separates HOW instruction is delivered (strand schedule) from WHAT is taught (curriculum).

The profile constrains the Instructional Design Plan generator. It does not change
curriculum, objectives, duration, or TEKS. It only controls how instructional strands
are distributed across available instructional time.

Hierarchy:
  District Curriculum → Pacing Guide → Instructional Delivery Profile
  → Classroom Instruction Profile → Instructional Design Plan → Lesson Generation → Artifacts

Modes:
  ai_optimized   — AI decides distribution. Default. No constraint applied.
  daily_balanced — All configured strands appear every scheduled day for the specified minutes.
  block_schedule — Strands alternate by day (e.g. Reading Mon/Wed/Fri, Writing Tue/Thu).
  custom         — Strand schedule specified explicitly per day.

Strand-level Classroom Instruction Profile (CIP) fields (additive — existing packages unaffected):

  delivery_mode             — HOW the teacher delivers the curriculum lesson (read aloud, shared, etc.)

  curriculum_text_access    — HOW students physically access the CURRICULUM-SELECTED text
                              (e.g. "The Crane Girl"). This is distinct from independent reading.
                              teacher_copy_only → only the teacher has the curriculum text;
                              students NEVER open, read, or reference it independently.

  independent_reading_access — Whether students have access to THEIR OWN books during
                               independent reading time (classroom library, school library, etc.).
                               This is independent of curriculum_text_access — a classroom can
                               have teacher_copy_only curriculum text AND classroom_library_available
                               for student choice reading.

  closure_required          — Whether each occurrence of this strand must end with its own exit ticket.

Stored as `instructional_delivery_profile` JSONB on the package.
Null or absent profile → ai_optimized (zero breaking changes for existing packages).
"""

from __future__ import annotations

import re
from typing import Any

from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS

PROFILE_MODE_AI_OPTIMIZED = "ai_optimized"
PROFILE_MODE_DAILY_BALANCED = "daily_balanced"
PROFILE_MODE_BLOCK_SCHEDULE = "block_schedule"
PROFILE_MODE_CUSTOM = "custom"

_ALL_MODES = frozenset(
    {
        PROFILE_MODE_AI_OPTIMIZED,
        PROFILE_MODE_DAILY_BALANCED,
        PROFILE_MODE_BLOCK_SCHEDULE,
        PROFILE_MODE_CUSTOM,
    }
)
_ALL_WEEKDAYS = set(WEEKDAY_LABELS)

# Classroom Instruction Profile — delivery mode options per strand
STRAND_DELIVERY_MODES_READING = frozenset(
    {
        "teacher_read_aloud",  # Teacher reads aloud; students listen, discuss, respond in notebooks
        "shared_reading",  # Whole class reads together (projected or class copies)
        "students_have_individual_copies",  # Each student has their own physical copy
        "small_group_reading",  # Teacher-led small group instruction
        "independent_reading",  # Students read independently from their own chosen texts
        "teacher_choice",  # Teacher determines mode each session
    }
)

STRAND_DELIVERY_MODES_WRITING = frozenset(
    {
        "guided_writing",  # Teacher models writing; students write alongside
        "independent_writing",  # Students write independently after mini-lesson
        "shared_writing",  # Whole class composes together with teacher as scribe
        "teacher_choice",  # Teacher determines mode each session
    }
)

STRAND_DELIVERY_MODES_ALL = STRAND_DELIVERY_MODES_READING | STRAND_DELIVERY_MODES_WRITING

# How students physically access the CURRICULUM-SELECTED lesson text.
# This is about the specific district/curriculum-assigned text (e.g. "The Crane Girl").
# Separate from whether students have books for independent reading.
CURRICULUM_TEXT_ACCESS_MODES = frozenset(
    {
        "teacher_copy_only",  # ONLY the teacher has the curriculum text; students never open/read it
        "projected_shared_display",  # Text displayed on projector/smartboard for whole class to see
        "class_set",  # Every student has their own individual copy of the curriculum text
        "small_group_sets",  # Copies available for small group rotation only
        "digital_student_access",  # Students access curriculum text on individual devices
        "student_choice_text",  # No single curriculum text; students choose their own
    }
)

# Whether and how students access books for INDEPENDENT READING TIME.
# Completely separate from curriculum_text_access — students may have classroom_library books
# even when they cannot access the curriculum text (teacher_copy_only).
INDEPENDENT_READING_ACCESS_MODES = frozenset(
    {
        "classroom_library_available",  # Classroom library books available for student choice
        "school_library_available",  # School library books (checked out)
        "student_brought_books",  # Students bring books from home
        "digital_library",  # Digital library (Epic, Sora, etc.)
        "none",  # No independent reading time in this strand configuration
    }
)

_DELIVERY_MODE_LABELS: dict[str, str] = {
    "teacher_read_aloud": "Teacher Read Aloud",
    "shared_reading": "Shared Reading",
    "students_have_individual_copies": "Student Individual Copies",
    "small_group_reading": "Small Group",
    "independent_reading": "Independent Reading",
    "guided_writing": "Guided Writing",
    "independent_writing": "Independent Writing",
    "shared_writing": "Shared Writing",
    "teacher_choice": "Teacher Choice",
}

_CURRICULUM_TEXT_ACCESS_LABELS: dict[str, str] = {
    "teacher_copy_only": "Teacher Copy Only",
    "projected_shared_display": "Projected / Shared Display",
    "class_set": "Class Set (individual copies)",
    "small_group_sets": "Small Group Sets",
    "digital_student_access": "Digital (student devices)",
    "student_choice_text": "Student Choice",
}

_INDEPENDENT_READING_ACCESS_LABELS: dict[str, str] = {
    "classroom_library_available": "Classroom Library",
    "school_library_available": "School Library",
    "student_brought_books": "Student-Brought Books",
    "digital_library": "Digital Library",
    "none": "None",
}

# Patterns forbidden in student-facing content when curriculum_text_access = teacher_copy_only.
# These indicate the AI told students to access/read the curriculum text independently.
_TEACHER_COPY_ONLY_VIOLATION_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\bopen\s+(your|the)\s+copy\b", re.IGNORECASE),
    re.compile(r"\bopen\s+(to\s+)?page\s+\d+\b", re.IGNORECASE),
    re.compile(r"\bin\s+your\s+copy\b", re.IGNORECASE),
    re.compile(r"\byour\s+copy\s+of\b", re.IGNORECASE),
    re.compile(r"\bflip\s+to\s+page\b", re.IGNORECASE),
    re.compile(r"\bturn\s+to\s+page\b", re.IGNORECASE),
    re.compile(r"\bread\s+page[s]?\s+\d+", re.IGNORECASE),
    re.compile(r"\bfind\s+page\s+\d+\b", re.IGNORECASE),
    re.compile(r"\bannotate\s+(your\s+)?copy\b", re.IGNORECASE),
    re.compile(r"\bunderline\s+in\s+(your\s+)?copy\b", re.IGNORECASE),
]


def check_curriculum_text_access_violations(
    content: Any,
    *,
    curriculum_text_names: list[str],
) -> list[str]:
    """Scan artifact content for violations of curriculum_text_access=teacher_copy_only.

    Returns a list of violation descriptions. Empty list means no violations found.
    Checks both always-forbidden patterns (page numbers, "open your copy") and
    text-name-specific patterns (continue reading [title], read [title] independently).
    """
    violations: list[str] = []
    flat = _flatten_strings(content)

    # Always-forbidden patterns when teacher_copy_only
    for text in flat:
        for pattern in _TEACHER_COPY_ONLY_VIOLATION_PATTERNS:
            if pattern.search(text):
                violations.append(
                    f"always-forbidden: {pattern.pattern!r} matched in: {text[:80]!r}"
                )
                break

    # Text-name-specific patterns
    for title in curriculum_text_names:
        if not title or len(title) < 3:
            continue
        escaped = re.escape(title)
        title_patterns = [
            re.compile(rf"\bcontinue\s+reading\s+{escaped}\b", re.IGNORECASE),
            re.compile(rf"\bread\s+{escaped}\s+independently\b", re.IGNORECASE),
            re.compile(rf"\bread\s+independently\s+from\s+{escaped}\b", re.IGNORECASE),
            re.compile(rf"\bchoose\s+a\s+passage\s+from\s+{escaped}\b", re.IGNORECASE),
            re.compile(rf"\bopen\s+{escaped}\b", re.IGNORECASE),
            re.compile(rf"\bin\s+{escaped},?\s+(find|look|locate|turn)\b", re.IGNORECASE),
            re.compile(rf"\byour\s+copy\s+of\s+{escaped}\b", re.IGNORECASE),
        ]
        for text in flat:
            for pattern in title_patterns:
                if pattern.search(text):
                    violations.append(
                        f"curriculum-text-specific ({title!r}): {pattern.pattern!r} matched in: {text[:80]!r}"
                    )
                    break

    return violations


def _flatten_strings(content: Any, _depth: int = 0) -> list[str]:
    """Recursively collect all string values from a JSON structure."""
    if _depth > 12:
        return []
    if isinstance(content, str):
        return [content]
    if isinstance(content, dict):
        result: list[str] = []
        for v in content.values():
            result.extend(_flatten_strings(v, _depth + 1))
        return result
    if isinstance(content, list):
        result = []
        for item in content:
            result.extend(_flatten_strings(item, _depth + 1))
        return result
    return []


def resolve_profile(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize and validate a raw profile dict.

    Returns a clean profile with mode guaranteed to be set.
    Invalid or missing input → ai_optimized (no constraint).
    Never raises.
    """
    if not raw or not isinstance(raw, dict):
        return {"mode": PROFILE_MODE_AI_OPTIMIZED}

    mode = (raw.get("mode") or "").strip().lower()
    if mode not in _ALL_MODES:
        mode = PROFILE_MODE_AI_OPTIMIZED

    if mode == PROFILE_MODE_AI_OPTIMIZED:
        return {"mode": PROFILE_MODE_AI_OPTIMIZED}

    strands = _normalize_strands(raw.get("strands") or [])
    if not strands:
        return {"mode": PROFILE_MODE_AI_OPTIMIZED}

    return {"mode": mode, "strands": strands}


def profile_is_constrained(profile: dict[str, Any] | None) -> bool:
    """True if the profile imposes strand constraints (i.e. not ai_optimized)."""
    return bool(profile) and profile.get("mode") != PROFILE_MODE_AI_OPTIMIZED


def get_strand_slots_for_day(
    profile: dict[str, Any] | None,
    day: str,
) -> list[dict[str, Any]]:
    """Return the ordered strand slots active on the given day.

    Each slot includes: strand_name, minutes_per_day (optional), delivery_mode (optional),
    curriculum_text_access (optional), independent_reading_access (optional), closure_required (optional).
    Returns [] when mode is ai_optimized or no strands are scheduled for this day.
    """
    if not profile_is_constrained(profile):
        return []
    return [
        {k: v for k, v in s.items() if k != "days"}
        for s in (profile.get("strands") or [])
        if day in (s.get("days") or [])
    ]


def build_planning_constraint(profile: dict[str, Any] | None) -> str | None:
    """Return the AI planning constraint text for this profile.

    Returns None when mode is ai_optimized — nothing is injected.
    The returned string is inserted verbatim into the Phase 0c instruction.
    """
    if not profile_is_constrained(profile):
        return None

    mode = profile.get("mode", "")
    strands = profile.get("strands") or []
    if not strands:
        return None

    mode_display = {
        PROFILE_MODE_DAILY_BALANCED: "Daily Balanced",
        PROFILE_MODE_BLOCK_SCHEDULE: "Block Schedule",
        PROFILE_MODE_CUSTOM: "Custom",
    }.get(mode, mode)

    lines = [
        "INSTRUCTIONAL DELIVERY PROFILE (non-negotiable planning constraint):",
        f"  Mode: {mode_display}",
        "",
        "  The following instructional strands MUST appear on every day where they are scheduled.",
        "  You do NOT decide which strands appear on which days — that is already determined.",
        "  Your task: given this strand schedule, assign the most appropriate district objective",
        "  to each strand slot on each day.",
        "",
        "  Strand schedule:",
    ]
    for s in strands:
        name = s.get("strand_name", "")
        mins = s.get("minutes_per_day")
        days = s.get("days") or list(WEEKDAY_LABELS)
        mins_str = f" ({mins} min/day)" if mins else ""
        dm = s.get("delivery_mode")
        cta = s.get("curriculum_text_access")
        ira = s.get("independent_reading_access")
        cr = s.get("closure_required")
        extras: list[str] = []
        if dm:
            extras.append(_DELIVERY_MODE_LABELS.get(dm, dm))
        if cta:
            extras.append(f"curriculum text: {_CURRICULUM_TEXT_ACCESS_LABELS.get(cta, cta)}")
        if ira and ira != "none":
            extras.append(f"indep. reading: {_INDEPENDENT_READING_ACCESS_LABELS.get(ira, ira)}")
        if cr:
            extras.append("closure required")
        extras_str = f" [{', '.join(extras)}]" if extras else ""
        lines.append(f"    - {name}{mins_str}{extras_str}: {', '.join(days)}")

    lines += [
        "",
        "  Rules:",
        "  - Every strand listed above MUST appear in each day it is scheduled.",
        "  - Strands must not be collapsed, merged, skipped, or redistributed.",
        "  - If no district objective perfectly matches a strand slot, assign the most relevant",
        "    objective from that strand's subject area for the week.",
        "  - Do not add unlisted strands. Do not remove any listed strand from its scheduled days.",
        "  - 'curriculum_text_access=teacher_copy_only' means students NEVER open, read, or",
        "    reference the curriculum text independently — teacher reads aloud only.",
        "  - 'independent_reading_access' describes student choice books, NOT the curriculum text.",
    ]
    return "\n".join(lines)


def build_artifact_delivery_context(
    profile: dict[str, Any] | None,
    day: str | None,
) -> dict[str, Any] | None:
    """Build the delivery profile context injected into each artifact prompt_payload.

    Returns None when no profile is active or no strands are scheduled for this day.
    Returns a dict with mode, strand_slots_today, and a constraint_directive string
    that the AI instruction references directly.
    """
    if not profile_is_constrained(profile) or not day:
        return None

    slots = get_strand_slots_for_day(profile, day)
    if not slots:
        return None

    mode = profile.get("mode", "")
    strand_parts: list[str] = []
    for s in slots:
        name = s["strand_name"]
        mins = s.get("minutes_per_day")
        dm = s.get("delivery_mode")
        cta = s.get("curriculum_text_access")
        label = name
        if mins:
            label += f" ({mins} min)"
        if dm:
            label += f" [{_DELIVERY_MODE_LABELS.get(dm, dm)}]"
        if cta:
            label += f" [curriculum: {_CURRICULUM_TEXT_ACCESS_LABELS.get(cta, cta)}]"
        strand_parts.append(label)
    strand_list = ", ".join(strand_parts)
    return {
        "mode": mode,
        "strand_slots_today": slots,
        "constraint_directive": (
            f"DELIVERY CONSTRAINT ({mode.replace('_', ' ').title()}): "
            f"Today's lesson MUST include all of the following instructional strands in this order: {strand_list}. "
            "Do not collapse these into a single block. Do not omit any strand. "
            "Each strand must have distinct instructional content aligned to its district objective. "
            "Honor delivery_mode, curriculum_text_access, and independent_reading_access per strand — "
            "see classroom_instruction_profile rules."
        ),
    }


def _normalize_strands(raw_strands: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_strands, list):
        return []
    result = []
    for entry in raw_strands:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("strand_name") or "").strip()
        if not name:
            continue

        raw_min = entry.get("minutes_per_day")
        minutes: int | None = None
        if isinstance(raw_min, (int, float)) and raw_min > 0:
            minutes = int(raw_min)

        raw_days = entry.get("days")
        if raw_days and isinstance(raw_days, list):
            days = [d for d in raw_days if isinstance(d, str) and d in _ALL_WEEKDAYS]
        else:
            days = list(WEEKDAY_LABELS)

        if not days:
            continue

        strand: dict[str, Any] = {"strand_name": name, "days": days}
        if minutes is not None:
            strand["minutes_per_day"] = minutes

        # Classroom Instruction Profile fields — optional, validated but not required
        dm = (entry.get("delivery_mode") or "").strip().lower()
        if dm and dm in STRAND_DELIVERY_MODES_ALL:
            strand["delivery_mode"] = dm

        # curriculum_text_access: how students access the district-assigned curriculum text
        cta = (entry.get("curriculum_text_access") or "").strip().lower()
        if cta and cta in CURRICULUM_TEXT_ACCESS_MODES:
            strand["curriculum_text_access"] = cta
        elif not cta:
            # Backward compat: map old student_text_access → curriculum_text_access
            _old = (entry.get("student_text_access") or "").strip().lower()
            _compat = {
                "teacher_copy_only": "teacher_copy_only",
                "class_copies": "class_set",
                "projected": "projected_shared_display",
            }.get(_old)
            if _compat:
                strand["curriculum_text_access"] = _compat

        # independent_reading_access: how students access books for independent reading time
        ira = (entry.get("independent_reading_access") or "").strip().lower()
        if ira and ira in INDEPENDENT_READING_ACCESS_MODES:
            strand["independent_reading_access"] = ira

        cr = entry.get("closure_required")
        if isinstance(cr, bool):
            strand["closure_required"] = cr
        elif isinstance(cr, str):
            strand["closure_required"] = cr.lower() in ("true", "yes", "1")

        result.append(strand)
    return result
