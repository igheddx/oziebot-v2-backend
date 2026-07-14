"""Artifact Intelligence Engine — Phase 3 of TeacherAssist v2 generation pipeline.

This module guarantees instructional alignment across every generated artifact by:

  Phase 0e  build_vocabulary_registry()
            Builds a per-(week_num, subject_name_lower) list of introduced academic
            vocabulary from KDG knowledge_nodes and objective descriptions. No AI call.

  Phase 1   extract_instructional_contract()
            Derives the binding instructional contract for one artifact from the
            Instructional Design Plan. Each generation job receives a contract before
            AI generation begins. Vocabulary registry feeds introduced_vocabulary.

  Phase 3   enforce_instructional_contract() + enforce_student_content_prohibitions()
            After AI generation, enforces the minimum alignment contracts in Python
            before persisting. Zero additional AI calls.
            - exit_ticket_stem: auto-injected into closure if absent (daily_lesson_plan
              and student_lesson_deck)
            - quiz_objectives: questions filtered to only allowed TEKS codes
            - rubric_primary_criterion: flagged only, not auto-corrected
            - core_activity_name: flagged only
            - student content: TEKS codes, district metadata, instructional internals
              stripped from all student-facing artifacts

  Phase 3b  validate_week_alignment()
            After all artifacts for a week are persisted, validates cross-artifact
            coherence and produces a week-level alignment report. Each check includes
            an alignment_explanation for root admin diagnostics. No additional AI calls.

Guiding principle: The Instructional Design Plan is the source of truth.
Artifacts must never infer instructional intent independently.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

ARTIFACT_ENGINE_VERSION = "1.0"

# Student-facing artifact types — student content prohibitions enforced on these.
_STUDENT_FACING_TYPES = frozenset({
    "student_lesson_deck",
    "quiz",
    "assignment",
    "writing_response",
    "parent_newsletter_summary",
})

# Assessment artifacts that receive vocabulary registry in the prompt.
_ASSESSMENT_TYPES = frozenset({"quiz", "assignment", "writing_response"})

# Stopwords excluded from objective-description vocabulary extraction.
_VOCAB_STOPWORDS = frozenset([
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "that", "this", "which", "when", "where",
    "how", "what", "who", "is", "are", "was", "were", "be", "been", "have",
    "has", "had", "do", "does", "did", "will", "would", "could", "should",
    "may", "might", "can", "using", "through", "including", "about", "as",
    "not", "no", "so", "if", "then", "than", "its", "their", "they", "them",
    "student", "students", "able", "demonstrate", "understand",
    "identify", "explain", "analyze", "describe", "use", "create",
    "write", "read", "apply", "evaluate", "compare", "contrast",
    "including", "such", "make", "give", "take", "find", "each", "also",
])


# ── Student content prohibition patterns ─────────────────────────────────────────
# Applied recursively to all string values in student-facing artifact content.
# Each entry: (compiled_pattern, replacement_text).
_STUDENT_PROHIBITION_SUBS: list[tuple[re.Pattern[str], str]] = [
    # TEKS codes with explicit prefix: "TEKS 5.8A", "TEKS 5.11"
    (re.compile(r'\bTEKS\s+\d{1,2}\.\d+[A-Za-z]?\b', re.IGNORECASE), ''),
    # Standalone TEKS codes with unambiguous letter suffix: "5.8A", "10.2Bi"
    (re.compile(r'\b\d{1,2}\.\d+[A-Z][a-z]?\b'), ''),
    # "Standard X.X" patterns
    (re.compile(r'\bstandard\s+\d{1,2}\.\d+[A-Za-z]?\b', re.IGNORECASE), ''),
    # UUID-formatted objective IDs
    (re.compile(
        r'\bobjective\s+[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}'
        r'-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b',
        re.IGNORECASE,
    ), ''),
    # Named internal planning documents
    (re.compile(r'\bessential\s+unit\s+of\s+study\b', re.IGNORECASE), 'this unit'),
    (re.compile(r'\bpacing\s+guide\b', re.IGNORECASE), 'our learning plan'),
    (re.compile(r'\bcurriculum\s+(?:document|map|guide|framework)\b', re.IGNORECASE), ''),
    (re.compile(r'\bdistrict\s+materials?\b', re.IGNORECASE), ''),
    (re.compile(r'\bunit\s+of\s+study\b', re.IGNORECASE), 'this unit'),
    (re.compile(r'\bcurriculum\s+alignment\b', re.IGNORECASE), ''),
    (re.compile(r'\bbenchmark\s+assessment\b', re.IGNORECASE), 'assessment'),
    # Instructional metadata field names that must never appear in student content
    (re.compile(r'\binstructional\s+purpose\b', re.IGNORECASE), ''),
    (re.compile(r'\blearning\s+journey\s+rationale\b', re.IGNORECASE), ''),
    (re.compile(r'\bmastery\s+gate\b', re.IGNORECASE), 'learning goal'),
    (re.compile(r'\bterminal\s+mastery\b', re.IGNORECASE), 'final goal'),
    (re.compile(r'\bknowledge\s+dependency\b', re.IGNORECASE), ''),
    (re.compile(r'\binstructional\s+contract\b', re.IGNORECASE), ''),
    (re.compile(r'\bdistrict\s+anchor\b', re.IGNORECASE), ''),
    (re.compile(r'\bteacher\s+goal\b', re.IGNORECASE), ''),
    (re.compile(r'\breteach\s+if\s+needed\b', re.IGNORECASE), ''),
]

# Only the TEKS-detection patterns (first 3) are used in the post-generation scan
# to avoid false positives from the metadata-phrase patterns during content inspection.
_TEKS_DETECTION_PATTERNS = _STUDENT_PROHIBITION_SUBS[:3]


# ── Phase 0e: Vocabulary Registry ────────────────────────────────────────────────

def build_vocabulary_registry(
    plan: dict[str, Any],
    generation_context: dict[str, Any],
) -> dict[tuple[int, str], list[str]]:
    """Build the introduced-vocabulary registry for the entire package.

    Returns a dict keyed by (week_num, subject_name_lower) whose value is the sorted
    list of vocabulary terms available for that week's assessment artifacts. Terms
    accumulate across weeks — a term introduced in week 1 is available in weeks 1..N.

    Two sources (no AI, < 5ms):
      1. KDG knowledge_nodes — prerequisite vocabulary phrases (available from week 1)
      2. Primary objective descriptions — key words from this week's TEKS descriptions
         (words ≥ 4 chars, not in stopword list)
    """
    from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
        get_plan_district_anchors,
    )

    # KDG prerequisite vocabulary: available from the start of the package.
    kdg_terms: set[str] = set()
    for entry in (plan.get("knowledge_dependency_graph") or []):
        for dep in (entry.get("dependencies") or []):
            node = (dep.get("knowledge_node") or "").strip()
            if len(node) >= 3:
                kdg_terms.add(node.lower())

    weeks = sorted(
        generation_context.get("weeks") or [],
        key=lambda w: int(w.get("sequence_number") or 0),
    )

    # Per-week, per-subject: new vocabulary from this week's objective descriptions.
    weekly_new: dict[tuple[int, str], set[str]] = {}
    for week in weeks:
        week_num = int(week.get("sequence_number") or 0)
        for ws in week.get("subjects") or []:
            subj_lower = (ws.get("subject_name") or "").strip().lower()
            if not subj_lower:
                continue
            anchors = get_plan_district_anchors(plan, week_num, subj_lower) or {}
            new_terms: set[str] = set()
            for obj in (anchors.get("primary_objectives") or []):
                desc = (obj.get("description") or "").strip()
                for word in re.split(r'\W+', desc.lower()):
                    if len(word) >= 4 and word not in _VOCAB_STOPWORDS:
                        new_terms.add(word)
            weekly_new[(week_num, subj_lower)] = new_terms

    # Build cumulative registry: for (week N, subject), available terms =
    # KDG background + objective-description terms from weeks 1..N for this subject.
    registry: dict[tuple[int, str], list[str]] = {}
    for subject_lower in {subj for _, subj in weekly_new}:
        cumulative: set[str] = set(kdg_terms)
        for week_num in range(1, len(weeks) + 1):
            cumulative |= weekly_new.get((week_num, subject_lower), set())
            if (week_num, subject_lower) in weekly_new:
                registry[(week_num, subject_lower)] = sorted(cumulative)

    return registry


# ── Phase 1: Instructional Contract Extraction ───────────────────────────────────

def extract_instructional_contract(
    artifact_type: str,
    plan: dict[str, Any],
    week_num: int,
    subject_name: str,
    day_label: str | None,
    vocabulary_registry: dict[tuple[int, str], list[str]],
) -> dict[str, Any]:
    """Extract the instructional contract for one artifact from the plan.

    The contract is the set of non-negotiable alignment requirements for this
    specific artifact — derived deterministically from the Instructional Design Plan
    in Python. It is not generated by AI and cannot be modified by AI generation.
    """
    from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
        get_plan_instructional_design_week,
        get_plan_for_day,
    )

    subj_lower = subject_name.strip().lower()
    design_week = get_plan_instructional_design_week(plan, week_num, subj_lower) or {}
    contracts = design_week.get("instructional_contracts") or {}
    day_entry = (
        get_plan_for_day(plan, week_num, subj_lower, day_label) if day_label else {}
    ) or {}

    vocab_key = (week_num, subj_lower)
    introduced_vocabulary = list(vocabulary_registry.get(vocab_key) or [])

    return {
        "exit_ticket_stem": contracts.get("exit_ticket_stem"),
        "rubric_primary_criterion": contracts.get("rubric_primary_criterion"),
        "quiz_objectives": set(contracts.get("quiz_objectives") or []),
        "core_activity_name": contracts.get("core_activity_name"),
        "observable_mastery_evidence": day_entry.get("observable_mastery_evidence"),
        "introduced_vocabulary": introduced_vocabulary,
        "student_facing": artifact_type in _STUDENT_FACING_TYPES,
        "week_num": week_num,
        "subject_name": subject_name,
        "day_label": day_label,
    }


# ── Phase 3: Contract Enforcement ────────────────────────────────────────────────

def _stem_in_text(stem: str, text: str) -> bool:
    """Case-insensitive check: first 40 chars of stem appear in text."""
    if not stem or not text:
        return False
    return stem.strip()[:40].lower() in text.lower()


def enforce_instructional_contract(
    artifact_type: str,
    content: dict[str, Any],
    contract: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Enforce the instructional contract on post-AI content before persistence.

    Returns (content, alignment_result).
    Content may be a new dict if auto-correction was applied.
    alignment_result is stored in artifact.metadata_json["alignment"].

    Auto-correct applies to: exit_ticket_stem, quiz_objectives.
    Flag only (no auto-correct): rubric_primary_criterion, core_activity_name.
    """
    contracts_enforced: list[str] = []
    contracts_failed: list[str] = []
    content = dict(content)

    # ── Exit ticket stem (daily_lesson_plan) ──────────────────────────────────────
    if artifact_type == "daily_lesson_plan":
        stem = contract.get("exit_ticket_stem")
        if stem:
            subjects = content.get("subjects")
            if isinstance(subjects, list):
                injected_count = 0
                for block in subjects:
                    if not isinstance(block, dict):
                        continue
                    closure = block.get("closure") or ""
                    assessment = block.get("assessment") or ""
                    if not _stem_in_text(stem, closure) and not _stem_in_text(stem, assessment):
                        block["closure"] = f"{closure}\n\nExit Ticket: {stem}".strip()
                        injected_count += 1
                if injected_count > 0:
                    content["subjects"] = subjects
                    contracts_enforced.append(
                        f"exit_ticket_stem: injected into closure of {injected_count} subject block(s)"
                    )
                else:
                    contracts_enforced.append("exit_ticket_stem: present in subject closure(s)")

    # ── Exit ticket stem (student_lesson_deck) ────────────────────────────────────
    elif artifact_type == "student_lesson_deck":
        stem = contract.get("exit_ticket_stem")
        if stem:
            slides = content.get("slides")
            if isinstance(slides, list):
                exit_slides = [
                    s for s in slides
                    if isinstance(s, dict) and (
                        (s.get("slide_type") or "").lower().replace(" ", "_") == "exit_ticket"
                        or "exit" in (s.get("title") or "").lower()
                        or ((s.get("engagement") or {}).get("type") or "").lower().replace(" ", "_") == "exit_ticket"
                    )
                ]
                stem_present = any(
                    _stem_in_text(stem, s.get("body") or "")
                    or _stem_in_text(stem, (s.get("engagement") or {}).get("prompt") or "")
                    for s in exit_slides
                )
                if exit_slides and not stem_present:
                    slide = exit_slides[0]
                    slide["body"] = f"{slide.get('body') or ''}\n\n{stem}".strip()
                    contracts_enforced.append("exit_ticket_stem: injected into student_lesson_deck exit slide")
                elif exit_slides and stem_present:
                    contracts_enforced.append("exit_ticket_stem: present in exit slide")
                else:
                    contracts_failed.append(
                        "exit_ticket_stem: no exit_ticket slide found in student_lesson_deck"
                    )

    # ── Quiz objective filtering (auto-correct) ───────────────────────────────────
    if artifact_type == "quiz":
        allowed = contract.get("quiz_objectives") or set()
        if allowed:
            questions = content.get("questions") or []
            if isinstance(questions, list) and questions:
                compliant = [
                    q for q in questions
                    if not isinstance(q, dict)
                    or not q.get("objective_id")
                    or (q.get("objective_id") or "").strip() in allowed
                ]
                removed = len(questions) - len(compliant)
                if removed > 0 and compliant:
                    content["questions"] = compliant
                    contracts_enforced.append(
                        f"quiz_objectives: removed {removed} question(s) with disallowed objectives"
                    )
                elif removed == 0:
                    contracts_enforced.append(
                        "quiz_objectives: all questions aligned to allowed objectives"
                    )
                else:
                    # All questions would be removed — keep original, flag failure
                    contracts_failed.append(
                        f"quiz_objectives: all {len(questions)} question(s) target disallowed "
                        "objectives; no safe questions to keep — original preserved"
                    )

    # ── Rubric primary criterion — flag only, no auto-correct ─────────────────────
    if artifact_type == "rubric":
        criterion = contract.get("rubric_primary_criterion")
        if criterion:
            criteria = content.get("criteria") or []
            first_name = (
                (criteria[0].get("name") or "")
                if isinstance(criteria, list) and criteria and isinstance(criteria[0], dict)
                else ""
            )
            crit_lower = criterion.strip().lower()
            name_lower = first_name.lower()
            if crit_lower in name_lower or name_lower in crit_lower:
                contracts_enforced.append("rubric_primary_criterion: matches first criterion")
            else:
                contracts_failed.append(
                    f"rubric_primary_criterion: expected '{criterion[:60]}', "
                    f"found '{first_name[:60]}'"
                )

    # ── Core activity name — flag only ────────────────────────────────────────────
    core_activity = contract.get("core_activity_name")
    if core_activity and artifact_type not in {"parent_newsletter_summary"}:
        if core_activity.lower() in json.dumps(content).lower():
            contracts_enforced.append("core_activity_name: present in artifact content")
        else:
            contracts_failed.append(
                f"core_activity_name: '{core_activity[:60]}' not found in artifact content"
            )

    # ── Deck structure validation (student_lesson_deck only) ─────────────────────
    deck_structure: dict[str, Any] | None = None
    if artifact_type == "student_lesson_deck":
        deck_structure = _validate_deck_structure(content)
        if deck_structure.get("status") == "fail":
            contracts_failed.extend(deck_structure.get("violations") or [])

    artifact_confidence = "Ready" if not contracts_failed else "Needs Review"
    alignment_result: dict[str, Any] = {
        "contracts_enforced": contracts_enforced,
        "contracts_failed": contracts_failed,
        "artifact_confidence": artifact_confidence,
        "student_facing": contract.get("student_facing", False),
        "student_prohibitions_scan": None,
    }
    if deck_structure is not None:
        alignment_result["deck_structure_validation"] = deck_structure
    return content, alignment_result


def _validate_deck_structure(content: dict[str, Any]) -> dict[str, Any]:
    """Deterministic structural check for student_lesson_deck content.

    Returns a check dict stored in alignment_result["deck_structure_validation"].
    No AI calls. Violations escalate artifact_confidence to Needs Review.
    Warnings are advisory only.
    """
    slides = content.get("slides") or []
    violations: list[str] = []
    warnings: list[str] = []

    slide_count = len(slides) if isinstance(slides, list) else 0

    if slide_count < 5:
        violations.append(f"slide_count {slide_count} is below minimum of 5")
    elif slide_count > 10:
        warnings.append(
            f"slide_count {slide_count} exceeds recommended maximum of 8; consider splitting the deck"
        )

    # Exit ticket or check_in required
    _EXIT_TYPES = {"exit_ticket", "check_in"}
    has_exit = any(
        isinstance(s, dict) and (s.get("slide_type") or "").lower() in _EXIT_TYPES
        for s in (slides if isinstance(slides, list) else [])
    )
    if not has_exit:
        violations.append("no exit_ticket or check_in slide found — instructional contract requires one")

    # Per-slide checks
    _ENGAGEMENT_OPTIONAL = {"hook", "connection", "wrap_up", "share", "read_aloud", "title_full"}
    missing_type: list[str] = []
    missing_visual: list[str] = []
    long_body: list[str] = []
    long_bullets: list[str] = []
    missing_engagement: list[str] = []

    for i, s in enumerate(slides if isinstance(slides, list) else [], start=1):
        if not isinstance(s, dict):
            continue
        slide_id = str(s.get("id") or f"slide-{i}")

        if not (s.get("slide_type") or "").strip():
            missing_type.append(slide_id)

        visual = s.get("visual")
        if not isinstance(visual, dict) or not (visual.get("type") or "").strip():
            missing_visual.append(slide_id)

        body = s.get("body") or ""
        if body and len(body.split()) > 40:
            long_body.append(f"{slide_id} ({len(body.split())} words)")

        for bullet in (s.get("bullets") or []):
            if isinstance(bullet, str) and len(bullet.split()) > 15:
                long_bullets.append(f"{slide_id}: '{bullet[:40]}'")

        slide_type = (s.get("slide_type") or "").lower()
        if slide_type not in _ENGAGEMENT_OPTIONAL:
            eng = s.get("engagement")
            if not isinstance(eng, dict) or not (eng.get("type") or "").strip():
                missing_engagement.append(slide_id)

    if missing_type:
        violations.append(f"missing slide_type on: {', '.join(missing_type)}")
    if missing_visual:
        warnings.append(f"missing visual block on: {', '.join(missing_visual)}")
    if long_body:
        warnings.append(f"body exceeds 40 words: {'; '.join(long_body)}")
    if long_bullets:
        warnings.append(f"bullets exceed 15 words: {'; '.join(long_bullets[:3])}")
    if missing_engagement:
        warnings.append(f"no engagement on instructional slides: {', '.join(missing_engagement)}")

    status = "fail" if violations else "pass"
    return {
        "status": status,
        "slide_count": slide_count,
        "violations": violations,
        "warnings": warnings,
        "alignment_explanation": (
            f"Deck has {slide_count} slides. "
            + (f"{len(violations)} violation(s): {'; '.join(violations[:2])}. " if violations else "No structural violations. ")
            + (f"{len(warnings)} advisory warning(s)." if warnings else "No warnings.")
        ),
    }


def enforce_student_content_prohibitions(
    artifact_type: str,
    content: dict[str, Any],
    alignment_result: dict[str, Any],
) -> dict[str, Any]:
    """Strip all internal metadata from student-facing artifact content.

    Removes TEKS codes, district planning document names, and instructional
    metadata references from every string value in the content dict recursively.
    Mutates alignment_result["student_prohibitions_scan"] in place.
    Returns the cleaned content dict (a new dict, original unchanged).
    """
    if artifact_type not in _STUDENT_FACING_TYPES:
        return content

    counter = [0]
    cleaned = _walk_and_clean(content, _STUDENT_PROHIBITION_SUBS, counter)
    total_subs = counter[0]

    scan_status = "clean" if total_subs == 0 else ("needs_review" if total_subs > 3 else "cleaned")
    alignment_result["student_prohibitions_scan"] = {
        "substitutions_made": total_subs,
        "status": scan_status,
    }
    if total_subs > 3:
        if alignment_result.get("artifact_confidence") == "Ready":
            alignment_result["artifact_confidence"] = "Needs Review"
        alignment_result["contracts_failed"] = list(
            alignment_result.get("contracts_failed") or []
        ) + [
            f"student_prohibitions: {total_subs} internal metadata item(s) removed — "
            "review content for completeness"
        ]

    return cleaned


def _walk_and_clean(
    obj: Any,
    patterns: list[tuple[re.Pattern[str], str]],
    counter: list[int],
) -> Any:
    """Recursively walk a JSON-compatible object and apply substitutions to strings.
    counter[0] accumulates the total number of substitutions made.
    """
    if isinstance(obj, str):
        result = obj
        for pattern, replacement in patterns:
            new_result, n = pattern.subn(replacement, result)
            counter[0] += n
            result = new_result
        return result.strip()
    if isinstance(obj, dict):
        return {k: _walk_and_clean(v, patterns, counter) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_walk_and_clean(item, patterns, counter) for item in obj]
    return obj


# ── Phase 3b: Cross-Artifact Alignment Validation ────────────────────────────────

def validate_week_alignment(
    *,
    package_id: Any,
    week_num: int,
    artifacts: list[Any],
    plan: dict[str, Any],
    vocabulary_registry: dict[tuple[int, str], list[str]],
) -> dict[str, Any]:
    """Produce a cross-artifact alignment report for all artifacts in one week.

    Reads content_json from already-committed ORM artifact objects. No AI calls.
    Returns a week-level report dict to be accumulated into
    package.instructional_alignment_report_json["weeks"].

    Each check includes an alignment_explanation field describing WHY the check
    passed or failed, for root admin diagnostics.
    """
    # Index artifacts by type
    by_type: dict[str, list[Any]] = {}
    for a in artifacts:
        atype = getattr(a, "artifact_type", None) or ""
        by_type.setdefault(atype, []).append(a)

    # Derive subject names for this week from the plan
    subject_names: list[str] = []
    for week_entry in (plan.get("weeks") or []):
        if int(week_entry.get("sequence_number") or 0) == week_num:
            for ws in week_entry.get("subjects") or []:
                sn = (ws.get("subject_name") or "").strip()
                if sn and sn.lower() not in {x.lower() for x in subject_names}:
                    subject_names.append(sn)

    checks: dict[str, Any] = {
        "exit_ticket_stem_compliance": _check_exit_ticket_stem(
            by_type, plan, week_num, subject_names
        ),
        "rubric_criterion_alignment": _check_rubric_criterion(
            by_type, plan, week_num, subject_names
        ),
        "quiz_objective_coverage": _check_quiz_objectives(
            by_type, plan, week_num, subject_names
        ),
        "vocabulary_sequence": _check_vocabulary_sequence(
            by_type, vocabulary_registry, week_num
        ),
        "student_prohibition_scan": _check_student_prohibitions(by_type),
    }

    statuses = [c.get("status") for c in checks.values()]
    week_confidence = (
        "Ready" if all(s in ("pass", "skipped") for s in statuses) else "Needs Review"
    )

    return {
        "week": week_num,
        "week_alignment_confidence": week_confidence,
        "checks": checks,
    }


def _check_exit_ticket_stem(
    by_type: dict[str, list[Any]],
    plan: dict[str, Any],
    week_num: int,
    subject_names: list[str],
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
        get_plan_instructional_design_week,
    )

    daily_plans = by_type.get("daily_lesson_plan") or []
    if not daily_plans:
        return {
            "status": "skipped",
            "alignment_explanation": "No daily_lesson_plan artifacts found for this week.",
            "artifacts_checked": [],
            "violations": [],
        }

    expected_stems: dict[str, str] = {}
    for sn in subject_names:
        design_week = get_plan_instructional_design_week(plan, week_num, sn.lower()) or {}
        stem = (design_week.get("instructional_contracts") or {}).get("exit_ticket_stem")
        if stem:
            expected_stems[sn.lower()] = stem

    if not expected_stems:
        return {
            "status": "skipped",
            "alignment_explanation": (
                "No exit_ticket_stem defined in instructional_contracts for this week."
            ),
            "artifacts_checked": [str(getattr(a, "id", "")) for a in daily_plans],
            "violations": [],
        }

    first_stem = next(iter(expected_stems.values()))
    violations: list[str] = []
    artifacts_checked: list[str] = []

    for artifact in daily_plans:
        artifacts_checked.append(str(getattr(artifact, "id", "")))
        content_str = json.dumps(getattr(artifact, "content_json", {}) or {})
        if not any(_stem_in_text(stem, content_str) for stem in expected_stems.values()):
            day = getattr(artifact, "day_label", "") or "?"
            violations.append(f"{day}: exit_ticket_stem not found in lesson plan content")

    n = len(daily_plans)
    if not violations:
        return {
            "status": "pass",
            "alignment_explanation": (
                f"Exit ticket stem present in all {n} of {n} daily lesson plan(s). "
                f"Verified stem (first 60 chars): '{first_stem[:60]}'"
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }
    return {
        "status": "fail",
        "alignment_explanation": (
            f"Exit ticket stem missing or not enforced in {len(violations)} of {n} "
            f"daily lesson plan(s). Expected stem: '{first_stem[:60]}'. "
            "Contract enforcement should have injected it — check generation logs."
        ),
        "artifacts_checked": artifacts_checked,
        "violations": violations,
    }


def _check_rubric_criterion(
    by_type: dict[str, list[Any]],
    plan: dict[str, Any],
    week_num: int,
    subject_names: list[str],
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
        get_plan_instructional_design_week,
    )

    rubrics = by_type.get("rubric") or []
    if not rubrics:
        return {
            "status": "skipped",
            "alignment_explanation": "No rubric artifacts found for this week.",
            "artifacts_checked": [],
            "violations": [],
        }

    expected_criterion: str | None = None
    for sn in subject_names:
        design_week = get_plan_instructional_design_week(plan, week_num, sn.lower()) or {}
        crit = (design_week.get("instructional_contracts") or {}).get("rubric_primary_criterion")
        if crit:
            expected_criterion = crit
            break

    artifacts_checked = [str(getattr(a, "id", "")) for a in rubrics]

    if not expected_criterion:
        return {
            "status": "skipped",
            "alignment_explanation": (
                "No rubric_primary_criterion defined in instructional_contracts for this week."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }

    violations: list[str] = []
    for artifact in rubrics:
        content = getattr(artifact, "content_json", {}) or {}
        criteria = content.get("criteria") or []
        first_name = (
            (criteria[0].get("name") or "")
            if isinstance(criteria, list) and criteria and isinstance(criteria[0], dict)
            else ""
        )
        crit_lower = expected_criterion.strip().lower()
        name_lower = first_name.lower()
        if not (crit_lower in name_lower or name_lower in crit_lower):
            violations.append(
                f"Rubric {str(getattr(artifact, 'id', ''))[:8]}: "
                f"expected '{expected_criterion[:50]}', found '{first_name[:50]}'"
            )

    n = len(rubrics)
    if not violations:
        return {
            "status": "pass",
            "alignment_explanation": (
                f"All {n} rubric(s) have a primary criterion consistent with "
                f"instructional_contracts.rubric_primary_criterion "
                f"('{expected_criterion[:60]}'). Criterion alignment confirmed."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }
    return {
        "status": "fail",
        "alignment_explanation": (
            f"{len(violations)} of {n} rubric(s) have a primary criterion that does not match "
            f"instructional_contracts ('{expected_criterion[:60]}'). "
            "Rubric criterion enforcement is flag-only — teacher review is required."
        ),
        "artifacts_checked": artifacts_checked,
        "violations": violations,
    }


def _check_quiz_objectives(
    by_type: dict[str, list[Any]],
    plan: dict[str, Any],
    week_num: int,
    subject_names: list[str],
) -> dict[str, Any]:
    from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
        get_plan_instructional_design_week,
    )

    quizzes = by_type.get("quiz") or []
    if not quizzes:
        return {
            "status": "skipped",
            "alignment_explanation": "No quiz artifacts found for this week.",
            "artifacts_checked": [],
            "violations": [],
        }

    allowed_objectives: set[str] = set()
    for sn in subject_names:
        design_week = get_plan_instructional_design_week(plan, week_num, sn.lower()) or {}
        for obj_code in (design_week.get("instructional_contracts") or {}).get("quiz_objectives") or []:
            allowed_objectives.add(str(obj_code).strip())

    artifacts_checked = [str(getattr(a, "id", "")) for a in quizzes]

    if not allowed_objectives:
        return {
            "status": "skipped",
            "alignment_explanation": (
                "No quiz_objectives defined in instructional_contracts for this week."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }

    violations: list[str] = []
    for artifact in quizzes:
        content = getattr(artifact, "content_json", {}) or {}
        questions = content.get("questions") or []
        disallowed = [
            (q.get("objective_id") or "").strip()
            for q in questions
            if isinstance(q, dict)
            and (q.get("objective_id") or "").strip()
            and (q.get("objective_id") or "").strip() not in allowed_objectives
        ]
        if disallowed:
            violations.append(
                f"Quiz {str(getattr(artifact, 'id', ''))[:8]}: "
                f"{len(disallowed)} question(s) reference disallowed objective(s): "
                f"{disallowed[:3]}"
            )

    n = len(quizzes)
    if not violations:
        return {
            "status": "pass",
            "alignment_explanation": (
                f"All {n} quiz(es) have questions aligned to required quiz_objectives "
                f"({sorted(allowed_objectives)}). Contract enforcement verified."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }
    return {
        "status": "warning",
        "alignment_explanation": (
            f"{len(violations)} of {n} quiz(es) contain questions that reference objectives "
            f"outside the required set ({sorted(allowed_objectives)}). "
            "Contract enforcement during generation may have partially corrected this."
        ),
        "artifacts_checked": artifacts_checked,
        "violations": violations,
    }


def _check_vocabulary_sequence(
    by_type: dict[str, list[Any]],
    vocabulary_registry: dict[tuple[int, str], list[str]],
    week_num: int,
) -> dict[str, Any]:
    assessment_artifacts = [
        a
        for atype in ("quiz", "assignment", "writing_response")
        for a in (by_type.get(atype) or [])
    ]
    if not assessment_artifacts:
        return {
            "status": "skipped",
            "alignment_explanation": "No assessment artifacts to check vocabulary against.",
            "artifacts_checked": [],
            "violations": [],
        }

    all_vocab: set[str] = set()
    for (wk, _), terms in vocabulary_registry.items():
        if wk == week_num:
            all_vocab |= set(terms)

    if not all_vocab:
        return {
            "status": "skipped",
            "alignment_explanation": (
                "Vocabulary registry is empty for this week — vocabulary check skipped."
            ),
            "artifacts_checked": [],
            "violations": [],
        }

    artifacts_checked = [str(getattr(a, "id", "")) for a in assessment_artifacts]
    total_significant = 0
    total_in_registry = 0

    for artifact in assessment_artifacts:
        content_str = json.dumps(getattr(artifact, "content_json", {}) or {}).lower()
        words = re.findall(r'\b[a-z]{4,}\b', content_str)
        significant = {w for w in words if w not in _VOCAB_STOPWORDS}
        in_registry = {w for w in significant if w in all_vocab}
        total_significant += len(significant)
        total_in_registry += len(in_registry)

    compliance_fraction = (
        round(total_in_registry / total_significant, 2) if total_significant > 0 else 1.0
    )
    n = len(assessment_artifacts)

    if compliance_fraction >= 0.70:
        return {
            "status": "pass",
            "alignment_explanation": (
                f"Vocabulary compliance: {compliance_fraction:.0%} of significant terms "
                f"in {n} assessment artifact(s) appear in the introduced_vocabulary registry "
                f"({total_in_registry}/{total_significant} unique terms). "
                "Assessment vocabulary is consistent with what has been taught."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
            "compliance_fraction": compliance_fraction,
        }
    return {
        "status": "warning",
        "alignment_explanation": (
            f"Vocabulary compliance: {compliance_fraction:.0%} of significant terms in "
            f"{n} assessment artifact(s) appear in the introduced_vocabulary registry. "
            "A ratio below 70% may indicate new domain vocabulary introduced in assessments "
            "that was not yet taught. Note: the registry covers KDG prerequisite nodes and "
            "primary objective descriptions; lesson-level vocabulary from teacher_modeling "
            "is not currently included in the registry."
        ),
        "artifacts_checked": artifacts_checked,
        "violations": [
            f"compliance_fraction={compliance_fraction:.2f} is below the 0.70 threshold"
        ],
        "compliance_fraction": compliance_fraction,
    }


def _check_student_prohibitions(by_type: dict[str, list[Any]]) -> dict[str, Any]:
    student_artifacts = [
        a for atype in _STUDENT_FACING_TYPES for a in (by_type.get(atype) or [])
    ]
    if not student_artifacts:
        return {
            "status": "skipped",
            "alignment_explanation": "No student-facing artifacts found for this week.",
            "artifacts_checked": [],
            "violations": [],
        }

    violations: list[str] = []
    artifacts_checked = [str(getattr(a, "id", "")) for a in student_artifacts]

    for artifact in student_artifacts:
        content_str = json.dumps(getattr(artifact, "content_json", {}) or {})
        artifact_violations: list[str] = []
        for pattern, _ in _TEKS_DETECTION_PATTERNS:
            matches = pattern.findall(content_str)
            if matches:
                artifact_violations.extend(matches[:3])
        if artifact_violations:
            atype = getattr(artifact, "artifact_type", "")
            violations.append(
                f"{atype} {str(getattr(artifact, 'id', ''))[:8]}: "
                f"found {len(artifact_violations)} prohibited pattern(s): "
                f"{artifact_violations[:3]}"
            )

    n = len(student_artifacts)
    if not violations:
        return {
            "status": "pass",
            "alignment_explanation": (
                f"Scanned {n} student-facing artifact(s). "
                "No TEKS codes or district metadata detected in student-visible content. "
                "Student content prohibition enforcement was effective."
            ),
            "artifacts_checked": artifacts_checked,
            "violations": [],
        }
    return {
        "status": "fail",
        "alignment_explanation": (
            f"{len(violations)} of {n} student-facing artifact(s) contain internal metadata "
            "that survived enforcement. These artifacts contain TEKS codes or district metadata "
            "in student-visible content and should be reviewed."
        ),
        "artifacts_checked": artifacts_checked,
        "violations": violations,
    }
