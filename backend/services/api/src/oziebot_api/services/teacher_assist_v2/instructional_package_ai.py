"""OpenAI generation for TeacherAssist v2 instructional package artifacts."""

from __future__ import annotations

import logging
import re
import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_mode import is_teacher_assist_real_ai_active
from oziebot_api.services.teacher_assist.ai_usage import (
    assert_teacher_assist_ai_cost_available,
    record_teacher_assist_ai_usage,
)
from sqlalchemy import select as _sa_select

from oziebot_api.models.teacher_assist_v2_document_extraction import (
    TeacherAssistV2DocumentExtraction,
)
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import (
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
    V2_PACKAGE_ARTIFACT_FEATURES,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.instructional_design_plan import (
    get_plan_district_anchors,
    get_plan_for_all_subjects_on_day,
    get_plan_for_day,
    get_plan_instructional_design_week,
    resolve_verified_previous_learning,
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import resolve_pacing_day_plan
from oziebot_api.services.teacher_assist_v2.instructional_delivery_profile import (
    build_artifact_delivery_context,
    check_curriculum_text_access_violations,
)
from oziebot_api.services.teacher_assist_v2.instructional_variety import STRATEGY_LIBRARY
from oziebot_api.services.teacher_assist_v2.instructional_resource_bank import (
    build_instructional_resource_bank,
    check_resource_sufficiency,
    named_books_from_bank,
)

logger = logging.getLogger(__name__)

V2_PACKAGE_PROMPT_VERSION = "v2-instructional-package-v1"

_REAL_AI_PROVIDERS = frozenset({"openai", "gemini"})


def _provider_api_params(settings: Settings) -> tuple[str, str | None, str | None]:
    """Return (provider_name, api_key, base_url) for the currently configured AI provider."""
    provider = (settings.teacher_assist_ai_provider or "mock").strip().lower()
    if provider == "gemini":
        return (
            "gemini",
            (settings.teacher_assist_gemini_api_key or "").strip() or None,
            (settings.teacher_assist_gemini_base_url or "").strip()
            or "https://generativelanguage.googleapis.com/v1beta/openai",
        )
    return (
        "openai",
        None,  # falls back to settings.teacher_assist_openai_api_key inside client
        None,  # falls back to settings.teacher_assist_openai_base_url inside client
    )


DAILY_LESSON_PLAN_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "subjects": [
        {
            "subject_name": "string",
            # ── TEKS visibility (teacher-facing only) ─────────────────────────
            "teks_alignment": [{"code": "string", "description": "string"}],
            "learning_target": "string",  # student-facing LT — primary goal (no TEKS codes)
            "learning_goals": ["string"],  # 1–2 'I can...' statements — MAX 2 per block
            "curriculum_resource": "string",  # exact title of the selected resource
            "resource_rationale": "string",  # why this resource serves today's objective
            # ── Step 1–2: Activation → Preteach ──────────────────────────────
            "preteach": {
                "activate_prior_knowledge": "string",  # how to surface what students already know
                "vocabulary_preview": "string",  # 1–2 key terms with student-friendly defs
                "student_friendly_explanation": "string",  # concept explained without jargon
                "why_this_matters": "string",  # real-world relevance in student language
                "engagement_question": "string",  # quick question to spark curiosity
            },
            # ── Step 3: Learning Target ───────────────────────────────────────
            # (rendered from learning_target above — no separate field needed)
            # ── Step 4: Teacher Modeling ──────────────────────────────────────
            "teacher_modeling_script": {
                "teacher_says": "string",  # exact teacher script for the model
                "students_do": "string",  # what students do while teacher models
                "teacher_questions": ["string"],  # questions teacher asks mid-model
                "expected_student_thinking": "string",  # what correct thinking sounds like
                "common_misconceptions": [
                    "string"
                ],  # what students typically get wrong + how to redirect
            },
            # ── Steps 5–8: Practice → CFU → Closure ──────────────────────────
            "objective": "string",  # teacher-facing objective (may include TEKS ref)
            "mini_lesson": "string",  # retained: legacy field, still populated
            "teacher_actions": ["string"],
            "student_activity": ["string"],
            "materials": ["string"],
            "assessment": "string",
            "notes": "string",
            "direct_instruction": "string",
            "guided_practice": "string",
            "independent_practice": "string",
            "checks_for_understanding": ["string"],
            "closure": "string",
            # ── Step 9: Transition ────────────────────────────────────────────
            "transition": "string",  # how this block hands off to the next subject/strand
        }
    ],
}

SLIDE_DECK_SCHEMA: dict[str, Any] = {
    "title": "string",
    "slides": [
        {
            "title": "string",
            "layout_type": "string",
            "body": "string",
            "bullets": ["string"],
            "speaker_notes": "string",
            "discussion_question": "string",
            "comparison_pairs": [
                {
                    "label_before": "string",
                    "text_before": "string",
                    "label_after": "string",
                    "text_after": "string",
                    "explanation": "string",
                }
            ],
        }
    ],
}

STUDENT_LESSON_DECK_SCHEMA: dict[str, Any] = {
    "title": "string",
    "slides": [
        {
            "id": "string",
            "slide_type": "string",
            "layout": "string",
            "title": "string",
            "body": "string",
            "bullets": ["string"],
            "student_emotion": "string",
            "visual_learning_goal": "string",
            "teacher_notes": "string",
            # Strand metadata — populated when instructional_delivery_context is present.
            # strand_separator slides use slide_type="strand_separator" and no engagement/visual.
            "strand_name": "string or null",
            "strand_minutes": "number or null",
            "instructional_block_order": "number or null",
            "engagement": {
                "type": "string",
                "prompt": "string",
            },
            "visual": {
                "type": "string",
                "placement": "string",
                "fallback_organizer_type": "string",
                "image_search": {
                    "search_terms": ["string"],
                    "educational_purpose": "string",
                    "target_grade_band": "string",
                    "preferred_image_type": "string",
                    "image_alt_text": "string",
                    "image_rationale": "string",
                    "instructional_activity": "string",
                },
                "organizer_data": {
                    "type": "string",
                },
            },
        }
    ],
}

GENERIC_ARTIFACT_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

QUIZ_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "questions": [{"prompt": "string", "objective_id": "string"}],
    "answer_key": [{"prompt": "string", "answer": "string"}],
    "objective_mapping": [{"objective_id": "string", "question_prompt": "string"}],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

RUBRIC_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "criteria": [
        {
            "name": "string",
            "point_value": "number",
            "performance_levels": [
                {"label": "string", "description": "string", "points": "number"}
            ],
        }
    ],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

NEWSLETTER_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "what_students_will_learn": ["string"],
    "reminders": ["string"],
    "upcoming_focus": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

ASSIGNMENT_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "objective_alignment": "string",
    "passage_title": "string",
    "passage_text": "string",
    "student_instructions": ["string"],
    "questions": [{"prompt": "string", "type": "string"}],
    "success_criteria": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

WRITING_RESPONSE_SCHEMA: dict[str, Any] = {
    "title": "string",
    "summary": "string",
    "objective_alignment": "string",
    "writing_prompt": "string",
    "student_instructions": ["string"],
    "sentence_starters": ["string"],
    "success_criteria": ["string"],
    "sections": [{"heading": "string", "body": "string", "bullets": ["string"]}],
}

ARTIFACT_SCHEMAS: dict[str, dict[str, Any]] = {
    "daily_lesson_plan": DAILY_LESSON_PLAN_SCHEMA,
    "subject_slide_deck": SLIDE_DECK_SCHEMA,
    "student_lesson_deck": STUDENT_LESSON_DECK_SCHEMA,
    "quiz": QUIZ_SCHEMA,
    "rubric": RUBRIC_SCHEMA,
    "assignment": ASSIGNMENT_SCHEMA,
    "writing_response": WRITING_RESPONSE_SCHEMA,
    "parent_newsletter_summary": NEWSLETTER_SCHEMA,
}


def _schema_for_artifact(artifact_type: str) -> dict[str, Any]:
    return ARTIFACT_SCHEMAS.get(artifact_type, GENERIC_ARTIFACT_SCHEMA)


_CURRICULUM_DOCUMENT_DIRECTIVE = (
    "CURRICULUM DOCUMENTS — PRIMARY SOURCE FOR HOW TO TEACH:\n"
    "district_document_context.used_documents contains extracted text from curriculum files uploaded by the "
    "district admin. Each entry has an 'excerpt' field with the actual curriculum content. "
    "READ THESE EXCERPTS — they are the authoritative source for:\n"
    "  - Which books, passages, or texts to use for instruction\n"
    "  - The recommended instructional sequence, pacing, and activity formats\n"
    "  - Assignment types, writing prompts, and assessment formats the district expects\n"
    "  - Vocabulary lists, skill progressions, and concept groupings tied to each TEKS\n"
    "Your lesson MUST reflect the approach in these documents. If a document names specific books or "
    "activities, use them. Do NOT default to generic lesson structures when curriculum documents exist. "
    "If no documents are uploaded or none are extracted yet, derive approach from resolved_objectives alone."
)

_OBJECTIVE_ALIGNMENT_DIRECTIVE = (
    "TEKS OBJECTIVES — COVERAGE AND PRIORITY:\n"
    "resolved_objectives lists every TEKS for this guide. Each has is_required (true or false).\n"
    "REQUIRED TEKS (is_required=true): Must be fully taught, practiced, and assessed every week they apply. "
    "Every lesson must explicitly address at least one required TEKS. Students must have multiple exposures "
    "across the duration — introduce, practice deeply, apply, and assess each one.\n"
    "OPTIONAL TEKS (is_required=false): Must be taught — exposure matters even when depth is limited. "
    "Introduce them after required TEKS are addressed in a lesson or week. Allocate roughly 80% of "
    "instructional time to required TEKS and 20% to optional ones. Never skip optional TEKS entirely — "
    "brief but intentional contact with enrichment content still benefits students.\n\n"
    "TEKS DISTRIBUTION ACROSS THE FULL DURATION:\n"
    "Use total_guide_weeks and current_week_number to determine what to foreground this week. "
    "Progress deliberately — do not repeat the same TEKS content each week:\n"
    "  - Week 1 of N: Introduce ALL required TEKS explicitly — vocabulary, first exposure, anchor lesson\n"
    "  - Middle weeks: One or two required TEKS per week, deep practice, increasing complexity\n"
    "  - Final week: Synthesize ALL required TEKS across a culminating task; optional TEKS as extension\n\n"
    "When resolved_daily_topics is non-empty, structure each day around those topics. "
    "When resolved_daily_topics is empty, YOU determine the daily sequence from the curriculum documents "
    "and TEKS — each day must be a clear step forward, not a repetition of the day before. "
    "Incorporate resolved_assessment_checks where provided.\n\n"
    "LEARNING ARC — WEEK POSITION:\n"
    "Week 1 of N: activate prior knowledge, establish vocabulary, introduce required TEKS explicitly. "
    "Middle weeks: build depth, increase student independence, layer complexity on required TEKS. "
    "Final week of N: consolidate all required TEKS, assess mastery, add optional TEKS as extension. "
    "Every lesson must feel like a purposeful step in a coherent multi-week progression — not standalone."
)

_CLASS_TIME_DIRECTIVE = (
    "CLASS TIME CONSTRAINTS — every activity must fit within the available period:\n"
    "- ELA Reading block: 35 minutes. Roughly: 5 min hook, 10 min direct instruction, "
    "10 min guided practice, 8 min independent practice, 2 min closure.\n"
    "- ELA Writing block: 30 minutes. Roughly: 3 min hook, 8 min mini-lesson/modeling, "
    "15 min independent writing, 4 min share/closure.\n"
    "- All other subjects: 40 minutes. Roughly: 5 min bell ringer, 10 min direct instruction, "
    "15 min guided/collaborative practice, 8 min independent practice, 2 min exit ticket.\n"
    "Do not plan more content than can be delivered in the available time."
)

_WEEK_PLAN_DIRECTIVE = (
    "WEEK CURRICULUM PLAN — PEDAGOGICAL SEQUENCE (highest priority):\n"
    "week_curriculum_plan is a pre-generated teaching plan created by analyzing the full curriculum. "
    "It defines THIS week's specific theme, reading_focus, writing_focus, primary_teks, mentor_texts, "
    "key_activities, and how this week builds on the previous week and prepares for the next.\n"
    "FOLLOW THIS PLAN. Do not invent a different focus or re-sequence the content. "
    "The plan was designed so that every week builds progressively toward the unit's goals:\n"
    "  - Use the week's theme as the organizing idea for all content this week\n"
    "  - Center reading activities on reading_focus; center writing on writing_focus\n"
    "  - Prioritize primary_teks; treat secondary_teks as supporting but not primary\n"
    "  - Reference mentor_texts by name; use key_activities as the basis for lessons\n"
    "  - Acknowledge builds_on (what students learned last week) and prepares_for (what comes next)\n"
    "If week_curriculum_plan is null or missing, fall back to resolved_objectives and curriculum documents."
)

_BOOK_GROUNDING_DIRECTIVE = (
    "INSTRUCTIONAL RESOURCE — AUTHORITATIVE SELECTION:\n"
    "selected_instructional_resource (when present) is the district-assigned resource for this specific "
    "lesson, deterministically selected from the instructional resource bank. Treat it as authoritative:\n"
    "  - Use this resource. Do NOT substitute a different title, author, or material.\n"
    "  - Reference it by its EXACT title field — never say 'a book' or 'a text' when a title is provided.\n"
    "  - When it is a text/book: draw on it specifically — character names, events, vocabulary, themes — "
    "content specific enough that a student who read it recognizes it.\n"
    "  - When it is a non-text resource (anchor_chart, graphic_organizer, video, diagram, manipulative): "
    "reference it by name and describe how students will interact with it during the lesson.\n"
    "  - If resource_adaptation_note is present: the selected resource may not be the ideal type for "
    "this lesson, but you MUST still use it as the instructional anchor. Follow the adaptation_note "
    "guidance rather than substituting generic placeholder language.\n"
    "  - If selected_instructional_resource is null: derive from named_books_and_texts, "
    "week_curriculum_plan.mentor_texts, district_document_context, or resolved_day_plan.materials_needed. "
    "NEVER invent a resource not present in any curriculum field.\n\n"
    "STRAND-SPECIFIC RESOURCES (strand_instructional_resources field):\n"
    "When strand_instructional_resources is present and non-empty, each strand has its own resource.\n"
    "  - Reading strand: use strand_instructional_resources['Reading'] as the primary text\n"
    "  - Writing strand: use strand_instructional_resources['Writing'] as the mentor text\n"
    "  - Other strands: follow the same pattern using their key in strand_instructional_resources\n"
    "When a strand has no entry in strand_instructional_resources, fall back to selected_instructional_resource.\n\n"
    "NAMED RESOURCE POOL (secondary reference only — use selected_instructional_resource first):\n"
    "named_books_and_texts lists all curriculum-assigned texts for the current week. "
    "You may reference additional texts from this list to supplement the selected resource, "
    "but the selected resource must remain the primary instructional anchor.\n\n"
    "PLACEHOLDER LANGUAGE IS A CURRICULUM FIDELITY FAILURE — see _CURRICULUM_FIDELITY_DIRECTIVE."
)


_CURRICULUM_FIDELITY_DIRECTIVE = (
    "\n\nCURRICULUM RESOURCE ALIGNMENT — AUTHORITATIVE RULES:\n"
    "The district curriculum has already selected the instructional resource for this lesson.\n"
    "TeacherAssist enriches instruction AROUND the selected resource. It never replaces it.\n\n"
    "RESOURCE FIELDS IN PROMPT PAYLOAD:\n"
    "  selected_instructional_resource — the pre-committed, district-authorized resource for today.\n"
    "    .title        — use this exact name in all activities\n"
    "    .resource_type — read_aloud | mentor_text | shared_reading | passage | poem | article | ...\n"
    "    .theme        — the weekly instructional theme (reference this in framing and discussion)\n"
    "    .topic        — the strand-level reading/writing focus\n"
    "    .allowed_uses — the list of classroom activities valid for this resource type.\n"
    "                    ONLY generate activities that appear in this list.\n"
    "  named_books_and_texts — secondary pool; all curriculum texts for this week.\n\n"
    "ALLOWED_USES ENFORCEMENT:\n"
    "  selected_instructional_resource.allowed_uses tells you HOW the resource can be used.\n"
    "  If allowed_uses includes 'teacher_read_aloud' but NOT 'independent_reading',\n"
    "  you MUST NOT tell students to 'read [title] independently'.\n"
    "  Design ONLY the activities listed in allowed_uses for this resource.\n"
    "  If allowed_uses is empty or absent: use best judgment based on resource_type.\n\n"
    "PLANNING RULE — match resource to objective:\n"
    "  The question is 'What resource best supports today's objective?' — not 'What book?'\n"
    "  The same resource may support multiple objectives across multiple days. Do NOT rotate\n"
    "  resources for variety. Use the pre-committed resource for the day.\n\n"
    "FORBIDDEN PLACEHOLDER PHRASES (any variation = generation failure):\n"
    "  'choose a story', 'choose a chapter', 'choose a text', 'choose a book',\n"
    "  'pick a book', 'pick a story', 'pick a text',\n"
    "  'select an article', 'select a passage', 'select a story',\n"
    "  'find a poem', 'find a passage', 'read any text', 'read any book', 'read any story',\n"
    "  'any article', 'any text', 'any story', 'a text of your choice', 'a book of your choice'\n\n"
    "REQUIRED BEHAVIOR when a curriculum resource exists:\n"
    "  Independent practice: 'Read pages ___ in [EXACT TITLE]' — not 'Read a chapter.'\n"
    "  Analysis activities: 'Find evidence in [EXACT TITLE]' — not 'Choose a text.'\n"
    "  Writing responses: 'Using [EXACT TITLE] as evidence' — not 'Using any story.'\n"
    "  Student slides: Name the book, chapter, or passage directly.\n\n"
    "Only omit the resource name if: (1) the curriculum explicitly provides student choice, OR\n"
    "(2) selected_instructional_resource and named_books_and_texts are both null/empty."
)


_CURRICULUM_FIDELITY_RETRY_ADDENDUM = (
    "\n\nCRITICAL REGENERATION ALERT — PLACEHOLDER LANGUAGE WAS DETECTED IN PREVIOUS OUTPUT:\n"
    "The previous generation contained generic placeholder language such as 'choose a story', "
    "'pick a book', or 'select a chapter'. This is a curriculum fidelity failure.\n"
    "selected_instructional_resource and named_books_and_texts contain the district-assigned texts. "
    "You MUST reference them by exact title in every activity that involves reading, analyzing, "
    "or responding to a text. Do NOT produce any placeholder language in this regeneration. "
    "Every independent practice, guided activity, and student prompt must name the specific book, "
    "passage, or material provided — never a generic stand-in."
)


_PLACEHOLDER_PATTERNS: list[re.Pattern[str]] = [
    re.compile(
        r"\b(choose|pick)\s+(a\s+)?(short\s+)?"
        r"(story|book|chapter|text|passage|poem|article|novel|event|chapter\s+book)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bselect\s+(a|an)\s+(story|book|chapter|text|passage|poem|article|novel|event)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bfind\s+(a|an)\s+(story|book|chapter|text|passage|poem|article|event)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bread\s+(any|a)\s+(short\s+)?(story|book|chapter|text|passage|poem|article|novel)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bany\s+(story|article|text|book|passage|poem|novel)\b", re.IGNORECASE),
    re.compile(
        r"\b(a|some)\s+(text|story|passage|book|article|poem|novel)\s+of\s+(your|their)\s+choice\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bchoose\s+a\s+specific\s+\w+\b", re.IGNORECASE),
]


def _count_ican_statements(text: str) -> int:
    """Count the number of 'I can' statements in a string."""
    import re

    return len(re.findall(r"\bI can\b", text, re.IGNORECASE))


def _goals_look_like_writing(goals: list[str]) -> bool:
    """Return True if any goal contains writing-composition keywords."""
    writing_keywords = (
        "write",
        "writing",
        "sentence",
        "paragraph",
        "compose",
        "draft",
        "revise",
        "edit",
        "respond in writing",
        "written response",
    )
    combined = " ".join(goals).lower()
    return any(kw in combined for kw in writing_keywords)


def _goals_look_like_reading(goals: list[str]) -> bool:
    """Return True if any goal contains reading-comprehension keywords."""
    reading_keywords = (
        "read",
        "reading",
        "identify",
        "main idea",
        "detail",
        "text evidence",
        "comprehend",
        "analyze",
        "infer",
        "summarize",
        "character",
        "theme",
        "compare",
        "author",
        "literary",
    )
    combined = " ".join(goals).lower()
    return any(kw in combined for kw in reading_keywords)


def _check_lesson_plan_completeness(content: Any) -> list[str]:
    """Check that a daily_lesson_plan has all required rhythm steps populated.

    Returns a list of missing-field descriptions. Empty list means complete.
    Checks each subject block for the 9-step rhythm fields and learning goal discipline.
    """
    missing: list[str] = []
    subjects = content.get("subjects") if isinstance(content, dict) else None
    if not subjects:
        return ["no subject blocks found"]

    # ── Cross-block: Reading/Writing separation analysis ─────────────────────
    reading_blocks = [b for b in subjects if "reading" in (b.get("subject_name") or "").lower()]
    writing_blocks = [b for b in subjects if "writing" in (b.get("subject_name") or "").lower()]

    for i, block in enumerate(subjects):
        label = block.get("subject_name") or f"block {i + 1}"
        label_lower = label.lower()
        preteach = block.get("preteach") or {}
        modeling = block.get("teacher_modeling_script") or {}

        # TEKS visibility
        if not (block.get("teks_alignment") or block.get("objective")):
            missing.append(f"{label}: missing teks_alignment")
        if not block.get("learning_target"):
            missing.append(f"{label}: missing learning_target")
        if not block.get("curriculum_resource"):
            missing.append(f"{label}: missing curriculum_resource")
        if not block.get("resource_rationale"):
            missing.append(f"{label}: missing resource_rationale")

        # ── Learning goal discipline checks ──────────────────────────────────
        raw_goals = block.get("learning_goals")
        goals_list: list[str] = []
        if isinstance(raw_goals, list):
            goals_list = [str(g) for g in raw_goals if g]
        elif not raw_goals and block.get("learning_target"):
            # Fallback: count "I can" occurrences in learning_target string
            target_str = str(block.get("learning_target") or "")
            if _count_ican_statements(target_str) > 2:
                missing.append(
                    f"{label}: learning_target contains more than 2 'I can' statements — "
                    "use learning_goals array with max 2 entries"
                )

        if goals_list:
            if len(goals_list) > 2:
                missing.append(
                    f"{label}: learning_goals has {len(goals_list)} entries — maximum is 2 per block"
                )
            if len(goals_list) == 0:
                missing.append(
                    f"{label}: learning_goals is empty — must have 1–2 'I can' statements"
                )

            # Reading block must not contain writing goals
            if "reading" in label_lower and _goals_look_like_writing(goals_list):
                missing.append(
                    f"{label}: Reading block learning_goals appear to include writing/composition goals — "
                    "Reading goals must focus on comprehension; Writing goals belong in the Writing block"
                )

            # Writing block must not contain only reading goals (must have at least one composition goal)
            if "writing" in label_lower and not _goals_look_like_writing(goals_list):
                missing.append(
                    f"{label}: Writing block learning_goals do not appear to include a writing/composition goal — "
                    "Writing blocks must have at least one 'I can write/compose/draft...' goal"
                )

        # ── Closure alignment check ───────────────────────────────────────────
        closure = (block.get("closure") or "").lower()
        if goals_list and closure:
            # Check whether the closure references the subject domain of the block's goals
            if "reading" in label_lower:
                # Reading closure should NOT be about writing/composition
                if _goals_look_like_writing([closure]) and not _goals_look_like_reading([closure]):
                    missing.append(
                        f"{label}: Reading closure appears to assess writing skill rather than the Reading learning goal"
                    )
            if "writing" in label_lower:
                # Writing closure should NOT be purely about reading comprehension
                if _goals_look_like_reading([closure]) and not _goals_look_like_writing([closure]):
                    missing.append(
                        f"{label}: Writing closure appears to assess reading comprehension rather than the Writing learning goal"
                    )

        # Preteach steps
        for field in (
            "activate_prior_knowledge",
            "vocabulary_preview",
            "student_friendly_explanation",
            "why_this_matters",
            "engagement_question",
        ):
            if not (preteach.get(field) or "").strip():
                missing.append(f"{label}: preteach.{field} is empty")

        # Teacher modeling
        for field in ("teacher_says", "students_do", "expected_student_thinking"):
            if not (modeling.get(field) or "").strip():
                missing.append(f"{label}: teacher_modeling_script.{field} is empty")
        if not (modeling.get("teacher_questions") or []):
            missing.append(f"{label}: teacher_modeling_script.teacher_questions is empty")
        if not (modeling.get("common_misconceptions") or []):
            missing.append(f"{label}: teacher_modeling_script.common_misconceptions is empty")

        # CFU
        cfu = block.get("checks_for_understanding") or []
        if len(cfu) < 3:
            missing.append(f"{label}: checks_for_understanding needs 3 questions (has {len(cfu)})")

        # Closure and transition
        if not (block.get("closure") or "").strip():
            missing.append(f"{label}: closure is empty")
        if not (block.get("transition") or "").strip():
            missing.append(f"{label}: transition is empty")

    # ── Cross-block: detect merged goals between Reading and Writing ─────────
    if reading_blocks and writing_blocks:
        for r_block in reading_blocks:
            r_goals = r_block.get("learning_goals") or []
            if isinstance(r_goals, list):
                r_goals = [str(g) for g in r_goals]
            for w_block in writing_blocks:
                w_goals = w_block.get("learning_goals") or []
                if isinstance(w_goals, list):
                    w_goals = [str(g) for g in w_goals]
                # Detect if the same goal text appears in both blocks
                r_set = {g.strip().lower() for g in r_goals}
                w_set = {g.strip().lower() for g in w_goals}
                overlap = r_set & w_set
                if overlap:
                    missing.append(
                        "Reading and Writing blocks share identical learning goals — "
                        "each block must have its own distinct goals"
                    )
                    break

    # ── Validate Writing block exists when lesson spans both Reading+Writing ──
    # When the lesson has reading blocks but no writing blocks, check whether the
    # reading blocks have absorbed writing goals (a common merge failure mode).
    if reading_blocks and not writing_blocks:
        all_r_goals = []
        for rb in reading_blocks:
            rg = rb.get("learning_goals") or []
            if isinstance(rg, list):
                all_r_goals.extend(str(g) for g in rg)
        if _goals_look_like_writing(all_r_goals):
            missing.append(
                "Reading block(s) appear to include Writing goals but no Writing block exists — "
                "Writing should have its own block with its own goals"
            )

    return missing


def _check_student_deck_completeness(content: Any) -> list[str]:
    """Check that a student_lesson_deck has the required preteach slide."""
    missing: list[str] = []
    slides = (content or {}).get("slides") or []
    if not slides:
        return ["no slides found"]
    slide_types = [s.get("slide_type", "") for s in slides]
    if "preteach" not in slide_types:
        missing.append("missing preteach slide (slide_type='preteach' required as slide 2)")
    return missing


def _has_placeholder_language(content: Any) -> bool:
    """Recursively scan all string values in the artifact JSON for forbidden placeholder patterns."""
    if isinstance(content, str):
        return any(p.search(content) for p in _PLACEHOLDER_PATTERNS)
    if isinstance(content, dict):
        return any(_has_placeholder_language(v) for v in content.values())
    if isinstance(content, list):
        return any(_has_placeholder_language(item) for item in content)
    return False


_LEARNING_GOAL_DISCIPLINE_DIRECTIVE = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "LEARNING GOAL DISCIPLINE — NON-NEGOTIABLE (enforced by validation)\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    "RULE 1 — MAXIMUM 2 LEARNING GOALS PER BLOCK:\n"
    "  Each subject block (Reading, Writing, Math, etc.) may have at most 2 'I can...' statements.\n"
    "  Do NOT write 3 or more goals in a single block. If a block has one clear objective, write 1 goal.\n"
    "  If a block serves two closely related skills, write exactly 2 goals.\n"
    "  NEVER combine 3+ skills into a single block's goals — split into separate blocks instead.\n\n"
    "  OUTPUT: populate learning_goals as a JSON array of 1–2 strings (never 0, never 3+).\n"
    "  Also populate learning_target (the first/primary goal, or a one-sentence summary).\n\n"
    "  Example — Reading block:\n"
    "    learning_goals: [\n"
    "      'I can identify the main idea of the read-aloud text.',\n"
    "      'I can support my thinking with one detail from the text.'\n"
    "    ]\n\n"
    "  Example — Writing block:\n"
    "    learning_goals: [\n"
    "      'I can write a response using a complete sentence.',\n"
    "      'I can include one detail from the text in my response.'\n"
    "    ]\n\n"
    "RULE 2 — READING AND WRITING MUST HAVE SEPARATE GOALS:\n"
    "  Reading goals → about comprehension, analysis, evidence, literary elements.\n"
    "  Writing goals → about composing, crafting, structuring, producing written text.\n"
    "  NEVER merge Reading and Writing goals into one shared list.\n"
    "  If the lesson has both Reading and Writing, each MUST have its own independent learning_goals.\n"
    "  WRONG: Reading block lists both a comprehension goal AND a writing goal.\n"
    "  RIGHT: Reading block lists comprehension goals. Writing block lists writing composition goals.\n\n"
    "RULE 3 — WRITING HAS ITS OWN LEARNING JOURNEY:\n"
    "  Writing is NOT merely a follow-up activity to Reading unless the curriculum explicitly connects them.\n"
    "  Writing may reference the Reading text as a source of ideas — but the WRITING skill being taught\n"
    "  must be independent: 'I can write a paragraph with a topic sentence' is a Writing goal,\n"
    "  regardless of what the content source is.\n"
    "  Do not treat Writing as 'respond to what you read.' Writing is a skill strand with its own arc.\n\n"
    "RULE 4 — ALIGNMENT: EVERY BLOCK COMPONENT MUST ALIGN TO THAT BLOCK'S GOALS:\n"
    "  The following must all point to the SAME learning goals for the block:\n"
    "    → teacher modeling (models the goal skill)\n"
    "    → guided practice (practices the goal skill)\n"
    "    → independent practice (applies the goal skill independently)\n"
    "    → checks_for_understanding (assesses the goal skill at each phase)\n"
    "    → closure / exit ticket (final check of the goal skill)\n\n"
    "  If the Reading goal is 'identify main idea' → the Reading exit ticket must assess main idea.\n"
    "  If the Writing goal is 'write a response with evidence' → the Writing closure must assess that.\n"
    "  DO NOT use a combined exit ticket that mixes Reading and Writing objectives unless\n"
    "    the curriculum explicitly designs a cross-strand culminating task.\n\n"
    "RULE 5 — EXIT TICKET / CLOSURE IS BLOCK-SPECIFIC:\n"
    "  Each block's closure field must directly reflect that block's learning_goals.\n"
    "  A Reading block closure asks students to demonstrate Reading understanding.\n"
    "  A Writing block closure asks students to demonstrate Writing skill.\n"
    "  Combined closures that blend unrelated objectives are FORBIDDEN unless explicitly configured.\n\n"
    "VALIDATION NOTE: The system checks these rules after generation and marks the artifact\n"
    "'needs_review' if any block has >2 goals, if Writing has no goal, if goals are merged,\n"
    "or if the closure/exit ticket does not reference the block's learning goals.\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
)

_CLASSROOM_AUTHENTICITY_DIRECTIVE = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "CLASSROOM AUTHENTICITY — THE 9-STEP INSTRUCTIONAL RHYTHM\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "Every instructional subject block in the daily lesson plan MUST include all 9 steps.\n"
    "A teacher should be able to say 'I could teach this tomorrow' without editing.\n\n"
    "STEP 1 — ACTIVATION (preteach.activate_prior_knowledge):\n"
    "  Surface what students already know about today's concept or skill.\n"
    "  Derive from knowledge_dependency_graph.activation_strategy for 'assumed_mastered' entries.\n"
    "  Must be SPECIFIC to the curriculum: name the book, concept, or prior lesson — never generic.\n"
    "  Examples: 'Think about when Esperanza arrived at the labor camp — what did she feel?' NOT 'Think about a time...'\n\n"
    "STEP 2 — PRETEACH (preteach fields — all five must be populated):\n"
    "  activate_prior_knowledge — see Step 1.\n"
    "  vocabulary_preview — 1–2 key terms with student-friendly definitions (no TEKS jargon).\n"
    "    Derive from knowledge_dependency_graph.introduced_vocabulary for 'develop_this_week' entries.\n"
    "  student_friendly_explanation — explain the concept in one sentence a 10-year-old understands.\n"
    "  why_this_matters — real-world relevance in student language. Why do readers/writers need this?\n"
    "  engagement_question — one quick question that sparks curiosity before instruction begins.\n\n"
    "STEP 3 — LEARNING TARGET (learning_target field):\n"
    "  Student-facing 'I can...' statement. NO TEKS codes. NO internal planning language.\n"
    "  Derived from instructional_design_day.student_goal — use verbatim when available.\n\n"
    "STEP 4 — TEACHER MODELING (teacher_modeling_script — all five sub-fields required):\n"
    "  teacher_says: The exact script a teacher would say while modeling. First person. "
    "Specific to the curriculum text. 4–6 sentences minimum.\n"
    "  students_do: What students do while the teacher models — NOT passive listening.\n"
    "  teacher_questions: 2–3 specific questions asked during modeling (think-alouds).\n"
    "    Probe the skill, not just the content. 'What is the author doing here?' not 'What happened?'\n"
    "  expected_student_thinking: What correct student reasoning sounds like.\n"
    "    Write it as a student sentence: 'The character changed because...'\n"
    "  common_misconceptions: The 1–2 most likely errors + the teacher redirect for each.\n"
    "    Format: 'Students may say X — redirect by Y.'\n\n"
    "STEP 5 — GUIDED PRACTICE (guided_practice field):\n"
    "  Collaborative application of what was just modeled. Named activity with specific steps.\n"
    "  Teacher and students work together — not just 'do the same thing independently.'\n"
    "  Name the exact passage, character, or concept from the curriculum resource.\n\n"
    "STEP 6 — INDEPENDENT PRACTICE (independent_practice field):\n"
    "  The exact task students complete on their own. Write student-facing instructions.\n"
    "  Must be achievable in the available time block. Must reference the curriculum resource.\n"
    "  Respect curriculum_text_access — if teacher_copy_only, IP is notebook-based, NOT text-based.\n\n"
    "STEP 7 — CHECK FOR UNDERSTANDING (checks_for_understanding — 3 questions required):\n"
    "  Question 1: after modeling (assesses understanding of the concept)\n"
    "  Question 2: mid-guided-practice (assesses application)\n"
    "  Question 3: before closure (assesses transfer — 'can students apply this on their own?')\n"
    "  Derived from instructional_design_day.formative_assessment when available.\n\n"
    "STEP 8 — CLOSURE (closure field):\n"
    "  Specific exit prompt or reflection tied to today's learning target.\n"
    "  Must reflect instructional_contracts.exit_ticket_stem if present.\n"
    "  Never generic ('Share one thing you learned today'). Always curriculum-specific.\n\n"
    "STEP 9 — TRANSITION (transition field):\n"
    "  How the class moves from this block to the next subject or strand.\n"
    "  Should be brief (1–2 sentences) and anticipate what comes next in the day.\n"
    "  If Reading ends before Writing begins: explicitly signal the shift.\n\n"
    "CURRICULUM RESOURCE VISIBILITY (teacher-facing lesson plan only):\n"
    "  curriculum_resource: exact title of the selected instructional resource.\n"
    "  resource_rationale: 1–2 sentences explaining WHY this specific resource supports today's TEKS.\n"
    "    Draw from the resource's theme, topic, and allowed_uses.\n"
    "    Example: 'The Crane Girl's multi-generational plot provides rich evidence for analyzing how\n"
    "    character relationships develop over time — directly aligned to TEKS 5.8A.'\n\n"
    "AUTHENTICITY TEST — before finalizing each subject block, verify:\n"
    "  ✓ A real teacher reading this could deliver the lesson verbatim\n"
    "  ✓ Every step names a specific book, character, concept, or passage — no generic filler\n"
    "  ✓ teacher_modeling_script reads as a natural teaching script, not a list of instructions\n"
    "  ✓ preteach is curriculum-specific, not a generic 'hook'\n"
    "  ✓ Reading block is complete before Writing block begins\n"
    "  ✓ learning_goals has 1–2 entries (never 3+) and reflects THIS block's skill only\n"
    "  ✓ Reading learning_goals are about comprehension — NOT about writing or composition\n"
    "  ✓ Writing learning_goals are about composing/crafting — NOT about reading comprehension\n"
    "  ✓ closure/exit ticket asks students to demonstrate THIS block's learning goals — nothing else\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
)


def _build_variety_directive(variety_context: dict[str, Any]) -> str:
    """Return the instructional variety directive for injection into a prompt.

    Only called when a variety_context with meaningful history is present.
    """
    _strategy_library_summary = "\n".join(
        f"  {meta['display']}: {meta['description']} (best for: {', '.join(meta['best_for'])})"
        for meta in STRATEGY_LIBRARY.values()
    )
    recent_display = ", ".join(variety_context.get("recent_strategies") or []) or "none yet"
    guidance = variety_context.get("guidance") or "Vary strategies based on today's objective."
    layout_note = ""
    recent_layouts = variety_context.get("recent_slide_layouts") or []
    if recent_layouts:
        layout_note = f"\nSlide layouts recently used: {', '.join(recent_layouts[-4:])}. Vary layout selection."

    return (
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "INSTRUCTIONAL VARIETY — SELECT BY OBJECTIVE, NOT HABIT\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "This package already includes these strategies (from prior lessons):\n"
        f"  {recent_display}\n\n"
        f"VARIETY GUIDANCE: {guidance}\n"
        f"{layout_note}\n\n"
        "STRATEGY LIBRARY — choose the strategy that best serves today's objective:\n"
        f"{_strategy_library_summary}\n\n"
        "SELECTION RULES:\n"
        "  1. Match strategy to objective. If today's goal is 'compare two characters', "
        "a Graphic Organizer or Think-Pair-Share serves better than a Quick Write.\n"
        "  2. Avoid consecutive same-strategy days. If Turn and Talk was used yesterday, "
        "choose a different engagement structure today.\n"
        "  3. Within a single lesson, vary engagement across subject blocks. "
        "If Reading uses Turn and Talk, Writing should use a different form.\n"
        "  4. Closures should reflect the objective — not default to 'share one thing you learned.' "
        "Anchor Chart works for skill closure; Notebook Reflection for synthesis; Quick Write for written response.\n"
        "  5. Hooks (activate_prior_knowledge) should vary in form: "
        "sometimes a question, sometimes a prediction, sometimes a vocabulary preview.\n\n"
        "IMAGE VARIETY RULES:\n"
        "  Every slide image must support instruction — show students doing the activity being taught.\n"
        "  If the slide teaches 'Turn and Talk', the image should show students discussing.\n"
        "  If the slide teaches 'Graphic Organizer', show students filling in a chart.\n"
        "  NEVER repeat the same image concept twice in the same day or same lesson.\n"
        "  Add field: visual.image_search.instructional_activity — "
        "describe what students are DOING in the image (e.g., 'students discussing in pairs', "
        "'students writing in notebooks', 'teacher modeling at board').\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )


_STUDENT_CONTENT_RULES_DIRECTIVE = (
    "STUDENT-FACING CONTENT RULES — strictly enforced:\n"
    "1. NO CURRICULUM DOCUMENT TITLES: Never write 'Essential Unit of Study', 'pacing guide', "
    "'curriculum document', 'district materials', 'unit of study', or any internal planning document "
    "name in student-visible content. Students see the book title, unit theme, or week topic — "
    "never the name of a teacher's planning document.\n"
    "2. NO TEKS CODES: Never write TEKS codes (e.g. '5.8A', 'TEKS 5.11', '5.6B') or standard IDs "
    "in any student-visible text — slides, questions, prompts, instructions, or success criteria. "
    "Students experience and practice the skills; they do not see the labels. "
    "TEKS references belong only in teacher-facing artifacts (daily lesson plans, rubrics).\n"
)

_GRADE_RIGOR_DIRECTIVE = (
    "GRADE LEVEL RIGOR — non-negotiable:\n"
    "grade_code specifies the exact grade. ALL content — vocabulary, text complexity, activities, "
    "discussion questions, writing prompts, and assessments — must match that grade level:\n"
    "  - Grade K–2: foundational phonics, sight words, decodable texts, oral language focus\n"
    "  - Grade 3–4: transitional chapter books, paragraph writing, basic literary analysis\n"
    "  - Grade 5 (10–11 year olds): Tier 2/3 academic vocabulary, multi-paragraph analytical writing, "
    "    close reading of chapter books and nonfiction articles, literary analysis of theme/author's craft/"
    "point of view/text structure, text-based evidence. Texts should be chapter books or nonfiction — "
    "NOT picture books. Activities should demand inference and analysis — NOT simple recall.\n"
    "  - Grade 6–8: middle school close reading, argumentative essays, research integration\n"
    "  - Grade 9–12: high school literary criticism, AP-level analysis, college-prep writing\n"
    "NEVER produce K-2 level content (simple phonics, basic sight words, picture-book activities, "
    "'color in this picture', 'draw a smiley face') for Grade 4+ students. "
    "If grade_code is '5', every lesson element must reflect 5th-grade Texas TEKS rigor throughout."
)


_CLASSROOM_INSTRUCTION_PROFILE_DIRECTIVE = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "CLASSROOM INSTRUCTION PROFILE (CIP) — NON-NEGOTIABLE PHYSICAL CLASSROOM RULES\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "instructional_delivery_context.strand_slots_today carries CIP fields per strand.\n"
    "These describe PHYSICAL CLASSROOM REALITY. Violating them produces unusable lesson content.\n\n"
    "TWO DISTINCT TEXT REALITIES (read carefully — these are DIFFERENT):\n\n"
    "  1. CURRICULUM TEXT = the district-selected lesson text for today (e.g. 'The Crane Girl').\n"
    "     curriculum_text_access describes whether students can physically access THIS specific text.\n\n"
    "  2. INDEPENDENT READING BOOKS = student-chosen books from the classroom or school library.\n"
    "     independent_reading_access describes whether students have books for THEIR OWN reading time.\n"
    "     These are NEVER the same as the curriculum text.\n\n"
    "Example: curriculum_text_access=teacher_copy_only AND independent_reading_access=classroom_library_available\n"
    "  → Teacher is the only one with 'The Crane Girl'. Students listen to the read-aloud.\n"
    "  → BUT during independent reading, students may choose books from the classroom library.\n"
    "  → CORRECT: 'Choose a book from the classroom library for independent reading.'\n"
    "  → INCORRECT: 'Continue reading The Crane Girl independently.' ← VIOLATION\n\n"
    "CURRICULUM TEXT ACCESS RULES:\n\n"
    "  'teacher_copy_only' — ONLY the teacher has the curriculum text. HARD RULES:\n"
    "    FORBIDDEN (generates a critical error):\n"
    "    - 'open your copy', 'open to page X', 'in your copy', 'your copy of [title]'\n"
    "    - 'turn to page', 'flip to page', 'read page X', 'find page X'\n"
    "    - 'continue reading [curriculum title]', 'read [curriculum title] independently'\n"
    "    - 'choose a passage from [curriculum title]', 'annotate your copy'\n"
    "    REQUIRED:\n"
    "    - 'listen as your teacher reads from [title]'\n"
    "    - 'follow along as you listen'\n"
    "    - 'respond in your Reader's Notebook about the section we read together'\n"
    "    - 'after the read-aloud, turn and talk with a partner about...'\n"
    "    Students interact with the TEXT only through LISTENING and RESPONDING — not physical access.\n\n"
    "  'projected_shared_display':\n"
    "    - Text on projector/smartboard. Reference 'as shown on the screen' — not page numbers.\n"
    "    - Annotation is a class activity pointing at the screen.\n\n"
    "  'class_set' / 'digital_student_access':\n"
    "    - Every student has their own copy or digital access. Page references are fine.\n"
    "    - Independent close reading, annotation, and evidence-citing tasks are appropriate.\n\n"
    "  'small_group_sets': copies circulate through small groups. Design for rotation.\n\n"
    "  'student_choice_text': no single curriculum text. Students work with chosen texts.\n\n"
    "INDEPENDENT READING ACCESS RULES:\n"
    "  When independent_reading_access is present and not 'none':\n"
    "    - Independent reading time is appropriate for this strand.\n"
    "    - Students read from THEIR OWN CHOSEN BOOKS — NOT the curriculum text.\n"
    "    - Reference the correct source based on the value:\n"
    "      'classroom_library_available' → 'Choose a book from the classroom library'\n"
    "      'school_library_available'   → 'Use your library book'\n"
    "      'student_brought_books'      → 'Use the book you brought from home'\n"
    "      'digital_library'            → 'Open your book on [Epic/Sora/digital library]'\n"
    "    - NEVER mix independent reading references with the curriculum text.\n"
    "  When independent_reading_access = 'none': do NOT include independent reading time.\n\n"
    "DELIVERY MODE RULES:\n"
    "  'teacher_read_aloud':\n"
    "    - Teacher reads; students LISTEN, DISCUSS, and RESPOND IN NOTEBOOKS.\n"
    "    - FORBIDDEN: 'read the passage', 'read independently', 'open your book'.\n"
    "    - REQUIRED: 'listen as your teacher reads', 'respond in your Reader's Notebook'.\n\n"
    "  'shared_reading': class reads together on screen or class copies. Choral reads fine.\n"
    "  'students_have_individual_copies': students may read, annotate, and cite page numbers.\n"
    "  'small_group_reading': design for rotating teacher-led small groups.\n"
    "  'independent_reading': student choice books (check independent_reading_access field).\n"
    "  'guided_writing' / 'shared_writing': teacher models writing first, then students write.\n"
    "  'independent_writing': brief mini-lesson, then maximize student independent writing time.\n"
    "  'teacher_choice': use best pedagogical judgment.\n\n"
    "CLOSURE REQUIREMENT:\n"
    "  When closure_required=true on a strand slot:\n"
    "    - That strand block MUST end with its own exit ticket or closure — strand-specific.\n"
    "    - Do NOT merge closures across strands.\n\n"
    "VIOLATION SEVERITY: curriculum_text_access violations (especially teacher_copy_only) are\n"
    "CRITICAL ERRORS. They produce a lesson plan a teacher CANNOT use in their classroom.\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
)

_VERIFIED_PRIOR_LEARNING_DIRECTIVE = (
    "VERIFIED PRIOR LEARNING — HARD RULE (no exceptions):\n"
    "The prompt_payload field 'verified_previous_learning' is the ONLY authoritative record of\n"
    "what students actually experienced in the previous lesson.\n\n"
    "  - If verified_previous_learning is null → This is the first day (Monday) or no prior data exists.\n"
    "    Do NOT write any continuity statement (no 'yesterday we...', 'building on last time',\n"
    "    'since we explored', etc.). Open the lesson fresh with a hook or direct objective statement.\n\n"
    "  - If verified_previous_learning is present → You MAY reference prior learning, but ONLY\n"
    "    using content that appears verbatim in verified_previous_learning (student_goal,\n"
    "    teacher_modeling, topic, objective_descriptions). Do not paraphrase beyond recognition.\n"
    "    Do not add content that is not listed there.\n\n"
    "FABRICATED CONTINUITY IS A CRITICAL DEFECT:\n"
    "  Never write: 'Yesterday we explored X' if X is not in verified_previous_learning.\n"
    "  Never infer what was taught from the unit theme, week topic, or your own knowledge.\n"
    "  If in doubt, make the connection optional or omit it entirely.\n"
    "  builds_from_yesterday in instructional_design_day is an AI-generated label —\n"
    "  it describes intent, not verified history. Treat verified_previous_learning as the ground truth."
)


_INSTRUCTIONAL_DESIGN_DIRECTIVE = (
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "INSTRUCTIONAL DESIGN PLAN — PRIMARY GUIDE (read before generating anything)\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    "TeacherAssist operates by one principle:\n"
    "  The district defines WHAT students learn.\n"
    "  TeacherAssist determines the most effective instructional HOW\n"
    "  while remaining faithful to district intent.\n\n"
    "The instructional_design_plan was created by an expert instructional designer\n"
    "who has already analyzed the full curriculum, TEKS, pacing guide, and all supporting\n"
    "materials using backward design. It is your primary guide. Artifact generators that\n"
    "deviate from it produce instructional incoherence across the package.\n\n"
    "DISTRICT ANCHORS (district_anchors field):\n"
    "  These are district-defined and extracted programmatically from the pacing guide.\n"
    "  Do not modify, substitute, or reorder any district anchor data.\n"
    "  primary_objectives → the TEKS being assessed this week\n"
    "  supporting_objectives → TEKS touched but not assessed this week\n"
    "  daily_topics → district-defined topic per day; center each day's lesson on this\n"
    "  assessment_checks → when/how the district assesses; do not move these\n"
    "  pacing_materials → district-approved materials; reference only these\n\n"
    "UNIT MASTERY ARC (unit_mastery_arc field — package level):\n"
    "  terminal_mastery → the end-of-unit standard everything in this package builds toward\n"
    "  mastery_gates → what students must demonstrate after each week to stay on the arc\n"
    "  Use these to calibrate this artifact — it must advance students toward the gate,\n"
    "  not merely cover today's topic.\n\n"
    "KNOWLEDGE DEPENDENCY GRAPH (knowledge_dependency_graph field — package level):\n"
    "  Per-objective dependency analysis. For each dependency entry:\n"
    "  status 'assumed_mastered'    → activate it (e.g., warm-up), do not reteach it\n"
    "  status 'develop_this_week'   → build this as part of the week's instruction\n"
    "  status 'may_need_activation' → brief activation moment; flag for teacher attention\n"
    "  gap_consequence → what breaks instructionally if a student lacks this dependency;\n"
    "    use this to design distractor choices (quizzes), rubric misconception criteria,\n"
    "    and warm-ups that surface dependency gaps before skill instruction begins\n"
    "  activation_strategy → the specific classroom move; use in bell ringers and warm-ups\n\n"
    "FOR DAY-BASED ARTIFACTS (lesson plans, daily slide decks, exit tickets, bell ringers):\n"
    "  Use instructional_design_day — the specific day's entry from daily_progression:\n"
    "  instructional_purpose  → TEACHER NOTES / SPEAKER NOTES ONLY. Never expose to students.\n"
    "                           Explains why this lesson comes today in the arc.\n"
    "  student_goal           → use verbatim as the student-facing learning target; no TEKS codes\n"
    "  teacher_goal           → teacher-facing only; the instructional intent of the lesson\n"
    "  builds_from_yesterday  → open with this connection (null on Monday)\n"
    "  prepares_for_tomorrow  → close with this preview (null on Friday)\n"
    "  teacher_modeling       → follow this specifically for your direct instruction section\n"
    "  guided_practice        → design the collaborative activity around this structure\n"
    "  independent_practice   → your student practice section must match this exactly\n"
    "  discussion_prompt      → use this exact prompt for turn-and-talk or partner discussion\n"
    "  formative_assessment   → teacher-facing only; align checks_for_understanding here\n"
    "  exit_ticket            → must reflect instructional_contracts.exit_ticket_stem exactly\n"
    "  observable_mastery_evidence → use for rubric criteria and success standards\n"
    "  differentiation.scaffold   → surface in teacher notes only; never in student materials\n"
    "  differentiation.extension  → surface in teacher notes only\n"
    "  reteach_if_needed      → surface in teacher notes / speaker notes only\n\n"
    "FOR WEEK-BASED ARTIFACTS (quiz, assignment, rubric, vocabulary, slide decks, newsletter):\n"
    "  Use instructional_design_week — the full instructional_design block for this week+subject:\n"
    "  end_of_week_mastery         → the standard this artifact measures or supports\n"
    "  learning_journey_rationale  → what students have practiced by this point in the week\n"
    "  instructional_contracts     → NON-NEGOTIABLE cross-artifact alignment:\n"
    "    exit_ticket_stem            → use verbatim in any exit ticket\n"
    "    quiz_objectives             → ONLY these TEKS codes may appear in quiz questions\n"
    "    rubric_primary_criterion    → use verbatim as the primary rubric criterion\n"
    "    core_activity_name          → use this exact name for the central activity\n"
    "  daily_progression           → understand what students have practiced vs. what is new\n"
    "  introduced_vocabulary       → academic terms from this unit's Knowledge Dependency Graph\n"
    "    and primary objectives that have been taught by this week. Assessment questions\n"
    "    (quiz, assignment, writing_response) may reference these terms. Do not introduce\n"
    "    new domain vocabulary in assessments that does not appear in this list.\n\n"
    "STUDENT-FACING ARTIFACTS — HARD PROHIBITIONS (strictly enforced):\n"
    "  Never expose to students:\n"
    "    instructional_purpose, teacher_goal, formative_assessment, reteach_if_needed,\n"
    "    differentiation (any part), instructional_contracts (as a block),\n"
    "    knowledge_dependency_graph (any field), learning_journey_rationale, district_anchors.\n"
    "  Never write TEKS codes (e.g. '5.8A', 'TEKS 5.11') in student-visible text.\n"
    "  Never name district documents ('Essential Unit of Study', 'pacing guide',\n"
    "    'curriculum map', 'unit of study', 'district materials') in student-visible text.\n\n"
    "TEACHER-FACING ARTIFACTS (daily_lesson_plan, rubric) MAY include:\n"
    "  teacher_goal, formative_assessment, reteach_if_needed, differentiation,\n"
    "  TEKS codes, objectives, and instructional_purpose (in the rationale or notes section).\n\n"
    "FALLBACK (when instructional_design_day or instructional_design_week is null):\n"
    "  Derive equivalent structure from district_anchors, week_curriculum_plan, and\n"
    "  resolved_objectives. Produce the same artifact quality without the plan slice.\n"
    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
)


def _build_image_search_directive(
    grade_code: str | None,
    grade_display_name: str | None,
    subject_name: str | None,
) -> str:
    """Return the grade-aware image search rules section for student_lesson_deck prompts.

    Replaces the old hardcoded "Grade 5 ELA" section so every grade level gets
    correctly-aged image search terms rather than defaulting to 5th-grade examples.
    """
    grade_label = grade_display_name or (
        f"Grade {grade_code}" if grade_code else "the assigned grade"
    )
    _gc = (grade_code or "5").strip().upper()

    if _gc in ("K", "1", "2"):
        age_desc = "5–7 years old"
        example_prefix = "young children"
        age_kws = "'children', 'kids', 'young', 'elementary', 'kindergarten'"
        photo_type = "illustration"
        grade_band = "elementary"
    elif _gc in ("3", "4", "5"):
        age_desc = "8–11 years old"
        example_prefix = "elementary students"
        age_kws = "'children', 'kids', 'elementary', 'students', 'school'"
        photo_type = "photo"
        grade_band = "elementary"
    elif _gc in ("6", "7", "8"):
        age_desc = "11–14 years old"
        example_prefix = "middle school students"
        age_kws = "'middle school', 'students', 'tweens', 'school'"
        photo_type = "photo"
        grade_band = "middle"
    else:
        age_desc = "14–18 years old"
        example_prefix = "high school students"
        age_kws = "'high school', 'students', 'teenagers', 'teen'"
        photo_type = "photo"
        grade_band = "high"

    return (
        f"\nIMAGE SEARCH — STRICT RULES for {grade_label} ({age_desc}):\n"
        f"  RULE 1 — EVERY search term MUST contain at least one of: {age_kws}. "
        "NO EXCEPTIONS. Abstract terms with no age/grade context return adult stock photos or business imagery.\n"
        "  RULE 2 — Pair every concept with age-and-classroom context:\n"
        f"    BAD: 'main idea diagram'  →  GOOD: 'main idea organizer {example_prefix} classroom'\n"
        f"    BAD: 'students discussing'  →  GOOD: '{grade_label.lower()} students turn and talk partners'\n"
        f"    BAD: 'reading comprehension'  →  GOOD: '{example_prefix} reading books classroom'\n"
        f"    BAD: 'collaboration'  →  GOOD: '{example_prefix} working together classroom activity'\n"
        f"    BAD: 'writing'  →  GOOD: '{example_prefix} writing notebooks classroom'\n"
        "  RULE 3 — ABSOLUTE PROHIBITED TERMS (any of these = wrong image, guaranteed):\n"
        "    'adult', 'professional', 'business', 'office', 'meeting', 'corporate',\n"
        "    'stock photo', 'clipart', 'presentation', 'boardroom', 'laptop professional',\n"
        "    'coworkers', 'colleagues', 'workplace', 'career', 'job', 'interview',\n"
        "    'handshake', 'suit', 'tie', 'executive', 'manager', 'team building'.\n"
        "    These terms will return adult business stock photography — completely wrong for a classroom.\n"
        "  RULE 4 — Every image must be appropriate for a school setting:\n"
        f"    All images must show {age_desc} children in educational or age-appropriate contexts.\n"
        "    Prefer: classroom scenes, school settings, children learning, reading, writing, or discussing.\n"
        f"- visual.image_search.search_terms: 3–5 terms — EVERY term must follow RULE 1 and avoid RULE 3\n"
        f"- visual.image_search.target_grade_band: '{grade_band}'\n"
        f"- visual.image_search.preferred_image_type: '{photo_type}'\n"
        "- visual.image_search.image_alt_text: descriptive alt text for accessibility\n"
        "- visual.image_search.educational_purpose: one sentence on WHY this image teaches the concept\n"
        "- visual.image_search.instructional_activity: what students or teacher ARE DOING in this image "
        "(e.g. 'students discussing in pairs', 'teacher modeling at board', 'students writing in notebooks', "
        "'students sorting vocabulary cards'). This drives more specific Pixabay searches.\n"
        "  IMPORTANT: Every slide in a lesson must have a DISTINCT instructional_activity — "
        "never repeat the same activity description within the same day's slides.\n"
        "- visual.organizer_data: ONLY for non-image types:\n"
        "  concept_map: {center_concept, branches: [{label, items: [string]}]}\n"
        "  venn: {left_label, right_label, left_items: [], overlap_items: [], right_items: []}\n"
        "  process_flow: {steps: [{number, label, description}]}\n"
        "  timeline: {events: [{label, date_or_label, description}]}\n"
        "  comparison_table: {col_a_label, col_b_label, rows: [{a, b}]}\n"
    )


def _build_strand_deck_directive(delivery_ctx: dict[str, Any] | None) -> str | None:
    """Build a strand-override directive for student_lesson_deck when multiple strands are active.

    Returns None when no delivery context or only one strand — existing behavior preserved.
    When returned, this string is appended to the student_lesson_deck instruction and
    SUPERSEDES the hardcoded day-specific format (Monday=Reading Workshop, etc.).
    """
    if not delivery_ctx:
        return None
    slots = delivery_ctx.get("strand_slots_today") or []
    if len(slots) < 2:
        return None

    mode_display = (delivery_ctx.get("mode") or "").replace("_", " ").title()

    # Build per-strand block descriptions with CIP-aware instructional cycles
    block_lines: list[str] = []
    total_slides_min = 0
    total_slides_max = 0
    for i, slot in enumerate(slots, start=1):
        name = slot.get("strand_name", f"Strand {i}")
        mins = slot.get("minutes_per_day")
        mins_str = f" ({mins} min)" if mins else ""
        dm = slot.get("delivery_mode", "")
        cta = slot.get("curriculum_text_access", "")
        ira = slot.get("independent_reading_access", "")
        closure_req = slot.get("closure_required", False)

        # Build the per-block instructional cycle based on delivery_mode
        if dm == "teacher_read_aloud":
            cycle = (
                "Activation → Learning Target → Teacher Read Aloud (teacher reads, students listen) → "
                "Listening/Discussion Response → Guided Practice (notebook or oral) → "
                "Independent Practice (notebook response, NO student text access) → "
                "CFU (check for understanding)"
            )
        elif dm == "shared_reading":
            cycle = (
                "Activation → Learning Target → Shared Reading (class reads together) → "
                "Text Analysis → Guided Practice (annotate together or discussion) → "
                "Independent Practice → CFU"
            )
        elif dm == "student_individual_copies":
            cycle = (
                "Activation → Learning Target → Teacher Modeling (with text) → "
                "Guided Practice (students follow along in their copy) → "
                "Independent Practice (students read/annotate their copy) → CFU"
            )
        elif dm in ("guided_writing", "shared_writing"):
            cycle = (
                "Activation → Learning Target → Teacher Modeling (teacher writes aloud, think-aloud) → "
                "Guided Practice (students write alongside teacher) → "
                "Independent Practice (students apply in writer's notebook) → CFU"
            )
        elif dm == "independent_writing":
            cycle = (
                "Activation → Learning Target → Mini-Lesson (5–8 min max) → "
                "Independent Writing (majority of block) → Share/Author's Chair → CFU"
            )
        else:
            cycle = (
                "Activation → Learning Target → Teacher Modeling → "
                "Guided Practice → Independent Practice → CFU"
            )

        if closure_req:
            cycle += (
                " → Closure/Exit Ticket (REQUIRED — strand-specific, not shared with other strands)"
            )

        # Curriculum text access and independent reading notes
        ta_note = ""
        if cta == "teacher_copy_only":
            ta_note = (
                f"\n    CURRICULUM TEXT HARD RULE for {name}: curriculum_text_access=teacher_copy_only. "
                "The teacher is the ONLY person with the curriculum text. "
                "FORBIDDEN: 'open to page X', 'read silently', 'continue reading [title]', "
                "'open your copy', 'read [title] independently', 'choose a passage from [title]'. "
                "ALL student activities must be LISTENING-based or NOTEBOOK RESPONSE-based."
            )
        elif cta == "projected_shared_display":
            ta_note = (
                f"\n    CURRICULUM TEXT for {name}: projected on screen. "
                "Reference 'as shown on the screen/board' — no page numbers. "
                "Annotation tasks are whole-class pointing at the screen."
            )
        if ira and ira not in ("none", ""):
            _ira_phrase = {
                "classroom_library_available": "the classroom library",
                "school_library_available": "their school library book",
                "student_brought_books": "the book they brought from home",
                "digital_library": "their digital library book",
            }.get(ira, "their independent reading book")
            ta_note += (
                f"\n    INDEPENDENT READING for {name}: students have access to {_ira_phrase}. "
                f"If independent reading time is included, students choose from {_ira_phrase} — "
                "NEVER from the curriculum text."
            )

        block_lines.append(
            f"  Block {i} — {name}{mins_str}: 5–7 slides\n"
            f"    Instructional Cycle: {cycle}{ta_note}\n"
            f"    All slides in this block: strand_name={name!r}, instructional_block_order={i}"
        )
        total_slides_min += 5
        total_slides_max += 7

    separator_count = len(slots) - 1
    total_min = total_slides_min + separator_count
    total_max = total_slides_max + separator_count

    separator_example = ""
    if len(slots) >= 2:
        next_strand = slots[1].get("strand_name", "Strand 2")
        separator_example = (
            f"\n\nSEPARATOR SLIDE (between each block):\n"
            f'  slide_type: "strand_separator"\n'
            f'  layout: "title_full"\n'
            f'  title: "{next_strand}" (or the next block\'s strand name)\n'
            f'  strand_name: "{next_strand}"\n'
            f"  instructional_block_order: 2 (index of the block that follows)\n"
            f'  body: brief transition phrase ("Now we move to Writing Workshop.")\n'
            f"  No engagement or visual required on separator slides."
        )

    return (
        f"\n\n"
        f"STRAND-BASED DECK OVERRIDE — supersedes the DAY-SPECIFIC LESSON FORMAT above:\n"
        f"This classroom uses a {mode_display} delivery profile. "
        f"instructional_delivery_context.strand_slots_today lists {len(slots)} required strands.\n\n"
        f"DO NOT generate a single-strand deck. "
        f"DO NOT follow the Monday/Tuesday/Wednesday/etc. day-specific format above — "
        f"it is overridden by this delivery profile.\n\n"
        f"REQUIRED DECK STRUCTURE (one block per strand, in order):\n"
        + "\n\n".join(block_lines)
        + separator_example
        + f"\n\nSLIDE METADATA — every slide MUST include:\n"
        f"  strand_name: the strand this slide belongs to (string, required on every slide)\n"
        f"  strand_minutes: minutes allocated to this strand (number or null)\n"
        f"  instructional_block_order: 1-based index of this strand block (number, required)\n\n"
        f"TOTAL SLIDE COUNT: {total_min}–{total_max} slides "
        f"({total_slides_min}–{total_slides_max} instructional + {separator_count} separator).\n\n"
        f"CONTENT RULE: Each strand block must teach distinct, strand-appropriate content. "
        f"Reading blocks address comprehension, text analysis, and reader response. "
        f"Writing blocks address craft, drafting, revision, and writer's notebook. "
        f"Other strands follow their own instructional purpose from district_document_context. "
        f"Blocks must NOT repeat the same activity, vocabulary, or engagement prompt.\n\n"
        f"CLOSURE RULE: If closure_required=true on a strand slot, that block MUST have its own "
        f"exit ticket/closure slide. Do NOT merge closures across strands."
    )


def _instruction_for_artifact(artifact_type: str) -> str:
    instructions = {
        "daily_lesson_plan": (
            "Generate a COMPLETE, teacher-ready daily lesson plan a teacher can follow verbatim in the classroom. "
            "The test: 'I could teach this tomorrow without editing.'\n\n"
            "REQUIRED FIELDS PER SUBJECT BLOCK — all must be fully populated:\n"
            "teks_alignment: list the specific TEKS code(s) + full description this lesson addresses.\n"
            "learning_target: student-facing 'I can...' statement — the PRIMARY goal (NO TEKS codes, NO jargon).\n"
            "learning_goals: JSON array of 1–2 'I can...' statements — MAXIMUM 2 PER BLOCK. "
            "Each entry is a single, specific, student-friendly goal. "
            "NEVER write 3 or more entries. If one goal, write one. If two, write two.\n"
            "  Reading block example: ['I can identify the main idea.', 'I can support it with one detail.']\n"
            "  Writing block example: ['I can write a complete sentence response.', 'I can include one text detail.']\n"
            "  RULE: Reading goals are about COMPREHENSION. Writing goals are about COMPOSITION. "
            "Never share goals between Reading and Writing blocks.\n\n"
            "curriculum_resource: exact title of today's selected instructional resource.\n"
            "resource_rationale: 1–2 sentences explaining why THIS resource serves TODAY's TEKS.\n"
            "  Example: 'The Crane Girl's multi-generational relationships provide rich evidence for\n"
            "  analyzing how characters change over time — directly aligned to TEKS 5.8A.'\n\n"
            "preteach (all 5 sub-fields — derived from knowledge_dependency_graph + curriculum):\n"
            "  activate_prior_knowledge — specific hook from today's curriculum (name the book/concept)\n"
            "  vocabulary_preview — 1–2 key terms with student-friendly definitions (no TEKS codes)\n"
            "  student_friendly_explanation — concept in one sentence any student understands\n"
            "  why_this_matters — real-world relevance in student language\n"
            "  engagement_question — one question that sparks curiosity before instruction\n\n"
            "teacher_modeling_script (all 5 sub-fields — write as a real teaching script):\n"
            "  teacher_says — exact script teacher reads aloud. 4–6 sentences. First person.\n"
            "    Reference the curriculum text by name, quote a specific line, or describe a scene.\n"
            "  students_do — what students do while teacher models (NOT passive listening)\n"
            "  teacher_questions — 2–3 probing questions asked during the model (think-aloud style)\n"
            "  expected_student_thinking — correct reasoning written as a student sentence\n"
            "  common_misconceptions — the 1–2 most common errors + redirect strategy for each\n\n"
            "direct_instruction: the core teaching point (4–6 sentences, curriculum-specific).\n"
            "guided_practice: step-by-step collaborative activity; name the steps and curriculum tie.\n"
            "  Must build toward the SAME learning_goals as this block — not a different skill.\n"
            "independent_practice: exact student-facing task instructions. Reference curriculum resource.\n"
            "  Honor curriculum_text_access — teacher_copy_only = notebook-based, not text-access-based.\n"
            "  Must align to this block's learning_goals — students independently practice what was modeled.\n"
            "checks_for_understanding: exactly 3 questions — post-model, mid-practice, pre-closure.\n"
            "  Each question must assess the SAME skill as this block's learning_goals.\n"
            "closure: specific exit prompt tied to today's learning_target and learning_goals.\n"
            "  Use exit_ticket_stem verbatim when available.\n"
            "  RULE: Reading closure assesses the Reading goal. Writing closure assesses the Writing goal.\n"
            "  Do NOT combine Reading and Writing in a single closure unless the curriculum explicitly requires it.\n"
            "transition: how this block hands off to the next subject/strand (1–2 sentences).\n"
            "  Reading MUST be complete before Writing begins.\n"
            "materials: actual named materials from pacing_materials and resolved_day_plan.\n"
            "notes: differentiation tips, pacing reminders, misconceptions to watch for.\n"
            + _LEARNING_GOAL_DISCIPLINE_DIRECTIVE
            + "\n"
            + _WEEK_PLAN_DIRECTIVE
            + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE
            + "\n"
            + _CLASS_TIME_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _GRADE_RIGOR_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "student_lesson_deck": (
            "Generate a STUDENT-FACING lesson presentation projected on a classroom screen. "
            "This is a VISUAL-FIRST teaching tool. Students should grasp the concept from the "
            "visual before the teacher speaks.\n\n"
            "PRESENTATION DESIGN RULES — every slide must satisfy:\n"
            "  • One dominant idea: one clear concept per slide; if a slide covers two ideas, split it\n"
            "  • One dominant visual: exactly one image or organizer per slide; no multi-image layouts\n"
            "  • Maximum 4 bullets: three or fewer is better — never exceed 4 bullets per slide\n"
            "  • Maximum 35 words in body: if you need more, create a second slide; do not shrink ideas\n"
            "  • One student action: every instructional slide has one engagement.type and one "
            "engagement.prompt (hook and wrap_up slides may have an implicit action)\n"
            "  • Large readable typography: titles ≤ 10 words, bullets ≤ 12 words each\n"
            "  • Instructional rhythm: the deck follows the 9-step classroom rhythm — "
            "hook → preteach → learning target → modeling → guided practice → independent practice → "
            "check for understanding → closure → transition. Students should feel the lesson building.\n\n"
            "SLIDE COUNT: Target 7–9 slides (the preteach slide adds one). The lesson structure drives "
            "the count — do not pad slides to reach 9 and do not compress distinct instructional moments. "
            "A well-structured 7-slide lesson is better than a padded 10-slide one.\n\n"
            "REQUIRED SLIDE SEQUENCE — every daily deck must include these slides in this order:\n"
            "  Slide 1 — hook/connection (slide_type='connection', layout='hook_full_image')\n"
            "  Slide 2 — PRETEACH (slide_type='preteach', layout='vocabulary_showcase'): "
            "Surface 1–2 key vocabulary words or concepts students need BEFORE the lesson. "
            "Draw vocabulary_preview and why_this_matters from the lesson plan's preteach fields. "
            "Use a concrete, student-friendly definition + a real-world connection. "
            "This slide is NOT optional — it bridges prior knowledge to today's new content.\n"
            "  Slide 3 — learning target (slide_type='today_we_learn', layout='objective_image')\n"
            "  Slides 4–6 — instruction (teaching_point, read_aloud, concept, guided_practice, example)\n"
            "  Slide 7 — independent practice or discussion (your_turn / discussion)\n"
            "  Slide 8 — check for understanding or exit ticket (check_in / exit_ticket)\n"
            "  Slide 9 — wrap_up / closure (wrap_up / share)\n\n"
            "CURRICULUM-FIRST: district_document_context tells you HOW this unit is structured. "
            "READ the excerpts carefully. If the curriculum describes a Reading Workshop, Writing Workshop, "
            "Interactive Read Aloud, Reader’s Notebook, or a specific mini-lesson system (e.g. Fountas & Pinnell RML), "
            "BUILD slides around THAT framework — NOT a generic sequence. "
            "Name the specific books, text sets, workshop anchors, and notebook activities the curriculum calls for. "
            "If the curriculum mentions an Interactive Read Aloud text set, a specific read-aloud title, "
            "or Reader’s Notebook prompts, those MUST appear in the lesson.\n\n"
            "BOOKS — SELECT FROM THE CURRICULUM LIST:\n"
            "week_curriculum_plan.mentor_texts and full_curriculum_documents contain the district’s "
            "approved book list for this unit. These books are curated because they are relevant to "
            "the objectives and unit theme. Rules:\n"
            "  - You do NOT need to use every book — select the ones that best fit today’s lesson focus\n"
            "  - When you choose to do a read-aloud, mini-lesson, or text reference, pick a book FROM "
            "this list — never invent a title or substitute a book not in the curriculum\n"
            "  - Reference chosen books by their exact title — never say ‘a book we are reading’ or ‘a story’\n"
            "  - Use the book naturally where it fits: a read_aloud slide, a teaching_point anchor, "
            "a connection slide — let the day’s workshop format guide when and how\n"
            "  - The goal is that students hear and engage with real, named curriculum books — "
            "not generic unnamed texts\n\n"
            "DAY-SPECIFIC LESSON FORMAT — every day starts with connection → preteach → today_we_learn, "
            "then follows the format that matches day_label:\n"
            "Monday   → Reading Workshop: connection → PRETEACH (vocabulary) → learning target → "
            "read-aloud excerpt or book introduction → teaching point (literary element, strategy) → "
            "turn & talk → independent reading prompt → share\n"
            "Tuesday  → Deep Reading/Analysis: connection → PRETEACH (key concept) → learning target → "
            "revisit text → analyze literary elements (character, theme, plot) → "
            "text evidence activity → partner discussion → Reader’s Notebook response → debrief\n"
            "Wednesday → Writing Workshop: connection → PRETEACH (craft vocabulary) → learning target → "
            "connection to mentor text or read-aloud → teaching point (craft/structure) → "
            "teacher models writing → students try it → share one example → independent writing link\n"
            "Thursday → Word Study + Revision: connection → PRETEACH (word pattern or root) → "
            "learning target → sort/categorize → use in writing context → revise/edit a sentence → writer’s notebook\n"
            "Friday   → Celebration & Synthesis: connection → PRETEACH (week recap vocabulary) → "
            "learning target → student share (author’s chair / book talk) → "
            "week recap (what we read, wrote, learned) → reflection → next steps preview\n\n"
            "NEVER produce the same hook→vocabulary→concept→example→your_turn→check_in→wrap_up "
            "sequence every day. Vary the slides after preteach to match the day’s workshop format above.\n\n"
            "SLIDE TYPES:\n"
            "  connection | preteach | today_we_learn | teaching_point | read_aloud | vocabulary |\n"
            "  word_study | concept | example | active_engagement | your_turn | guided_practice |\n"
            "  independent_practice | discussion | share | check_in | exit_ticket | wrap_up\n"
            "  slide_type = ‘connection’ for the opening hook/link to prior learning (Slide 1)\n"
            "  slide_type = ‘preteach’ for pre-teaching key vocabulary and concepts (Slide 2 — REQUIRED)\n"
            "  slide_type = ‘today_we_learn’ for the student-facing learning target (Slide 3)\n"
            "  slide_type = ‘teaching_point’ for the direct instruction mini-lesson slide\n"
            "  slide_type = ‘read_aloud’ for a slide displaying a passage, poem, or book excerpt\n"
            "  slide_type = ‘active_engagement’ for turn & talk, think-pair-share, or quick write\n"
            "  slide_type = ‘word_study’ for phonics/spelling/word pattern slides\n"
            "  slide_type = ‘independent_practice’ for the link/send-off to independent work\n"
            "  slide_type = ‘exit_ticket’ for the final summative exit check\n"
            "  slide_type = ‘share’ for the closing celebration or debrief\n\n"
            "LAYOUT OPTIONS — choose the best fit for the archetype:\n"
            "  ‘hook_full_image’        — opening / connection slide; dominant image top 60%\n"
            "  ‘title_full’             — full-screen background image with overlaid title (wrap_up, share)\n"
            "  ‘objective_image’        — learning goal left, image right (today_we_learn)\n"
            "  ‘vocabulary_showcase’    — word banner top, image + definition below (vocabulary, word_study)\n"
            "  ‘teacher_modeling’       — image left, numbered teaching steps right (teaching_point, concept)\n"
            "  ‘before_after’           — before card / arrow / after card (example, writing craft revision)\n"
            "  ‘organizer_full’         — graphic organizer fills screen (concept map, Venn, timeline)\n"
            "  ‘guided_practice_image’  — title top, image middle, student action strip bottom\n"
            "  ‘discussion_image’       — discussion question left, image right\n"
            "  ‘exit_ticket_image’      — exit question left, image right (exit_ticket, check_in)\n"
            "  ‘text_left_image_right’  — equal text/image split (read_aloud, concept, default)\n\n"
            "FIELD RULES for each slide:\n"
            "- id: unique slug (‘slide-1’, ‘slide-2’, etc.)\n"
            "- title: Student-friendly heading, max 10 words\n"
            "- body: Max 35 words, grade-appropriate, references the specific book/text/activity\n"
            "- bullets: 3–4 short student-friendly phrases, max 12 words each\n"
            "- student_emotion: The intended emotional state this slide creates — ONE word from: "
            "Wonder | Curiosity | Confidence | Collaboration | Reflection | Agency | Celebration. "
            "Sequence the arc: hook=Wonder → teaching=Curiosity → practice=Confidence → "
            "discussion=Collaboration → exit=Reflection. This guides engagement pacing for the teacher.\n"
            "- visual_learning_goal: ONE sentence explaining how this slide’s specific image helps "
            "students understand today’s learning objective. Connect the image to the concept — "
            "not a generic description. Example: ‘A photo of children revising notebooks primes students "
            "to connect revision as a physical, iterative act before the mini-lesson on revision begins.’\n"
            "- teacher_notes: 2–3 sentences for the teacher only. Include: what to say while this slide "
            "is displayed, any common misconception to address, one differentiation tip if relevant. "
            "Reference instructional_purpose or reteach_if_needed from the design plan where useful. "
            "NEVER shown to students.\n"
            "- engagement.type: think_pair_share|turn_and_talk|show_fingers|whiteboard|quick_draw|exit_ticket\n"
            "- engagement.prompt: Exact student action as a sentence (‘Turn to a partner: …’)\n"
            "- visual.type: Use ‘image_search’ for all real-world content. "
            "Only use ‘concept_map’|’venn’|’process_flow’|’timeline’|’comparison_table’ "
            "for abstract diagrams students fill in.\n"
            "- visual.placement: full_width (hook_full_image/title_full) | right | left\n"
            "- visual.fallback_organizer_type: organizer type shown if no Pixabay image is found\n\n"
            "DAY-BY-DAY CONTENT RULE: Each day’s lesson must cover DISTINCT content. "
            "Monday–Friday must NEVER repeat the same hook question, vocabulary term, book excerpt, "
            "or student activity. Use day_label and resolved_day_plan.daily_topic to anchor today’s thread. "
            "Build on the previous day without repeating it.\n\n"
            + _STUDENT_CONTENT_RULES_DIRECTIVE
            + "\n"
            + _WEEK_PLAN_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _GRADE_RIGOR_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
            # NOTE: grade-aware image search rules are appended dynamically in
            # generate_v2_instructional_artifact() via _build_image_search_directive().
        ),
        "subject_slide_deck": (
            "Generate a classroom-ready slide deck that TEACHES the lesson — these slides are projected to students "
            "and must contain real instructional content, not objective restatements or bullet stubs.\n"
            "For EACH slide, fill every field with substantive content:\n"
            "- title: A specific, descriptive topic title.\n"
            "- body: A full 3-5 sentence paragraph that TEACHES the concept. Include definitions, examples, "
            "and explanations drawn from named books or texts. This is NOT a repeat of the title — it is the "
            "main content students read on the slide.\n"
            "- bullets: 4-6 specific, information-dense bullet points. Each bullet must contain an actual fact, "
            "vocabulary word with definition, quote, or concept — NOT generic objective language.\n"
            "- speaker_notes: A 3-5 sentence script of what the teacher says while this slide is displayed — "
            "include what to point to, discussion prompts, and transitions.\n"
            "- discussion_question: One specific question for students to discuss in pairs or as a class.\n"
            "Build slides in this order: (1) title/hook slide, (2) one content slide per topic in "
            "resolved_daily_topics, (3) key vocabulary slide, (4) guided practice slide, (5) wrap-up slide.\n"
            "For each slide, set layout_type to one of: 'standard' (default), 'before_after' (for comparing "
            "examples, before/after sentences, original vs revised), 'two_column' (for comparing two concepts "
            "side by side), 'vocabulary_showcase' (for key vocabulary terms with definitions). "
            "When layout_type is 'before_after', populate comparison_pairs with before/after examples grounded "
            "in the lesson content — each pair must have actual content in text_before and text_after, not "
            "placeholders. Example: for a grammar or writing lesson, text_before = the weak or incorrect version, "
            "text_after = the strong or corrected version, explanation = what changed and why it is better.\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + " "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "quiz": (
            "Generate a substantive quiz aligned to required TEKS first, optional TEKS as extension.\n"
            "Write questions about SPECIFIC content from the curriculum documents and named texts — "
            "not generic questions that could apply to any lesson.\n"
            "Include a mix: 2-3 recall questions about specific content, 2-3 inference or analysis "
            "questions, 1-2 vocabulary-in-context questions.\n"
            "answer_key must provide complete, specific answers — not 'varies' or 'see rubric'.\n"
            "Each question must map to a specific objective_id from resolved_objectives, "
            "prioritizing is_required=true objectives.\n"
            + _STUDENT_CONTENT_RULES_DIRECTIVE
            + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "rubric": (
            "Generate a rubric whose criteria map directly to resolved_objectives — required TEKS first.\n"
            "Name each criterion after the specific skill or objective it measures. "
            "Performance level descriptions must reference what students do with the actual content — "
            "not generic phrases like 'demonstrates understanding'.\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "assignment": (
            "Generate a complete, ready-to-use written assignment grounded in curriculum documents and "
            "required TEKS.\n"
            "passage_title and passage_text: Write a 150-300 word passage using content from the curriculum "
            "documents or named books. Draw on specific themes, vocabulary, events, characters, or arguments "
            "from those materials — NOT a generic placeholder.\n"
            "questions: 5-7 questions covering required TEKS — recall, inference, vocabulary-in-context, "
            "and at least one extended written response.\n"
            "student_instructions: Clear, grade-appropriate directions.\n"
            "success_criteria: 3-5 specific, observable criteria students use to self-check.\n"
            "objective_alignment: Name exactly which required TEKS this addresses and how.\n"
            + _STUDENT_CONTENT_RULES_DIRECTIVE
            + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "writing_response": (
            "Generate a writing response prompt scaffolded around curriculum documents and required TEKS.\n"
            "writing_prompt: A specific task tied to the required TEKS and the curriculum materials — "
            "reference specific books, passages, or concepts from district_document_context.\n"
            "student_instructions: Step-by-step directions (plan → draft → revise).\n"
            "sentence_starters: 4-6 frames that scaffold the TEKS language and curriculum vocabulary.\n"
            "success_criteria: 4-5 specific, observable criteria tied to required TEKS.\n"
            + _STUDENT_CONTENT_RULES_DIRECTIVE
            + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE
            + "\n"
            + _BOOK_GROUNDING_DIRECTIVE
            + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "parent_newsletter_summary": (
            "Generate a parent-friendly weekly newsletter summary in plain, warm language. "
            "what_students_will_learn must describe the actual objectives from resolved_objectives in "
            "plain language — name any books or texts students are reading this week. "
            "upcoming_focus should mention the specific titles or topics coming next. "
            "Keep the tone encouraging and family-friendly."
        ),
    }
    default = (
        f"Generate a teacher-usable {artifact_type.replace('_', ' ')} with specific, substantive content. "
        + _BOOK_GROUNDING_DIRECTIVE
        + " "
        + _OBJECTIVE_ALIGNMENT_DIRECTIVE
    )
    base = instructions.get(artifact_type, default)
    # Prepend the instructional design directive universally to all artifact types.
    # This is the primary guide for all generation. The directive itself contains
    # student-facing prohibition rules, so student-facing artifacts receive them too.
    return _INSTRUCTIONAL_DESIGN_DIRECTIVE + base


def _normalize_artifact_content(artifact_type: str, content: Any) -> dict[str, Any]:
    # Gemini 2.5 Flash sometimes returns a bare JSON array instead of an object.
    # Wrap it into the expected top-level dict shape before normalizing.
    if isinstance(content, list):
        if artifact_type == "student_lesson_deck":
            content = {"slides": content}
        elif artifact_type in ("subject_slide_deck", "daily_lesson_plan"):
            content = {"slides": content}
        else:
            content = {"items": content}
    normalized = dict(content)
    if artifact_type == "quiz" and not normalized.get("sections"):
        sections = []
        questions = normalized.get("questions") or []
        if questions:
            sections.append(
                {
                    "heading": "Questions",
                    "bullets": [
                        str(item.get("prompt") or item) if isinstance(item, dict) else str(item)
                        for item in questions
                    ],
                }
            )
        answer_key = normalized.get("answer_key") or []
        if answer_key:
            sections.append(
                {
                    "heading": "Answer Key",
                    "bullets": [
                        f"{item.get('prompt', 'Question')}: {item.get('answer', '')}"
                        for item in answer_key
                        if isinstance(item, dict)
                    ],
                }
            )
        normalized["sections"] = sections
    if artifact_type == "rubric" and not normalized.get("sections"):
        criteria = normalized.get("criteria") or []
        normalized["sections"] = [
            {
                "heading": "Rubric Criteria",
                "bullets": [
                    f"{item.get('name', 'Criterion')} ({item.get('point_value', 0)} pts)"
                    for item in criteria
                    if isinstance(item, dict)
                ],
            }
        ]
    if artifact_type == "parent_newsletter_summary" and not normalized.get("sections"):
        normalized["sections"] = [
            {
                "heading": "What students will learn",
                "bullets": normalized.get("what_students_will_learn") or [],
            },
            {"heading": "Reminders", "bullets": normalized.get("reminders") or []},
            {"heading": "Upcoming focus", "bullets": normalized.get("upcoming_focus") or []},
        ]
    if not normalized.get("sections") and artifact_type not in {
        "daily_lesson_plan",
        "subject_slide_deck",
        "student_lesson_deck",
    }:
        normalized.setdefault(
            "sections",
            [
                {
                    "heading": "Overview",
                    "body": normalized.get("summary") or "Generated instructional resource.",
                }
            ],
        )
    return normalized


def generate_curriculum_sequence_plan(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    generation_context: dict[str, Any],
) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
    """One-shot AI call that reads the full curriculum and returns a fixed week-by-week
    teaching sequence plan. Called ONCE before any artifact generation so every artifact
    is grounded in a consistent, pedagogically-ordered plan rather than each week
    independently deciding what to emphasize.

    Returns (curriculum_sequence_plan, instructional_resource_bank):
      - curriculum_sequence_plan: dict keyed by week number {1: {...}, 2: {...}, ...}
      - instructional_resource_bank: structured resource entries built from the full docs
    Falls back to empty dicts on any failure — callers must handle gracefully.
    """
    if not is_teacher_assist_real_ai_active(db, settings):
        return {}, {}

    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name, _api_key, _base_url = _provider_api_params(effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)

    total_weeks = generation_context.get("total_guide_weeks") or 1
    grade_code = generation_context.get("grade_code")
    grade_display_name = generation_context.get("grade_display_name")

    # Load the FULL text of every curriculum file directly from the DB.
    # This bypasses the DOCUMENT_CONTEXT_ITEM_LIMIT/TOTAL_LIMIT excerpts so the AI
    # can read the complete Essential Unit of Study and extract week-by-week book lists.
    _FULL_DOC_CHAR_LIMIT = 60_000
    full_curriculum_docs: list[dict[str, Any]] = []
    _pacing_mats = generation_context.get("pacing_materials") or []
    _file_mat_ids = [
        uuid.UUID(str(m["id"]))
        for m in _pacing_mats
        if m.get("material_kind") == "file" and m.get("id")
    ]
    if _file_mat_ids:
        _ext_rows = db.scalars(
            _sa_select(TeacherAssistV2DocumentExtraction).where(
                TeacherAssistV2DocumentExtraction.supporting_material_id.in_(_file_mat_ids)
            )
        ).all()
        _ext_by_mat = {str(row.supporting_material_id): row for row in _ext_rows}
        for _mat in _pacing_mats:
            if _mat.get("material_kind") != "file":
                continue
            _mid = str(_mat.get("id", ""))
            _row = _ext_by_mat.get(_mid)
            if _row is None:
                continue
            _text = (_row.teacher_edited_text or _row.extracted_text or "").strip()
            if _text:
                full_curriculum_docs.append(
                    {
                        "title": _mat.get("title"),
                        "filename": _mat.get("original_filename"),
                        "resource_type": _mat.get("resource_type"),
                        "text": _text[:_FULL_DOC_CHAR_LIMIT],
                        "truncated": len(_text) > _FULL_DOC_CHAR_LIMIT,
                    }
                )

    grade_clause = (
        f" This curriculum is for Grade {grade_code} ({grade_display_name}). "
        "Calibrate all week themes, mentor text selections, and activity complexity to that grade level."
        if grade_code
        else ""
    )

    instruction = (
        "You are a curriculum sequencing expert. Analyze the provided pacing guide objectives, "
        "curriculum documents, and teacher materials to create a FIXED, PEDAGOGICALLY-SEQUENCED "
        f"week-by-week teaching plan for a {total_weeks}-week instructional package.{grade_clause}\n\n"
        "CRITICAL — READ full_curriculum_documents FIRST:\n"
        "full_curriculum_documents contains the COMPLETE extracted text of every curriculum file "
        "uploaded for this pacing guide (e.g. 'Essential Unit of Study' PDFs). Read every document "
        "fully before producing your plan. These documents contain:\n"
        "  - Specific mentor texts and read-aloud books assigned to each week\n"
        "  - Week-by-week instructional sequences, workshop structures, and activities\n"
        "  - Unit themes, essential questions, and culminating tasks\n"
        "Use EXACT book titles and activity names from these documents — do NOT substitute or invent.\n\n"
        "TASK: Determine the natural pedagogical progression — "
        "what concepts must be taught FIRST as a foundation, what builds on those foundations, "
        "and what synthesis or advanced work comes at the end. Do NOT assign the same emphasis "
        "every week. Each week must have a DISTINCT primary focus that builds on the prior week.\n\n"
        "For a Reading + Writing curriculum:\n"
        "- Early weeks: establish reading identity, book selection habits, reading stamina, "
        "  introduce writing as a response to reading\n"
        "- Middle weeks: dive into literary analysis (character, theme, author's craft), "
        "  connect reading to writing craft lessons\n"
        "- Later weeks: deepen analysis, independent writing, revision, genre study\n"
        "- Final week: synthesis, celebration, reflection on the learning journey\n\n"
        "GROUNDING RULE: Base your plan on full_curriculum_documents — they are the authoritative "
        "source. If they name specific books, activities, or workshop structures per week, use those EXACTLY. "
        "If a book is listed for Week 3, put it in Week 3's mentor_texts — do not move it.\n\n"
        "TEKS FULL-COVERAGE REQUIREMENT — mandatory:\n"
        "resolved_objectives (in the subjects data) lists every TEKS for this curriculum. "
        "By Week {total_weeks}, EVERY TEKS must have been the primary focus of at least one week's lessons. "
        "Rules for primary_teks and secondary_teks in your plan:\n"
        "  - primary_teks: 1-3 TEKS that are the main teaching focus this week — students are introduced, "
        "    practice deeply, and are assessed on these\n"
        "  - secondary_teks: TEKS that are touched on or reinforced this week but not the primary focus\n"
        "  - No TEKS should appear as primary_teks every single week — distribute coverage deliberately\n"
        "  - By the final week, every required TEKS must have been primary at least once\n"
        "  - Optional TEKS (is_required=false) must also be distributed — brief but intentional\n"
        "Assignments and assessments generated each week will test the primary_teks for that week, "
        "so your distribution here directly determines what students are graded on each week.\n\n"
        f"Return a JSON object with exactly {total_weeks} entries, one per week, in this schema:\n"
        "{\n"
        '  "teaching_sequence": [\n'
        "    {\n"
        '      "week": 1,\n'
        '      "theme": "Short memorable theme title for the week",\n'
        '      "reading_focus": "What specific reading skill/concept students practice this week",\n'
        '      "writing_focus": "What specific writing skill/activity students do this week",\n'
        '      "primary_teks": ["code1", "code2"],\n'
        '      "secondary_teks": ["code3"],\n'
        '      "mentor_texts": ["Exact book/text title from curriculum documents"],\n'
        '      "key_activities": ["specific activity 1", "specific activity 2"],\n'
        '      "builds_on": null,\n'
        '      "prepares_for": "Brief description of what Week 2 will build toward"\n'
        "    }\n"
        "  ]\n"
        "}\n"
    )

    prompt_payload = {
        "total_guide_weeks": total_weeks,
        "grade_code": grade_code,
        "grade_display_name": grade_display_name,
        "subjects": generation_context.get("subjects"),
        "weeks": generation_context.get("weeks"),
        "resolved_objectives": generation_context.get("resolved_objectives"),
        "full_curriculum_documents": full_curriculum_docs,
        "pacing_materials": generation_context.get("pacing_materials"),
        "district_document_context": generation_context.get("district_document_context"),
        "district_materials_summary": generation_context.get("district_materials_summary"),
        "teacher_supplemental_files": generation_context.get("teacher_supplemental_files"),
        "teacher_document_context": generation_context.get("teacher_document_context"),
        "teacher_supplemental_notes": generation_context.get("teacher_supplemental_notes"),
    }

    schema = {
        "teaching_sequence": [
            {
                "week": 1,
                "theme": "string",
                "reading_focus": "string",
                "writing_focus": "string",
                "primary_teks": ["string"],
                "secondary_teks": ["string"],
                "mentor_texts": ["string"],
                "key_activities": ["string"],
                "builds_on": "string or null",
                "prepares_for": "string or null",
            }
        ]
    }

    try:
        result = execute_openai_json_completion(
            effective_settings,
            model_name=model_name,
            instruction=instruction,
            prompt_payload=prompt_payload,
            required_output_schema=schema,
            _api_key=_api_key,
            _base_url=_base_url,
            _provider=provider_name,
        )
        record_teacher_assist_ai_usage(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            feature=V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
            provider=result.provider,
            model=result.model,
            input_tokens=result.input_tokens,
            output_tokens=result.output_tokens,
            estimated_cost_cents=result.estimated_cost_cents,
            metadata={
                "operation_type": "curriculum_sequence_plan",
                "package_id": str(package_id),
                "related_entity_type": "instructional_package",
                "related_entity_id": str(package_id),
            },
        )
        sequence = result.content_json.get("teaching_sequence") or []
        sequence_plan = {int(entry["week"]): entry for entry in sequence if "week" in entry}
        resource_bank = build_instructional_resource_bank(
            full_curriculum_docs,
            sequence_plan,
            generation_context=generation_context,
        )
        return sequence_plan, resource_bank
    except Exception:
        import logging

        logging.getLogger(__name__).exception(
            "Curriculum sequence plan failed for package %s — proceeding without plan", package_id
        )
        return {}, {}


def generate_v2_instructional_artifact(
    db: Session,
    *,
    settings: Settings,
    user: User,
    tenant_id: uuid.UUID,
    package_id: uuid.UUID,
    artifact_type: str,
    generation_context: dict[str, Any],
    week: dict[str, Any],
    subject_meta: dict[str, Any] | None = None,
    week_subject: dict[str, Any] | None = None,
    day_label: str | None = None,
    title_hint: str | None = None,
    introduced_vocabulary: list[str] | None = None,
    instructional_day_number: int | None = None,
    total_planned_days: int | None = None,
) -> dict[str, Any] | None:
    if not is_teacher_assist_real_ai_active(db, settings):
        return None

    effective_settings = resolve_teacher_assist_settings(db, settings)
    assert_teacher_assist_ai_cost_available(db, effective_settings)
    provider_name, _api_key, _base_url = _provider_api_params(effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)
    feature = V2_PACKAGE_ARTIFACT_FEATURES.get(
        artifact_type, V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE
    )

    # Surface pacing guide objectives and grounding fields at the top level so the AI
    # instruction directives can reference them by name without the model having to
    # discover them buried inside week_subject.pacing_context.
    resolved_objectives = [
        {
            "code": obj.get("objective_code"),
            "description": obj.get("description"),
            "is_required": bool(obj.get("is_required", True)),
        }
        for obj in (week_subject or {}).get("objectives") or []
        if obj.get("objective_code") or obj.get("description")
    ]
    pacing_ctx = (week_subject or {}).get("pacing_context") or {}
    resolved_daily_topics = [
        str(day["daily_topic"]) for day in pacing_ctx.get("days") or [] if day.get("daily_topic")
    ]
    resolved_assessment_checks = [
        str(day["assessment_check"])
        for day in pacing_ctx.get("days") or []
        if day.get("assessment_check")
    ]
    resolved_day_plan = (
        resolve_pacing_day_plan(week_subject, day_label) if week_subject and day_label else None
    )

    # Build named_books_and_texts from the instructional_resource_bank when available.
    # The bank contains actual book/text titles extracted from curriculum PDFs (not filenames).
    # Fall back to pacing material metadata only when the bank is empty.
    _resource_bank = generation_context.get("instructional_resource_bank") or {}
    _seen_books: set[str] = set()
    _fallback_books: list[str] = []
    for _mat in generation_context.get("pacing_materials") or []:
        _title = _mat.get("title") or _mat.get("display_name") or _mat.get("original_filename")
        if _title and _title not in _seen_books:
            _seen_books.add(_title)
            _fallback_books.append(_title)
    if resolved_day_plan:
        _day_mats = resolved_day_plan.get("materials_needed") or ""
        if _day_mats and _day_mats not in _seen_books:
            _seen_books.add(_day_mats)
            _fallback_books.append(_day_mats)
    for _subj in (week_subject or {}).get("objectives") or []:
        for _bucket in ("attached_files", "reference_links"):
            for _row in pacing_ctx.get(_bucket) or []:
                _t = _row.get("title") or _row.get("display_name")
                if _t and _t not in _seen_books:
                    _seen_books.add(_t)
                    _fallback_books.append(_t)
    total_guide_weeks = generation_context.get("total_guide_weeks")
    current_week_number = (week or {}).get("sequence_number")

    # ── Instructional Design Plan — slice for this artifact ───────────────────────────
    # The plan was generated once before any artifact generation began and stored on
    # the generation_context. Each artifact receives only the slice it needs so prompt
    # size stays bounded while alignment is maintained across the package.
    _idp = generation_context.get("instructional_design_plan") or {}
    _unit_mastery_arc = _idp.get("unit_mastery_arc")
    _kdg = _idp.get("knowledge_dependency_graph") or []
    _subj_name = (subject_meta or {}).get("subject_name") or ""

    # Resolve named books from the resource bank (now that current_week_number and _subj_name
    # are defined). Falls back to the pacing material metadata collected above.
    _named_books = named_books_from_bank(_resource_bank, current_week_number, _fallback_books)

    # ── Strand-aware resource selection ──────────────────────────────────────────────────
    # When the delivery profile specifies multiple strands (e.g. Reading + Writing), each
    # strand gets its own resource selection so the AI doesn't anchor a Writing lesson on a
    # read-aloud resource or vice versa. The primary strand's resource is also exposed as
    # selected_instructional_resource for backward compatibility.
    _delivery_profile = generation_context.get("instructional_delivery_profile") or {}
    _profile_mode = _delivery_profile.get("mode") or "ai_optimized"
    _strand_slots: list[dict[str, Any]] = []
    if _profile_mode != "ai_optimized":
        from oziebot_api.services.teacher_assist_v2.instructional_delivery_profile import (
            get_strand_slots_for_day,
        )

        _strand_slots = (
            get_strand_slots_for_day(_delivery_profile, day_label or "") if day_label else []
        )

    _obj_codes = [o["code"] for o in resolved_objectives if o.get("code")]

    # Per-strand resource map (populated when multiple strands are active)
    _strand_resources: dict[str, dict[str, Any]] = {}
    _strand_adaptation_notes: dict[str, str] = {}

    if len(_strand_slots) >= 2:
        for _slot in _strand_slots:
            _sn = _slot.get("strand_name") or ""
            if not _sn:
                continue
            _sufficiency = check_resource_sufficiency(
                _resource_bank,
                week=current_week_number,
                day=day_label,
                subject_name=_subj_name or None,
                artifact_type=artifact_type,
                strand_name=_sn,
                objective_codes=_obj_codes,
            )
            if _sufficiency.get("selected"):
                _strand_resources[_sn] = _sufficiency["selected"]
            if _sufficiency.get("adaptation_note"):
                _strand_adaptation_notes[_sn] = _sufficiency["adaptation_note"]

    # Primary resource selection (single-strand or first strand when multi-strand)
    _primary_strand = (_strand_slots[0].get("strand_name") or None) if _strand_slots else None
    _primary_sufficiency = check_resource_sufficiency(
        _resource_bank,
        week=current_week_number,
        day=day_label,
        subject_name=_subj_name or None,
        artifact_type=artifact_type,
        strand_name=_primary_strand,
        objective_codes=_obj_codes,
    )
    _selected_resource = _primary_sufficiency.get("selected")
    _resource_adaptation_note = _primary_sufficiency.get("adaptation_note")
    _resource_sufficient = _primary_sufficiency.get("sufficient", True)

    _instructional_design_week: dict[str, Any] | None = None
    _instructional_contracts: dict[str, Any] | None = None
    _district_anchors_slice: dict[str, Any] | None = None
    _instructional_design_day: Any = (
        None  # dict for single-subject; dict[str,dict] for daily_lesson_plan
    )

    if current_week_number:
        if artifact_type == "daily_lesson_plan":
            # daily_lesson_plan covers all subjects for one day. Build a map so the AI
            # can see each subject's instructional intent for this specific day.
            _subject_names = [
                s.get("subject_name") or ""
                for s in (generation_context.get("subjects") or [])
                if s.get("subject_name")
            ]
            _instructional_design_day = (
                get_plan_for_all_subjects_on_day(
                    _idp, current_week_number, _subject_names, day_label or ""
                )
                if day_label
                else None
            )
            # For the daily lesson plan, week-level design comes from the primary subject
            if _subject_names:
                _instructional_design_week = get_plan_instructional_design_week(
                    _idp, current_week_number, _subject_names[0]
                )
                _district_anchors_slice = get_plan_district_anchors(
                    _idp, current_week_number, _subject_names[0]
                )
        elif _subj_name:
            _instructional_design_week = get_plan_instructional_design_week(
                _idp, current_week_number, _subj_name
            )
            _district_anchors_slice = get_plan_district_anchors(
                _idp, current_week_number, _subj_name
            )
            _instructional_design_day = (
                get_plan_for_day(_idp, current_week_number, _subj_name, day_label)
                if day_label
                else None
            )

        if _instructional_design_week:
            _instructional_contracts = _instructional_design_week.get("instructional_contracts")

    # ── Verified prior learning — deterministic, never inferred ──────────────────────────
    # For day-based artifacts only. Built from the IDP's stored day entries so the AI
    # cannot fabricate continuity statements about content that was never actually taught.
    _DAY_BASED_ARTIFACT_TYPES = frozenset(
        {
            "daily_lesson_plan",
            "student_lesson_deck",
            "exit_ticket",
            "bell_ringer",
        }
    )
    _verified_previous_learning: dict[str, Any] | None = None
    if artifact_type in _DAY_BASED_ARTIFACT_TYPES and current_week_number and day_label:
        if artifact_type == "daily_lesson_plan":
            _vpl_subjects = [
                s.get("subject_name") or ""
                for s in (generation_context.get("subjects") or [])
                if s.get("subject_name")
            ]
        else:
            _vpl_subjects = [_subj_name] if _subj_name else []
        _verified_previous_learning = resolve_verified_previous_learning(
            _idp, current_week_number, _vpl_subjects, day_label
        )

    prompt_payload = {
        "prompt_version": V2_PACKAGE_PROMPT_VERSION,
        "artifact_type": artifact_type,
        "title_hint": title_hint,
        "day_label": day_label,
        # Deterministically selected district resource for this artifact (authoritative).
        # The AI must teach FROM this resource — enriching instruction around it, never replacing it.
        # .allowed_uses lists the classroom activities valid for this resource type.
        # .theme and .topic provide the week's instructional frame for contextual alignment.
        "selected_instructional_resource": _selected_resource,
        # Per-strand resources when the delivery profile has multiple strands.
        # Keys are strand names ("Reading", "Writing"); values are resource entries.
        # When present, each strand block in student_lesson_deck must use its own resource.
        "strand_instructional_resources": _strand_resources or None,
        # Adaptation note when the selected resource type doesn't perfectly match lesson needs.
        # Instructs AI to use the available resource creatively rather than substituting
        # placeholder language.
        "resource_adaptation_note": _resource_adaptation_note,
        "named_books_and_texts": _named_books,
        "school_year": generation_context.get("school_year"),
        "state_id": generation_context.get("state_id"),
        "district_id": generation_context.get("district_id"),
        "school_id": generation_context.get("school_id"),
        "grade_id": generation_context.get("grade_id"),
        "grade_code": generation_context.get("grade_code"),
        "grade_display_name": generation_context.get("grade_display_name"),
        "subjects": generation_context.get("subjects"),
        "pacing_guide_ids": generation_context.get("pacing_guide_ids"),
        # Pacing guide duration context — tells AI where we are in the learning arc.
        "total_guide_weeks": total_guide_weeks,
        "current_week_number": current_week_number,
        "week": week,
        "subject": subject_meta,
        "week_subject": week_subject,
        # Top-level grounding fields — referenced directly by the instruction directives.
        "resolved_objectives": resolved_objectives,
        "resolved_daily_topics": resolved_daily_topics,
        "resolved_assessment_checks": resolved_assessment_checks,
        "resolved_day_plan": resolved_day_plan,
        "pacing_materials": generation_context.get("pacing_materials"),
        "district_materials_summary": generation_context.get("district_materials_summary"),
        "district_document_context": generation_context.get("district_document_context"),
        "district_link_context": generation_context.get("district_link_context"),
        "teacher_supplemental_files": generation_context.get("teacher_supplemental_files"),
        "teacher_supplemental_links": generation_context.get("teacher_supplemental_links"),
        "teacher_supplemental_notes": generation_context.get("teacher_supplemental_notes"),
        "teacher_document_context": generation_context.get("teacher_document_context"),
        "teacher_link_context": generation_context.get("teacher_link_context"),
        "ai_readiness_summary": generation_context.get("ai_readiness_summary"),
        "selected_output_types": generation_context.get("selected_output_types"),
        "teaching_order": generation_context.get("teaching_order"),
        "week_curriculum_plan": generation_context.get("week_curriculum_plan"),
        "generation_mode": generation_context.get("generation_mode"),
        "teacher_generation_notes": generation_context.get("teacher_generation_notes"),
        "existing_package_assignments": generation_context.get("existing_package_assignments"),
        "require_distinct_from_existing": generation_context.get("require_distinct_from_existing"),
        # ── Instructional Design Plan slices ──────────────────────────────────────────
        # unit_mastery_arc and knowledge_dependency_graph are package-level; every
        # artifact receives them to maintain unit coherence and address misconceptions.
        # district_anchors, instructional_design_week, and instructional_design_day
        # are the specific slices for this artifact's week + subject + day.
        "unit_mastery_arc": _unit_mastery_arc,
        "knowledge_dependency_graph": _kdg,
        "district_anchors": _district_anchors_slice,
        "instructional_design_week": _instructional_design_week,
        "instructional_design_day": _instructional_design_day,
        # instructional_contracts extracted as a top-level field so the AI can reference
        # it directly without navigating the full instructional_design_week structure.
        "instructional_contracts": _instructional_contracts,
        # Instructional Delivery Profile — classroom strand constraint. When present and
        # mode is not ai_optimized, the AI must generate all listed strands for today.
        # The constraint_directive inside this dict states the rule in plain language.
        "instructional_delivery_context": build_artifact_delivery_context(
            generation_context.get("instructional_delivery_profile"),
            day_label,
        ),
        # Academic vocabulary introduced by this point in the unit (Phase 0e registry).
        # Only populated for assessment artifact types; None for others.
        "introduced_vocabulary": introduced_vocabulary or [],
        # Deterministic record of prior-day learning from the IDP.
        # None on Monday or when no prior IDP data exists for the previous day.
        # The AI MUST use this as the sole source for any "yesterday we..." references.
        "verified_previous_learning": _verified_previous_learning,
        # Variety context from the VarietyTracker — instructs AI to avoid repeating
        # strategies and image concepts already used in prior artifacts.
        # None on the first artifact in the package (no history yet).
        "instructional_variety": generation_context.get("variety_context") or None,
        # Strand learning journey context (Phase 0f). Provides the cross-week arc for each
        # strand so the AI knows which objective, resource, and mastery gate apply today.
        # None until Phase 0f completes; artifacts fall back gracefully when absent.
        "strand_learning_journeys": generation_context.get("strand_learning_journeys") or None,
        # Instructional day position (v3 pacing engine). Present for daily_lesson_plan artifacts.
        "instructional_day_number": instructional_day_number,
        "total_planned_days": total_planned_days or generation_context.get("total_planned_days"),
    }

    instruction = _instruction_for_artifact(artifact_type) + _CURRICULUM_FIDELITY_DIRECTIVE
    if artifact_type == "daily_lesson_plan":
        instruction = _CLASSROOM_AUTHENTICITY_DIRECTIVE + instruction
    if artifact_type in _DAY_BASED_ARTIFACT_TYPES:
        instruction = _VERIFIED_PRIOR_LEARNING_DIRECTIVE + "\n\n" + instruction

    # Instructional Day header — prepended for day-based artifacts when pacing data is present.
    if instructional_day_number and (
        total_planned_days or generation_context.get("total_planned_days")
    ):
        _total = total_planned_days or generation_context.get("total_planned_days")
        _id_header = (
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"INSTRUCTIONAL DAY {instructional_day_number} OF {_total}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"This is Instructional Day {instructional_day_number} of {_total} planned instructional days.\n"
            f"Calendar reference: {day_label} of Week {week.get('sequence_number', '?')} "
            f"(for teacher reference only — do not use weekday names in generated content).\n"
            f"Generated artifacts must reference 'Instructional Day {instructional_day_number}' "
            f"— not 'Monday', 'Tuesday', or any calendar weekday.\n\n"
        )
        instruction = _id_header + instruction
    # Inject CIP directive whenever any strand has a delivery_mode, curriculum_text_access, or
    # independent_reading_access configured (also check legacy student_text_access for compat)
    _delivery_ctx = prompt_payload.get("instructional_delivery_context")
    if _delivery_ctx:
        _slots_today = _delivery_ctx.get("strand_slots_today") or []
        _has_cip = any(
            s.get("delivery_mode")
            or s.get("curriculum_text_access")
            or s.get("independent_reading_access")
            or s.get("student_text_access")
            or s.get("closure_required")
            for s in _slots_today
        )
        if _has_cip:
            instruction = _CLASSROOM_INSTRUCTION_PROFILE_DIRECTIVE + instruction
    # Inject variety directive when strategy history exists (not the first artifact)
    _variety_ctx = generation_context.get("variety_context")
    if _variety_ctx and (
        _variety_ctx.get("recent_strategies") or _variety_ctx.get("recent_slide_layouts")
    ):
        instruction = _build_variety_directive(_variety_ctx) + instruction
    # Inject strand learning journey position context for day-based artifacts (Phase 0f)
    if artifact_type in {"daily_lesson_plan", "student_lesson_deck"}:
        _slj = generation_context.get("strand_learning_journeys")
        if _slj:
            from oziebot_api.services.teacher_assist_v2.strand_learning_journeys import (
                build_journey_position_context,
            )

            _journey_ctx = build_journey_position_context(
                _slj,
                week=current_week_number,
                day_label=day_label,
                subject_name=_subj_name if artifact_type != "daily_lesson_plan" else None,
            )
            if _journey_ctx:
                instruction = _journey_ctx + "\n\n" + instruction
    if artifact_type == "student_lesson_deck":
        instruction += _build_image_search_directive(
            grade_code=generation_context.get("grade_code"),
            grade_display_name=generation_context.get("grade_display_name"),
            subject_name=(subject_meta or {}).get("subject_name"),
        )
        _delivery_ctx_for_deck = prompt_payload.get("instructional_delivery_context")
        _strand_override = _build_strand_deck_directive(_delivery_ctx_for_deck)
        if _strand_override:
            instruction += _strand_override
    if generation_context.get("generation_mode") == "package_additional_assignment":
        instruction += (
            " This is an ADDITIONAL assignment for an existing instructional package. "
            "It must be clearly different from existing_package_assignments in focus, format, and tasks. "
            "Follow teacher_generation_notes closely."
        )
    result = execute_openai_json_completion(
        effective_settings,
        model_name=model_name,
        instruction=instruction,
        prompt_payload=prompt_payload,
        required_output_schema=_schema_for_artifact(artifact_type),
        _api_key=_api_key,
        _base_url=_base_url,
        _provider=provider_name,
    )
    record_teacher_assist_ai_usage(
        db,
        tenant_id=tenant_id,
        user_id=user.id,
        feature=feature,
        provider=result.provider,
        model=result.model,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        estimated_cost_cents=result.estimated_cost_cents,
        metadata={
            "operation_type": feature,
            "artifact_type": artifact_type,
            "package_id": str(package_id),
            "related_entity_type": "instructional_package",
            "related_entity_id": str(package_id),
            "teacher_review_required": True,
        },
    )
    content = _normalize_artifact_content(artifact_type, result.content_json)

    # ── Resource sufficiency flag ─────────────────────────────────────────────────────────
    # When no curriculum resource exists for this lesson, flag for teacher review immediately.
    # The artifact is still generated (AI has permission to write general content per the
    # curriculum fidelity directive) but the teacher must verify before using.
    if not _resource_sufficient:
        content["_needs_review"] = True
        content["_review_reason"] = "no_curriculum_resource_for_lesson"

    # ── Curriculum fidelity validation ───────────────────────────────────────────────────
    # When a curriculum resource exists, generic placeholder language (e.g. "choose a story")
    # is a generation failure. Retry once with an amplified directive; if it still appears,
    # flag the artifact for teacher review rather than silently returning bad content.
    _has_curriculum_resource = bool(
        _selected_resource or prompt_payload.get("named_books_and_texts")
    )
    if _has_curriculum_resource and _has_placeholder_language(content):
        _retry_result = execute_openai_json_completion(
            effective_settings,
            model_name=model_name,
            instruction=instruction + _CURRICULUM_FIDELITY_RETRY_ADDENDUM,
            prompt_payload=prompt_payload,
            required_output_schema=_schema_for_artifact(artifact_type),
            _api_key=_api_key,
            _base_url=_base_url,
            _provider=provider_name,
        )
        record_teacher_assist_ai_usage(
            db,
            tenant_id=tenant_id,
            user_id=user.id,
            feature=feature,
            provider=_retry_result.provider,
            model=_retry_result.model,
            input_tokens=_retry_result.input_tokens,
            output_tokens=_retry_result.output_tokens,
            estimated_cost_cents=_retry_result.estimated_cost_cents,
            metadata={
                "operation_type": feature,
                "artifact_type": artifact_type,
                "package_id": str(package_id),
                "related_entity_type": "instructional_package",
                "related_entity_id": str(package_id),
                "teacher_review_required": True,
                "is_fidelity_retry": True,
            },
        )
        retry_content = _normalize_artifact_content(artifact_type, _retry_result.content_json)
        if not _has_placeholder_language(retry_content):
            content = retry_content
        else:
            # Both attempts produced placeholder language — flag for teacher review.
            content["_needs_review"] = True
            content["_review_reason"] = "placeholder_language_detected_after_retry"

    # ── Curriculum text access validation ────────────────────────────────────────────────
    # When any active strand has curriculum_text_access=teacher_copy_only, scan for
    # violations that would direct students to independently access the curriculum text.
    # These are CRITICAL errors — the teacher cannot physically use the artifact.
    _slots_today = (_delivery_ctx or {}).get("strand_slots_today") or []
    _teacher_copy_slots = [
        s for s in _slots_today if s.get("curriculum_text_access") == "teacher_copy_only"
    ]
    if _teacher_copy_slots:
        # Collect curriculum text names from the prompt payload for name-specific pattern matching
        _curriculum_titles: list[str] = []
        if _selected_resource:
            _curriculum_titles.append(_selected_resource.get("title") or "")
        for _entry in prompt_payload.get("named_books_and_texts") or []:
            if isinstance(_entry, dict):
                _curriculum_titles.append(_entry.get("title") or "")
            elif isinstance(_entry, str):
                _curriculum_titles.append(_entry)
        _curriculum_titles = [t.strip() for t in _curriculum_titles if t and t.strip()]

        _cta_violations = check_curriculum_text_access_violations(
            content,
            curriculum_text_names=_curriculum_titles,
        )
        if _cta_violations:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "curriculum_text_access=teacher_copy_only violation detected in %s "
                "(package %s): %d violation(s): %s",
                artifact_type,
                package_id,
                len(_cta_violations),
                _cta_violations[:3],
            )
            content["_needs_review"] = True
            content["_review_reason"] = "curriculum_text_access_violation"
            content["_review_detail"] = (
                f"{len(_cta_violations)} violation(s) detected. "
                "Students were directed to access the curriculum text, but teacher_copy_only is set."
            )

    # ── Lesson plan completeness validation ──────────────────────────────────────────────
    # For daily_lesson_plan artifacts: verify all 9 rhythm steps are populated.
    # Incomplete lesson plans flag for teacher review but are still returned —
    # a partial plan is better than nothing when content is present.
    if artifact_type == "daily_lesson_plan":
        _missing = _check_lesson_plan_completeness(content)
        if _missing:
            logger.warning(
                "package=%s: lesson plan completeness check found %d missing field(s): %s",
                package_id,
                len(_missing),
                _missing[:5],
            )
            # Classify the primary reason — learning goal violations take priority over generic structure issues
            _goal_violations = [
                m
                for m in _missing
                if any(
                    kw in m
                    for kw in (
                        "learning_goals",
                        "learning goal",
                        "Writing goals",
                        "Reading goals",
                        "Writing block",
                        "Reading block",
                        "closure assesses",
                        "separate goals",
                        "'I can'",
                        "merged",
                    )
                )
            ]
            if not content.get("_needs_review"):
                content["_needs_review"] = True
                if _goal_violations:
                    content["_review_reason"] = "learning_goal_discipline_violation"
                else:
                    content["_review_reason"] = "incomplete_lesson_structure"
            elif (
                _goal_violations and content.get("_review_reason") == "incomplete_lesson_structure"
            ):
                content["_review_reason"] = "learning_goal_discipline_violation"
            content["_completeness_issues"] = _missing[:10]

    if artifact_type == "student_lesson_deck":
        _missing = _check_student_deck_completeness(content)
        if _missing:
            logger.warning(
                "package=%s: student lesson deck missing preteach slide: %s",
                package_id,
                _missing,
            )
            if not content.get("_needs_review"):
                content["_needs_review"] = True
                content["_review_reason"] = "missing_preteach_slide"
            content["_completeness_issues"] = _missing[:5]

    return content
