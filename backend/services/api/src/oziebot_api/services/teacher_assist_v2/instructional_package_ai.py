"""OpenAI generation for TeacherAssist v2 instructional package artifacts."""

from __future__ import annotations

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

from oziebot_api.models.teacher_assist_v2_document_extraction import TeacherAssistV2DocumentExtraction
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
)
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import resolve_pacing_day_plan

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
            "objective": "string",
            "mini_lesson": "string",
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
            "performance_levels": [{"label": "string", "description": "string", "points": "number"}],
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
    "NAMED BOOKS AND TEXTS — APPROVED RESOURCE POOL:\n"
    "week_curriculum_plan.mentor_texts, full_curriculum_documents, named_books_and_texts, "
    "and pacing_materials list books curated by the district because they are relevant to the "
    "objectives and unit theme. Use these books as your resource pool:\n"
    "  - SELECT from this list when the lesson calls for a read-aloud, text reference, or example — "
    "you do NOT need to use every book listed\n"
    "  - Reference any chosen book by its EXACT title — never say 'a book we are reading' or 'a text'\n"
    "  - NEVER invent a book title or substitute a book not in the curriculum materials\n"
    "  - When you do use a book, draw on it specifically: character names, events, vocabulary, themes — "
    "content specific enough that a student who read it recognizes it\n"
    "  - If no books are listed, derive titles from district_document_context or resolved_day_plan.materials_needed"
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
    grade_label = grade_display_name or (f"Grade {grade_code}" if grade_code else "the assigned grade")
    _gc = (grade_code or "5").strip().upper()

    if _gc in ("K", "1", "2"):
        age_desc = "5–7 years old"
        inject_prefix = "young children kindergarten early elementary"
        example_prefix = "young children"
        age_kws = "'children', 'kids', 'young', 'elementary', 'kindergarten'"
        photo_type = "illustration"
        grade_band = "elementary"
    elif _gc in ("3", "4", "5"):
        age_desc = "8–11 years old"
        inject_prefix = "elementary school children"
        example_prefix = "elementary students"
        age_kws = "'children', 'kids', 'elementary', 'students', 'school'"
        photo_type = "photo"
        grade_band = "elementary"
    elif _gc in ("6", "7", "8"):
        age_desc = "11–14 years old"
        inject_prefix = "middle school students"
        example_prefix = "middle school students"
        age_kws = "'middle school', 'students', 'tweens', 'school'"
        photo_type = "photo"
        grade_band = "middle"
    else:
        age_desc = "14–18 years old"
        inject_prefix = "high school students"
        example_prefix = "high school students"
        age_kws = "'high school', 'students', 'teenagers', 'teen'"
        photo_type = "photo"
        grade_band = "high"

    return (
        f"\nIMAGE SEARCH — STRICT RULES for {grade_label} ({age_desc}):\n"
        f"  RULE 1 — EVERY search term MUST contain at least one of: {age_kws}. "
        "NO EXCEPTIONS. Abstract terms with no age/grade context return adult stock photos or diagrams.\n"
        "  RULE 2 — Pair every concept with age context:\n"
        f"    BAD: 'main idea diagram'  →  GOOD: 'main idea organizer {example_prefix} classroom'\n"
        f"    BAD: 'students discussing'  →  GOOD: '{grade_label.lower()} students turn and talk partners'\n"
        f"    BAD: 'reading comprehension'  →  GOOD: '{example_prefix} reading books classroom'\n"
        "  RULE 3 — NEVER use: 'adult professional', 'business meeting', 'office', "
        "'generic education stock', 'presentation clipart'.\n"
        f"- visual.image_search.search_terms: 3–5 terms — EVERY term must follow RULE 1\n"
        f"- visual.image_search.target_grade_band: '{grade_band}'\n"
        f"- visual.image_search.preferred_image_type: '{photo_type}'\n"
        "- visual.image_search.image_alt_text: descriptive alt text for accessibility\n"
        "- visual.image_search.educational_purpose: one sentence on WHY this image teaches the concept\n"
        "- visual.organizer_data: ONLY for non-image types:\n"
        "  concept_map: {center_concept, branches: [{label, items: [string]}]}\n"
        "  venn: {left_label, right_label, left_items: [], overlap_items: [], right_items: []}\n"
        "  process_flow: {steps: [{number, label, description}]}\n"
        "  timeline: {events: [{label, date_or_label, description}]}\n"
        "  comparison_table: {col_a_label, col_b_label, rows: [{a, b}]}\n"
    )


def _instruction_for_artifact(artifact_type: str) -> str:
    instructions = {
        "daily_lesson_plan": (
            "Generate a COMPLETE, teacher-ready daily lesson plan a teacher can follow verbatim in the classroom. "
            "For EACH subject block write FULL, substantive content in every field — no placeholders, no generic filler:\n"
            "- direct_instruction: A 4-6 sentence teaching script the teacher reads aloud. Explain the concept "
            "clearly, give a concrete definition, walk through a specific example drawn from named books or texts, "
            "and connect it to students' experience.\n"
            "- mini_lesson: An engaging 2-3 minute hook that activates prior knowledge or sparks curiosity — "
            "reference the specific book, character, or concept students are studying.\n"
            "- guided_practice: A specific, step-by-step collaborative activity. Name the activity, describe "
            "each step, state what the teacher models, and tie it to a specific passage or concept from named materials.\n"
            "- independent_practice: The exact task students complete on their own. Write the student-facing "
            "instructions they would read. Reference the named books or objectives specifically.\n"
            "- checks_for_understanding: 3 specific questions the teacher asks — one after direct instruction, "
            "one mid-practice, one before closure. Questions must probe the specific objective and named content.\n"
            "- closure: How the lesson ends — a specific exit prompt or reflection question tied to today's objective.\n"
            "- materials: List actual named materials from pacing_materials and resolved_day_plan.materials_needed.\n"
            "- notes: Differentiation tips, pacing reminders, and key misconceptions to watch for.\n"
            + _WEEK_PLAN_DIRECTIVE + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _CLASS_TIME_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
            + _GRADE_RIGOR_DIRECTIVE + "\n"
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
            "  • Instructional rhythm: the deck arcs from curiosity → modeling → practice → reflection; "
            "students should feel the lesson building, not a list of topics\n\n"

            "SLIDE COUNT: Target 6–8 slides. The lesson structure drives the count — "
            "do not pad slides to reach 8 and do not compress distinct instructional moments into fewer. "
            "A 5-slide lesson that teaches well is better than a 9-slide lesson that dilutes focus.\n\n"

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

            "DAY-SPECIFIC LESSON FORMAT — select the format that matches day_label:\n"
            "Monday   → Reading Workshop: connection to prior reading → read-aloud excerpt or book introduction "
            "→ teaching point (literary element, strategy) → turn & talk → independent reading prompt → share\n"
            "Tuesday  → Deep Reading/Analysis: revisit text → analyze literary elements (character, theme, plot) "
            "→ text evidence activity → partner discussion → Reader’s Notebook response → debrief\n"
            "Wednesday → Writing Workshop: connection to mentor text or read-aloud → "
            "teaching point (craft/structure) → teacher models writing → students try it → "
            "share one example → independent writing link\n"
            "Thursday → Word Study + Revision: word pattern or vocabulary from the text → "
            "sort/categorize → use in writing context → revise/edit a sentence → writer’s notebook\n"
            "Friday   → Celebration & Synthesis: student share (author’s chair / book talk) → "
            "week recap (what we read, wrote, learned) → reflection → next steps preview\n\n"

            "NEVER produce the same hook→vocabulary→concept→example→your_turn→check_in→wrap_up "
            "sequence every day. Vary the slide structure to match the day’s workshop format above.\n\n"

            "SLIDE TYPES:\n"
            "  hook | connection | today_we_learn | teaching_point | read_aloud | vocabulary |\n"
            "  word_study | concept | example | active_engagement | your_turn | guided_practice |\n"
            "  independent_practice | discussion | share | check_in | exit_ticket | wrap_up\n"
            "  slide_type = ‘connection’ for the opening hook/link to prior learning\n"
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

            + _STUDENT_CONTENT_RULES_DIRECTIVE + "\n"
            + _WEEK_PLAN_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
            + _GRADE_RIGOR_DIRECTIVE + "\n"
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
            + _BOOK_GROUNDING_DIRECTIVE + " "
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
            + _STUDENT_CONTENT_RULES_DIRECTIVE + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "rubric": (
            "Generate a rubric whose criteria map directly to resolved_objectives — required TEKS first.\n"
            "Name each criterion after the specific skill or objective it measures. "
            "Performance level descriptions must reference what students do with the actual content — "
            "not generic phrases like 'demonstrates understanding'.\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
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
            + _STUDENT_CONTENT_RULES_DIRECTIVE + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "writing_response": (
            "Generate a writing response prompt scaffolded around curriculum documents and required TEKS.\n"
            "writing_prompt: A specific task tied to the required TEKS and the curriculum materials — "
            "reference specific books, passages, or concepts from district_document_context.\n"
            "student_instructions: Step-by-step directions (plan → draft → revise).\n"
            "sentence_starters: 4-6 frames that scaffold the TEKS language and curriculum vocabulary.\n"
            "success_criteria: 4-5 specific, observable criteria tied to required TEKS.\n"
            + _STUDENT_CONTENT_RULES_DIRECTIVE + "\n"
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
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
        + _BOOK_GROUNDING_DIRECTIVE + " "
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
            {"heading": "What students will learn", "bullets": normalized.get("what_students_will_learn") or []},
            {"heading": "Reminders", "bullets": normalized.get("reminders") or []},
            {"heading": "Upcoming focus", "bullets": normalized.get("upcoming_focus") or []},
        ]
    if not normalized.get("sections") and artifact_type not in {"daily_lesson_plan", "subject_slide_deck", "student_lesson_deck"}:
        normalized.setdefault(
            "sections",
            [{"heading": "Overview", "body": normalized.get("summary") or "Generated instructional resource."}],
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
) -> dict[str, Any]:
    """One-shot AI call that reads the full curriculum and returns a fixed week-by-week
    teaching sequence plan. Called ONCE before any artifact generation so every artifact
    is grounded in a consistent, pedagogically-ordered plan rather than each week
    independently deciding what to emphasize.

    Returns a dict keyed by week number (int): {1: {...}, 2: {...}, ...}.
    Falls back to an empty dict on any failure — callers must handle gracefully.
    """
    if not is_teacher_assist_real_ai_active(db, settings):
        return {}

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
                full_curriculum_docs.append({
                    "title": _mat.get("title"),
                    "filename": _mat.get("original_filename"),
                    "resource_type": _mat.get("resource_type"),
                    "text": _text[:_FULL_DOC_CHAR_LIMIT],
                    "truncated": len(_text) > _FULL_DOC_CHAR_LIMIT,
                })

    grade_clause = (
        f" This curriculum is for Grade {grade_code} ({grade_display_name}). "
        "Calibrate all week themes, mentor text selections, and activity complexity to that grade level."
        if grade_code else ""
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
        return {int(entry["week"]): entry for entry in sequence if "week" in entry}
    except Exception:
        import logging
        logging.getLogger(__name__).exception(
            "Curriculum sequence plan failed for package %s — proceeding without plan", package_id
        )
        return {}


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
) -> dict[str, Any] | None:
    if not is_teacher_assist_real_ai_active(db, settings):
        return None

    effective_settings = resolve_teacher_assist_settings(db, settings)
    assert_teacher_assist_ai_cost_available(db, effective_settings)
    provider_name, _api_key, _base_url = _provider_api_params(effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name=provider_name)
    feature = V2_PACKAGE_ARTIFACT_FEATURES.get(artifact_type, V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE)

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
        str(day["daily_topic"])
        for day in pacing_ctx.get("days") or []
        if day.get("daily_topic")
    ]
    resolved_assessment_checks = [
        str(day["assessment_check"])
        for day in pacing_ctx.get("days") or []
        if day.get("assessment_check")
    ]
    resolved_day_plan = resolve_pacing_day_plan(week_subject, day_label) if week_subject and day_label else None

    # Collect all named books/texts from pacing materials and day plan so they appear
    # at the top level of the prompt — makes it unambiguous to the model which specific
    # books it should draw on for grounding.
    _seen_books: set[str] = set()
    _named_books: list[str] = []
    for _mat in (generation_context.get("pacing_materials") or []):
        _title = (
            _mat.get("title")
            or _mat.get("display_name")
            or _mat.get("original_filename")
        )
        if _title and _title not in _seen_books:
            _seen_books.add(_title)
            _named_books.append(_title)
    if resolved_day_plan:
        _day_mats = resolved_day_plan.get("materials_needed") or ""
        if _day_mats and _day_mats not in _seen_books:
            _seen_books.add(_day_mats)
            _named_books.append(_day_mats)
    for _subj in (week_subject or {}).get("objectives") or []:
        for _bucket in ("attached_files", "reference_links"):
            for _row in (pacing_ctx.get(_bucket) or []):
                _t = _row.get("title") or _row.get("display_name")
                if _t and _t not in _seen_books:
                    _seen_books.add(_t)
                    _named_books.append(_t)

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

    _instructional_design_week: dict[str, Any] | None = None
    _instructional_contracts: dict[str, Any] | None = None
    _district_anchors_slice: dict[str, Any] | None = None
    _instructional_design_day: Any = None  # dict for single-subject; dict[str,dict] for daily_lesson_plan

    if current_week_number:
        if artifact_type == "daily_lesson_plan":
            # daily_lesson_plan covers all subjects for one day. Build a map so the AI
            # can see each subject's instructional intent for this specific day.
            _subject_names = [
                s.get("subject_name") or ""
                for s in (generation_context.get("subjects") or [])
                if s.get("subject_name")
            ]
            _instructional_design_day = get_plan_for_all_subjects_on_day(
                _idp, current_week_number, _subject_names, day_label or ""
            ) if day_label else None
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
                if day_label else None
            )

        if _instructional_design_week:
            _instructional_contracts = _instructional_design_week.get("instructional_contracts")

    prompt_payload = {
        "prompt_version": V2_PACKAGE_PROMPT_VERSION,
        "artifact_type": artifact_type,
        "title_hint": title_hint,
        "day_label": day_label,
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
        # Academic vocabulary introduced by this point in the unit (Phase 0e registry).
        # Only populated for assessment artifact types; None for others.
        "introduced_vocabulary": introduced_vocabulary or [],
    }

    instruction = _instruction_for_artifact(artifact_type)
    if artifact_type == "student_lesson_deck":
        instruction += _build_image_search_directive(
            grade_code=generation_context.get("grade_code"),
            grade_display_name=generation_context.get("grade_display_name"),
            subject_name=(subject_meta or {}).get("subject_name"),
        )
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
    return _normalize_artifact_content(artifact_type, result.content_json)
