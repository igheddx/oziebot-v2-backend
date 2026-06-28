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
from oziebot_api.services.teacher_assist.openai_json_client import execute_openai_json_completion
from oziebot_api.services.teacher_assist.prompt_contracts import (
    V2_INSTRUCTIONAL_PACKAGE_GENERATION_FEATURE,
    V2_PACKAGE_ARTIFACT_FEATURES,
)
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_provider_model
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist_v2.pacing_plan_resolver import resolve_pacing_day_plan

V2_PACKAGE_PROMPT_VERSION = "v2-instructional-package-v1"

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

_BOOK_GROUNDING_DIRECTIVE = (
    "NAMED BOOKS AND TEXTS — USE SPECIFICALLY:\n"
    "If books, texts, articles, or named curriculum resources appear in named_books_and_texts, "
    "district_document_context, pacing_materials, or resolved_day_plan.materials_needed, draw FULLY on "
    "your knowledge of those works. Use specific titles, character names, plot events, vocabulary, themes, "
    "and arguments. Content must be SPECIFIC enough that a student who read those books recognizes it. "
    "Do NOT write generic content that ignores named materials."
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
            + _CURRICULUM_DOCUMENT_DIRECTIVE + "\n"
            + _CLASS_TIME_DIRECTIVE + "\n"
            + _BOOK_GROUNDING_DIRECTIVE + "\n"
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
        ),
        "student_lesson_deck": (
            "Generate a STUDENT-FACING lesson presentation projected on a classroom screen. "
            "This is a VISUAL-FIRST teaching tool — images are first-class teaching elements that must occupy "
            "30–60% of each slide. Students must be able to understand the concept from the visual alone "
            "BEFORE the teacher speaks.\n\n"
            "DESIGN RULES:\n"
            "- Max 35 words of body text per slide. Never write walls of text.\n"
            "- Max 3–4 bullets per slide. Each bullet max 12 words.\n"
            "- Every slide MUST have a `visual` block. Slides without a visual are INVALID.\n"
            "- Design for the grade level in grade_id. Use grade-appropriate vocabulary and image choices.\n\n"
            "BEFORE GENERATING EACH SLIDE, ASK: ‘What image would a student in the back row of a classroom "
            "immediately understand about this concept?’\n\n"
            "For EACH slide populate ALL fields:\n"
            "- id: unique slug like ‘slide-1’, ‘slide-2’, etc.\n"
            "- slide_type: one of: hook|today_we_learn|vocabulary|concept|example|your_turn|check_in|wrap_up\n"
            "- layout: REQUIRED. Choose the layout that matches the slide type and visual:\n"
            "  ‘hook_full_image’ — hook/intro slide with dominant image (image = top 60% of screen)\n"
            "  ‘title_full’ — full-screen background image with overlaid title (hook, wrap_up)\n"
            "  ‘objective_image’ — today_we_learn: objectives left, image right\n"
            "  ‘vocabulary_showcase’ — word banner top, image + definition below\n"
            "  ‘teacher_modeling’ — image left, numbered steps right\n"
            "  ‘before_after’ — before card / arrow / after card (for example/comparison slides)\n"
            "  ‘organizer_full’ — graphic organizer fills most of screen (concept map, Venn, timeline)\n"
            "  ‘guided_practice_image’ — your_turn: title top, image middle, student action strip bottom\n"
            "  ‘discussion_image’ — large discussion question left, image right\n"
            "  ‘exit_ticket_image’ — check_in/exit ticket: question left, image right\n"
            "  ‘text_left_image_right’ — general concept slide with equal text/image split\n"
            "- title: Student-friendly heading, max 10 words\n"
            "- body: Max 35 words, grade-appropriate, connects to named books/texts\n"
            "- bullets: 3–4 short student-friendly phrases (not objectives), max 12 words each\n"
            "- engagement.type: one of: think_pair_share|turn_and_talk|show_fingers|whiteboard|quick_draw|exit_ticket\n"
            "- engagement.prompt: The exact student action (‘Turn to a partner and explain...’, ‘Show me 1–5 fingers...’, etc.)\n"
            "- visual.type: ALWAYS ‘image_search’ (the backend fetches real Pixabay images). "
            "ONLY use ‘concept_map’|’venn’|’process_flow’|’timeline’|’comparison_table’ "
            "for abstract diagrams or organizers students fill in — not for real-world content that a photo conveys better.\n"
            "- visual.placement: full_width (for hook_full_image/title_full)|right|left\n"
            "- visual.fallback_organizer_type: organizer type if no image found (concept_map, venn, process_flow, etc.)\n"
            "- visual.image_search.search_terms: 3–5 SPECIFIC search terms that will retrieve a REAL photo or illustration.\n"
            "  NEVER search by slide title alone. Build terms from: [grade] + [subject] + [TEKS topic] + [image type].\n"
            "  BAD: ‘Main Idea’. GOOD: ‘grade 5 students reading classroom discussion informational text’.\n"
            "  BAD: ‘Vocabulary’. GOOD: ‘grade 5 ELA vocabulary word wall classroom colorful’.\n"
            "  GRADE-AWARE EXAMPLES:\n"
            "    Grade K–2: ‘colorful [topic] illustration children friendly cartoon’\n"
            "    Grade 3–5: ‘[topic] elementary classroom students learning photo’\n"
            "    Grade 6–8: ‘[topic] middle school historical photo diagram’\n"
            "    Grade 9–12: ‘[topic] academic photo literary analysis historical’\n"
            "- visual.image_search.target_grade_band: elementary (K–5)|middle (6–8)|high (9–12)\n"
            "- visual.image_search.preferred_image_type: illustration (K–3)|photo (4–8)|photo or diagram (9–12)\n"
            "- visual.image_search.image_alt_text: specific descriptive alt text (e.g., ‘Two students sitting at desks comparing texts’)\n"
            "- visual.image_search.educational_purpose: one sentence on WHY this specific image teaches the concept\n"
            "- visual.organizer_data: ONLY for non-image visual types. Populate with actual content:\n"
            "  concept_map: {center_concept, branches: [{label, items: [string]}]}\n"
            "  venn: {left_label, right_label, left_items: [], overlap_items: [], right_items: []}\n"
            "  process_flow: {steps: [{number, label, description}]}\n"
            "  timeline: {events: [{label, date_or_label, description}]}\n"
            "  comparison_table: {col_a_label, col_b_label, rows: [{a, b}]}\n\n"
            "DAY-BY-DAY PROGRESSION — day_label tells you where in the week this lesson falls:\n"
            "Use day_label AND resolved_day_plan to determine today’s specific focus. "
            "Each day’s slides must be a DISTINCT step — Monday introduces, Tuesday practices, "
            "Wednesday applies, Thursday extends, Friday synthesizes. NEVER repeat the same hook, "
            "vocabulary term, example, or your_turn prompt across different days. "
            "If resolved_day_plan has a daily_topic, that becomes the lesson’s central thread. "
            "If resolved_daily_topics lists topics by day, use the one matching today’s day_label.\n\n"
            "BUILD SLIDES IN ORDER:\n"
            "1. hook (slide_type=hook) — Attention-grabbing question or connection tied to TODAY’S specific topic. "
            "Layout: hook_full_image. Image: striking real photo that makes students curious. 1–2 sentences max.\n"
            "2. today_we_learn (slide_type=today_we_learn) — Objectives in kid language reflecting TODAY’s focus. "
            "Layout: objective_image. 3–4 bullets: ‘I can...’ statements. Image: students in a learning/discovery context.\n"
            "3. vocabulary (slide_type=vocabulary, 1–2 slides) — One key term relevant to TODAY’s lesson per slide. "
            "Layout: vocabulary_showcase. body = definition. bullets = [example sentence, connection to text]. Image: visual of the word.\n"
            "4. concept slides (slide_type=concept, 2–3 slides) — ONE concept per slide matching TODAY’s step in the week. "
            "Layout: text_left_image_right or teacher_modeling. 3–4 bullets max. Image: real photo showing the concept.\n"
            "5. example (slide_type=example) — Before/after or worked example grounded in TODAY’s content. "
            "Layout: before_after. Use comparisonPairs with actual content. Image search optional.\n"
            "6. your_turn (slide_type=your_turn) — Student practice that matches TODAY’s depth and concept. "
            "Layout: guided_practice_image. body = the exact task instruction. Image: students working.\n"
            "7. check_in (slide_type=check_in) — Exit ticket checking TODAY’s specific learning. "
            "Layout: exit_ticket_image. body = the question students answer. engagement.type = exit_ticket.\n"
            "8. wrap_up (slide_type=wrap_up) — Celebrate today’s specific learning. Layout: title_full. "
            "Image: achievement/celebration. engagement.type = think_pair_share.\n"
            + _BOOK_GROUNDING_DIRECTIVE + " "
            + _OBJECTIVE_ALIGNMENT_DIRECTIVE
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
    return instructions.get(artifact_type, default)


def _normalize_artifact_content(artifact_type: str, content: dict[str, Any]) -> dict[str, Any]:
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
) -> dict[str, Any] | None:
    if not is_teacher_assist_real_ai_active(db, settings):
        return None

    effective_settings = resolve_teacher_assist_settings(db, settings)
    assert_teacher_assist_ai_cost_available(db, effective_settings)
    model_name = get_teacher_assist_provider_model(effective_settings, provider_name="openai")
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
        "generation_mode": generation_context.get("generation_mode"),
        "teacher_generation_notes": generation_context.get("teacher_generation_notes"),
        "existing_package_assignments": generation_context.get("existing_package_assignments"),
        "require_distinct_from_existing": generation_context.get("require_distinct_from_existing"),
    }

    instruction = _instruction_for_artifact(artifact_type)
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
