"""Classroom-ready deterministic artifact content when real AI is unavailable."""

from __future__ import annotations

import re
from typing import Any

from oziebot_api.services.teacher_assist_v2.planning_constants import WEEKDAY_LABELS
from oziebot_api.services.teacher_assist_v2.slide_visuals import add_slide_visual_metadata


def _objective_mapping(
    objective_code: str | None,
    objective_text: str,
    *,
    objective_ids: list[str] | None = None,
    teks_ids: list[str] | None = None,
    daily_topic: str | None = None,
) -> dict[str, Any]:
    mapping = {
        "objective_code": objective_code,
        "objective_text": objective_text,
        "standard_set": "TEKS" if objective_code else None,
        "objective_ids": list(objective_ids or []),
        "teks_ids": list(teks_ids or ([objective_code] if objective_code else [])),
        "alignment_summary": (
            f"Aligned to {objective_code or 'selected objective'}: {objective_text}"
            if objective_text
            else None
        ),
    }
    if daily_topic:
        mapping["daily_topic"] = daily_topic
    return mapping


def _with_alignment(content: dict[str, Any], mapping: dict[str, Any]) -> dict[str, Any]:
    content["objective_mapping"] = mapping
    content["objective_ids"] = list(mapping.get("objective_ids") or [])
    content["teks_ids"] = list(mapping.get("teks_ids") or [])
    content["alignment_summary"] = mapping.get("alignment_summary")
    return content


def _unique_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _clean_material_label(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    if " (" in normalized and normalized.endswith(")"):
        normalized = normalized.split(" (", 1)[0].strip()
    return normalized or None


def _source_texts_from_materials(materials: list[str] | None) -> list[str]:
    if not materials:
        return []
    source_texts: list[str] = []
    for item in materials:
        cleaned = _clean_material_label(item)
        if not cleaned:
            continue
        parts = [segment.strip() for segment in re.split(r",|\band\b", cleaned) if segment.strip()]
        for part in parts:
            source_texts.append(part)
    return _unique_strings(source_texts)


def _source_excerpts(excerpts: list[str] | None, *, limit: int = 3) -> list[str]:
    cleaned = [" ".join(str(item).split()) for item in excerpts or [] if str(item).strip()]
    return [item[:700] for item in _unique_strings(cleaned)[:limit]]


def _join_human_list(values: list[str]) -> str:
    if not values:
        return ""
    if len(values) == 1:
        return values[0]
    if len(values) == 2:
        return f"{values[0]} and {values[1]}"
    return f"{', '.join(values[:-1])}, and {values[-1]}"


def _daily_rows(
    *,
    daily_topics: list[str] | None = None,
    daily_objectives: list[str] | None = None,
    assessment_checks: list[str] | None = None,
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    max_len = max(
        len(daily_topics or []),
        len(daily_objectives or []),
        len(assessment_checks or []),
        0,
    )
    for index in range(max_len):
        rows.append(
            {
                "topic": str((daily_topics or [])[index]).strip()
                if index < len(daily_topics or [])
                else "",
                "objective": str((daily_objectives or [])[index]).strip()
                if index < len(daily_objectives or [])
                else "",
                "assessment": str((assessment_checks or [])[index]).strip()
                if index < len(assessment_checks or [])
                else "",
            }
        )
    return [row for row in rows if row["topic"] or row["objective"] or row["assessment"]]


def _subject_lesson_block(
    *,
    subject_name: str,
    objective_text: str,
    daily_topic: str | None,
    day_focus: str,
    materials: list[str] | None = None,
) -> dict[str, Any]:
    topic = daily_topic or f"{subject_name} instructional focus"
    resolved_materials = materials or [
        f"{subject_name} district curriculum materials",
        "Graphic organizer or notebook",
        "Chart paper for modeling",
    ]
    return {
        "subject_name": subject_name,
        "daily_topic": topic,
        "objective": objective_text,
        "materials": resolved_materials,
        "mini_lesson": f"Introduce today's focus: {day_focus} Apply this to {topic}.",
        "teacher_modeling": [
            f"Model how to approach {topic} using the weekly objective.",
            "Think aloud and annotate key ideas students should notice.",
        ],
        "teacher_actions": [
            f"Model how to approach {topic} using the weekly objective.",
            "Guide students through a worked example.",
        ],
        "guided_practice": [
            "Partners apply the skill with teacher support.",
            "Discuss responses and clarify misconceptions.",
        ],
        "student_activity": [
            "Students practice the target skill in pairs.",
            "Students complete an independent application task.",
        ],
        "independent_practice": [
            "Students complete an independent check aligned to the objective.",
        ],
        "check_for_understanding": "Quick verbal or written check: Can students explain the learning target in their own words?",
        "assessment": "Exit prompt aligned to today's objective.",
        "closure": "Restate the objective and preview the next lesson.",
        "notes": day_focus,
    }


_DAILY_FOCUS_ROTATION = [
    "Launch the week's learning target and activate prior knowledge.",
    "Build understanding through guided practice and discussion.",
    "Apply the skill with collaborative and independent tasks.",
    "Use evidence from text or problems to justify thinking.",
    "Review, reflect, and prepare for the weekly assessment.",
]


def build_daily_lesson_plan(
    *,
    day_label: str,
    week_label: str,
    package_title: str,
    subject_blocks: list[dict[str, Any]],
    objective_code: str | None,
    objective_text: str,
    objective_ids: list[str] | None = None,
    teks_ids: list[str] | None = None,
    summary: str | None = None,
    daily_topic: str | None = None,
) -> dict[str, Any]:
    day_index = WEEKDAY_LABELS.index(day_label) if day_label in WEEKDAY_LABELS else 0
    focus = summary or _DAILY_FOCUS_ROTATION[day_index % len(_DAILY_FOCUS_ROTATION)]
    resolved_topic = daily_topic or focus
    mapping = _objective_mapping(
        objective_code,
        objective_text,
        objective_ids=objective_ids,
        teks_ids=teks_ids,
        daily_topic=resolved_topic,
    )
    slides = add_slide_visual_metadata(
        [
            {
                "id": f"{day_label.lower()}-title",
                "slideType": "title",
                "title": f"{day_label} Focus",
                "subtitle": resolved_topic,
                "bullets": [focus],
                "layout": "full_width_visual",
                "teacherNotes": "State the daily focus and set students up for the lesson sequence.",
            },
            *[
                {
                    "id": f"{day_label.lower()}-{index}-objective",
                    "slideType": "objective",
                    "title": f"{block['subject_name']} Objective",
                    "bullets": [str(block.get("objective") or objective_text)],
                    "layout": "concept_map",
                    "teacherNotes": "\n".join(
                        str(item) for item in (block.get("teacher_actions") or [])[:2]
                    ),
                }
                for index, block in enumerate(subject_blocks, start=1)
            ],
            *[
                {
                    "id": f"{day_label.lower()}-{index}-vocabulary",
                    "slideType": "vocabulary",
                    "title": f"{block['subject_name']} Vocabulary & Materials",
                    "bullets": list(block.get("materials") or []),
                    "layout": "vocabulary_card",
                    "teacherNotes": str(block.get("notes") or ""),
                }
                for index, block in enumerate(subject_blocks, start=1)
            ],
            *[
                {
                    "id": f"{day_label.lower()}-{index}-guided",
                    "slideType": "guided_practice",
                    "title": f"{block['subject_name']} Guided Practice",
                    "bullets": list(
                        block.get("guided_practice") or block.get("student_activity") or []
                    ),
                    "layout": "guided_practice",
                    "teacherNotes": str(block.get("mini_lesson") or ""),
                }
                for index, block in enumerate(subject_blocks, start=1)
            ],
            *[
                {
                    "id": f"{day_label.lower()}-{index}-independent",
                    "slideType": "independent_practice",
                    "title": f"{block['subject_name']} Independent Practice",
                    "bullets": list(block.get("independent_practice") or []),
                    "layout": "independent_practice",
                    "teacherNotes": str(block.get("closure") or ""),
                }
                for index, block in enumerate(subject_blocks, start=1)
            ],
            *[
                {
                    "id": f"{day_label.lower()}-{index}-exit",
                    "slideType": "exit_ticket",
                    "title": f"{block['subject_name']} Exit Ticket",
                    "bullets": [str(block.get("assessment") or "")],
                    "layout": "exit_ticket",
                    "teacherNotes": str(block.get("notes") or ""),
                }
                for index, block in enumerate(subject_blocks, start=1)
            ],
        ],
        objective_text=objective_text,
        teks_ids=list(mapping.get("teks_ids") or []),
    )
    return _with_alignment(
        {
            "title": f"{day_label} Daily Teaching Plan — {package_title}",
            "summary": focus,
            "daily_topic": resolved_topic,
            "description": f"Full-day plan for {day_label} covering all subjects in teaching order.",
            "subjects": subject_blocks,
            "slides": slides,
        },
        mapping,
    )


def build_subject_slide_deck(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    objectives_list: list[str],
    objective_ids: list[str] | None = None,
    teks_ids: list[str] | None = None,
    source_materials: list[str] | None = None,
    source_excerpts: list[str] | None = None,
    daily_topics: list[str] | None = None,
    daily_objectives: list[str] | None = None,
    assessment_checks: list[str] | None = None,
) -> dict[str, Any]:
    obj_bullets = objectives_list[:3] if objectives_list else [objective_text]
    mapping = _objective_mapping(
        objective_code,
        objective_text,
        objective_ids=objective_ids,
        teks_ids=teks_ids,
    )
    source_texts = _source_texts_from_materials(source_materials)
    excerpts = _source_excerpts(source_excerpts)
    daily_rows = _daily_rows(
        daily_topics=daily_topics,
        daily_objectives=daily_objectives,
        assessment_checks=assessment_checks,
    )
    slides: list[dict[str, Any]] = [
        {
            "id": "title-slide",
            "slideType": "title",
            "title": package_title,
            "subtitle": f"{subject_name} · {week_label}",
            "bullets": [objective_text],
            "layout": "full_width_visual",
            "teacherNotes": "Welcome students and state the learning objective.",
        },
        {
            "id": "learning-objective",
            "slideType": "objective",
            "title": "Learning Objective",
            "bullets": obj_bullets + ([f"Standard: {objective_code}"] if objective_code else []),
            "layout": "concept_map",
            "teacherNotes": "Students restate the objective in student-friendly language.",
        },
    ]
    if source_texts:
        slides.append(
            {
                "id": "texts-materials",
                "slideType": "vocabulary",
                "title": "Texts & Materials",
                "bullets": source_texts[:5],
                "layout": "vocabulary_card",
                "teacherNotes": f"Anchor student discussion in these texts: {_join_human_list(source_texts[:3])}.",
            }
        )
    if excerpts:
        slides.append(
            {
                "id": "source-evidence",
                "slideType": "mini_lesson",
                "title": "Source Evidence",
                "bullets": excerpts[:3],
                "layout": "text_left_visual_right",
                "teacherNotes": "Use these supplied/extracted excerpts as the anchor text for discussion.",
            }
        )
    for index, row in enumerate(daily_rows[:5], start=1):
        title = row["topic"] or f"Day {index} Focus"
        bullets = _unique_strings(
            [
                row["objective"],
                row["assessment"],
                f"Use evidence from {source_texts[min(index - 1, len(source_texts) - 1)]}."
                if source_texts
                else "",
            ]
        )
        slides.append(
            {
                "id": f"day-{index}-focus",
                "slideType": "guided_practice"
                if index < len(daily_rows)
                else "independent_practice",
                "title": title,
                "bullets": bullets[:3]
                or ["Discuss the weekly objective with evidence from the lesson text."],
                "layout": "text_left_visual_right" if index % 2 else "visual_left_text_right",
                "teacherNotes": row["objective"]
                or "Guide students to connect the day's text to the objective.",
            }
        )
    weekly_check = next(
        (item for item in reversed(assessment_checks or []) if str(item).strip()), ""
    )
    slides.extend(
        [
            {
                "id": "check-understanding",
                "slideType": "check_for_understanding",
                "title": "Check for Understanding",
                "bullets": [
                    weekly_check
                    or "Ask students to explain the learning target using evidence from the text."
                ],
                "layout": "question_prompt",
            },
            {
                "id": "wrap-up",
                "slideType": "closing",
                "title": "Wrap-Up",
                "bullets": [
                    (
                        f"Review how {source_texts[0]} supports this week's objective."
                        if source_texts
                        else "Review the week's key understanding."
                    ),
                    "Preview the next reading or discussion.",
                ],
                "layout": "title_only",
            },
        ]
    )
    slides = add_slide_visual_metadata(
        slides, objective_text=objective_text, teks_ids=list(mapping.get("teks_ids") or [])
    )
    return _with_alignment(
        {
            "title": f"{subject_name} {week_label} — {package_title}",
            "summary": f"Classroom slide deck for {subject_name}.",
            "description": "Presentation-ready slides for one subject block or week overview.",
            "slides": slides,
        },
        mapping,
    )


def _default_passage(subject_name: str, topic: str) -> tuple[str, str]:
    title = f"{subject_name} Focus Passage"
    text = (
        f"This week's {subject_name} instruction focuses on {topic}. "
        "Students read closely to identify key ideas and support their thinking with evidence from the text. "
        "Strong readers ask what the author wants them to learn and how each detail connects to that goal."
    )
    return title, text


def build_quiz(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    source_materials: list[str] | None = None,
    source_excerpts: list[str] | None = None,
    daily_topics: list[str] | None = None,
    daily_objectives: list[str] | None = None,
    assessment_checks: list[str] | None = None,
) -> dict[str, Any]:
    source_texts = _source_texts_from_materials(source_materials)
    excerpts = _source_excerpts(source_excerpts, limit=2)
    primary_text = source_texts[0] if source_texts else f"{subject_name} class text"
    secondary_text = source_texts[1] if len(source_texts) > 1 else primary_text
    weekly_check = next(
        (item for item in reversed(assessment_checks or []) if str(item).strip()), ""
    )
    first_day_objective = next(
        (item for item in daily_objectives or [] if str(item).strip()), objective_text
    )
    questions = [
        {
            "number": 1,
            "type": "multiple_choice",
            "prompt": f"Which text from this week helped students practice the objective '{first_day_objective}'?",
            "choices": [
                primary_text,
                "A random science article",
                "An unrelated math worksheet",
                "A spelling list with no story",
            ],
            "answer": primary_text,
            "explanation": "The assessment should stay grounded in the texts selected for this week.",
            "points": 1,
        },
        {
            "number": 2,
            "type": "multiple_choice",
            "prompt": f"When reading {primary_text}, which action best helps you identify evidence for the objective?",
            "choices": [
                "Rereading and annotating important ideas",
                "Ignoring headings and captions",
                "Reading only the first sentence",
                "Avoiding discussion with peers",
            ],
            "answer": "Rereading and annotating important ideas",
            "explanation": "Annotation helps students track evidence.",
            "points": 1,
        },
        {
            "number": 3,
            "type": "short_answer",
            "prompt": first_day_objective
            if first_day_objective.endswith("?")
            else f"State this objective in your own words: {first_day_objective}",
            "answer": f"Responses should reflect: {objective_text}",
            "explanation": "Accept paraphrases that preserve the learning target.",
            "points": 2,
            "response_lines": 4,
        },
        {
            "number": 4,
            "type": "evidence_based",
            "prompt": (
                f'Use this supplied excerpt from {primary_text}: "{excerpts[0]}" '
                f"Then answer: {assessment_checks[0] if assessment_checks else objective_text}"
                if excerpts
                else (
                    f"Use {primary_text} to answer this question: {assessment_checks[0]}"
                    if assessment_checks
                    else f"Cite one detail from {primary_text} that supports the weekly learning goal and explain why."
                )
            ),
            "answer": f"Accept responses that reference {primary_text} with a clear connection to the objective.",
            "explanation": "Evidence must link detail to the objective.",
            "points": 3,
            "response_lines": 5,
        },
        {
            "number": 5,
            "type": "multiple_choice",
            "prompt": f"When comparing ideas from {primary_text} or {secondary_text}, supporting details should —",
            "choices": [
                "explain or prove the main learning goal",
                "replace the learning goal",
                "introduce an unrelated topic",
                "repeat the title only",
            ],
            "answer": "explain or prove the main learning goal",
            "explanation": "Details support the central idea.",
            "points": 1,
        },
        {
            "number": 6,
            "type": "short_answer",
            "prompt": f"Name one strategy you used during {week_label} instruction with {primary_text}.",
            "answer": f"Accept annotation, rereading, partner discussion, or graphic organizers used with {primary_text}.",
            "points": 2,
            "response_lines": 3,
        },
        {
            "number": 7,
            "type": "multiple_choice",
            "prompt": "Which response best shows understanding of the week's reading objective?",
            "choices": [
                "answer the lesson objective with evidence",
                "copy the objective without explanation",
                "ignore the question",
                "list random words",
            ],
            "answer": "answer the lesson objective with evidence",
            "points": 1,
        },
        {
            "number": 8,
            "type": "evidence_based",
            "prompt": (
                f"Compare evidence from the supplied class text excerpts: {excerpts[0]}"
                if excerpts
                else (
                    f"Use evidence from {primary_text} and {secondary_text} to respond: {weekly_check}"
                    if weekly_check and source_texts
                    else f"Write one sentence explaining what you learned this week in {subject_name}."
                )
            ),
            "answer": (
                f"Responses should align with {weekly_check}"
                if weekly_check
                else f"Responses should align with: {objective_text}"
            ),
            "points": 3,
            "response_lines": 5,
        },
    ]
    title = f"{package_title} — {subject_name} Quiz"
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": title,
            "summary": f"Formative quiz for {week_label} {subject_name}.",
            "description": "Eight-question quiz with mixed item types.",
            "student_number_field": True,
            "questions": questions,
            "answer_key": [
                {
                    "number": q["number"],
                    "answer": q["answer"],
                    "explanation": q.get("explanation"),
                    "points": q.get("points", 1),
                }
                for q in questions
            ],
            "google_forms_package": None,
        },
        mapping,
    )


def build_exit_ticket(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Exit Ticket",
            "summary": "End-of-lesson check for understanding.",
            "description": "Short constructed-response exit ticket.",
            "questions": [
                {"prompt": "What was today's learning target?", "response_lines": 2},
                {"prompt": "Write one detail or example from today's lesson.", "response_lines": 2},
                {
                    "prompt": "How does that detail support the learning target?",
                    "response_lines": 3,
                },
            ],
        },
        mapping,
    )


def build_writing_response(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    prompt = (
        f"Write a clear, organized response showing your understanding of this week's {subject_name} learning target: "
        f"{objective_text}"
    )
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Writing Response",
            "summary": "Constructed writing response aligned to the weekly objective.",
            "description": "Students write directly on lined response pages.",
            "prompt": prompt,
            "instructions": [
                "Read the writing prompt carefully.",
                "Write in complete sentences with a clear beginning, middle, and end.",
                "Use evidence, examples, or details that support the learning objective.",
                "Revise for clarity, organization, and conventions before submitting.",
            ],
            "response_pages": 1,
            "writing_lines_per_page": 14,
        },
        mapping,
    )


def build_rubric_for_writing_response(
    *,
    writing_content: dict[str, Any],
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    prompt = str(writing_content.get("prompt") or objective_text).strip()
    instructions = [
        str(item).strip()
        for item in (writing_content.get("instructions") or [])
        if str(item).strip()
    ]
    criteria = [
        {
            "name": "Addresses the writing prompt",
            "points": 5,
            "levels": [
                "Fully addresses the prompt with a focused response",
                "Partially addresses the prompt",
                "Does not address the prompt",
            ],
        },
        {
            "name": "Objective and content",
            "points": 5,
            "levels": [
                "Demonstrates strong understanding of the learning objective",
                "Shows partial understanding",
                "Shows little or no understanding",
            ],
        },
        {
            "name": "Evidence and support",
            "points": 5,
            "levels": [
                "Uses relevant evidence, examples, or details",
                "Uses some support but it is limited or uneven",
                "Support is missing or unrelated",
            ],
        },
        {
            "name": "Organization",
            "points": 3,
            "levels": [
                "Clear structure with a logical flow",
                "Some organization with minor lapses",
                "Disorganized or difficult to follow",
            ],
        },
        {
            "name": "Grammar and conventions",
            "points": 2,
            "levels": [
                "Strong conventions with few errors",
                "Minor errors that do not block meaning",
                "Frequent errors that affect meaning",
            ],
        },
    ]
    if instructions:
        criteria[0]["levels"][0] = f"Fully addresses the prompt: {instructions[0]}"
    total_points = sum(int(row["points"]) for row in criteria)
    rubric_title = f"{package_title} — {subject_name} Writing Rubric"
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": rubric_title,
            "summary": "Rubric for the writing response assignment.",
            "description": prompt,
            "total_points": total_points,
            "criteria": criteria,
            "writing_prompt": prompt,
            "writing_response_title": writing_content.get("title"),
        },
        mapping,
    )


def build_written_assignment(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    daily_topic: str | None,
    source_materials: list[str] | None = None,
    source_excerpts: list[str] | None = None,
    daily_topics: list[str] | None = None,
    daily_objectives: list[str] | None = None,
    assessment_checks: list[str] | None = None,
) -> dict[str, Any]:
    topic = daily_topic or f"{subject_name} weekly focus"
    source_texts = _source_texts_from_materials(source_materials)
    excerpts = _source_excerpts(source_excerpts, limit=4)
    weekly_check = next(
        (item for item in reversed(assessment_checks or []) if str(item).strip()), ""
    )
    passage_title = "Source Text Set"
    if excerpts:
        passage_text = "\n\n".join(excerpts)
    elif source_texts:
        passage_text = (
            f"Use this week's class texts: {_join_human_list(source_texts[:4])}. "
            f"Focus on the weekly objective: {objective_text}. "
            f"{weekly_check or f'Use details from the texts to explain {topic}.'}"
        )
    else:
        passage_title, passage_text = _default_passage(subject_name, topic)
    rubric_title = f"{package_title} — {subject_name} Rubric"
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Written Assignment",
            "summary": "Written response aligned to the weekly objective.",
            "description": (
                f"Students use class texts and evidence to respond to {objective_text}."
                if source_texts
                else "Students read a passage and write a paragraph with supporting details."
            ),
            "passage_title": passage_title,
            "passage_text": passage_text,
            "student_instructions": [
                (
                    "Reread the supplied source excerpt(s) and choose evidence that matches the objective."
                    if excerpts
                    else f"Reread {_join_human_list(source_texts[:3])} and choose evidence that matches the objective."
                    if source_texts
                    else "Read the passage carefully."
                ),
                (
                    weekly_check
                    if weekly_check
                    else "Write one paragraph explaining the main idea or learning target."
                ),
                (
                    "Include at least two supporting details from the supplied source excerpt(s)."
                    if excerpts
                    else f"Include at least two supporting details from {source_texts[0]} or another class text."
                    if source_texts
                    else "Include at least two supporting details from the text."
                ),
                "Use complete sentences and clear organization.",
            ],
            "success_criteria": [
                "Clear statement of the learning target or claim",
                (
                    "At least two accurate supporting details from the supplied source excerpt(s)"
                    if excerpts
                    else f"At least two accurate supporting details from {_join_human_list(source_texts[:2])}"
                    if source_texts
                    else "At least two accurate supporting details"
                ),
                "Evidence clearly connected to the objective",
                "Organized paragraph with conventions",
            ],
            "rubric_reference": rubric_title,
            "writing_lines": 12,
        },
        mapping,
    )


def build_rubric_for_written_assignment(
    *,
    assignment_content: dict[str, Any],
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    passage_title = str(
        assignment_content.get("passage_title") or f"{subject_name} passage"
    ).strip()
    success_criteria = [
        str(item).strip()
        for item in (assignment_content.get("success_criteria") or [])
        if str(item).strip()
    ]
    criteria = [
        {
            "name": "Main idea / learning target",
            "points": 5,
            "levels": [
                success_criteria[0]
                if success_criteria
                else "Clear statement of the learning target",
                "Partial statement of the learning target",
                "Missing or inaccurate main idea",
            ],
        },
        {
            "name": "Text evidence and support",
            "points": 5,
            "levels": [
                success_criteria[1]
                if len(success_criteria) > 1
                else "Two or more accurate supporting details",
                "Some supporting details with gaps",
                "Details missing or unrelated",
            ],
        },
        {
            "name": "Explanation",
            "points": 4,
            "levels": [
                success_criteria[2]
                if len(success_criteria) > 2
                else "Explains how evidence supports the target",
                "Partial explanation",
                "No explanation",
            ],
        },
        {
            "name": "Organization",
            "points": 3,
            "levels": [
                success_criteria[3]
                if len(success_criteria) > 3
                else "Organized paragraph with clear flow",
                "Some organization with lapses",
                "Disorganized or difficult to follow",
            ],
        },
        {
            "name": "Grammar and conventions",
            "points": 3,
            "levels": [
                "Strong conventions with few errors",
                "Minor errors that do not block meaning",
                "Frequent errors that affect meaning",
            ],
        },
    ]
    total_points = sum(int(row["points"]) for row in criteria)
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Written Assignment Rubric",
            "summary": "Rubric for the written assignment.",
            "description": f"Rubric aligned to {passage_title} and {objective_text}",
            "total_points": total_points,
            "criteria": criteria,
            "assignment_title": assignment_content.get("title"),
            "passage_title": passage_title,
        },
        mapping,
    )


def build_rubric(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    criteria = [
        {
            "name": "Learning Target",
            "points": 4,
            "levels": [
                "Meets the objective clearly",
                "Partial understanding",
                "Does not meet the objective",
            ],
        },
        {
            "name": "Supporting Details",
            "points": 4,
            "levels": [
                "Two or more relevant details",
                "One relevant detail",
                "Details missing or unrelated",
            ],
        },
        {
            "name": "Evidence and Explanation",
            "points": 4,
            "levels": [
                "Explains how details support the target",
                "Partial explanation",
                "No explanation",
            ],
        },
        {
            "name": "Organization",
            "points": 4,
            "levels": ["Logical structure", "Some organization", "Disorganized"],
        },
        {
            "name": "Grammar / Conventions",
            "points": 4,
            "levels": ["Strong conventions", "Minor errors", "Frequent errors affecting meaning"],
        },
    ]
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Rubric",
            "summary": "20-point rubric for the written assignment.",
            "description": f"Rubric aligned to {objective_text}",
            "total_points": 20,
            "criteria": criteria,
        },
        mapping,
    )


def build_parent_newsletter(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — Parent Newsletter Summary",
            "summary": f"Weekly family update for {subject_name}.",
            "description": "Parent-friendly summary of learning and home practice.",
            "sections": [
                {
                    "heading": "What We Are Learning",
                    "body": f"In {subject_name} this week, students are working on: {objective_text}",
                },
                {
                    "heading": "In Family-Friendly Language",
                    "body": "Ask your child to explain what they practiced and give an example from class.",
                },
                {
                    "heading": "Practice at Home",
                    "bullets": [
                        "Read together for 20 minutes.",
                        f"Ask: What was the main idea of today's {subject_name} lesson?",
                    ],
                },
                {
                    "heading": "Upcoming Assessment",
                    "body": f"Students will complete a {week_label} check aligned to the objective.",
                },
                {
                    "heading": "Reminders",
                    "bullets": [
                        "Return signed forms by Friday.",
                        "Contact the teacher with questions about assignments.",
                    ],
                },
            ],
        },
        mapping,
    )


def build_bell_ringer(
    *, subject_name: str, package_title: str, objective_text: str, objective_code: str | None
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Bell Ringer",
            "summary": "Daily warm-up prompts.",
            "description": "Short opener activities for the week.",
            "sections": [
                {"heading": "Monday", "body": "Define the week's objective in your own words."},
                {"heading": "Tuesday", "body": "List two details from yesterday's lesson."},
                {
                    "heading": "Wednesday",
                    "body": "Explain how one detail supports the learning target.",
                },
            ],
        },
        mapping,
    )


def build_vocabulary_list(
    *, subject_name: str, package_title: str, objective_text: str, objective_code: str | None
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Vocabulary",
            "summary": "Key terms for the week.",
            "description": "Vocabulary connected to the weekly objective.",
            "terms": [
                {"term": "objective", "definition": "What students should learn this week."},
                {
                    "term": "evidence",
                    "definition": "Proof from the text or lesson that supports your thinking.",
                },
                {
                    "term": "main idea",
                    "definition": "The most important point the author or lesson wants you to understand.",
                },
                {
                    "term": "supporting detail",
                    "definition": "A fact or example that explains the main idea.",
                },
            ],
        },
        mapping,
    )


def _student_deck_visual(
    *,
    search_terms: list[str],
    purpose: str,
    alt_text: str,
    placement: str = "right",
    fallback_organizer: str = "concept_map",
) -> dict[str, Any]:
    return {
        "type": "image_search",
        "placement": placement,
        "fallback_organizer_type": fallback_organizer,
        "image_search": {
            "search_terms": search_terms,
            "educational_purpose": purpose,
            "target_grade_band": "elementary",
            "preferred_image_type": "photo",
            "image_alt_text": alt_text,
            "image_rationale": purpose,
        },
    }


def build_student_lesson_deck(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    objectives_list: list[str],
    day_label: str | None = None,
    day_topic: str | None = None,
    student_goal: str | None = None,
    objective_ids: list[str] | None = None,
    teks_ids: list[str] | None = None,
    source_materials: list[str] | None = None,
    daily_topics: list[str] | None = None,
) -> dict[str, Any]:
    mapping = _objective_mapping(
        objective_code,
        objective_text,
        objective_ids=objective_ids,
        teks_ids=teks_ids,
    )
    source_texts = _source_texts_from_materials(source_materials)
    day_context = f" — {day_label}" if day_label else ""
    title_label = f"{subject_name}{day_context}" if subject_name else package_title
    # Use day-specific topic when available; fall back to first weekly topic.
    _primary_topic = day_topic or (daily_topics[0] if daily_topics else None)
    # Use day topic as the student-facing subject label so "ELA" never appears verbatim.
    subj = _primary_topic or subject_name or "today's lesson"

    # "Today You Will Learn To..." slide content.
    # Prefer IDP student_goal ("I can..." ≤10 words) — it's student-friendly.
    # Fall back to truncated objective_text only when IDP hasn't run yet.
    if student_goal:
        obj_body = student_goal
        obj_bullets = [student_goal]
    else:
        obj_body = objective_text
        if len(obj_body.split()) > 20:
            obj_body = " ".join(obj_body.split()[:20]) + "..."
        obj_bullets = [obj_body]

    slides: list[dict[str, Any]] = []
    slide_num = 0

    def next_id() -> str:
        nonlocal slide_num
        slide_num += 1
        return f"slide-{slide_num}"

    # Hook
    hook_body = f"Today we explore {_primary_topic or subject_name or 'this topic'}."
    if source_texts:
        hook_body += f" We will work with {source_texts[0]}."
    hook_body += " Get ready to think and discover!"
    slides.append(
        {
            "id": next_id(),
            "slide_type": "hook",
            "layout": "hook_full_image",
            "title": f"Ready to Learn: {_primary_topic or subject_name}",
            "body": hook_body,
            "bullets": [f"Today: {_primary_topic or subject_name}", f"Week: {week_label}"],
            "engagement": {
                "type": "think_pair_share",
                "prompt": "What do you already know about this topic? Share with a partner!",
            },
            "visual": _student_deck_visual(
                search_terms=[
                    f"{subj} students learning classroom",
                    "elementary students excited classroom",
                    "classroom discovery learning",
                ],
                purpose=f"Sets an inviting tone and connects {subj} to students' experience.",
                alt_text=f"Students learning about {subj} in an elementary classroom",
                placement="full_width",
                fallback_organizer="concept_map",
            ),
        }
    )

    # Today we learn
    slides.append(
        {
            "id": next_id(),
            "slide_type": "today_we_learn",
            "layout": "objective_image",
            "title": "Today You Will Learn To...",
            "body": obj_body,
            "bullets": obj_bullets,
            "engagement": {
                "type": "turn_and_talk",
                "prompt": "Read the learning goal with your partner. Can you say it in your own words?",
            },
            "visual": _student_deck_visual(
                search_terms=[
                    "students reading classroom discussion",
                    "learning goal target education elementary",
                    "classroom reading lesson",
                ],
                purpose="Shows students engaged in learning to connect the objective to real classroom experience.",
                alt_text="Students engaged in classroom learning activity",
                placement="right",
                fallback_organizer="concept_map",
            ),
        }
    )

    # Vocabulary
    if source_texts:
        vocab_bullets = [f"{text}" for text in source_texts[:4]]
        slides.append(
            {
                "id": next_id(),
                "slide_type": "vocabulary",
                "layout": "vocabulary_showcase",
                "title": "Words to Know",
                "body": f"These key words will help you understand {subj} today.",
                "bullets": vocab_bullets,
                "engagement": {
                    "type": "whiteboard",
                    "prompt": "Point to a word you know. Tell your neighbor what it means!",
                },
                "visual": _student_deck_visual(
                    search_terms=[
                        f"{subj} vocabulary words elementary",
                        "word wall classroom vocabulary",
                        "vocabulary flashcards students",
                    ],
                    purpose="Visual word wall helps students connect vocabulary terms to meaning.",
                    alt_text="Vocabulary word wall with key lesson terms",
                    placement="right",
                    fallback_organizer="vocabulary_card",
                ),
            }
        )

    # Concept slides — use day-specific topic first, then remaining weekly topics
    _remaining_topics = [t for t in (daily_topics or []) if t != _primary_topic]
    concept_topics = ([_primary_topic] + _remaining_topics[:2]) if _primary_topic else (daily_topics or [])[:3]
    if concept_topics:
        for index, topic in enumerate(concept_topics, start=1):
            slides.append(
                {
                    "id": next_id(),
                    "slide_type": "concept",
                    "layout": "teacher_modeling",
                    "title": topic,
                    "body": f"In {subj}, we focus on: {topic}.",
                    "bullets": [
                        f"Key idea {index}: {topic}",
                        "Look for evidence.",
                        "Ask: How does this connect?",
                    ],
                    "engagement": {
                        "type": "turn_and_talk",
                        "prompt": f"How does '{topic}' connect to what you already know? Discuss with your partner.",
                    },
                    "visual": _student_deck_visual(
                        search_terms=[
                            f"{topic} {subj} elementary illustration",
                            f"{topic} classroom visual",
                            f"{subj} students lesson",
                        ],
                        purpose=f"Visual anchor for the concept '{topic}' in {subj}.",
                        alt_text=f"Educational illustration showing {topic}",
                        placement="right",
                        fallback_organizer="concept_map",
                    ),
                }
            )
    else:
        slides.append(
            {
                "id": next_id(),
                "slide_type": "concept",
                "layout": "teacher_modeling",
                "title": f"The Big Idea in {subj}",
                "body": f"The main idea: {obj_body}",
                "bullets": [
                    "Find the main idea.",
                    "Support it with details.",
                    "Connect it to what you know.",
                ],
                "engagement": {
                    "type": "think_pair_share",
                    "prompt": "What is the main idea? Write it in your own words.",
                },
                "visual": _student_deck_visual(
                    search_terms=[
                        f"{subj} main idea graphic organizer",
                        f"{subj} concept map students",
                        "main idea supporting details elementary",
                    ],
                    purpose=f"Helps students visualize the central concept of {subj}.",
                    alt_text=f"Graphic organizer showing the main idea of {subj}",
                    placement="right",
                    fallback_organizer="concept_map",
                ),
            }
        )

    # Your turn
    slides.append(
        {
            "id": next_id(),
            "slide_type": "your_turn",
            "layout": "guided_practice_image",
            "title": "Your Turn — Let's Practice!",
            "body": f"Show what you know about {subj}. Work with a partner to complete the activity.",
            "bullets": [
                "Read the prompt carefully.",
                "Use evidence from the lesson.",
                "Be ready to share!",
            ],
            "engagement": {
                "type": "think_pair_share",
                "prompt": "Complete the practice activity with your partner. You have 5 minutes — go!",
            },
            "visual": _student_deck_visual(
                search_terms=[
                    "students working together classroom elementary",
                    "partner activity collaborative learning",
                    "students discussing classroom",
                ],
                purpose="Shows collaborative learning to motivate students during practice.",
                alt_text="Elementary students working together on a classroom activity",
                placement="right",
                fallback_organizer="process_flow",
            ),
        }
    )

    # Check in
    slides.append(
        {
            "id": next_id(),
            "slide_type": "check_in",
            "layout": "exit_ticket_image",
            "title": "Check In — How Are You Doing?",
            "body": "Let's see how well you understand today's lesson.",
            "bullets": [
                "1 finger = I need help",
                "3 fingers = I'm getting it",
                "5 fingers = I've got this!",
            ],
            "engagement": {
                "type": "show_fingers",
                "prompt": f"Hold up fingers to show your confidence. Can you explain the main idea of {subj}?",
            },
            "visual": _student_deck_visual(
                search_terms=[
                    "student confidence check elementary classroom",
                    "students showing understanding hands",
                    "self assessment elementary",
                ],
                purpose="Confidence check visual prompts honest student self-reflection.",
                alt_text="Students showing hand signals for understanding in classroom",
                placement="right",
                fallback_organizer="concept_map",
            ),
        }
    )

    # Wrap up
    slides.append(
        {
            "id": next_id(),
            "slide_type": "wrap_up",
            "layout": "title_full",
            "title": "Great Work Today!",
            "body": f"You explored {subj} and practiced the key ideas. Keep thinking about this!",
            "bullets": [
                f"We explored: {subj}",
                "You practiced with a partner.",
                "Next time we go deeper!",
            ],
            "engagement": {
                "type": "exit_ticket",
                "prompt": "Write your exit thought: 'Today I learned...' Share it before you leave!",
            },
            "visual": _student_deck_visual(
                search_terms=[
                    "students celebrating learning classroom achievement",
                    "classroom success celebration elementary",
                    "happy students school achievement",
                ],
                purpose="Celebrates student effort and closes the lesson on a positive note.",
                alt_text="Students celebrating their learning achievement in classroom",
                placement="full_width",
                fallback_organizer="concept_map",
            ),
        }
    )

    return _with_alignment(
        {
            "title": f"{title_label} — Student Lesson",
            "summary": f"Student-facing lesson presentation for {subject_name}.",
            "description": "Projected student lesson deck for classroom display.",
            "slides": slides,
        },
        mapping,
    )


def build_study_guide(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_text: str,
    objective_code: str | None,
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment(
        {
            "title": f"{package_title} — {subject_name} Study Guide",
            "summary": f"Review guide for {week_label}.",
            "description": "Student review before the weekly assessment.",
            "sections": [
                {"heading": "Learning Target", "body": objective_text},
                {
                    "heading": "Review Steps",
                    "bullets": [
                        "Reread your notes.",
                        "Practice explaining the objective.",
                        "Find two supporting details.",
                    ],
                },
                {
                    "heading": "Practice",
                    "body": f"Prepare for the {subject_name} quiz and written assignment.",
                },
            ],
        },
        mapping,
    )


def build_deterministic_fallback(
    artifact_type: str,
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    day_label: str | None = None,
    daily_topic: str | None = None,
    subject_blocks: list[dict[str, Any]] | None = None,
    objectives_list: list[str] | None = None,
    objective_ids: list[str] | None = None,
    teks_ids: list[str] | None = None,
    source_materials: list[str] | None = None,
    source_excerpts: list[str] | None = None,
    daily_topics: list[str] | None = None,
    daily_objectives: list[str] | None = None,
    assessment_checks: list[str] | None = None,
) -> dict[str, Any]:
    objective_text = objective_text or f"Students demonstrate understanding in {subject_name}."
    builders = {
        "daily_lesson_plan": lambda: build_daily_lesson_plan(
            day_label=day_label or "Monday",
            week_label=week_label,
            package_title=package_title,
            subject_blocks=subject_blocks or [],
            objective_code=objective_code,
            objective_text=objective_text,
            objective_ids=objective_ids,
            teks_ids=teks_ids,
        ),
        "subject_slide_deck": lambda: build_subject_slide_deck(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
            objectives_list=objectives_list or [],
            objective_ids=objective_ids,
            teks_ids=teks_ids,
            source_materials=source_materials,
            source_excerpts=source_excerpts,
            daily_topics=daily_topics,
            daily_objectives=daily_objectives,
            assessment_checks=assessment_checks,
        ),
        "student_lesson_deck": lambda: build_student_lesson_deck(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
            objectives_list=objectives_list or [],
            day_label=day_label,
            objective_ids=objective_ids,
            teks_ids=teks_ids,
            source_materials=source_materials,
            daily_topics=daily_topics,
        ),
        "quiz": lambda: build_quiz(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
            source_materials=source_materials,
            source_excerpts=source_excerpts,
            daily_topics=daily_topics,
            daily_objectives=daily_objectives,
            assessment_checks=assessment_checks,
        ),
        "exit_ticket": lambda: build_exit_ticket(
            subject_name=subject_name,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
        ),
        "assignment": lambda: build_written_assignment(
            subject_name=subject_name,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
            daily_topic=daily_topic,
            source_materials=source_materials,
            source_excerpts=source_excerpts,
            daily_topics=daily_topics,
            daily_objectives=daily_objectives,
            assessment_checks=assessment_checks,
        ),
        "writing_response": lambda: build_writing_response(
            subject_name=subject_name,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
        ),
        "rubric": lambda: build_rubric(
            subject_name=subject_name,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
        ),
        "parent_newsletter_summary": lambda: build_parent_newsletter(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
        ),
        "bell_ringer": lambda: build_bell_ringer(
            subject_name=subject_name,
            package_title=package_title,
            objective_text=objective_text,
            objective_code=objective_code,
        ),
        "vocabulary_list": lambda: build_vocabulary_list(
            subject_name=subject_name,
            package_title=package_title,
            objective_text=objective_text,
            objective_code=objective_code,
        ),
        "study_guide": lambda: build_study_guide(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_text=objective_text,
            objective_code=objective_code,
        ),
    }
    builder = builders.get(artifact_type)
    if builder is None:
        mapping = _objective_mapping(
            objective_code,
            objective_text,
            objective_ids=objective_ids,
            teks_ids=teks_ids,
        )
        return _with_alignment(
            {
                "title": f"{package_title} — {subject_name} Resource",
                "summary": f"Instructional resource for {week_label}.",
                "description": f"Supports {objective_text}",
                "sections": [{"heading": "Overview", "body": objective_text}],
            },
            mapping,
        )
    content = builder()
    if objective_ids is not None and "objective_ids" not in content:
        content["objective_ids"] = list(objective_ids)
    if teks_ids is not None and "teks_ids" not in content:
        content["teks_ids"] = list(teks_ids)
    if content.get("alignment_summary") is None:
        content["alignment_summary"] = (
            f"Aligned to {objective_code or 'selected objective'}: {objective_text}"
        )
    if isinstance(content.get("objective_mapping"), dict):
        if objective_ids is not None and "objective_ids" not in content["objective_mapping"]:
            content["objective_mapping"]["objective_ids"] = list(objective_ids)
        if teks_ids is not None and "teks_ids" not in content["objective_mapping"]:
            content["objective_mapping"]["teks_ids"] = list(teks_ids)
        if content["objective_mapping"].get("alignment_summary") is None:
            content["objective_mapping"]["alignment_summary"] = content["alignment_summary"]
    if artifact_type == "quiz" and content.get("google_forms_package") is None:
        from oziebot_api.services.teacher_assist_v2.package_export import (
            build_google_forms_package_payload,
        )

        content["google_forms_package"] = build_google_forms_package_payload(content)
    return content
