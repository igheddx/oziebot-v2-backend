"""Classroom-ready deterministic artifact content when real AI is unavailable."""

from __future__ import annotations

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
                    "teacherNotes": "\n".join(str(item) for item in (block.get("teacher_actions") or [])[:2]),
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
                    "bullets": list(block.get("guided_practice") or block.get("student_activity") or []),
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
    return _with_alignment({
        "title": f"{day_label} Daily Teaching Plan — {package_title}",
        "summary": focus,
        "daily_topic": resolved_topic,
        "description": f"Full-day plan for {day_label} covering all subjects in teaching order.",
        "subjects": subject_blocks,
        "slides": slides,
    }, mapping)


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
) -> dict[str, Any]:
    obj_bullets = objectives_list[:3] if objectives_list else [objective_text]
    mapping = _objective_mapping(
        objective_code,
        objective_text,
        objective_ids=objective_ids,
        teks_ids=teks_ids,
    )
    slides = add_slide_visual_metadata([
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
        {
            "id": "key-vocabulary",
            "slideType": "vocabulary",
            "title": "Key Vocabulary",
            "bullets": ["Review academic vocabulary for this week's topic.", "Use student-friendly definitions."],
            "layout": "vocabulary_card",
        },
        {
            "id": "concept-introduction",
            "slideType": "mini_lesson",
            "title": f"{subject_name} — Concept Introduction",
            "bullets": ["Introduce the core concept for the week.", "Connect to prior learning."],
            "layout": "text_left_visual_right",
            "teacherNotes": "Use the graphic to organize main ideas and supporting details.",
        },
        {
            "id": "teacher-modeling",
            "slideType": "mini_lesson",
            "title": "Teacher Modeling",
            "bullets": ["Model the target skill step by step.", "Think aloud so students see your process."],
            "layout": "visual_left_text_right",
        },
        {
            "id": "guided-practice",
            "slideType": "guided_practice",
            "title": "Guided Practice",
            "bullets": ["Work through an example together.", "Invite student responses and discussion."],
            "layout": "guided_practice",
        },
        {
            "id": "independent-practice",
            "slideType": "independent_practice",
            "title": "Independent Practice",
            "bullets": ["Students apply the skill independently.", "Circulate and support as needed."],
            "layout": "independent_practice",
        },
        {
            "id": "check-understanding",
            "slideType": "check_for_understanding",
            "title": "Check for Understanding",
            "bullets": ["Ask students to explain the learning target.", "Collect a quick formative response."],
            "layout": "question_prompt",
        },
        {
            "id": "exit-ticket",
            "slideType": "exit_ticket",
            "title": "Exit Ticket",
            "bullets": ["One question aligned to today's objective.", "Use responses to plan tomorrow's lesson."],
            "layout": "exit_ticket",
        },
        {
            "id": "wrap-up",
            "slideType": "closing",
            "title": "Wrap-Up",
            "bullets": ["Celebrate strong work.", "Preview the next lesson."],
            "layout": "title_only",
        },
    ], objective_text=objective_text, teks_ids=list(mapping.get("teks_ids") or []))
    return _with_alignment({
        "title": f"{subject_name} {week_label} — {package_title}",
        "summary": f"Classroom slide deck for {subject_name}.",
        "description": "Presentation-ready slides for one subject block or week overview.",
        "slides": slides,
    }, mapping)


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
) -> dict[str, Any]:
    questions = [
        {
            "number": 1,
            "type": "multiple_choice",
            "prompt": f"What is the main learning focus for {subject_name} this week?",
            "choices": [objective_text, "Memorizing unrelated facts", "Copying text without thinking", "Skipping the introduction"],
            "answer": objective_text,
            "explanation": "The weekly objective defines what students should understand.",
            "points": 1,
        },
        {
            "number": 2,
            "type": "multiple_choice",
            "prompt": "Which action best supports close reading?",
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
            "prompt": "State the weekly objective in your own words.",
            "answer": f"Responses should reflect: {objective_text}",
            "explanation": "Accept paraphrases that preserve the learning target.",
            "points": 2,
        },
        {
            "number": 4,
            "type": "evidence_based",
            "prompt": "Cite one detail that supports the weekly learning goal and explain why.",
            "answer": "Accept responses that reference the passage or lesson with a clear connection.",
            "explanation": "Evidence must link detail to the objective.",
            "points": 3,
        },
        {
            "number": 5,
            "type": "multiple_choice",
            "prompt": "Supporting details should —",
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
            "prompt": "Name one strategy you used during {week_label} instruction.",
            "answer": "Accept annotation, rereading, partner discussion, or graphic organizers.",
            "points": 2,
        },
        {
            "number": 7,
            "type": "multiple_choice",
            "prompt": "A strong exit ticket response should —",
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
            "prompt": "Write one sentence explaining what you learned this week in {subject_name}.",
            "answer": f"Responses should align with: {objective_text}",
            "points": 3,
        },
    ]
    title = f"{package_title} — {subject_name} Quiz"
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
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
    }, mapping)


def build_exit_ticket(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Exit Ticket",
        "summary": "End-of-lesson check for understanding.",
        "description": "Short constructed-response exit ticket.",
        "questions": [
            {"prompt": "What was today's learning target?", "response_lines": 2},
            {"prompt": "Write one detail or example from today's lesson.", "response_lines": 2},
            {"prompt": "How does that detail support the learning target?", "response_lines": 3},
        ],
    }, mapping)


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
    return _with_alignment({
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
    }, mapping)


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
    return _with_alignment({
        "title": rubric_title,
        "summary": "Rubric for the writing response assignment.",
        "description": prompt,
        "total_points": total_points,
        "criteria": criteria,
        "writing_prompt": prompt,
        "writing_response_title": writing_content.get("title"),
    }, mapping)


def build_written_assignment(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
    daily_topic: str | None,
) -> dict[str, Any]:
    topic = daily_topic or f"{subject_name} weekly focus"
    passage_title, passage_text = _default_passage(subject_name, topic)
    rubric_title = f"{package_title} — {subject_name} Rubric"
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Written Assignment",
        "summary": "Written response aligned to the weekly objective.",
        "description": "Students read a passage and write a paragraph with supporting details.",
        "passage_title": passage_title,
        "passage_text": passage_text,
        "student_instructions": [
            "Read the passage carefully.",
            "Write one paragraph explaining the main idea or learning target.",
            "Include at least two supporting details from the text.",
            "Use complete sentences and clear organization.",
        ],
        "success_criteria": [
            "Clear statement of the learning target",
            "At least two accurate supporting details",
            "Evidence connected to the main idea",
            "Organized paragraph with conventions",
        ],
        "rubric_reference": rubric_title,
        "writing_lines": 12,
    }, mapping)


def build_rubric_for_written_assignment(
    *,
    assignment_content: dict[str, Any],
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    passage_title = str(assignment_content.get("passage_title") or f"{subject_name} passage").strip()
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
                success_criteria[0] if success_criteria else "Clear statement of the learning target",
                "Partial statement of the learning target",
                "Missing or inaccurate main idea",
            ],
        },
        {
            "name": "Text evidence and support",
            "points": 5,
            "levels": [
                success_criteria[1] if len(success_criteria) > 1 else "Two or more accurate supporting details",
                "Some supporting details with gaps",
                "Details missing or unrelated",
            ],
        },
        {
            "name": "Explanation",
            "points": 4,
            "levels": [
                success_criteria[2] if len(success_criteria) > 2 else "Explains how evidence supports the target",
                "Partial explanation",
                "No explanation",
            ],
        },
        {
            "name": "Organization",
            "points": 3,
            "levels": [
                success_criteria[3] if len(success_criteria) > 3 else "Organized paragraph with clear flow",
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
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Written Assignment Rubric",
        "summary": "Rubric for the written assignment.",
        "description": f"Rubric aligned to {passage_title} and {objective_text}",
        "total_points": total_points,
        "criteria": criteria,
        "assignment_title": assignment_content.get("title"),
        "passage_title": passage_title,
    }, mapping)


def build_rubric(
    *,
    subject_name: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    criteria = [
        {"name": "Learning Target", "points": 4, "levels": ["Meets the objective clearly", "Partial understanding", "Does not meet the objective"]},
        {"name": "Supporting Details", "points": 4, "levels": ["Two or more relevant details", "One relevant detail", "Details missing or unrelated"]},
        {"name": "Evidence and Explanation", "points": 4, "levels": ["Explains how details support the target", "Partial explanation", "No explanation"]},
        {"name": "Organization", "points": 4, "levels": ["Logical structure", "Some organization", "Disorganized"]},
        {"name": "Grammar / Conventions", "points": 4, "levels": ["Strong conventions", "Minor errors", "Frequent errors affecting meaning"]},
    ]
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Rubric",
        "summary": "20-point rubric for the written assignment.",
        "description": f"Rubric aligned to {objective_text}",
        "total_points": 20,
        "criteria": criteria,
    }, mapping)


def build_parent_newsletter(
    *,
    subject_name: str,
    week_label: str,
    package_title: str,
    objective_code: str | None,
    objective_text: str,
) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — Parent Newsletter Summary",
        "summary": f"Weekly family update for {subject_name}.",
        "description": "Parent-friendly summary of learning and home practice.",
        "sections": [
            {"heading": "What We Are Learning", "body": f"In {subject_name} this week, students are working on: {objective_text}"},
            {"heading": "In Family-Friendly Language", "body": "Ask your child to explain what they practiced and give an example from class."},
            {
                "heading": "Practice at Home",
                "bullets": [
                    "Read together for 20 minutes.",
                    f"Ask: What was the main idea of today's {subject_name} lesson?",
                ],
            },
            {"heading": "Upcoming Assessment", "body": f"Students will complete a {week_label} check aligned to the objective."},
            {"heading": "Reminders", "bullets": ["Return signed forms by Friday.", "Contact the teacher with questions about assignments."]},
        ],
    }, mapping)


def build_bell_ringer(*, subject_name: str, package_title: str, objective_text: str, objective_code: str | None) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Bell Ringer",
        "summary": "Daily warm-up prompts.",
        "description": "Short opener activities for the week.",
        "sections": [
            {"heading": "Monday", "body": "Define the week's objective in your own words."},
            {"heading": "Tuesday", "body": "List two details from yesterday's lesson."},
            {"heading": "Wednesday", "body": "Explain how one detail supports the learning target."},
        ],
    }, mapping)


def build_vocabulary_list(*, subject_name: str, package_title: str, objective_text: str, objective_code: str | None) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Vocabulary",
        "summary": "Key terms for the week.",
        "description": "Vocabulary connected to the weekly objective.",
        "terms": [
            {"term": "objective", "definition": "What students should learn this week."},
            {"term": "evidence", "definition": "Proof from the text or lesson that supports your thinking."},
            {"term": "main idea", "definition": "The most important point the author or lesson wants you to understand."},
            {"term": "supporting detail", "definition": "A fact or example that explains the main idea."},
        ],
    }, mapping)


def build_study_guide(*, subject_name: str, week_label: str, package_title: str, objective_text: str, objective_code: str | None) -> dict[str, Any]:
    mapping = _objective_mapping(objective_code, objective_text)
    return _with_alignment({
        "title": f"{package_title} — {subject_name} Study Guide",
        "summary": f"Review guide for {week_label}.",
        "description": "Student review before the weekly assessment.",
        "sections": [
            {"heading": "Learning Target", "body": objective_text},
            {"heading": "Review Steps", "bullets": ["Reread your notes.", "Practice explaining the objective.", "Find two supporting details."]},
            {"heading": "Practice", "body": f"Prepare for the {subject_name} quiz and written assignment."},
        ],
    }, mapping)


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
        ),
        "quiz": lambda: build_quiz(
            subject_name=subject_name,
            week_label=week_label,
            package_title=package_title,
            objective_code=objective_code,
            objective_text=objective_text,
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
        return _with_alignment({
            "title": f"{package_title} — {subject_name} Resource",
            "summary": f"Instructional resource for {week_label}.",
            "description": f"Supports {objective_text}",
            "sections": [{"heading": "Overview", "body": objective_text}],
        }, mapping)
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
        from oziebot_api.services.teacher_assist_v2.package_export import build_google_forms_package_payload

        content["google_forms_package"] = build_google_forms_package_payload(content)
    return content
