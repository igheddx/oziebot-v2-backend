from __future__ import annotations

from typing import Any


def _as_lines(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [line.strip() for line in value.splitlines() if line.strip()]
    if isinstance(value, list):
        lines: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                lines.append(item.strip())
            elif isinstance(item, dict):
                text = item.get("text") or item.get("title") or item.get("description")
                if isinstance(text, str) and text.strip():
                    lines.append(text.strip())
        return lines
    return []


def _plan_overview(content: dict[str, Any]) -> str:
    overview = content.get("overview")
    return overview.strip() if isinstance(overview, str) else ""


def _plan_objectives(content: dict[str, Any]) -> list[str]:
    objectives = content.get("weekly_objectives") or content.get("objectives") or []
    return _as_lines(objectives)


def _plan_vocabulary(content: dict[str, Any]) -> list[str]:
    vocabulary = content.get("vocabulary") or []
    if isinstance(vocabulary, list):
        terms: list[str] = []
        for item in vocabulary:
            if isinstance(item, str):
                terms.append(item.strip())
            elif isinstance(item, dict):
                term = item.get("term") or item.get("word")
                definition = item.get("definition")
                if term and definition:
                    terms.append(f"{term}: {definition}")
                elif term:
                    terms.append(str(term))
        return [entry for entry in terms if entry]
    return _as_lines(vocabulary)


def _plan_standards(content: dict[str, Any]) -> list[str]:
    standards: list[str] = []
    for subject in content.get("subjects") or []:
        if not isinstance(subject, dict):
            continue
        for standard in subject.get("standards") or []:
            if isinstance(standard, dict):
                code = standard.get("code") or standard.get("standard_code")
                title = standard.get("title") or standard.get("description")
                if code and title:
                    standards.append(f"{code} — {title}")
                elif code:
                    standards.append(str(code))
            elif isinstance(standard, str):
                standards.append(standard.strip())
    return [entry for entry in standards if entry]


def _daily_prompts(content: dict[str, Any]) -> list[str]:
    prompts: list[str] = []
    daily = content.get("daily_breakdown") or []
    if not isinstance(daily, list):
        return prompts
    for day in daily:
        if not isinstance(day, dict):
            continue
        label = day.get("day_label") or day.get("day") or "Day"
        focus = day.get("focus") or day.get("lesson_focus") or day.get("objective")
        if focus:
            prompts.append(f"{label}: {focus}")
    return prompts


def build_slides_preview(
    *,
    plan_title: str,
    content_json: dict[str, Any],
    artifact_type: str,
) -> dict[str, Any]:
    objectives = _plan_objectives(content_json) or ["Review weekly objectives with students."]
    vocabulary = _plan_vocabulary(content_json) or ["Key vocabulary from this week's plan."]
    standards = _plan_standards(content_json) or ["Standards aligned to this instructional plan."]
    daily_prompts = _daily_prompts(content_json) or [
        "Mini lesson: model the target skill.",
        "Guided practice: work through examples together.",
        "Independent practice: students apply the skill.",
    ]
    slides = [
        {
            "key": "title",
            "title": plan_title,
            "bullets": [
                "TeacherAssist export preview",
                f"Artifact type: {artifact_type.replace('_', ' ')}",
            ],
        },
        {
            "key": "standards_objectives",
            "title": "Standards & Objectives",
            "bullets": standards[:4] + objectives[:4],
        },
        {
            "key": "vocabulary",
            "title": "Vocabulary",
            "bullets": vocabulary[:6],
        },
        {
            "key": "mini_lesson",
            "title": "Mini Lesson",
            "bullets": daily_prompts[:2] or ["Introduce the concept with a concise teacher model."],
        },
        {
            "key": "guided_practice",
            "title": "Guided Practice",
            "bullets": [
                "Work through an example as a class.",
                "Check for understanding with quick prompts.",
            ],
        },
        {
            "key": "independent_practice",
            "title": "Independent Practice",
            "bullets": [
                "Students complete aligned practice independently.",
                "Teacher circulates for formative checks.",
            ],
        },
        {
            "key": "discussion_prompts",
            "title": "Discussion Prompts",
            "bullets": [
                "What strategy helped you most today?",
                "Where could this skill apply outside class?",
            ],
        },
        {
            "key": "assessment_checkpoint",
            "title": "Assessment Checkpoint",
            "bullets": ["Use an exit understanding check before dismissal."],
        },
        {
            "key": "exit_ticket",
            "title": "Exit Ticket",
            "bullets": ["One sentence summary of today's learning."],
        },
    ]
    if artifact_type == "guided_notes":
        for slide in slides:
            if slide["key"] in {"guided_practice", "independent_practice", "discussion_prompts"}:
                slide["bullets"] = [f"Notes: {bullet}" for bullet in slide["bullets"]]
    return {
        "artifact_kind": "slides",
        "artifact_type": artifact_type,
        "title": plan_title,
        "slides": slides,
        "metadata": {
            "is_mock": True,
            "generator": "mock",
            "layout_version": "slides-v1",
        },
    }


def build_quiz_preview(
    *,
    plan_title: str,
    content_json: dict[str, Any],
    artifact_type: str,
) -> dict[str, Any]:
    objectives = _plan_objectives(content_json)
    standards = _plan_standards(content_json)
    vocabulary = _plan_vocabulary(content_json)
    base_question = objectives[0] if objectives else f"What is the main focus of {plan_title}?"

    if artifact_type == "exit_ticket":
        questions = [
            {
                "question_type": "short_answer",
                "question_text": "Summarize today's learning in one or two sentences.",
                "answer_key": "Teacher-reviewed response.",
                "standards": standards[:1],
                "difficulty": "formative",
            }
        ]
    elif artifact_type == "short_answer_quiz":
        questions = [
            {
                "question_type": "short_answer",
                "question_text": base_question,
                "answer_key": "Sample teacher answer keyed to plan objectives.",
                "standards": standards[:2],
                "difficulty": "medium",
            },
            {
                "question_type": "short_answer",
                "question_text": "Explain one vocabulary term from this week.",
                "answer_key": vocabulary[0]
                if vocabulary
                else "Teacher-reviewed vocabulary response.",
                "standards": standards[:1],
                "difficulty": "medium",
            },
        ]
    else:
        questions = [
            {
                "question_type": "multiple_choice",
                "question_text": base_question,
                "choices": [
                    "Objective A aligned to the weekly plan",
                    "Unrelated topic",
                    "Off-plan enrichment only",
                    "No learning target",
                ],
                "answer_key": "Objective A aligned to the weekly plan",
                "standards": standards[:2],
                "difficulty": "medium",
            },
            {
                "question_type": "true_false",
                "question_text": "This week's plan includes formative assessment checkpoints.",
                "choices": ["True", "False"],
                "answer_key": "True",
                "standards": standards[:1],
                "difficulty": "low",
            },
        ]

    return {
        "artifact_kind": "quiz",
        "artifact_type": artifact_type,
        "title": plan_title,
        "questions": questions,
        "metadata": {
            "is_mock": True,
            "generator": "mock",
            "layout_version": "quiz-v1",
        },
    }


def build_export_preview(
    *,
    plan_title: str,
    content_json: dict[str, Any],
    artifact_type: str,
) -> dict[str, Any]:
    if artifact_type in {"lesson_slides", "guided_notes"}:
        return build_slides_preview(
            plan_title=plan_title,
            content_json=content_json,
            artifact_type=artifact_type,
        )
    return build_quiz_preview(
        plan_title=plan_title,
        content_json=content_json,
        artifact_type=artifact_type,
    )
