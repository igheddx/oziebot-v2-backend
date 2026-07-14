"""Deterministic classroom-ready ELA Week 1: Main Idea demo content."""

from __future__ import annotations

from typing import Any

OBJECTIVE_CODE = "5.6E"
OBJECTIVE_TEXT = "Students identify the main idea and supporting details in informational text."
WEEK_LABEL = "Week 1"
PACKAGE_TITLE = "ELA Week 1: Main Idea"
SUBJECT_NAME = "ELA"
PASSAGE_TITLE = "How Honeybees Help Farmers"

COMMON_METADATA = {
    "objective_code": OBJECTIVE_CODE,
    "objective_text": OBJECTIVE_TEXT,
    "week_label": WEEK_LABEL,
    "subject_name": SUBJECT_NAME,
}


def objective_mapping() -> dict[str, Any]:
    return {
        "objective_code": OBJECTIVE_CODE,
        "objective_text": OBJECTIVE_TEXT,
        "standard_set": "TEKS",
    }


def _ela_subject_block(
    *,
    day_focus: str,
    mini_lesson: str,
    teacher_modeling: list[str],
    guided_practice: list[str],
    independent_practice: list[str],
    check_for_understanding: str,
    closure: str,
    teacher_notes: str,
) -> dict[str, Any]:
    return {
        "subject_name": SUBJECT_NAME,
        "objective": OBJECTIVE_TEXT,
        "materials": [
            "Informational passage: How Honeybees Help Farmers",
            "Main idea graphic organizer",
            "Student notebooks",
            "Chart paper and markers",
        ],
        "mini_lesson": mini_lesson,
        "teacher_modeling": teacher_modeling,
        "teacher_actions": teacher_modeling,
        "guided_practice": guided_practice,
        "student_activity": guided_practice + independent_practice,
        "independent_practice": independent_practice,
        "check_for_understanding": check_for_understanding,
        "assessment": check_for_understanding,
        "closure": closure,
        "notes": teacher_notes,
    }


DAILY_FOCUS: dict[str, str] = {
    "Monday": "Launch main idea vocabulary and preview the week's informational text.",
    "Tuesday": "Identify the main idea using title, headings, and repeated ideas.",
    "Wednesday": "Distinguish main idea from supporting details with a graphic organizer.",
    "Thursday": "Use text evidence to explain the main idea in a short response.",
    "Friday": "Apply the skill independently and reflect on learning.",
}


def build_daily_lesson_plan(day_label: str) -> dict[str, Any]:
    focus = DAILY_FOCUS[day_label]
    block = _ela_subject_block(
        day_focus=focus,
        mini_lesson=(
            f"Today we focus on {focus.lower()} "
            f"Read the short passage together and annotate words that repeat or seem most important."
        ),
        teacher_modeling=[
            "Read the first paragraph aloud and think aloud about what the author wants readers to remember.",
            "Underline repeated words and circle the sentence that states the big idea.",
            "Name one supporting detail that proves the main idea.",
        ],
        guided_practice=[
            "Partners reread one section and agree on the main idea in one sentence.",
            "Teams list two supporting details on sticky notes and place them on the organizer.",
        ],
        independent_practice=[
            "Students write the main idea in their own words.",
            "Students add two supporting details from the text.",
        ],
        check_for_understanding=(
            "Turn and talk: What is the main idea, and which detail best supports it?"
        ),
        closure=f"Exit reflection: {focus}",
        teacher_notes=f"{day_label} focus — {focus} Adjust pacing if students need another modeled paragraph.",
    )
    return {
        "title": f"{day_label} Daily Teaching Plan — {PACKAGE_TITLE}",
        "summary": focus,
        "description": f"Full-day ELA plan for {day_label} covering main idea and supporting details.",
        "objective_mapping": objective_mapping(),
        "subjects": [block],
        **COMMON_METADATA,
    }


def build_subject_slide_deck() -> dict[str, Any]:
    slides: list[dict[str, Any]] = [
        {
            "title": PACKAGE_TITLE,
            "subtitle": "Informational Text — Main Idea & Supporting Details",
            "bullets": [OBJECTIVE_TEXT],
            "layout": "title_only",
            "visualType": "none",
            "teacherNotes": "Welcome students and preview the week's learning goal.",
        },
        {
            "title": "Learning Objective",
            "bullets": [OBJECTIVE_TEXT, f"TEKS {OBJECTIVE_CODE}"],
            "layout": "text_only",
            "visualType": "checklist",
            "teacherNotes": "Students restate the objective in kid-friendly language.",
        },
        {
            "title": "Vocabulary",
            "bullets": [
                "main idea",
                "supporting detail",
                "evidence",
                "informational text",
                "summarize",
            ],
            "layout": "two_column",
            "visualType": "vocabulary_card",
            "teacherNotes": "Quick choral read; add motions for main idea vs. detail.",
        },
        {
            "title": "What Is Main Idea?",
            "bullets": [
                "The main idea is what the author most wants you to understand.",
                "It is supported by facts, examples, and explanations.",
            ],
            "layout": "text_left_visual_right",
            "visualType": "main_idea_web",
            "teacherNotes": "Use the web graphic to show one central idea with branches.",
        },
        {
            "title": "Supporting Details",
            "bullets": [
                "Details explain, prove, or describe the main idea.",
                "Ask: Does this detail help prove the big idea?",
            ],
            "layout": "visual_top_text_bottom",
            "visualType": "supporting_details_chart",
            "teacherNotes": "Sort sample sentences into main idea vs. supporting detail.",
        },
        {
            "title": "Teacher Modeling",
            "bullets": [
                "Read aloud the opening paragraph.",
                "Think aloud: 'The author keeps saying bees help crops grow.'",
                "Write the main idea on chart paper.",
            ],
            "layout": "text_only",
            "visualType": "paragraph_structure",
            "teacherNotes": "Slow down think-aloud so students can copy your process.",
        },
        {
            "title": "Guided Practice",
            "bullets": [
                "Partners read paragraph 2 together.",
                "Highlight two supporting details.",
                "Share out and justify choices.",
            ],
            "layout": "practice_activity",
            "visualType": "supporting_details_chart",
            "teacherNotes": "Circulate and prompt with: 'How does that detail connect?'",
        },
        {
            "title": "Independent Practice",
            "bullets": [
                "Write the main idea in one sentence.",
                "Add two supporting details from the text.",
            ],
            "layout": "practice_activity",
            "visualType": "paragraph_structure",
            "teacherNotes": "Provide sentence frames for students who need support.",
        },
        {
            "title": "Exit Ticket",
            "bullets": [
                "What is the main idea of today's passage?",
                "Cite one detail that supports your answer.",
            ],
            "layout": "exit_ticket",
            "visualType": "text_evidence_icon",
            "teacherNotes": "Collect quickly; sort for reteach tomorrow if needed.",
        },
        {
            "title": "Wrap-Up",
            "bullets": [
                "Celebrate strong evidence-based responses.",
                "Preview tomorrow: finding main idea using headings.",
            ],
            "layout": "title_only",
            "visualType": "none",
            "teacherNotes": "Assign optional reread of the passage at home.",
        },
    ]
    return {
        "title": PACKAGE_TITLE,
        "summary": "Classroom slide deck for main idea and supporting details.",
        "description": "Presentation-ready slides with visuals for ELA Week 1.",
        "objective_mapping": objective_mapping(),
        "slides": slides,
        **COMMON_METADATA,
    }


def build_quiz() -> dict[str, Any]:
    questions = [
        {
            "number": 1,
            "type": "multiple_choice",
            "prompt": "What is the main idea of an informational passage?",
            "choices": [
                "The most important point the author wants readers to understand",
                "The first sentence only",
                "A interesting fact with no connection",
                "The longest paragraph",
            ],
            "answer": "The most important point the author wants readers to understand",
        },
        {
            "number": 2,
            "type": "multiple_choice",
            "prompt": "Which detail best supports the main idea that honeybees help farmers?",
            "choices": [
                "Bees pollinate crops so plants can produce fruit.",
                "Bees make honey in hives.",
                "Farmers wear hats in the sun.",
                "Some farms have barns.",
            ],
            "answer": "Bees pollinate crops so plants can produce fruit.",
        },
        {
            "number": 3,
            "type": "short_answer",
            "prompt": "Write the main idea of How Honeybees Help Farmers in one sentence.",
            "answer": "Honeybees help farmers by pollinating crops.",
        },
        {
            "number": 4,
            "type": "evidence_based",
            "prompt": "Give one supporting detail from the passage and explain how it supports the main idea.",
            "answer": "Accept responses that cite pollination, crop growth, or food production with a clear link.",
        },
        {
            "number": 5,
            "type": "multiple_choice",
            "prompt": "Supporting details are meant to —",
            "choices": [
                "prove or explain the main idea",
                "replace the main idea",
                "introduce a new topic",
                "list random facts",
            ],
            "answer": "prove or explain the main idea",
        },
        {
            "number": 6,
            "type": "short_answer",
            "prompt": "Name two words from this week's vocabulary that help readers find the main idea.",
            "answer": "Accept main idea, supporting detail, evidence, summarize, or informational text.",
        },
        {
            "number": 7,
            "type": "multiple_choice",
            "prompt": "Which question helps you find the main idea?",
            "choices": [
                "What is the author trying to teach me?",
                "How many pages are in the book?",
                "What color is the cover?",
                "Who printed the passage?",
            ],
            "answer": "What is the author trying to teach me?",
        },
        {
            "number": 8,
            "type": "evidence_based",
            "prompt": "Underline the sentence in the passage that best states the main idea and copy it here.",
            "answer": "Accept a sentence about bees helping farmers through pollination.",
        },
    ]
    return {
        "title": f"{PACKAGE_TITLE} Quiz",
        "summary": "Formative quiz on main idea and supporting details.",
        "description": "8-question quiz with mixed item types and TEKS alignment.",
        "objective_mapping": objective_mapping(),
        "student_number_field": True,
        "questions": questions,
        "answer_key": [{"number": q["number"], "answer": q["answer"]} for q in questions],
        "google_forms_package": None,
        **COMMON_METADATA,
    }


def build_exit_ticket() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Exit Ticket",
        "summary": "Quick check for main idea and supporting details.",
        "description": "Short constructed-response exit ticket for end of lesson.",
        "objective_mapping": objective_mapping(),
        "questions": [
            {"prompt": "What is the main idea of today's passage?", "response_lines": 2},
            {"prompt": "Write one supporting detail from the text.", "response_lines": 2},
            {"prompt": "How do you know that detail supports the main idea?", "response_lines": 3},
        ],
        **COMMON_METADATA,
    }


def build_written_assignment() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Written Assignment",
        "summary": "Paragraph response explaining main idea with supporting details.",
        "description": "Students read an informational passage and write an evidence-based paragraph.",
        "objective_mapping": objective_mapping(),
        "passage_title": PASSAGE_TITLE,
        "passage_text": (
            "Honeybees visit flowers to collect nectar. As they move from flower to flower, pollen sticks to their "
            "bodies and spreads to other plants. This process, called pollination, helps crops grow fruits and "
            "vegetables. Farmers depend on healthy bee populations to produce much of our food. Without bees, many "
            "plants would not make seeds or fruit."
        ),
        "student_instructions": [
            "Read the passage carefully.",
            "Write one paragraph explaining the main idea.",
            "Include at least two supporting details from the text.",
            "Use complete sentences and text evidence.",
        ],
        "success_criteria": [
            "Clear main idea statement",
            "At least two accurate supporting details",
            "Evidence connected to the main idea",
            "Organized paragraph with conventions",
        ],
        "rubric_reference": f"{PACKAGE_TITLE} Rubric",
        "writing_lines": 12,
        **COMMON_METADATA,
    }


def build_rubric() -> dict[str, Any]:
    criteria = [
        {
            "name": "Main Idea Identification",
            "points": 4,
            "levels": [
                "States a clear, accurate main idea",
                "Main idea is vague or partially accurate",
                "Main idea missing or incorrect",
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
                "Explains how details support the main idea",
                "Partial explanation",
                "No explanation",
            ],
        },
        {
            "name": "Organization",
            "points": 4,
            "levels": ["Logical paragraph structure", "Some organization", "Disorganized"],
        },
        {
            "name": "Grammar / Conventions",
            "points": 4,
            "levels": [
                "Strong conventions",
                "Minor errors",
                "Frequent errors that interfere with meaning",
            ],
        },
    ]
    return {
        "title": f"{PACKAGE_TITLE} Rubric",
        "summary": "20-point rubric for the written assignment.",
        "description": "Criteria-based rubric aligned to main idea performance.",
        "objective_mapping": objective_mapping(),
        "total_points": 20,
        "criteria": criteria,
        **COMMON_METADATA,
    }


def build_parent_newsletter() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Parent Newsletter Summary",
        "summary": "Weekly family communication for main idea learning.",
        "description": "Parent-friendly overview of class learning and home practice.",
        "objective_mapping": objective_mapping(),
        "sections": [
            {
                "heading": "What We Are Learning",
                "body": "Students are reading informational text and identifying the main idea and supporting details.",
            },
            {
                "heading": "Learning Goal in Family Language",
                "body": "Your child is learning to find the big idea in a passage and explain it using proof from the text.",
            },
            {
                "heading": "Practice at Home",
                "bullets": [
                    "Ask your child to summarize a short news or magazine paragraph.",
                    "Prompt: What is the main idea? What details prove it?",
                ],
            },
            {
                "heading": "Upcoming Assessment",
                "body": "A short quiz and written paragraph on main idea will be completed this week.",
            },
            {
                "heading": "Reminders",
                "bullets": [
                    "Ensure your child reads 20 minutes nightly.",
                    "Return any signed assignment pages by Friday.",
                ],
            },
        ],
        **COMMON_METADATA,
    }


def build_bell_ringer() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Bell Ringer",
        "summary": "Warm-up prompts to activate main idea thinking.",
        "description": "Daily opener questions for the week.",
        "objective_mapping": objective_mapping(),
        "sections": [
            {"heading": "Monday", "body": "Define main idea in your own words."},
            {"heading": "Tuesday", "body": "List three details from a headline article."},
            {"heading": "Wednesday", "body": "Sort two sentences: main idea vs. detail."},
        ],
        **COMMON_METADATA,
    }


def build_vocabulary_list() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Vocabulary List",
        "summary": "Key terms for main idea instruction.",
        "description": "Student-friendly definitions for the week.",
        "objective_mapping": objective_mapping(),
        "terms": [
            {
                "term": "main idea",
                "definition": "The most important point the author wants readers to understand.",
            },
            {
                "term": "supporting detail",
                "definition": "A fact or example that proves or explains the main idea.",
            },
            {"term": "evidence", "definition": "Proof from the text that supports your thinking."},
            {"term": "summarize", "definition": "Tell the most important ideas in a shorter way."},
        ],
        **COMMON_METADATA,
    }


def build_study_guide() -> dict[str, Any]:
    return {
        "title": f"{PACKAGE_TITLE} Study Guide",
        "summary": "Review guide for main idea and supporting details.",
        "description": "Student study support for the week assessment.",
        "objective_mapping": objective_mapping(),
        "sections": [
            {
                "heading": "Key Question",
                "body": "What is the main idea, and how do details support it?",
            },
            {
                "heading": "Steps to Find Main Idea",
                "bullets": [
                    "Read the title and headings.",
                    "Look for repeated ideas.",
                    "Ask what the author wants you to learn.",
                ],
            },
            {
                "heading": "Practice",
                "body": "Reread How Honeybees Help Farmers and write the main idea with two details.",
            },
        ],
        **COMMON_METADATA,
    }


CONTENT_BUILDERS: dict[str, Any] = {
    "daily_lesson_plan": build_daily_lesson_plan,
    "subject_slide_deck": lambda: build_subject_slide_deck(),
    "quiz": build_quiz,
    "exit_ticket": build_exit_ticket,
    "assignment": build_written_assignment,
    "rubric": build_rubric,
    "parent_newsletter_summary": build_parent_newsletter,
    "bell_ringer": build_bell_ringer,
    "vocabulary_list": build_vocabulary_list,
    "study_guide": build_study_guide,
}
