from __future__ import annotations

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist_v2.deterministic_package_content import (
    build_deterministic_fallback,
)
from oziebot_api.services.teacher_assist_v2.instructional_package_generation import (
    _real_ai_requested_but_inactive,
)


def test_v2_package_generation_blocks_inactive_real_ai(db_session) -> None:
    settings = Settings(
        teacher_assist_ai_provider="openai",
        teacher_assist_real_provider_enabled=True,
        teacher_assist_real_provider_model="gpt-4o-mini",
        teacher_assist_allowed_models="mock,gpt-4o-mini",
        teacher_assist_ai_daily_cost_limit_cents=500,
        teacher_assist_openai_api_key=None,
    )

    reason = _real_ai_requested_but_inactive(db_session, settings)

    assert reason is not None
    assert "TEACHER_ASSIST_OPENAI_API_KEY" in reason


def test_deterministic_package_content_uses_source_excerpts() -> None:
    source_excerpt = (
        "Chapter 1 explains that careful readers make an inference by combining clues "
        "from the text with what they already know."
    )
    common = {
        "subject_name": "ELA",
        "week_label": "Week 1",
        "package_title": "Week 1 ELA",
        "objective_code": "5.6F",
        "objective_text": "Make inferences using text evidence.",
        "source_materials": ["HMH Into Reading Module 1"],
        "source_excerpts": [source_excerpt],
        "daily_topics": ["Introduce inference and evidence"],
        "daily_objectives": ["Use clues from the anchor text to make an inference."],
        "assessment_checks": ["What evidence supports your inference?"],
    }

    assignment = build_deterministic_fallback("assignment", daily_topic="Inference", **common)
    quiz = build_deterministic_fallback("quiz", **common)
    deck = build_deterministic_fallback("subject_slide_deck", objectives_list=["5.6F"], **common)

    assert "Chapter 1 explains" in assignment["passage_text"]
    assert "supplied source excerpt" in assignment["student_instructions"][0]
    assert any("Chapter 1 explains" in question["prompt"] for question in quiz["questions"])
    assert any(slide["id"] == "source-evidence" for slide in deck["slides"])
    assert any(
        "Chapter 1 explains" in bullet
        for slide in deck["slides"]
        for bullet in slide.get("bullets", [])
    )
