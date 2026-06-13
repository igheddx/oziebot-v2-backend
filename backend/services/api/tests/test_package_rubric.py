from __future__ import annotations

import pytest

from oziebot_api.services.teacher_assist_v2.deterministic_package_content import build_rubric_for_writing_response
from oziebot_api.services.teacher_assist_v2.package_artifact_update import _validate_rubric_content


def test_build_rubric_for_writing_response_uses_prompt() -> None:
    writing_content = {
        "title": "Week 1 — ELA Writing Response",
        "prompt": "Explain how the author develops the main idea using text evidence.",
        "instructions": ["Use two details from the text.", "Write one paragraph."],
    }
    rubric = build_rubric_for_writing_response(
        writing_content=writing_content,
        subject_name="ELA",
        package_title="Week 1 Instructional Package",
        objective_code="5.6A",
        objective_text="Students analyze the author's use of text evidence.",
    )
    assert rubric["writing_prompt"] == writing_content["prompt"]
    assert rubric["total_points"] == sum(row["points"] for row in rubric["criteria"])
    assert rubric["criteria"][0]["name"] == "Addresses the writing prompt"


def test_validate_rubric_content_normalizes_total_points() -> None:
    content = _validate_rubric_content(
        {
            "title": "Writing Rubric",
            "criteria": [
                {
                    "name": "Prompt focus",
                    "points": 5,
                    "levels": ["Strong", "Partial", "Limited"],
                },
                {
                    "name": "Conventions",
                    "points": 3,
                    "levels": ["Strong", "Partial", "Limited"],
                },
            ],
        }
    )
    assert content["total_points"] == 8
    assert len(content["criteria"]) == 2


def test_validate_rubric_content_requires_criteria() -> None:
    with pytest.raises(ValueError):
        _validate_rubric_content({"title": "Empty rubric", "criteria": []})
