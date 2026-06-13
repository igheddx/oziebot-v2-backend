from __future__ import annotations

import uuid

import pytest

from oziebot_api.services.teacher_assist_v2.package_additional_assignments import (
    ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES,
    ADDITIONAL_ASSIGNMENT_TYPE_LABELS,
    generate_additional_package_assignment,
)


def test_additional_assignment_types_are_supported_mvp_outputs() -> None:
    assert "quiz" in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES
    assert "assignment" in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES
    assert "writing_response" in ADDITIONAL_ASSIGNMENT_ARTIFACT_TYPES
    assert ADDITIONAL_ASSIGNMENT_TYPE_LABELS["writing_response"] == "Writing response"


def test_generate_requires_teacher_notes() -> None:
    with pytest.raises(ValueError) as exc_info:
        generate_additional_package_assignment(
            None,  # type: ignore[arg-type]
            settings=None,  # type: ignore[arg-type]
            user=None,  # type: ignore[arg-type]
            package_id=uuid.uuid4(),
            subject_id=uuid.uuid4(),
            artifact_type="quiz",
            teacher_notes="   ",
        )
    assert "teacher_notes" in str(exc_info.value)
