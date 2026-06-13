from __future__ import annotations

import pytest

from oziebot_api.services.teacher_assist_v2.mastery_constants import (
    format_mastery_level_label,
    resolve_mastery_level,
    serialize_mastery_level_fields,
)


@pytest.mark.parametrize(
    ("percentage", "expected"),
    [
        (100, "mastery"),
        (80, "mastery"),
        (79.9, "developing"),
        (60, "developing"),
        (59.9, "beginning"),
        (0, "beginning"),
    ],
)
def test_resolve_mastery_level_thresholds(percentage: float, expected: str) -> None:
    assert resolve_mastery_level(percentage) == expected


def test_format_mastery_level_label() -> None:
    assert format_mastery_level_label("mastery") == "Mastery"
    assert format_mastery_level_label("developing") == "Developing"
    assert format_mastery_level_label("beginning") == "Beginning"


def test_serialize_mastery_level_fields_uses_stored_level_when_valid() -> None:
    payload = serialize_mastery_level_fields(percentage=50, mastery_level="developing")
    assert payload == {"mastery_level": "developing", "mastery_level_label": "Developing"}
