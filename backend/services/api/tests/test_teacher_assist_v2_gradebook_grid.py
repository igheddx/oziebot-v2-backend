from __future__ import annotations

from oziebot_api.services.teacher_assist_v2.gradebook_grid import (
    _assignment_cell,
    _teks_summary_cell,
)
from oziebot_api.services.teacher_assist_v2.mastery_constants import (
    format_mastery_level_label,
    serialize_mastery_level_fields,
)


def test_serialize_mastery_level_fields_missing() -> None:
    payload = serialize_mastery_level_fields(mastery_level="missing")
    assert payload == {"mastery_level": "missing", "mastery_level_label": "Missing"}


def test_format_mastery_level_label_missing() -> None:
    assert format_mastery_level_label("missing") == "Missing"


def test_assignment_cell_missing_without_record() -> None:
    cell = _assignment_cell(record=None)
    assert cell["mastery_level"] == "missing"
    assert cell["has_grade"] is False


def test_teks_summary_missing_when_any_assignment_missing() -> None:
    cell = _teks_summary_cell(
        assignment_cells=[
            _assignment_cell(record=None),
            {
                "has_grade": True,
                "percentage": 90.0,
                "mastery_level": "mastery",
                "mastery_level_label": "Mastery",
                "mastery_level_short": "M",
            },
        ]
    )
    assert cell["mastery_level"] == "missing"


def test_teks_summary_average_when_all_graded() -> None:
    cell = _teks_summary_cell(
        assignment_cells=[
            {
                "has_grade": True,
                "percentage": 80.0,
                "mastery_level": "mastery",
                "mastery_level_label": "Mastery",
                "mastery_level_short": "M",
            },
            {
                "has_grade": True,
                "percentage": 70.0,
                "mastery_level": "developing",
                "mastery_level_label": "Developing",
                "mastery_level_short": "D",
            },
        ]
    )
    assert cell["percentage"] == 75.0
    assert cell["mastery_level"] == "developing"
