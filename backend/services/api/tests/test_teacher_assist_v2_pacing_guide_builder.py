"""Tests for TeacherAssist v2 pacing guide builder."""

from __future__ import annotations

import pytest

from oziebot_api.services.teacher_assist_v2.pacing_guide_builder import _validate_weeks


def test_validate_weeks_requires_at_least_one_week():
    with pytest.raises(ValueError, match="weeks"):
        _validate_weeks([])


def test_validate_weeks_requires_daily_plan():
    with pytest.raises(ValueError, match="Week 1"):
        _validate_weeks([{"title": "Week 1", "daily_plans": []}])


def test_validate_weeks_requires_daily_topic():
    with pytest.raises(ValueError, match="Tuesday"):
        _validate_weeks(
            [
                {
                    "title": "Week 1",
                    "daily_plans": [{"day_label": "Tuesday", "daily_topic": ""}],
                }
            ]
        )


def test_validate_weeks_accepts_daily_plan():
    weeks = _validate_weeks(
        [
            {
                "title": "Week 1",
                "daily_plans": [{"day_label": "Monday", "daily_topic": "Introduce main idea"}],
            }
        ]
    )
    assert weeks[0]["title"] == "Week 1"
    assert weeks[0]["daily_plans"][0]["day_label"] == "Monday"
