from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy.orm import Session

from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.instructional_asset_reuse import (
    InstructionalAssetReuseService,
)


def build_week_recommendations(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user: User,
    period_id: uuid.UUID,
) -> dict[str, Any]:
    rows = InstructionalAssetReuseService.search(
        db, tenant_id=tenant_id, user=user, period_id=period_id, limit=8
    )
    grouped = {
        "previous_lessons": [row for row in rows if row.get("artifact_type") == "LESSON_PLAN"],
        "previous_assignments": [row for row in rows if row.get("artifact_type") == "ASSIGNMENT"],
        "similar_quizzes": [row for row in rows if row.get("artifact_type") == "QUIZ"],
        "matching_rubrics": [row for row in rows if row.get("artifact_type") == "RUBRIC"],
        "matching_newsletters": [row for row in rows if row.get("artifact_type") == "NEWSLETTER"],
        "top_reusable": rows[:5],
    }
    return {
        "period_id": str(period_id),
        "recommended_for_this_week": grouped,
        "all_candidates": rows,
    }
