"""Seed generated week artifacts linked to pacing guide weeks."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.scripts.seed_pacing_guides import _seed_actor


def seed_week_generated_artifacts(db: Session) -> dict[str, int]:
    counts = {"artifacts": 0}
    actor, tenant_id = _seed_actor(db)
    week = db.scalars(
        select(TeacherAssistPacingGuidePeriod)
        .join(TeacherAssistPacingGuide, TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id)
        .where(
            TeacherAssistPacingGuide.tenant_id == tenant_id,
            TeacherAssistPacingGuidePeriod.period_type == "WEEK",
        )
        .order_by(TeacherAssistPacingGuidePeriod.sequence_number)
    ).first()
    if week is None:
        return counts

    existing = db.scalars(
        select(TeacherAssistGeneratedArtifact).where(
            TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
            TeacherAssistGeneratedArtifact.pacing_guide_period_id == week.id,
        )
    ).first()
    if existing is not None:
        return counts

    guide = db.get(TeacherAssistPacingGuide, week.pacing_guide_id)
    if guide is None:
        return counts

    now = datetime.now(UTC)
    examples = [
        ("LESSON_PLAN", f"{week.title} Instructional Plan", "draft"),
        ("ASSIGNMENT", f"{week.title} Assignment", "draft"),
        ("QUIZ", f"{week.title} Quiz", "draft"),
        ("RUBRIC", f"{week.title} Rubric", "draft"),
        ("NEWSLETTER", f"Weekly Newsletter — {week.title}", "draft"),
    ]
    for artifact_type, title, status in examples:
        db.add(
            TeacherAssistGeneratedArtifact(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                created_by_user_id=actor.id,
                pacing_guide_id=guide.id,
                pacing_guide_period_id=week.id,
                artifact_type=artifact_type,
                title=title,
                status=status,
                instructional_plan_id=None,
                planning_draft_id=None,
                assignment_id=None,
                export_artifact_id=None,
                newsletter_id=None,
                resource_links_json=[],
                metadata_json={
                    "week_context": {
                        "pacing_guide_id": str(guide.id),
                        "pacing_guide_period_id": str(week.id),
                    },
                    "seed_example": True,
                },
                created_at=now,
                updated_at=now,
            )
        )
        counts["artifacts"] += 1
    if examples:
        db.add(
            TeacherAssistGeneratedArtifact(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                created_by_user_id=actor.id,
                pacing_guide_id=guide.id,
                pacing_guide_period_id=week.id,
                artifact_type="PARENT_COMMUNICATION",
                title=f"Parent Update — {week.title}",
                status="draft",
                instructional_plan_id=None,
                planning_draft_id=None,
                assignment_id=None,
                export_artifact_id=None,
                newsletter_id=None,
                resource_links_json=[],
                metadata_json={
                    "draft_body": f"This week we focused on {week.title}.",
                    "outbound_send_enabled": False,
                    "seed_example": True,
                },
                created_at=now,
                updated_at=now,
            )
        )
        counts["artifacts"] += 1
    return counts


def main() -> None:
    from oziebot_api.config import get_settings
    from oziebot_api.db.session import make_session_factory

    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")

    session = factory()
    try:
        counts = seed_week_generated_artifacts(session)
        session.commit()
        print("Week artifact seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()
