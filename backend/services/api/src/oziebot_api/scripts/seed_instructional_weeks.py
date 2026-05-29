"""Seed instructional weeks for Texas demo data."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.scripts.seed_pacing_guides import _seed_actor, seed_pacing_guides
from oziebot_api.services.teacher_assist.instructional_weeks import (
    create_instructional_week_from_pacing_period,
    find_instructional_week_for_period,
    link_entities_to_instructional_week,
)


def seed_instructional_weeks(db: Session) -> dict[str, int]:
    counts = {"instructional_weeks": 0, "artifacts": 0}
    seed_pacing_guides(db)
    actor, tenant_id = _seed_actor(db)

    guides = list(
        db.scalars(
            select(TeacherAssistPacingGuide).where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuide.title.ilike("%Grade 5%"),
            )
        ).all()
    )
    for guide in guides[:2]:
        periods = list(
            db.scalars(
                select(TeacherAssistPacingGuidePeriod)
                .where(
                    TeacherAssistPacingGuidePeriod.pacing_guide_id == guide.id,
                    TeacherAssistPacingGuidePeriod.period_type == "WEEK",
                )
                .order_by(TeacherAssistPacingGuidePeriod.sequence_number)
            ).all()
        )
        for period in periods[:3]:
            week = find_instructional_week_for_period(
                db, tenant_id=tenant_id, user_id=actor.id, pacing_guide_period_id=period.id
            )
            if week is None:
                week = create_instructional_week_from_pacing_period(
                    db,
                    tenant_id=tenant_id,
                    user=actor,
                    pacing_guide_period_id=period.id,
                    status="ACTIVE" if period.sequence_number <= 2 else "DRAFT",
                )
                counts["instructional_weeks"] += 1

            existing_artifacts = list(
                db.scalars(
                    select(TeacherAssistGeneratedArtifact).where(
                        TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
                        TeacherAssistGeneratedArtifact.pacing_guide_period_id == period.id,
                    )
                ).all()
            )
            if existing_artifacts:
                for artifact in existing_artifacts:
                    if artifact.instructional_week_id is None:
                        artifact.instructional_week_id = week.id
                        counts["artifacts"] += 1
                link_entities_to_instructional_week(
                    db,
                    tenant_id=tenant_id,
                    user_id=actor.id,
                    pacing_guide_period_id=period.id,
                )
                continue

            now = datetime.now(UTC)
            examples = [
                ("LESSON_PLAN", f"{period.title} Instructional Plan"),
                ("ASSIGNMENT", f"{period.title} Assignment"),
                ("QUIZ", f"{period.title} Quiz"),
                ("RUBRIC", f"{period.title} Rubric"),
                ("NEWSLETTER", f"Weekly Newsletter — {period.title}"),
            ]
            for artifact_type, title in examples:
                db.add(
                    TeacherAssistGeneratedArtifact(
                        tenant_id=tenant_id,
                        created_by_user_id=actor.id,
                        pacing_guide_id=guide.id,
                        pacing_guide_period_id=period.id,
                        instructional_week_id=week.id,
                        artifact_type=artifact_type,
                        title=title,
                        status="draft",
                        resource_links_json=[],
                        metadata_json={
                            "seed_example": True,
                            "instructional_week_id": str(week.id),
                            "objective_codes": [],
                        },
                        created_at=now,
                        updated_at=now,
                    )
                )
                counts["artifacts"] += 1
            link_entities_to_instructional_week(
                db,
                tenant_id=tenant_id,
                user_id=actor.id,
                pacing_guide_period_id=period.id,
            )
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
        counts = seed_instructional_weeks(session)
        session.commit()
        print("Instructional week seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()
