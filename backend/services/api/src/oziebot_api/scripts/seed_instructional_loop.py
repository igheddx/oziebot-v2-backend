"""Seed Phase 38 instructional loop demo data for Texas Grade 5."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_instructional_evidence import (
    TeacherAssistInstructionalEvidence,
)
from oziebot_api.models.teacher_assist_instructional_reflection import (
    TeacherAssistInstructionalReflection,
)
from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.models.teacher_assist_reteach_effectiveness import (
    TeacherAssistReteachEffectivenessRecord,
)
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_student_support_group import (
    TeacherAssistStudentSupportGroup,
    TeacherAssistStudentSupportGroupMember,
)
from oziebot_api.scripts.seed_instructional_weeks import seed_instructional_weeks
from oziebot_api.scripts.seed_pacing_guides import _seed_actor
from oziebot_api.services.teacher_assist.instructional_week_closure import (
    generate_instructional_week_summary,
)


def seed_instructional_loop(db: Session) -> dict[str, int]:
    counts = {
        "instructional_evidence": 0,
        "support_groups": 0,
        "reflections": 0,
        "effectiveness": 0,
        "summaries": 0,
    }
    seed_instructional_weeks(db)
    actor, tenant_id = _seed_actor(db)
    week = db.scalars(
        select(TeacherAssistInstructionalWeek).where(
            TeacherAssistInstructionalWeek.tenant_id == tenant_id,
            TeacherAssistInstructionalWeek.created_by_user_id == actor.id,
        )
    ).first()
    if week is None:
        return counts

    reteach_plan = db.scalars(
        select(TeacherAssistReteachPlan).where(
            TeacherAssistReteachPlan.tenant_id == tenant_id,
            TeacherAssistReteachPlan.owner_user_id == actor.id,
        )
    ).first()

    now = datetime.now(UTC)
    if (
        db.scalars(
            select(TeacherAssistInstructionalEvidence).where(
                TeacherAssistInstructionalEvidence.tenant_id == tenant_id,
                TeacherAssistInstructionalEvidence.instructional_week_id == week.id,
            )
        ).first()
        is None
    ):
        for student, score, level in [
            ("1", 62.0, "developing"),
            ("2", 41.0, "beginning"),
            ("3", 88.0, "mastery"),
        ]:
            db.add(
                TeacherAssistInstructionalEvidence(
                    tenant_id=tenant_id,
                    owner_user_id=actor.id,
                    instructional_week_id=week.id,
                    student_identifier=student,
                    source_type="ASSIGNMENT",
                    source_id=uuid.uuid4(),
                    score=score,
                    mastery_level=level,
                    teacher_confirmed=True,
                    created_at=now,
                    updated_at=now,
                )
            )
            counts["instructional_evidence"] += 1

    if (
        db.scalars(
            select(TeacherAssistStudentSupportGroup).where(
                TeacherAssistStudentSupportGroup.tenant_id == tenant_id,
                TeacherAssistStudentSupportGroup.instructional_week_id == week.id,
            )
        ).first()
        is None
    ):
        from oziebot_api.models.teacher_assist_class import TeacherAssistClass
        from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject

        school_class = db.scalars(
            select(TeacherAssistClass).where(TeacherAssistClass.tenant_id == tenant_id)
        ).first()
        subject = db.scalars(
            select(TeacherAssistSubject).where(TeacherAssistSubject.tenant_id == tenant_id)
        ).first()
        if school_class is not None and subject is not None:
            group = TeacherAssistStudentSupportGroup(
                tenant_id=tenant_id,
                owner_user_id=actor.id,
                class_id=school_class.id,
                subject_id=subject.id,
                instructional_week_id=week.id,
                title="Fraction Support Group",
                status="draft",
                notes="Generated from mastery data — review before activating.",
                suggested_activities_json=["Guided practice", "Number line review"],
                created_at=now,
                updated_at=now,
            )
            db.add(group)
            db.flush()
            for student in ("1", "2"):
                db.add(
                    TeacherAssistStudentSupportGroupMember(
                        tenant_id=tenant_id,
                        support_group_id=group.id,
                        student_identifier=student,
                        created_at=now,
                    )
                )
            counts["support_groups"] += 1

    if (
        db.scalars(
            select(TeacherAssistInstructionalReflection).where(
                TeacherAssistInstructionalReflection.tenant_id == tenant_id,
                TeacherAssistInstructionalReflection.instructional_week_id == week.id,
            )
        ).first()
        is None
    ):
        db.add(
            TeacherAssistInstructionalReflection(
                tenant_id=tenant_id,
                owner_user_id=actor.id,
                instructional_week_id=week.id,
                what_worked="Small-group fraction models helped struggling learners.",
                what_didnt_work="Independent practice was too long for some students.",
                student_challenges="Equivalent fractions remained difficult for 2 students.",
                adjustments_needed="Add a short reteach checkpoint before the quiz.",
                future_recommendations="Reuse the fraction support group structure next unit.",
                status="review",
                created_at=now,
                updated_at=now,
            )
        )
        counts["reflections"] += 1

    if (
        reteach_plan is not None
        and db.scalars(
            select(TeacherAssistReteachEffectivenessRecord).where(
                TeacherAssistReteachEffectivenessRecord.reteach_plan_id == reteach_plan.id
            )
        ).first()
        is None
    ):
        db.add(
            TeacherAssistReteachEffectivenessRecord(
                tenant_id=tenant_id,
                owner_user_id=actor.id,
                reteach_plan_id=reteach_plan.id,
                before_mastery_pct=38.0,
                after_mastery_pct=61.0,
                improvement_pct=23.0,
                teacher_reflection="Targeted reteach improved exit ticket performance.",
                recorded_at=now,
                created_at=now,
            )
        )
        counts["effectiveness"] += 1

    generate_instructional_week_summary(
        db,
        tenant_id=tenant_id,
        user_id=actor.id,
        instructional_week_id=week.id,
    )
    counts["summaries"] += 1
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
        counts = seed_instructional_loop(session)
        session.commit()
        print("Instructional loop seed complete:", counts)
    finally:
        session.close()


if __name__ == "__main__":
    main()
