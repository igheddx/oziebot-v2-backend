"""Validate Texas / LISD / Mason Elementary Grade 5 pilot seed completeness."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationCurriculumResource,
    EducationDistrict,
    EducationObjective,
    EducationSchool,
    EducationState,
)
from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_copilot_session import TeacherCopilotSession
from oziebot_api.scripts.seed_pacing_guides import _seed_actor


def validate_pilot_seed_data(db: Session) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    def add(name: str, ok: bool, detail: str, count: int | None = None) -> None:
        checks.append({"name": name, "ok": ok, "detail": detail, "count": count})

    texas = db.scalars(select(EducationState).where(EducationState.abbreviation == "TX")).first()
    add("Texas state catalog", texas is not None, "Education catalog state TX", 1 if texas else 0)

    lisd = None
    mason = None
    if texas is not None:
        lisd = db.scalars(
            select(EducationDistrict).where(
                EducationDistrict.state_id == texas.id,
                EducationDistrict.name.ilike("%LISD%"),
            )
        ).first()
        add("LISD district", lisd is not None, "Lewisville ISD district record", 1 if lisd else 0)
        if lisd is not None:
            mason = db.scalars(
                select(EducationSchool).where(
                    EducationSchool.district_id == lisd.id,
                    EducationSchool.name.ilike("%Mason%"),
                )
            ).first()
            add(
                "Mason Elementary",
                mason is not None,
                "Mason Elementary school record",
                1 if mason else 0,
            )

    objective_count = int(db.scalar(select(func.count()).select_from(EducationObjective)) or 0)
    add("Objectives", objective_count > 0, "Catalog objectives present", objective_count)

    resource_count = int(
        db.scalar(select(func.count()).select_from(EducationCurriculumResource)) or 0
    )
    add("Curriculum resources", resource_count > 0, "Catalog resources present", resource_count)

    try:
        actor, tenant_id = _seed_actor(db)
        guide_count = int(
            db.scalar(
                select(func.count())
                .select_from(TeacherAssistPacingGuide)
                .where(TeacherAssistPacingGuide.tenant_id == tenant_id)
            )
            or 0
        )
        week_count = int(
            db.scalar(
                select(func.count())
                .select_from(TeacherAssistInstructionalWeek)
                .where(TeacherAssistInstructionalWeek.tenant_id == tenant_id)
            )
            or 0
        )
        copilot_count = int(
            db.scalar(
                select(func.count())
                .select_from(TeacherCopilotSession)
                .where(TeacherCopilotSession.tenant_id == tenant_id)
            )
            or 0
        )
        add(
            "Pacing guides (seed tenant)",
            guide_count > 0,
            "Teacher pacing guides for seed actor",
            guide_count,
        )
        add(
            "Instructional weeks (seed tenant)",
            week_count > 0,
            "Instructional weeks for seed actor",
            week_count,
        )
        add(
            "Copilot sessions (seed tenant)",
            copilot_count >= 0,
            "Copilot demo sessions (optional until seed_teacher_copilot run)",
            copilot_count,
        )
        add("Seed actor", True, f"Seed actor {actor.email}", None)
    except Exception as exc:  # pragma: no cover - seed actor may be absent in empty DB
        add("Seed actor", False, str(exc), 0)

    passed = sum(1 for row in checks if row["ok"])
    return {
        "summary": {
            "checks_total": len(checks),
            "checks_passed": passed,
            "ready_for_pilot": passed >= max(1, len(checks) - 2),
        },
        "checks": checks,
        "recommended_seed_commands": [
            "python3 -m oziebot_api.scripts.seed_education_catalog",
            "python3 -m oziebot_api.scripts.seed_pacing_guides",
            "python3 -m oziebot_api.scripts.seed_instructional_weeks",
            "python3 -m oziebot_api.scripts.seed_instructional_loop",
            "python3 -m oziebot_api.scripts.seed_teacher_copilot",
        ],
    }
