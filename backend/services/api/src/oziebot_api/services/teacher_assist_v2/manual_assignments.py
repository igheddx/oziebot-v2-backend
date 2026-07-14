"""Teacher-created manual assignments and cover sheet workflow."""

from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationObjective
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackage,
)
from oziebot_api.models.teacher_assist_v2_onboarding import TeacherAssistV2PacingGuideAssignment
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.assignment_constants import (
    ASSIGNMENT_TYPES,
    MVP_ASSIGNMENT_TYPES,
)
from oziebot_api.services.teacher_assist_v2.assignment_print_packets import (
    generate_assignment_cover_sheets,
    resolve_student_count,
)
from oziebot_api.services.teacher_assist_v2.assignments import (
    _validate_assignment_anchors,
    get_teacher_assignment_detail,
)
from oziebot_api.services.teacher_assist_v2.planning_workflow import (
    _assignment_context,
    _guide_for_assignment,
    _require_planning_ready,
    _teacher_assignments,
    _validate_week_range,
    _week_periods,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _field_errors(**errors: str) -> ValueError:
    return ValueError({key: value for key, value in errors.items() if value})


def _ensure_manual_shell_package(
    db: Session, *, user: User, base: dict[str, Any]
) -> TeacherAssistV2InstructionalPackage:
    onboarding = base["onboarding"]
    ctx = base["ctx"]
    platform_year = base["platform_year"]
    packages = db.scalars(
        select(TeacherAssistV2InstructionalPackage).where(
            TeacherAssistV2InstructionalPackage.teacher_user_id == user.id,
            TeacherAssistV2InstructionalPackage.platform_school_year_id == platform_year.id,
        )
    ).all()
    for row in packages:
        metadata = row.metadata_json if isinstance(row.metadata_json, dict) else {}
        if metadata.get("manual_assignment_shell"):
            return row

    subject_ids = [row["subject_id"] for row in base["subjects"]]
    pacing_guide_ids = [row["pacing_guide_id"] for row in base["subjects"]]
    max_periods = max(row["period_count"] for row in base["subjects"])
    today = date.today()
    now = _now()
    package = TeacherAssistV2InstructionalPackage(
        id=uuid.uuid4(),
        tenant_id=ctx.tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=platform_year.id,
        catalog_state_id=onboarding.state_id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        subject_ids_json=subject_ids,
        pacing_guide_ids_json=pacing_guide_ids,
        primary_pacing_guide_id=uuid.UUID(pacing_guide_ids[0]) if pacing_guide_ids else None,
        title="Teacher-created assignments",
        week_start=1,
        week_end=max_periods,
        plan_start_date=today,
        plan_end_date=today,
        teaching_order_json=subject_ids,
        selected_outputs_json=[],
        status="active",
        provider_name="teacher_manual",
        metadata_json={"manual_assignment_shell": True},
        created_at=now,
        updated_at=now,
    )
    db.add(package)
    db.flush()
    return package


def _resolve_subject_context(
    db: Session,
    *,
    user: User,
    subject_id: uuid.UUID,
    week_number: int,
) -> tuple[TeacherAssistV2PacingGuideAssignment, uuid.UUID, list[dict[str, Any]]]:
    base = _assignment_context(db, user=user)
    subject_key = str(subject_id)
    subject_row = next((row for row in base["subjects"] if row["subject_id"] == subject_key), None)
    if subject_row is None:
        raise _field_errors(subject_id="Selected subject is not part of your teaching assignments.")

    min_periods = min(row["period_count"] for row in base["subjects"])
    _validate_week_range(week_start=week_number, week_end=week_number, max_periods=min_periods)

    assignment = next(
        (row for row in _teacher_assignments(db, user=user) if str(row.subject_id) == subject_key),
        None,
    )
    if assignment is None:
        raise _field_errors(subject_id="Selected subject is not linked to a pacing guide.")

    guide = _guide_for_assignment(
        db,
        tenant_id=base["ctx"].tenant_id,
        platform_tenant_id=base["platform_tenant_id"],
        assignment=assignment,
    )
    periods = _week_periods(guide)
    if week_number > len(periods):
        raise _field_errors(week_number="Selected week exceeds pacing guide length.")
    period = periods[week_number - 1]
    objectives = [
        {
            "education_objective_id": str(mapped.objective_id),
            "objective_code": getattr(mapped.objective, "objective_id", None),
            "description": getattr(mapped.objective, "description", None),
        }
        for mapped in period.objectives
    ]
    return assignment, guide.id, objectives


def build_manual_assignment_form(db: Session, *, user: User) -> dict[str, Any]:
    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    return {
        "week_ranges": base["week_ranges"],
        "subjects": [
            {
                "subject_id": row["subject_id"],
                "subject_name": row["subject_name"],
                "pacing_guide_id": row["pacing_guide_id"],
                "period_count": row["period_count"],
            }
            for row in base["subjects"]
        ],
        "assignment_types": list(MVP_ASSIGNMENT_TYPES) + ["HOMEWORK", "PROJECT"],
        "student_count": resolve_student_count(db, teacher_user_id=user.id),
        "grade_id": str(onboarding.grade_id),
    }


def create_teacher_manual_assignment(
    db: Session,
    *,
    settings: Settings,
    user: User,
    title: str,
    week_number: int,
    subject_id: uuid.UUID,
    education_objective_ids: list[uuid.UUID],
    assignment_type: str = "WRITTEN_ASSIGNMENT",
    description: str | None = None,
    generate_cover_sheets: bool = True,
) -> dict[str, Any]:
    _require_planning_ready(db, user=user)
    normalized_title = title.strip()
    if not normalized_title:
        raise _field_errors(title="Assignment title is required.")
    if assignment_type not in ASSIGNMENT_TYPES:
        raise _field_errors(assignment_type=f"Unsupported assignment type '{assignment_type}'.")
    if not education_objective_ids:
        raise _field_errors(education_objective_ids="Select at least one TEKS objective.")

    base = _assignment_context(db, user=user)
    onboarding = base["onboarding"]
    _, pacing_guide_id, available_objectives = _resolve_subject_context(
        db,
        user=user,
        subject_id=subject_id,
        week_number=week_number,
    )
    allowed_objective_ids = {row["education_objective_id"] for row in available_objectives}
    normalized_objective_ids = [str(value) for value in education_objective_ids]
    if any(value not in allowed_objective_ids for value in normalized_objective_ids):
        raise _field_errors(
            education_objective_ids="Selected objectives must belong to the chosen week and subject."
        )

    objectives = db.scalars(
        select(EducationObjective).where(EducationObjective.id.in_(education_objective_ids))
    ).all()
    if len(objectives) != len(education_objective_ids):
        raise _field_errors(education_objective_ids="One or more objectives could not be found.")

    shell_package = _ensure_manual_shell_package(db, user=user, base=base)
    _validate_assignment_anchors(
        platform_school_year_id=base["platform_year"].id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        catalog_subject_id=subject_id,
        instructional_package_id=shell_package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        education_objective_ids=education_objective_ids,
    )

    now = _now()
    assignment = TeacherAssistV2Assignment(
        id=uuid.uuid4(),
        tenant_id=base["ctx"].tenant_id,
        teacher_user_id=user.id,
        platform_school_year_id=base["platform_year"].id,
        catalog_district_id=onboarding.district_id,
        catalog_school_id=onboarding.school_id,
        catalog_grade_id=onboarding.grade_id,
        catalog_subject_id=subject_id,
        instructional_package_id=shell_package.id,
        pacing_guide_id=pacing_guide_id,
        week_number=week_number,
        assignment_type=assignment_type,
        title=normalized_title,
        description=description.strip() if description else None,
        status="ACTIVE",
        creation_origin="TEACHER_MANUAL",
        education_objective_ids_json=normalized_objective_ids,
        created_by_user_id=user.id,
        created_at=now,
        updated_at=now,
    )
    db.add(assignment)
    db.flush()

    cover_sheet = None
    if generate_cover_sheets:
        cover_sheet = generate_assignment_cover_sheets(db, settings=settings, assignment=assignment)

    detail = get_teacher_assignment_detail(
        db, user=user, assignment_id=assignment.id, settings=settings
    )
    detail["cover_sheet"] = cover_sheet
    return detail


def list_manual_assignment_objectives(
    db: Session,
    *,
    user: User,
    week_number: int,
    subject_id: uuid.UUID,
) -> list[dict[str, Any]]:
    _require_planning_ready(db, user=user)
    _, _, objectives = _resolve_subject_context(
        db,
        user=user,
        subject_id=subject_id,
        week_number=week_number,
    )
    return objectives
