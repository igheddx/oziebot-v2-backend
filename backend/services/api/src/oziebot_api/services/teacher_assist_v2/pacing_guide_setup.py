"""TeacherAssist v2 pacing guide setup gate."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import EducationSubject
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_v2_onboarding import (
    TeacherAssistV2Onboarding,
    TeacherAssistV2PacingGuideAssignment,
)
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.pacing_guide_foundation import (
    _copy_guide_tree,
    create_catalog_pacing_guide,
    get_catalog_pacing_guide_detail,
)
from oziebot_api.services.teacher_assist.setup import teacher_assist_context_for_user
from oziebot_api.services.teacher_assist.user_preferences import update_user_preferences
from oziebot_api.services.teacher_assist_v2.pacing_guides import (
    ensure_tenant_school_year,
    list_v2_district_pacing_guides,
)
from oziebot_api.services.teacher_assist_v2.platform_context import (
    resolve_instructional_catalog_tenant_id,
)
from oziebot_api.services.teacher_assist_v2.school_years import get_platform_school_year_or_404
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import (
    get_v2_onboarding,
    is_v2_onboarding_complete,
    is_v2_pacing_setup_complete,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _copy_platform_guide_for_teacher(
    db: Session,
    *,
    platform_tenant_id: uuid.UUID,
    teacher_tenant_id: uuid.UUID,
    actor: User,
    source_guide_id: uuid.UUID,
    school_year_id: uuid.UUID,
    title: str,
) -> uuid.UUID:
    source = get_catalog_pacing_guide_detail(
        db,
        tenant_id=platform_tenant_id,
        pacing_guide_id=source_guide_id,
    )
    target = create_catalog_pacing_guide(
        db,
        tenant_id=teacher_tenant_id,
        actor=actor,
        school_year_id=school_year_id,
        guide_type="TEACHER",
        title=title,
        description=source.description,
        catalog_state_id=source.catalog_state_id,
        catalog_district_id=source.catalog_district_id,
        catalog_school_id=source.catalog_school_id,
        catalog_grade_id=source.catalog_grade_id,
        catalog_subject_id=source.catalog_subject_id,
        is_template=False,
        is_shared=False,
    )
    _copy_guide_tree(db, source=source, target=target)
    metadata = target.metadata_json if isinstance(target.metadata_json, dict) else {}
    target.metadata_json = {
        **metadata,
        "source_district_guide_id": str(source.id),
    }
    db.flush()
    return target.id


def _resolve_district_source_guide_id(
    db: Session,
    *,
    source_guide_id: uuid.UUID,
    platform_tenant_id: uuid.UUID,
    teacher_tenant_id: uuid.UUID,
    available_guides: list[TeacherAssistPacingGuide],
) -> uuid.UUID | None:
    if any(guide.id == source_guide_id for guide in available_guides):
        return source_guide_id

    for tenant_id in (platform_tenant_id, teacher_tenant_id):
        try:
            guide = get_catalog_pacing_guide_detail(
                db,
                tenant_id=tenant_id,
                pacing_guide_id=source_guide_id,
            )
        except LookupError:
            continue
        metadata = guide.metadata_json if isinstance(guide.metadata_json, dict) else {}
        stored = metadata.get("source_district_guide_id")
        if stored:
            stored_id = uuid.UUID(str(stored))
            if any(row.id == stored_id for row in available_guides):
                return stored_id
        if guide.title.startswith("My "):
            candidate_title = guide.title[3:]
            for row in available_guides:
                if row.title == candidate_title:
                    return row.id
    return None


def _load_guide_summary(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    platform_tenant_id: uuid.UUID,
    pacing_guide_id: uuid.UUID,
):
    try:
        return get_catalog_pacing_guide_detail(
            db, tenant_id=tenant_id, pacing_guide_id=pacing_guide_id
        )
    except LookupError:
        return get_catalog_pacing_guide_detail(
            db,
            tenant_id=platform_tenant_id,
            pacing_guide_id=pacing_guide_id,
        )
    return datetime.now(UTC)


def _require_completed_onboarding(
    row: TeacherAssistV2Onboarding | None,
) -> TeacherAssistV2Onboarding:
    if row is None or not is_v2_onboarding_complete(row):
        raise ValueError("Complete onboarding before setting up pacing guides.")
    return row


def _guide_matches_teacher_school_year(guide: TeacherAssistPacingGuide, *, platform_year) -> bool:
    if guide.school_year_label and guide.school_year_label != platform_year.title:
        metadata = guide.metadata_json if isinstance(guide.metadata_json, dict) else {}
        platform_year_id = metadata.get("platform_school_year_id")
        if platform_year_id and str(platform_year_id) != str(platform_year.id):
            return False
        if not platform_year_id:
            return False
    return True


def _guide_matches_teacher_school_scope(
    guide: TeacherAssistPacingGuide, *, school_id: uuid.UUID | None
) -> bool:
    if guide.catalog_school_id is None:
        return True
    return school_id is not None and guide.catalog_school_id == school_id


def list_teacher_available_pacing_guides(
    db: Session,
    *,
    platform_tenant_id: uuid.UUID,
    onboarding: TeacherAssistV2Onboarding,
    platform_year,
    subject_id: uuid.UUID | None = None,
) -> list[TeacherAssistPacingGuide]:
    assert onboarding.district_id is not None
    assert onboarding.grade_id is not None

    guides = list_v2_district_pacing_guides(
        db,
        tenant_id=platform_tenant_id,
        catalog_district_id=onboarding.district_id,
        catalog_grade_id=onboarding.grade_id,
        active_only=True,
    )
    filtered: list[TeacherAssistPacingGuide] = []
    for guide in guides:
        if subject_id is not None and guide.catalog_subject_id != subject_id:
            continue
        if not _guide_matches_teacher_school_scope(guide, school_id=onboarding.school_id):
            continue
        if not _guide_matches_teacher_school_year(guide, platform_year=platform_year):
            continue
        filtered.append(guide)
    return filtered


def build_pacing_guide_setup_form(db: Session, *, user: User) -> dict[str, Any]:
    ctx = teacher_assist_context_for_user(db, user)
    onboarding = _require_completed_onboarding(get_v2_onboarding(db, user_id=user.id))
    assert onboarding.school_year_id is not None
    assert onboarding.grade_id is not None
    assert onboarding.district_id is not None

    platform_year = get_platform_school_year_or_404(db, school_year_id=onboarding.school_year_id)
    platform_tenant_id = resolve_instructional_catalog_tenant_id(db)
    tenant_year = ensure_tenant_school_year(
        db, tenant_id=ctx.tenant_id, platform_year=platform_year
    )

    selected_subject_ids = [
        uuid.UUID(subject_id) for subject_id in (onboarding.selected_subject_ids or [])
    ]
    subjects = db.scalars(
        select(EducationSubject).where(EducationSubject.id.in_(selected_subject_ids))
    ).all()
    subject_by_id = {row.id: row for row in subjects}

    district_guides = list_teacher_available_pacing_guides(
        db,
        platform_tenant_id=platform_tenant_id,
        onboarding=onboarding,
        platform_year=platform_year,
    )

    guides_by_subject: dict[str, list[dict[str, Any]]] = {}
    for subject_id in selected_subject_ids:
        subject = subject_by_id.get(subject_id)
        if subject is None:
            continue
        matches = [
            {
                "id": str(guide.id),
                "title": guide.title,
                "description": guide.description,
                "guide_type": guide.guide_type,
                "school_year_label": guide.school_year_label,
            }
            for guide in district_guides
            if guide.catalog_subject_id == subject.id
        ]
        guides_by_subject[str(subject.id)] = matches

    existing = db.scalars(
        select(TeacherAssistV2PacingGuideAssignment).where(
            TeacherAssistV2PacingGuideAssignment.user_id == user.id,
            TeacherAssistV2PacingGuideAssignment.active.is_(True),
        )
    ).all()

    return {
        "school_year_title": platform_year.title,
        "grade_id": str(onboarding.grade_id),
        "subjects": [
            {
                "id": str(subject.id),
                "display_name": subject.display_name,
                "subject_code": subject.subject_code,
                "available_guides": guides_by_subject.get(str(subject.id), []),
            }
            for subject in subjects
        ],
        "existing_assignments": [
            {
                "subject_id": str(row.subject_id),
                "pacing_guide_id": str(row.pacing_guide_id),
                "guide_scope": row.guide_scope,
                "source_district_guide_id": (
                    str(resolved)
                    if (
                        resolved := _resolve_district_source_guide_id(
                            db,
                            source_guide_id=row.pacing_guide_id,
                            platform_tenant_id=platform_tenant_id,
                            teacher_tenant_id=ctx.tenant_id,
                            available_guides=[
                                guide
                                for guide in district_guides
                                if guide.catalog_subject_id == row.subject_id
                            ],
                        )
                    )
                    is not None
                    else None
                ),
            }
            for row in existing
        ],
        "grade_level_guide_enabled": False,
        "tenant_school_year_id": str(tenant_year.id),
        "setup_complete": is_v2_pacing_setup_complete(onboarding),
    }


def save_pacing_guide_setup(
    db: Session,
    *,
    user: User,
    selections: list[dict[str, Any]],
) -> TeacherAssistV2Onboarding:
    ctx = teacher_assist_context_for_user(db, user)
    onboarding = _require_completed_onboarding(get_v2_onboarding(db, user_id=user.id))
    assert onboarding.school_year_id is not None
    assert onboarding.grade_id is not None

    platform_year = get_platform_school_year_or_404(db, school_year_id=onboarding.school_year_id)
    platform_tenant_id = resolve_instructional_catalog_tenant_id(db)
    tenant_year = ensure_tenant_school_year(
        db, tenant_id=ctx.tenant_id, platform_year=platform_year
    )

    selected_subject_ids = {
        str(subject_id) for subject_id in (onboarding.selected_subject_ids or [])
    }
    if not selections:
        raise ValueError({"selections": "Select at least one pacing guide to include in planning."})

    covered_subjects: set[str] = set()
    now = _now()

    existing_by_subject: dict[uuid.UUID, TeacherAssistV2PacingGuideAssignment] = {
        row.subject_id: row
        for row in db.scalars(
            select(TeacherAssistV2PacingGuideAssignment).where(
                TeacherAssistV2PacingGuideAssignment.user_id == user.id,
                TeacherAssistV2PacingGuideAssignment.school_year_id == tenant_year.id,
            )
        ).all()
    }

    for row in db.scalars(
        select(TeacherAssistV2PacingGuideAssignment).where(
            TeacherAssistV2PacingGuideAssignment.user_id == user.id,
            TeacherAssistV2PacingGuideAssignment.school_year_id != tenant_year.id,
            TeacherAssistV2PacingGuideAssignment.active.is_(True),
        )
    ).all():
        row.active = False
        row.updated_at = now

    first_active_guide_id: uuid.UUID | None = None

    for selection in selections:
        subject_id = uuid.UUID(str(selection["subject_id"]))
        source_guide_id = uuid.UUID(str(selection["source_guide_id"]))
        mode = str(selection.get("mode") or "district")
        if str(subject_id) not in selected_subject_ids:
            raise ValueError(
                {"selections": "One or more subjects are not part of your onboarding profile."}
            )
        if mode not in {"district", "teacher_copy"}:
            raise ValueError({"selections": "Invalid pacing guide option."})

        district_guides = list_teacher_available_pacing_guides(
            db,
            platform_tenant_id=platform_tenant_id,
            onboarding=onboarding,
            platform_year=platform_year,
            subject_id=subject_id,
        )
        resolved_source_id = _resolve_district_source_guide_id(
            db,
            source_guide_id=source_guide_id,
            platform_tenant_id=platform_tenant_id,
            teacher_tenant_id=ctx.tenant_id,
            available_guides=district_guides,
        )
        source = next((guide for guide in district_guides if guide.id == resolved_source_id), None)
        if source is None or source.catalog_subject_id != subject_id:
            raise ValueError(
                {"selections": "Selected pacing guide is not available for this subject."}
            )

        if mode == "teacher_copy":
            pacing_guide_id = _copy_platform_guide_for_teacher(
                db,
                platform_tenant_id=platform_tenant_id,
                teacher_tenant_id=ctx.tenant_id,
                actor=user,
                source_guide_id=source.id,
                school_year_id=tenant_year.id,
                title=f"My {source.title}",
            )
            guide_scope = "teacher"
        else:
            pacing_guide_id = source.id
            guide_scope = "district"

        existing = existing_by_subject.get(subject_id)
        if existing is not None:
            existing.pacing_guide_id = pacing_guide_id
            existing.guide_scope = guide_scope
            existing.grade_id = onboarding.grade_id
            existing.active = True
            existing.updated_at = now
        else:
            existing = TeacherAssistV2PacingGuideAssignment(
                id=uuid.uuid4(),
                user_id=user.id,
                tenant_id=ctx.tenant_id,
                school_year_id=tenant_year.id,
                grade_id=onboarding.grade_id,
                subject_id=subject_id,
                pacing_guide_id=pacing_guide_id,
                guide_scope=guide_scope,
                active=True,
                created_at=now,
                updated_at=now,
            )
            db.add(existing)
            existing_by_subject[subject_id] = existing
        covered_subjects.add(str(subject_id))
        if first_active_guide_id is None:
            first_active_guide_id = pacing_guide_id

    for subject_id_str in selected_subject_ids:
        if subject_id_str in covered_subjects:
            continue
        existing = existing_by_subject.get(uuid.UUID(subject_id_str))
        if existing is not None:
            existing.active = False
            existing.updated_at = now

    if not covered_subjects:
        raise ValueError({"selections": "Select at least one pacing guide to include in planning."})

    onboarding.pacing_guide_setup_completed_at = now
    onboarding.updated_at = now
    if first_active_guide_id is not None:
        update_user_preferences(
            db,
            tenant_id=ctx.tenant_id,
            user_id=user.id,
            active_pacing_guide_id=first_active_guide_id,
        )
    db.flush()
    return onboarding


def build_teacher_home_summary(db: Session, *, user: User) -> dict[str, Any]:
    ctx = teacher_assist_context_for_user(db, user)
    onboarding = get_v2_onboarding(db, user_id=user.id)
    if onboarding is None or not is_v2_pacing_setup_complete(onboarding):
        raise ValueError("Complete pacing guide setup before opening home.")

    platform_year = (
        get_platform_school_year_or_404(db, school_year_id=onboarding.school_year_id)
        if onboarding.school_year_id
        else None
    )
    school_name = None
    grade_name = None
    if onboarding.school_id is not None:
        from oziebot_api.services.teacher_assist.education_catalog import get_school_or_404

        school_name = get_school_or_404(db, onboarding.school_id).name
    if onboarding.grade_id is not None:
        from oziebot_api.services.teacher_assist.education_catalog import get_grade_or_404

        grade_name = get_grade_or_404(db, onboarding.grade_id).display_name

    subject_ids = [uuid.UUID(subject_id) for subject_id in (onboarding.selected_subject_ids or [])]
    subjects = db.scalars(
        select(EducationSubject).where(EducationSubject.id.in_(subject_ids))
    ).all()
    platform_tenant_id = resolve_instructional_catalog_tenant_id(db)
    assignments = db.scalars(
        select(TeacherAssistV2PacingGuideAssignment).where(
            TeacherAssistV2PacingGuideAssignment.user_id == user.id,
            TeacherAssistV2PacingGuideAssignment.active.is_(True),
        )
    ).all()
    assignment_by_subject = {str(row.subject_id): row for row in assignments}

    active_guides = []
    for subject in subjects:
        assignment = assignment_by_subject.get(str(subject.id))
        if assignment is None:
            continue
        guide = _load_guide_summary(
            db,
            tenant_id=ctx.tenant_id,
            platform_tenant_id=platform_tenant_id,
            pacing_guide_id=assignment.pacing_guide_id,
        )
        active_guides.append(
            {
                "subject_id": str(subject.id),
                "subject_name": subject.display_name,
                "pacing_guide_id": str(guide.id),
                "pacing_guide_title": guide.title,
                "guide_scope": assignment.guide_scope,
            }
        )

    return {
        "school_year_title": platform_year.title if platform_year else None,
        "school_name": school_name,
        "grade_name": grade_name,
        "subjects": [{"id": str(row.id), "display_name": row.display_name} for row in subjects],
        "active_pacing_guides": active_guides,
        "ready_to_plan": True,
        "assignments_requiring_review_count": _count_assignments_requiring_review(db, user=user),
        "recent_grades_confirmed_count": _count_recent_confirmed_grades(db, user=user),
        "objectives_assessed_count": _count_objectives_assessed(db, user=user),
        "mastery_alerts_count": _count_mastery_alerts(db, user=user),
    }


def _count_assignments_requiring_review(db: Session, *, user: User) -> int:
    from oziebot_api.services.teacher_assist_v2.grade_reviews import (
        count_assignments_requiring_review,
    )

    return count_assignments_requiring_review(db, user=user)


def _count_recent_confirmed_grades(db: Session, *, user: User) -> int:
    from oziebot_api.services.teacher_assist_v2.gradebook_workspace import (
        count_recent_confirmed_grades,
    )

    return count_recent_confirmed_grades(db, user=user)


def _count_objectives_assessed(db: Session, *, user: User) -> int:
    from oziebot_api.services.teacher_assist_v2.objective_performance import (
        count_objectives_assessed,
    )

    return count_objectives_assessed(db, user=user)


def _count_mastery_alerts(db: Session, *, user: User) -> int:
    from oziebot_api.services.teacher_assist_v2.objective_performance import count_mastery_alerts

    return count_mastery_alerts(db, user=user)
