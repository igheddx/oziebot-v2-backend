"""Admin provisioning for catalog teacher school assignments."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import uuid

from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import (
    EducationDistrict,
    EducationSchool,
    EducationState,
    TeacherSchoolAssignment,
)
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User
from oziebot_api.services.product_access import (
    SELECTABLE_PRODUCT_STATUSES,
    TEACHER_ASSIST_PRODUCT_KEY,
)
from oziebot_api.services.teacher_assist.access_seed import (
    ensure_existing_user_teacher_assist_access,
    ensure_user_teacher_assist_access,
)
from oziebot_api.services.teacher_assist.education_catalog import (
    get_district_or_404,
    get_school_or_404,
    get_state_or_404,
)
from oziebot_api.services.teacher_assist.teacher_school_setup import (
    sync_my_teaching_subjects,
    upsert_my_school_assignment,
)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class AvailableTeacherRow:
    user_id: uuid.UUID
    email: str
    full_name: str | None


@dataclass(frozen=True)
class TeacherAssignmentProvisionResult:
    assignment: TeacherSchoolAssignment
    user_id: uuid.UUID
    email: str
    full_name: str | None
    created_user: bool
    temporary_password: str | None
    grade_setup_applied: bool


@dataclass(frozen=True)
class TeacherAssignmentListRow:
    assignment: TeacherSchoolAssignment
    user_email: str | None
    user_full_name: str | None
    state_name: str | None
    district_name: str | None
    school_name: str | None


def _teacher_assist_product_id(db: Session) -> uuid.UUID:
    product = db.scalars(
        select(PlatformProduct).where(PlatformProduct.product_key == TEACHER_ASSIST_PRODUCT_KEY)
    ).one()
    return product.id


def search_available_teachers_for_school(
    db: Session,
    *,
    school_id: uuid.UUID,
    q: str | None = None,
    limit: int = 25,
) -> list[AvailableTeacherRow]:
    get_school_or_404(db, school_id)
    assigned_user_ids = select(TeacherSchoolAssignment.user_id).where(
        TeacherSchoolAssignment.school_id == school_id,
        TeacherSchoolAssignment.active.is_(True),
    )
    product_id = _teacher_assist_product_id(db)
    stmt = (
        select(User)
        .join(TenantMembership, TenantMembership.user_id == User.id)
        .join(
            TenantProductAccess,
            TenantProductAccess.tenant_id == TenantMembership.tenant_id,
        )
        .where(
            TenantProductAccess.product_id == product_id,
            TenantProductAccess.status.in_(tuple(SELECTABLE_PRODUCT_STATUSES)),
            User.is_active.is_(True),
            User.id.not_in(assigned_user_ids),
        )
        .distinct()
        .order_by(User.email.asc())
        .limit(max(1, min(limit, 50)))
    )
    if q and q.strip():
        pattern = f"%{q.strip()}%"
        stmt = stmt.where(
            or_(
                User.email.ilike(pattern),
                User.full_name.ilike(pattern),
            )
        )
    return [
        AvailableTeacherRow(user_id=row.id, email=row.email, full_name=row.full_name)
        for row in db.scalars(stmt).all()
    ]


def list_teacher_assignment_rows(
    db: Session,
    *,
    user_id: uuid.UUID | None = None,
    state_id: uuid.UUID | None = None,
    district_id: uuid.UUID | None = None,
    school_id: uuid.UUID | None = None,
    active_only: bool = False,
) -> list[TeacherAssignmentListRow]:
    stmt = select(TeacherSchoolAssignment).order_by(TeacherSchoolAssignment.created_at.desc())
    if user_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.user_id == user_id)
    if state_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.state_id == state_id)
    if district_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.district_id == district_id)
    if school_id is not None:
        stmt = stmt.where(TeacherSchoolAssignment.school_id == school_id)
    if active_only:
        stmt = stmt.where(TeacherSchoolAssignment.active.is_(True))
    assignments = db.scalars(stmt).all()
    if not assignments:
        return []

    user_ids = {row.user_id for row in assignments}
    state_ids = {row.state_id for row in assignments}
    district_ids = {row.district_id for row in assignments}
    school_ids = {row.school_id for row in assignments}

    users = {row.id: row for row in db.scalars(select(User).where(User.id.in_(user_ids))).all()}
    states = {
        row.id: row
        for row in db.scalars(select(EducationState).where(EducationState.id.in_(state_ids))).all()
    }
    districts = {
        row.id: row
        for row in db.scalars(
            select(EducationDistrict).where(EducationDistrict.id.in_(district_ids))
        ).all()
    }
    schools = {
        row.id: row
        for row in db.scalars(
            select(EducationSchool).where(EducationSchool.id.in_(school_ids))
        ).all()
    }

    return [
        TeacherAssignmentListRow(
            assignment=row,
            user_email=users.get(row.user_id).email if users.get(row.user_id) else None,
            user_full_name=users.get(row.user_id).full_name if users.get(row.user_id) else None,
            state_name=states.get(row.state_id).name if states.get(row.state_id) else None,
            district_name=districts.get(row.district_id).name
            if districts.get(row.district_id)
            else None,
            school_name=schools.get(row.school_id).name if schools.get(row.school_id) else None,
        )
        for row in assignments
    ]


def _primary_membership(db: Session, *, user_id: uuid.UUID) -> TenantMembership:
    membership = db.scalars(
        select(TenantMembership)
        .where(TenantMembership.user_id == user_id)
        .order_by(TenantMembership.created_at.asc())
        .limit(1)
    ).first()
    if membership is None:
        raise LookupError("User has no tenant membership")
    return membership


def provision_teacher_school_assignment(
    db: Session,
    *,
    state_id: uuid.UUID,
    district_id: uuid.UUID,
    school_id: uuid.UUID,
    active: bool = True,
    user_id: uuid.UUID | None = None,
    email: str | None = None,
    full_name: str | None = None,
    tenant_name: str | None = None,
    catalog_grade_id: uuid.UUID | None = None,
) -> TeacherAssignmentProvisionResult:
    get_state_or_404(db, state_id)
    district = get_district_or_404(db, district_id)
    if district.state_id != state_id:
        raise ValueError("District does not belong to the selected state")
    school = get_school_or_404(db, school_id)
    if school.district_id != district_id:
        raise ValueError("School does not belong to the selected district")

    created_user = False
    temporary_password: str | None = None

    if user_id is not None:
        user = db.get(User, user_id)
        if user is None:
            raise LookupError("User not found")
        ensure_existing_user_teacher_assist_access(db, email=user.email)
        resolved_email = user.email
        resolved_full_name = user.full_name
    else:
        if not email or not email.strip():
            raise ValueError("Email is required when creating a new teacher profile")
        if not full_name or not full_name.strip():
            raise ValueError("Full name is required when creating a new teacher profile")
        resolved_tenant_name = (tenant_name or f"{full_name.strip()}'s Classroom").strip()
        access = ensure_user_teacher_assist_access(
            db,
            email=email.strip(),
            full_name=full_name.strip(),
            tenant_name=resolved_tenant_name,
            password=None,
        )
        user = db.get(User, access.user_id)
        if user is None:
            raise LookupError("User not found after provisioning")
        created_user = access.created_user
        temporary_password = access.temporary_password
        resolved_email = access.email
        resolved_full_name = user.full_name

    already_assigned = db.scalars(
        select(TeacherSchoolAssignment).where(
            TeacherSchoolAssignment.user_id == user.id,
            TeacherSchoolAssignment.school_id == school_id,
            TeacherSchoolAssignment.active.is_(True),
        )
    ).first()
    if already_assigned is not None:
        raise ValueError("Teacher is already assigned to this school")

    assignment = upsert_my_school_assignment(
        db,
        user_id=user.id,
        state_id=state_id,
        district_id=district_id,
        school_id=school_id,
    )
    if not active:
        assignment.active = False
        assignment.updated_at = _now()
        db.flush()

    grade_setup_applied = False
    if catalog_grade_id is not None and active:
        membership = _primary_membership(db, user_id=user.id)
        sync_my_teaching_subjects(
            db,
            tenant_id=membership.tenant_id,
            user_id=user.id,
            catalog_grade_id=catalog_grade_id,
        )
        grade_setup_applied = True

    _apply_teacher_account_flags(db, user=user, temporary_password=temporary_password)

    return TeacherAssignmentProvisionResult(
        assignment=assignment,
        user_id=user.id,
        email=resolved_email,
        full_name=resolved_full_name,
        created_user=created_user,
        temporary_password=temporary_password,
        grade_setup_applied=grade_setup_applied,
    )


def _apply_teacher_account_flags(
    db: Session, *, user: User, temporary_password: str | None
) -> None:
    if user.teacher_assist_role != "root_admin" and not user.is_root_admin:
        user.teacher_assist_role = "teacher"
    if temporary_password:
        user.must_change_password = True
    user.updated_at = _now()
    db.flush()
