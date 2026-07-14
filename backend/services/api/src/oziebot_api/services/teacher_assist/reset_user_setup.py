"""Reset TeacherAssist onboarding and user associations without deleting accounts or seed catalog."""

from __future__ import annotations

from datetime import UTC, datetime
import uuid

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session

from oziebot_api.models.education_catalog import TeacherSchoolAssignment
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.teacher_assist_assignment import TeacherAssistAssignment
from oziebot_api.models.teacher_assist_assignment_grading_review import (
    TeacherAssistAssignmentGradingReview,
)
from oziebot_api.models.teacher_assist_assignment_print_packet import (
    TeacherAssistAssignmentPrintPacket,
)
from oziebot_api.models.teacher_assist_class import TeacherAssistClass
from oziebot_api.models.teacher_assist_class_subject import TeacherAssistClassSubject
from oziebot_api.models.teacher_assist_export_artifact import TeacherAssistExportArtifact
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_instructional_evidence import (
    TeacherAssistInstructionalEvidence,
)
from oziebot_api.models.teacher_assist_instructional_reflection import (
    TeacherAssistInstructionalReflection,
)
from oziebot_api.models.teacher_assist_instructional_week import TeacherAssistInstructionalWeek
from oziebot_api.models.teacher_assist_lesson_reflection import TeacherAssistLessonReflection
from oziebot_api.models.teacher_assist_mastery_matrix import TeacherAssistMasteryMatrix
from oziebot_api.models.teacher_assist_newsletter import TeacherAssistNewsletter
from oziebot_api.models.teacher_assist_pacing_guide_period_note import (
    TeacherAssistPacingGuidePeriodNote,
)
from oziebot_api.models.teacher_assist_pilot_feedback import TeacherAssistPilotFeedback
from oziebot_api.models.teacher_assist_planning_input_draft import TeacherAssistPlanningInputDraft
from oziebot_api.models.teacher_assist_profile import TeacherAssistProfile
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_standard import TeacherAssistStandard
from oziebot_api.models.teacher_assist_student_support_group import (
    TeacherAssistStudentSupportGroup,
    TeacherAssistStudentSupportGroupMember,
)
from oziebot_api.models.teacher_assist_subject import TeacherAssistSubject
from oziebot_api.models.teacher_assist_time_savings import (
    TeacherAssistPlanningGroup,
    TeacherAssistPlanningGroupMember,
    TeacherAssistReuseEvent,
    TeacherAssistWeekTemplate,
)
from oziebot_api.models.teacher_assist_usage_metric import TeacherAssistUsageMetric
from oziebot_api.models.teacher_assist_student_work_submission import (
    TeacherAssistStudentWorkSubmission,
)
from oziebot_api.models.teacher_assist_user_preference import TeacherAssistUserPreference
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.models.teacher_copilot_session import TeacherCopilotSession
from oziebot_api.models.user import User


def _normalized_email(value: str) -> str:
    return value.strip().lower()


def _get_user_by_email(db: Session, email: str) -> User | None:
    return db.scalars(
        select(User).where(func.lower(User.email) == _normalized_email(email))
    ).one_or_none()


def _tenant_ids_for_user(db: Session, *, user_id: uuid.UUID) -> list[uuid.UUID]:
    return list(
        db.scalars(
            select(TenantMembership.tenant_id).where(TenantMembership.user_id == user_id)
        ).all()
    )


def _delete_user_scoped_rows(db: Session, *, user_id: uuid.UUID) -> None:
    """Remove teacher-operational rows keyed by user. Order matters for non-cascading FKs."""
    db.execute(
        delete(TeacherAssistPilotFeedback).where(TeacherAssistPilotFeedback.user_id == user_id)
    )
    db.execute(
        delete(TeacherAssistActivityEvent).where(TeacherAssistActivityEvent.user_id == user_id)
    )
    db.execute(delete(TeacherCopilotSession).where(TeacherCopilotSession.teacher_id == user_id))
    db.execute(
        delete(TeacherAssistExportArtifact).where(TeacherAssistExportArtifact.user_id == user_id)
    )
    db.execute(
        delete(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.teacher_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistExtractedTextRecord).where(
            TeacherAssistExtractedTextRecord.teacher_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistAssignmentPrintPacket).where(
            TeacherAssistAssignmentPrintPacket.teacher_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistAssignmentGradingReview).where(
            TeacherAssistAssignmentGradingReview.teacher_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistStudentWorkSubmission).where(
            TeacherAssistStudentWorkSubmission.teacher_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistReteachPlan).where(TeacherAssistReteachPlan.owner_user_id == user_id)
    )
    db.execute(delete(TeacherAssistReuseEvent).where(TeacherAssistReuseEvent.user_id == user_id))
    db.execute(
        delete(TeacherAssistWeekTemplate).where(
            TeacherAssistWeekTemplate.created_by_user_id == user_id
        )
    )
    planning_group_ids = list(
        db.scalars(
            select(TeacherAssistPlanningGroup.id).where(
                TeacherAssistPlanningGroup.created_by_user_id == user_id
            )
        ).all()
    )
    if planning_group_ids:
        db.execute(
            delete(TeacherAssistPlanningGroupMember).where(
                TeacherAssistPlanningGroupMember.group_id.in_(planning_group_ids)
            )
        )
        db.execute(
            delete(TeacherAssistPlanningGroup).where(
                TeacherAssistPlanningGroup.id.in_(planning_group_ids)
            )
        )
    db.execute(delete(TeacherAssistUsageMetric).where(TeacherAssistUsageMetric.user_id == user_id))
    db.execute(
        delete(TeacherAssistLessonReflection).where(
            TeacherAssistLessonReflection.owner_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistMasteryMatrix).where(
            TeacherAssistMasteryMatrix.owner_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistInstructionalReflection).where(
            TeacherAssistInstructionalReflection.owner_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistInstructionalEvidence).where(
            TeacherAssistInstructionalEvidence.owner_user_id == user_id
        )
    )
    support_group_ids = list(
        db.scalars(
            select(TeacherAssistStudentSupportGroup.id).where(
                TeacherAssistStudentSupportGroup.owner_user_id == user_id
            )
        ).all()
    )
    if support_group_ids:
        db.execute(
            delete(TeacherAssistStudentSupportGroupMember).where(
                TeacherAssistStudentSupportGroupMember.support_group_id.in_(support_group_ids)
            )
        )
        db.execute(
            delete(TeacherAssistStudentSupportGroup).where(
                TeacherAssistStudentSupportGroup.id.in_(support_group_ids)
            )
        )
    db.execute(delete(TeacherAssistWorkflow).where(TeacherAssistWorkflow.user_id == user_id))
    db.execute(
        delete(TeacherAssistWeeklyPlan).where(
            or_(
                TeacherAssistWeeklyPlan.user_id == user_id,
                TeacherAssistWeeklyPlan.owner_user_id == user_id,
            )
        )
    )
    db.execute(
        delete(TeacherAssistNewsletter).where(TeacherAssistNewsletter.owner_user_id == user_id)
    )
    db.execute(
        delete(TeacherAssistGeneratedArtifact).where(
            TeacherAssistGeneratedArtifact.created_by_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistAssignment).where(TeacherAssistAssignment.teacher_user_id == user_id)
    )
    db.execute(
        delete(TeacherAssistPlanningInputDraft).where(
            TeacherAssistPlanningInputDraft.user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistInstructionalWeek).where(
            TeacherAssistInstructionalWeek.created_by_user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistPacingGuidePeriodNote).where(
            TeacherAssistPacingGuidePeriodNote.user_id == user_id
        )
    )
    db.execute(
        delete(TeacherAssistUserPreference).where(TeacherAssistUserPreference.user_id == user_id)
    )
    db.execute(
        update(TeacherAssistProfile)
        .where(TeacherAssistProfile.user_id == user_id)
        .values(
            preferred_grade_level=None,
            default_student_count=None,
            preferred_grading_period_type=None,
            updated_at=datetime.now(UTC),
        )
    )
    db.execute(delete(TeacherSchoolAssignment).where(TeacherSchoolAssignment.user_id == user_id))


def _reset_tenant_setup(db: Session, *, tenant_id: uuid.UUID) -> None:
    """Clear classroom setup on a tenant. Preserves school years and pacing guides."""
    class_ids = list(
        db.scalars(
            select(TeacherAssistClass.id).where(TeacherAssistClass.tenant_id == tenant_id)
        ).all()
    )
    if class_ids:
        db.execute(
            delete(TeacherAssistClassSubject).where(
                TeacherAssistClassSubject.class_id.in_(class_ids)
            )
        )
    db.execute(delete(TeacherAssistClass).where(TeacherAssistClass.tenant_id == tenant_id))
    db.execute(
        update(TeacherAssistUserPreference)
        .where(TeacherAssistUserPreference.tenant_id == tenant_id)
        .values(
            last_class_id=None,
            last_grading_period_id=None,
            last_subject_id=None,
            active_pacing_guide_id=None,
            manual_pacing_period_id=None,
            onboarding_progress_json={},
            onboarding_completed_at=None,
            recently_viewed_json=[],
            updated_at=datetime.now(UTC),
        )
    )


def _blank_slate_tenant_setup(db: Session, *, tenant_id: uuid.UUID) -> None:
    """Clear teacher-created tenant data while preserving seed school years and pacing guides."""
    class_ids = list(
        db.scalars(
            select(TeacherAssistClass.id).where(TeacherAssistClass.tenant_id == tenant_id)
        ).all()
    )
    if class_ids:
        db.execute(
            delete(TeacherAssistClassSubject).where(
                TeacherAssistClassSubject.class_id.in_(class_ids)
            )
        )
    db.execute(delete(TeacherAssistClass).where(TeacherAssistClass.tenant_id == tenant_id))

    db.execute(
        delete(TeacherAssistWeeklyPlan).where(TeacherAssistWeeklyPlan.tenant_id == tenant_id)
    )
    db.execute(delete(TeacherAssistWorkflow).where(TeacherAssistWorkflow.tenant_id == tenant_id))
    db.execute(
        delete(TeacherAssistPlanningInputDraft).where(
            TeacherAssistPlanningInputDraft.tenant_id == tenant_id
        )
    )
    db.execute(
        delete(TeacherAssistGeneratedArtifact).where(
            TeacherAssistGeneratedArtifact.tenant_id == tenant_id
        )
    )
    db.execute(
        delete(TeacherAssistInstructionalWeek).where(
            TeacherAssistInstructionalWeek.tenant_id == tenant_id
        )
    )
    db.execute(
        delete(TeacherAssistAssignment).where(TeacherAssistAssignment.tenant_id == tenant_id)
    )
    db.execute(delete(TeacherAssistStandard).where(TeacherAssistStandard.tenant_id == tenant_id))
    db.execute(delete(TeacherAssistSubject).where(TeacherAssistSubject.tenant_id == tenant_id))
    db.execute(
        delete(TeacherAssistUserPreference).where(
            TeacherAssistUserPreference.tenant_id == tenant_id
        )
    )


def reset_teacher_assist_user_setup(
    db: Session,
    email: str,
    *,
    reset_shared_tenant_setup: bool = True,
    blank_slate: bool = False,
) -> dict[str, object]:
    user = _get_user_by_email(db, email)
    if user is None:
        raise LookupError(f"User not found: {email}")

    tenant_ids = _tenant_ids_for_user(db, user_id=user.id)
    _delete_user_scoped_rows(db, user_id=user.id)
    if reset_shared_tenant_setup:
        for tenant_id in tenant_ids:
            if blank_slate:
                _blank_slate_tenant_setup(db, tenant_id=tenant_id)
            else:
                _reset_tenant_setup(db, tenant_id=tenant_id)

    db.flush()
    preserved = [
        "user account",
        "teacher profile row",
        "tenant membership",
        "education catalog",
    ]
    preserved.extend(["pacing guides", "school years"])
    return {
        "email": user.email,
        "user_id": str(user.id),
        "tenant_ids": [str(tenant_id) for tenant_id in tenant_ids],
        "blank_slate": blank_slate,
        "preserved": preserved,
    }
