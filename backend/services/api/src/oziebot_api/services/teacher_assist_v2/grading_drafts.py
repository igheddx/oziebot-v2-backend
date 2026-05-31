"""TeacherAssist v2 grading jobs and AI draft persistence."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_grading_draft import TeacherAssistV2GradingDraft
from oziebot_api.models.teacher_assist_v2_grading_job import TeacherAssistV2GradingJob
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.constants import validate_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.runtime_settings import resolve_teacher_assist_settings
from oziebot_api.services.teacher_assist.prompt_contracts import GRADING_ASSIST_FEATURE
from oziebot_api.services.teacher_assist_v2.grading_ai import generate_grading_draft_ai_result
from oziebot_api.services.teacher_assist_v2.grading_constants import GRADING_JOB_STATUSES
from oziebot_api.services.teacher_assist_v2.grading_context import build_grading_context
from oziebot_api.services.teacher_assist_v2.submission_intake import (
    _get_assignment_or_404,
    get_student_submission_or_404,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _validate_job_status(status: str) -> str:
    normalized = status.strip().upper()
    if normalized not in GRADING_JOB_STATUSES:
        raise ValueError(f"Unsupported grading job status '{status}'")
    return normalized


def serialize_grading_draft(row: TeacherAssistV2GradingDraft) -> dict[str, Any]:
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "student_submission_id": str(row.student_submission_id),
        "grading_job_id": str(row.grading_job_id),
        "score": row.score,
        "max_score": row.max_score,
        "percentage": row.percentage,
        "rubric_json": row.rubric_json,
        "teacher_comment_draft": row.teacher_comment_draft,
        "strengths": row.strengths,
        "improvements": row.improvements,
        "objective_evidence": row.objective_evidence,
        "confidence_score": row.confidence_score,
        "provider": row.provider,
        "model": row.model,
        "created_at": row.created_at.isoformat(),
        "teacher_review_required": True,
    }


def serialize_grading_job(row: TeacherAssistV2GradingJob) -> dict[str, Any]:
    payload = {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "student_submission_id": str(row.student_submission_id),
        "status": row.status,
        "provider": row.provider,
        "model": row.model,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "completed_at": row.completed_at.isoformat() if row.completed_at else None,
        "error_message": row.error_message,
        "created_at": row.created_at.isoformat(),
    }
    if row.draft is not None:
        payload["draft"] = serialize_grading_draft(row.draft)
    return payload


def count_grading_metrics(db: Session, *, assignment_id: uuid.UUID) -> dict[str, int]:
    submission_ids = db.scalars(
        select(TeacherAssistV2StudentSubmission.id).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.status != "ARCHIVED",
        )
    ).all()
    if not submission_ids:
        return {"grading_complete_count": 0, "teacher_reviewed_count": 0}

    grading_complete_count = db.scalar(
        select(func.count(func.distinct(TeacherAssistV2GradingDraft.student_submission_id))).where(
            TeacherAssistV2GradingDraft.student_submission_id.in_(submission_ids)
        )
    ) or 0
    return {
        "grading_complete_count": int(grading_complete_count),
        "teacher_reviewed_count": 0,
    }


def get_latest_grading_draft(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
) -> dict[str, Any] | None:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    row = db.scalars(
        select(TeacherAssistV2GradingDraft)
        .where(
            TeacherAssistV2GradingDraft.student_submission_id == submission.id,
            TeacherAssistV2GradingDraft.teacher_user_id == user.id,
        )
        .order_by(TeacherAssistV2GradingDraft.created_at.desc())
    ).first()
    if row is None:
        return None
    return serialize_grading_draft(row)


def _require_ready_submission(submission: TeacherAssistV2StudentSubmission) -> None:
    if submission.status != "READY_FOR_GRADING":
        raise ValueError("Only submissions marked READY_FOR_GRADING can enter the grading workflow.")
    if submission.student_number is None:
        raise ValueError("Submission must have an assigned student number before grading.")


def create_grading_job_for_submission(
    db: Session,
    *,
    user: User,
    submission_id: uuid.UUID,
    settings: Settings,
) -> dict[str, Any]:
    submission = get_student_submission_or_404(db, user=user, submission_id=submission_id)
    assignment = _get_assignment_or_404(db, user=user, assignment_id=submission.assignment_id)
    _require_ready_submission(submission)

    effective_settings = resolve_teacher_assist_settings(db, settings)
    provider_name = validate_teacher_assist_ai_provider(effective_settings.teacher_assist_ai_provider)
    now = _now()
    job = TeacherAssistV2GradingJob(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        student_submission_id=submission.id,
        status="QUEUED",
        provider=provider_name,
        model=None,
        started_at=None,
        completed_at=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )
    db.add(job)
    db.flush()

    job.status = "RUNNING"
    job.started_at = _now()
    job.updated_at = job.started_at
    db.flush()

    try:
        context = build_grading_context(db, assignment=assignment, submission=submission)
        ai_result = generate_grading_draft_ai_result(settings=settings, context=context, db=db)
        job.provider = ai_result.provider
        job.model = ai_result.model

        draft = TeacherAssistV2GradingDraft(
            id=uuid.uuid4(),
            tenant_id=assignment.tenant_id,
            teacher_user_id=user.id,
            assignment_id=assignment.id,
            student_submission_id=submission.id,
            grading_job_id=job.id,
            score=ai_result.score,
            max_score=ai_result.max_score,
            percentage=ai_result.percentage,
            rubric_json=ai_result.rubric_json,
            teacher_comment_draft=ai_result.teacher_comment_draft,
            strengths=ai_result.strengths,
            improvements=ai_result.improvements,
            objective_evidence=ai_result.objective_evidence,
            confidence_score=ai_result.confidence_score,
            provider=ai_result.provider,
            model=ai_result.model,
            created_at=_now(),
        )
        db.add(draft)

        usage_event = TeacherAssistAIUsageEvent(
            tenant_id=assignment.tenant_id,
            user_id=user.id,
            workflow_id=None,
            provider=ai_result.provider,
            model=ai_result.model,
            feature=GRADING_ASSIST_FEATURE,
            input_tokens=ai_result.input_tokens,
            output_tokens=ai_result.output_tokens,
            estimated_cost_cents=ai_result.estimated_cost_cents,
            metadata_json={
                "grading_job_id": str(job.id),
                "student_submission_id": str(submission.id),
                "assignment_id": str(assignment.id),
                "teacher_review_required": True,
            },
            created_at=_now(),
        )
        db.add(usage_event)

        job.status = "COMPLETED"
        job.completed_at = _now()
        job.updated_at = job.completed_at
        job.draft = draft
        db.flush()
        return serialize_grading_job(job)
    except Exception as exc:
        job.status = "FAILED"
        job.error_message = str(exc)
        job.completed_at = _now()
        job.updated_at = job.completed_at
        db.flush()
        raise


def generate_all_grading_drafts(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    settings: Settings,
) -> dict[str, Any]:
    _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    submissions = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment_id,
            TeacherAssistV2StudentSubmission.teacher_user_id == user.id,
            TeacherAssistV2StudentSubmission.status == "READY_FOR_GRADING",
        )
    ).all()
    if not submissions:
        raise ValueError("No submissions are ready for grading.")

    results: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    for submission in submissions:
        try:
            results.append(
                create_grading_job_for_submission(
                    db,
                    user=user,
                    submission_id=submission.id,
                    settings=settings,
                )
            )
        except Exception as exc:
            errors.append(
                {
                    "student_submission_id": str(submission.id),
                    "student_number": str(submission.student_number),
                    "error": str(exc),
                }
            )
    return {
        "generated_count": len(results),
        "failed_count": len(errors),
        "jobs": results,
        "errors": errors,
    }
