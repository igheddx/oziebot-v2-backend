from __future__ import annotations

from datetime import UTC, datetime
import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.models.teacher_assist_mastery_audit_event import TeacherAssistMasteryAuditEvent
from oziebot_api.models.teacher_assist_mastery_commit import TeacherAssistMasteryCommit
from oziebot_api.models.teacher_assist_mastery_evaluation import TeacherAssistMasteryEvaluation
from oziebot_api.models.teacher_assist_mastery_matrix_standard import TeacherAssistMasteryMatrixStandard
from oziebot_api.services.teacher_assist.activity_events import record_activity_event
from oziebot_api.services.teacher_assist.assignments import get_assignment_or_404
from oziebot_api.services.teacher_assist.constants import (
    validate_mastery_audit_event_type,
    validate_mastery_commit_status,
    validate_mastery_commit_type,
    validate_mastery_confidence_level,
    validate_mastery_evaluation_status,
    validate_mastery_evidence_source_type,
    validate_mastery_level,
)
from oziebot_api.services.teacher_assist.gradebook_commits import get_gradebook_commit_or_404
from oziebot_api.services.teacher_assist.grading_reviews import get_grading_review_or_404
from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content
from oziebot_api.services.teacher_assist.mastery_matrix import (
    get_matrix_standard_or_404,
    get_mastery_matrix_or_404,
)


def _normalize_string(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _validate_student_number(student_number: int) -> int:
    if student_number < 1:
        raise ValueError("Student number must be a positive integer")
    return student_number


def _validate_teacher_notes(value: str | None) -> str | None:
    normalized = _normalize_string(value)
    if normalized and contains_pii_like_content({"teacher_notes": normalized}):
        raise ValueError("Teacher notes cannot include student-identifying or PII-like content")
    return normalized


def _validate_evidence_source(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    matrix_class_id: uuid.UUID,
    matrix_subject_id: uuid.UUID,
    evidence_source_type: str | None,
    evidence_source_id: uuid.UUID | None,
) -> tuple[str | None, uuid.UUID | None]:
    normalized_type = validate_mastery_evidence_source_type(evidence_source_type)
    if normalized_type is None and evidence_source_id is None:
        return None, None
    if normalized_type == "manual_observation":
        return normalized_type, evidence_source_id
    if normalized_type is None or evidence_source_id is None:
        raise ValueError("Evidence source type and evidence source id must be provided together")

    if normalized_type == "assignment":
        assignment = get_assignment_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            assignment_id=evidence_source_id,
        )
        if assignment.class_id != matrix_class_id or assignment.subject_id != matrix_subject_id:
            raise ValueError("Assignment evidence must match the mastery matrix class and subject")
    elif normalized_type == "grading_review":
        review = get_grading_review_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            grading_review_id=evidence_source_id,
        )
        if review.class_id != matrix_class_id or review.subject_id != matrix_subject_id:
            raise ValueError("Grading review evidence must match the mastery matrix class and subject")
    elif normalized_type == "gradebook_commit":
        commit = get_gradebook_commit_or_404(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            gradebook_commit_id=evidence_source_id,
        )
        if commit.class_id != matrix_class_id or commit.subject_id != matrix_subject_id:
            raise ValueError("Gradebook commit evidence must match the mastery matrix class and subject")
    return normalized_type, evidence_source_id


def _record_mastery_audit_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    owner_user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    event_type: str,
    summary_text: str,
    mastery_evaluation_id: uuid.UUID | None = None,
    mastery_commit_id: uuid.UUID | None = None,
    student_number: int | None = None,
    standard_id: uuid.UUID | None = None,
    details_json: dict[str, Any] | None = None,
) -> TeacherAssistMasteryAuditEvent:
    row = TeacherAssistMasteryAuditEvent(
        tenant_id=tenant_id,
        owner_user_id=owner_user_id,
        mastery_matrix_id=mastery_matrix_id,
        mastery_evaluation_id=mastery_evaluation_id,
        mastery_commit_id=mastery_commit_id,
        student_number=student_number,
        standard_id=standard_id,
        event_type=validate_mastery_audit_event_type(event_type),
        summary_text=summary_text.strip(),
        details_json=details_json,
        created_at=datetime.now(UTC),
    )
    db.add(row)
    db.flush()
    return row


def get_mastery_evaluation_or_404(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
) -> TeacherAssistMasteryEvaluation:
    row = db.scalars(
        select(TeacherAssistMasteryEvaluation).where(
            TeacherAssistMasteryEvaluation.id == mastery_evaluation_id,
            TeacherAssistMasteryEvaluation.tenant_id == tenant_id,
            TeacherAssistMasteryEvaluation.owner_user_id == user_id,
        )
    ).one_or_none()
    if row is None:
        raise LookupError("Mastery evaluation not found")
    return row


def list_mastery_evaluations(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    standard_id: uuid.UUID | None = None,
    student_number: int | None = None,
    evaluation_status: str | None = None,
) -> list[TeacherAssistMasteryEvaluation]:
    get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
    )
    query = select(TeacherAssistMasteryEvaluation).where(
        TeacherAssistMasteryEvaluation.tenant_id == tenant_id,
        TeacherAssistMasteryEvaluation.owner_user_id == user_id,
        TeacherAssistMasteryEvaluation.mastery_matrix_id == mastery_matrix_id,
    )
    if standard_id is not None:
        query = query.where(TeacherAssistMasteryEvaluation.standard_id == standard_id)
    if student_number is not None:
        query = query.where(TeacherAssistMasteryEvaluation.student_number == _validate_student_number(student_number))
    if evaluation_status is not None:
        query = query.where(
            TeacherAssistMasteryEvaluation.evaluation_status
            == validate_mastery_evaluation_status(evaluation_status)
        )
    return db.scalars(
        query.order_by(
            TeacherAssistMasteryEvaluation.student_number.asc(),
            TeacherAssistMasteryEvaluation.updated_at.desc(),
        )
    ).all()


def create_mastery_evaluation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_matrix_id: uuid.UUID,
    student_number: int,
    standard_id: uuid.UUID,
    mastery_level: str,
    confidence_level: str | None = None,
    evidence_source_type: str | None = None,
    evidence_source_id: uuid.UUID | None = None,
    teacher_notes: str | None = None,
) -> TeacherAssistMasteryEvaluation:
    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
    )
    get_matrix_standard_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        standard_id=standard_id,
    )
    normalized_student_number = _validate_student_number(student_number)
    normalized_level = validate_mastery_level(mastery_level)
    normalized_confidence = validate_mastery_confidence_level(confidence_level)
    normalized_notes = _validate_teacher_notes(teacher_notes)
    normalized_evidence_type, normalized_evidence_id = _validate_evidence_source(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        matrix_class_id=matrix.class_id,
        matrix_subject_id=matrix.subject_id,
        evidence_source_type=evidence_source_type,
        evidence_source_id=evidence_source_id,
    )

    existing = db.scalars(
        select(TeacherAssistMasteryEvaluation).where(
            TeacherAssistMasteryEvaluation.mastery_matrix_id == mastery_matrix_id,
            TeacherAssistMasteryEvaluation.student_number == normalized_student_number,
            TeacherAssistMasteryEvaluation.standard_id == standard_id,
        )
    ).one_or_none()
    if existing is not None:
        raise ValueError("A mastery evaluation already exists for this student number and standard")

    now = datetime.now(UTC)
    evaluation = TeacherAssistMasteryEvaluation(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_matrix_id=mastery_matrix_id,
        student_number=normalized_student_number,
        standard_id=standard_id,
        evaluation_status=validate_mastery_evaluation_status("draft"),
        mastery_level=normalized_level,
        confidence_level=normalized_confidence,
        evidence_source_type=normalized_evidence_type,
        evidence_source_id=normalized_evidence_id,
        teacher_notes=normalized_notes,
        confirmed_by_user_id=None,
        confirmed_at=None,
        current_commit_id=None,
        created_at=now,
        updated_at=now,
    )
    db.add(evaluation)
    db.flush()
    return evaluation


def update_mastery_evaluation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
    mastery_level: str | None = None,
    confidence_level: str | None = None,
    evidence_source_type: str | None = None,
    evidence_source_id: uuid.UUID | None = None,
    teacher_notes: str | None = None,
    clear_evidence: bool = False,
) -> TeacherAssistMasteryEvaluation:
    evaluation = get_mastery_evaluation_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_evaluation_id=mastery_evaluation_id,
    )
    if evaluation.evaluation_status != "draft":
        raise ValueError("Only draft mastery evaluations can be updated")

    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
    )

    if mastery_level is not None:
        evaluation.mastery_level = validate_mastery_level(mastery_level)
    if confidence_level is not None:
        evaluation.confidence_level = validate_mastery_confidence_level(confidence_level)
    if teacher_notes is not None:
        evaluation.teacher_notes = _validate_teacher_notes(teacher_notes)
    if clear_evidence:
        evaluation.evidence_source_type = None
        evaluation.evidence_source_id = None
    elif evidence_source_type is not None or evidence_source_id is not None:
        normalized_evidence_type, normalized_evidence_id = _validate_evidence_source(
            db,
            tenant_id=tenant_id,
            user_id=user_id,
            matrix_class_id=matrix.class_id,
            matrix_subject_id=matrix.subject_id,
            evidence_source_type=evidence_source_type or evaluation.evidence_source_type,
            evidence_source_id=evidence_source_id or evaluation.evidence_source_id,
        )
        evaluation.evidence_source_type = normalized_evidence_type
        evaluation.evidence_source_id = normalized_evidence_id

    evaluation.updated_at = datetime.now(UTC)
    db.flush()
    return evaluation


def commit_mastery_evaluation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
    commit_reason: str | None = None,
) -> tuple[TeacherAssistMasteryEvaluation, TeacherAssistMasteryCommit]:
    evaluation = get_mastery_evaluation_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_evaluation_id=mastery_evaluation_id,
    )
    if evaluation.evaluation_status not in {"draft", "active"}:
        raise ValueError("Reversed mastery evaluations cannot be committed again without a new evaluation")
    if evaluation.evaluation_status == "active" and evaluation.current_commit_id is not None:
        raise ValueError("Mastery evaluation is already committed")

    matrix = get_mastery_matrix_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
    )
    if evaluation.mastery_level == "not_assessed":
        raise ValueError("Teacher-confirmed mastery commits require a mastery level beyond not_assessed")

    now = datetime.now(UTC)
    previous_level = None
    commit_type = validate_mastery_commit_type("initial_commit")

    commit = TeacherAssistMasteryCommit(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_evaluation_id=evaluation.id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        commit_type=commit_type,
        commit_status=validate_mastery_commit_status("active"),
        previous_mastery_level=previous_level,
        new_mastery_level=evaluation.mastery_level,
        confidence_level=evaluation.confidence_level,
        evidence_source_type=evaluation.evidence_source_type,
        evidence_source_id=evaluation.evidence_source_id,
        teacher_notes=evaluation.teacher_notes,
        commit_reason=_normalize_string(commit_reason),
        supersedes_commit_id=None,
        reversed_by_commit_id=None,
        reversed_at=None,
        reversed_by_user_id=None,
        created_at=now,
    )
    db.add(commit)
    db.flush()

    evaluation.evaluation_status = validate_mastery_evaluation_status("active")
    evaluation.confirmed_by_user_id = user_id
    evaluation.confirmed_at = now
    evaluation.current_commit_id = commit.id
    evaluation.updated_at = now

    matrix_standard = db.scalars(
        select(TeacherAssistMasteryMatrixStandard).where(
            TeacherAssistMasteryMatrixStandard.mastery_matrix_id == evaluation.mastery_matrix_id,
            TeacherAssistMasteryMatrixStandard.standard_id == evaluation.standard_id,
        )
    ).one()
    matrix_standard.assessment_count += 1
    matrix_standard.updated_at = now
    matrix.updated_at = now
    db.flush()

    _record_mastery_audit_event(
        db,
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        mastery_evaluation_id=evaluation.id,
        mastery_commit_id=commit.id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        event_type="mastery_commit_created",
        summary_text=(
            f"Teacher confirmed mastery '{evaluation.mastery_level}' for STUDENT #{evaluation.student_number}."
        ),
        details_json={
            "new_mastery_level": evaluation.mastery_level,
            "evidence_source_type": evaluation.evidence_source_type,
            "evidence_source_id": str(evaluation.evidence_source_id) if evaluation.evidence_source_id else None,
        },
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="mastery_commit_created",
        event_category="grading",
        entity_type="mastery_evaluation",
        entity_id=evaluation.id,
        class_id=matrix.class_id,
        subject_id=matrix.subject_id,
        school_year_id=matrix.school_year_id,
        grading_period_id=matrix.grading_period_id,
        summary_text=(
            f"Teacher confirmed mastery for STUDENT #{evaluation.student_number} in {matrix.title}."
        ),
        details_json={"mastery_level": evaluation.mastery_level, "standard_id": str(evaluation.standard_id)},
    )
    return evaluation, commit


def correct_mastery_evaluation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
    mastery_level: str,
    confidence_level: str | None = None,
    teacher_notes: str | None = None,
    commit_reason: str | None = None,
) -> tuple[TeacherAssistMasteryEvaluation, TeacherAssistMasteryCommit]:
    evaluation = get_mastery_evaluation_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_evaluation_id=mastery_evaluation_id,
    )
    if evaluation.evaluation_status != "active":
        raise ValueError("Only active mastery evaluations can be corrected")
    if evaluation.current_commit_id is None:
        raise ValueError("Active mastery evaluation is missing a current commit")

    normalized_reason = _normalize_string(commit_reason)
    if not normalized_reason:
        raise ValueError("Mastery corrections require a commit reason")

    current_commit = db.get(TeacherAssistMasteryCommit, evaluation.current_commit_id)
    if current_commit is None or current_commit.commit_status != "active":
        raise ValueError("Current mastery commit is not active")

    new_level = validate_mastery_level(mastery_level)
    if new_level == "not_assessed":
        raise ValueError("Mastery corrections cannot set mastery level to not_assessed")

    now = datetime.now(UTC)
    correction = TeacherAssistMasteryCommit(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_evaluation_id=evaluation.id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        commit_type=validate_mastery_commit_type("correction"),
        commit_status=validate_mastery_commit_status("active"),
        previous_mastery_level=current_commit.new_mastery_level,
        new_mastery_level=new_level,
        confidence_level=validate_mastery_confidence_level(confidence_level) or evaluation.confidence_level,
        evidence_source_type=evaluation.evidence_source_type,
        evidence_source_id=evaluation.evidence_source_id,
        teacher_notes=_validate_teacher_notes(teacher_notes) if teacher_notes is not None else evaluation.teacher_notes,
        commit_reason=normalized_reason,
        supersedes_commit_id=current_commit.id,
        reversed_by_commit_id=None,
        reversed_at=None,
        reversed_by_user_id=None,
        created_at=now,
    )
    db.add(correction)
    db.flush()

    current_commit.commit_status = validate_mastery_commit_status("superseded")
    evaluation.mastery_level = new_level
    if confidence_level is not None:
        evaluation.confidence_level = validate_mastery_confidence_level(confidence_level)
    if teacher_notes is not None:
        evaluation.teacher_notes = _validate_teacher_notes(teacher_notes)
    evaluation.current_commit_id = correction.id
    evaluation.confirmed_by_user_id = user_id
    evaluation.confirmed_at = now
    evaluation.updated_at = now
    db.flush()

    _record_mastery_audit_event(
        db,
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        mastery_evaluation_id=evaluation.id,
        mastery_commit_id=correction.id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        event_type="mastery_commit_corrected",
        summary_text=(
            f"Corrected mastery for STUDENT #{evaluation.student_number} from "
            f"{current_commit.new_mastery_level} to {new_level}."
        ),
        details_json={"previous_mastery_level": current_commit.new_mastery_level, "new_mastery_level": new_level},
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="mastery_commit_corrected",
        event_category="grading",
        entity_type="mastery_evaluation",
        entity_id=evaluation.id,
        summary_text=f"Mastery corrected for STUDENT #{evaluation.student_number}.",
        details_json={"new_mastery_level": new_level},
    )
    return evaluation, correction


def reverse_mastery_evaluation(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
    commit_reason: str | None = None,
) -> tuple[TeacherAssistMasteryEvaluation, TeacherAssistMasteryCommit]:
    evaluation = get_mastery_evaluation_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_evaluation_id=mastery_evaluation_id,
    )
    if evaluation.evaluation_status != "active":
        raise ValueError("Only active mastery evaluations can be reversed")
    if evaluation.current_commit_id is None:
        raise ValueError("Active mastery evaluation is missing a current commit")

    normalized_reason = _normalize_string(commit_reason)
    if not normalized_reason:
        raise ValueError("Mastery reversals require a commit reason")

    current_commit = db.get(TeacherAssistMasteryCommit, evaluation.current_commit_id)
    if current_commit is None or current_commit.commit_status != "active":
        raise ValueError("Current mastery commit is not active")

    now = datetime.now(UTC)
    reversal = TeacherAssistMasteryCommit(
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_evaluation_id=evaluation.id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        commit_type=validate_mastery_commit_type("reversal"),
        commit_status=validate_mastery_commit_status("active"),
        previous_mastery_level=current_commit.new_mastery_level,
        new_mastery_level=validate_mastery_level("not_assessed"),
        confidence_level=evaluation.confidence_level,
        evidence_source_type=evaluation.evidence_source_type,
        evidence_source_id=evaluation.evidence_source_id,
        teacher_notes=evaluation.teacher_notes,
        commit_reason=normalized_reason,
        supersedes_commit_id=None,
        reversed_by_commit_id=current_commit.id,
        reversed_at=now,
        reversed_by_user_id=user_id,
        created_at=now,
    )
    db.add(reversal)
    db.flush()

    current_commit.commit_status = validate_mastery_commit_status("reversed")
    current_commit.reversed_by_commit_id = reversal.id
    current_commit.reversed_at = now
    current_commit.reversed_by_user_id = user_id

    evaluation.evaluation_status = validate_mastery_evaluation_status("reversed")
    evaluation.mastery_level = validate_mastery_level("not_assessed")
    evaluation.current_commit_id = reversal.id
    evaluation.updated_at = now
    db.flush()

    _record_mastery_audit_event(
        db,
        tenant_id=tenant_id,
        owner_user_id=user_id,
        mastery_matrix_id=evaluation.mastery_matrix_id,
        mastery_evaluation_id=evaluation.id,
        mastery_commit_id=reversal.id,
        student_number=evaluation.student_number,
        standard_id=evaluation.standard_id,
        event_type="mastery_commit_reversed",
        summary_text=f"Reversed mastery for STUDENT #{evaluation.student_number}.",
        details_json={"previous_mastery_level": current_commit.new_mastery_level},
    )
    record_activity_event(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        event_type="mastery_commit_reversed",
        event_category="grading",
        entity_type="mastery_evaluation",
        entity_id=evaluation.id,
        summary_text=f"Mastery reversed for STUDENT #{evaluation.student_number}.",
        details_json={"reason": normalized_reason},
    )
    return evaluation, reversal


def list_mastery_evaluation_commits(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    mastery_evaluation_id: uuid.UUID,
) -> list[TeacherAssistMasteryCommit]:
    get_mastery_evaluation_or_404(
        db,
        tenant_id=tenant_id,
        user_id=user_id,
        mastery_evaluation_id=mastery_evaluation_id,
    )
    return db.scalars(
        select(TeacherAssistMasteryCommit)
        .where(
            TeacherAssistMasteryCommit.tenant_id == tenant_id,
            TeacherAssistMasteryCommit.owner_user_id == user_id,
            TeacherAssistMasteryCommit.mastery_evaluation_id == mastery_evaluation_id,
        )
        .order_by(TeacherAssistMasteryCommit.created_at.asc())
    ).all()


def serialize_mastery_evaluation(evaluation: TeacherAssistMasteryEvaluation) -> dict[str, Any]:
    return {
        "id": evaluation.id,
        "tenant_id": evaluation.tenant_id,
        "owner_user_id": evaluation.owner_user_id,
        "mastery_matrix_id": evaluation.mastery_matrix_id,
        "student_number": evaluation.student_number,
        "standard_id": evaluation.standard_id,
        "evaluation_status": evaluation.evaluation_status,
        "mastery_level": evaluation.mastery_level,
        "confidence_level": evaluation.confidence_level,
        "evidence_source_type": evaluation.evidence_source_type,
        "evidence_source_id": evaluation.evidence_source_id,
        "teacher_notes": evaluation.teacher_notes,
        "confirmed_by_user_id": evaluation.confirmed_by_user_id,
        "confirmed_at": evaluation.confirmed_at,
        "current_commit_id": evaluation.current_commit_id,
        "created_at": evaluation.created_at,
        "updated_at": evaluation.updated_at,
    }


def serialize_mastery_commit(commit: TeacherAssistMasteryCommit) -> dict[str, Any]:
    return {
        "id": commit.id,
        "mastery_evaluation_id": commit.mastery_evaluation_id,
        "mastery_matrix_id": commit.mastery_matrix_id,
        "student_number": commit.student_number,
        "standard_id": commit.standard_id,
        "commit_type": commit.commit_type,
        "commit_status": commit.commit_status,
        "previous_mastery_level": commit.previous_mastery_level,
        "new_mastery_level": commit.new_mastery_level,
        "confidence_level": commit.confidence_level,
        "evidence_source_type": commit.evidence_source_type,
        "evidence_source_id": commit.evidence_source_id,
        "teacher_notes": commit.teacher_notes,
        "commit_reason": commit.commit_reason,
        "supersedes_commit_id": commit.supersedes_commit_id,
        "reversed_by_commit_id": commit.reversed_by_commit_id,
        "reversed_at": commit.reversed_at,
        "reversed_by_user_id": commit.reversed_by_user_id,
        "created_at": commit.created_at,
    }
