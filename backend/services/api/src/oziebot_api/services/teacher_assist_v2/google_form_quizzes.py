"""Assignment-scoped Google Form quiz creation and result import."""

from __future__ import annotations

import csv
import io
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.education_catalog import EducationGrade, EducationSubject
from oziebot_api.models.teacher_assist_v2_assignment import TeacherAssistV2Assignment
from oziebot_api.models.teacher_assist_v2_assignment_google_form import (
    TeacherAssistV2AssignmentGoogleForm,
)
from oziebot_api.models.teacher_assist_v2_assignment_grade import TeacherAssistV2AssignmentGrade
from oziebot_api.models.teacher_assist_v2_instructional_package import (
    TeacherAssistV2InstructionalPackageArtifact,
)
from oziebot_api.models.teacher_assist_v2_student_submission import TeacherAssistV2StudentSubmission
from oziebot_api.models.teacher_assist_v2_submission_batch import TeacherAssistV2SubmissionBatch
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist_v2.google_forms_client import (
    build_assignment_description,
    create_assignment_quiz_form,
    extract_response_rows,
    get_form_with_questions,
    list_form_responses,
)
from oziebot_api.services.teacher_assist_v2.google_integration_constants import (
    GOOGLE_FORM_IMPORT_MATCH_METHOD,
)
from oziebot_api.services.teacher_assist_v2.google_oauth import (
    get_teacher_google_connection,
    get_valid_access_token,
    google_oauth_configured,
    serialize_teacher_google_connection,
)
from oziebot_api.services.teacher_assist_v2.submission_intake import (
    _get_assignment_or_404,
    _validate_student_number,
)
from oziebot_api.services.teacher_assist_v2.teacher_onboarding import get_v2_onboarding


def _now() -> datetime:
    return datetime.now(UTC)


def get_assignment_google_form(
    db: Session, *, assignment_id: uuid.UUID
) -> TeacherAssistV2AssignmentGoogleForm | None:
    return db.scalars(
        select(TeacherAssistV2AssignmentGoogleForm).where(
            TeacherAssistV2AssignmentGoogleForm.assignment_id == assignment_id
        )
    ).one_or_none()


def serialize_assignment_google_form(
    row: TeacherAssistV2AssignmentGoogleForm | None,
) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "id": str(row.id),
        "assignment_id": str(row.assignment_id),
        "artifact_id": str(row.artifact_id),
        "google_form_id": row.google_form_id,
        "google_form_url": row.google_form_url,
        "google_edit_url": row.google_edit_url,
        "google_response_url": row.google_response_url,
        "google_created_at": row.google_created_at.isoformat(),
        "google_sync_status": row.google_sync_status,
        "last_import_at": row.last_import_at.isoformat() if row.last_import_at else None,
        "last_import_count": row.last_import_count,
    }


def build_teacher_google_status(db: Session, *, user: User, settings: Settings) -> dict[str, Any]:
    connection = get_teacher_google_connection(db, teacher_user_id=user.id)
    return {
        **serialize_teacher_google_connection(connection, settings=settings),
        **{
            "server_integration_ready": google_oauth_configured(settings),
        },
    }


def _resolve_quiz_artifact(
    db: Session, *, assignment: TeacherAssistV2Assignment
) -> TeacherAssistV2InstructionalPackageArtifact:
    artifact = db.scalars(
        select(TeacherAssistV2InstructionalPackageArtifact).where(
            TeacherAssistV2InstructionalPackageArtifact.assignment_id == assignment.id,
            TeacherAssistV2InstructionalPackageArtifact.artifact_type == "quiz",
        )
    ).first()
    if artifact is None:
        raise ValueError("Quiz artifact not found for this assignment.")
    return artifact


def create_google_form_for_assignment(
    db: Session,
    *,
    settings: Settings,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    if not google_oauth_configured(settings):
        raise ValueError(
            "Google integration is not configured on the server. Contact your administrator."
        )
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    if assignment.assignment_type != "QUIZ":
        raise ValueError("Google Forms can only be created for quiz assignments.")

    existing = get_assignment_google_form(db, assignment_id=assignment.id)
    if existing is not None:
        return {
            "google_form": serialize_assignment_google_form(existing),
            "message": "Google Form already exists for this assignment.",
        }

    artifact = _resolve_quiz_artifact(db, assignment=assignment)
    content = artifact.content_json if isinstance(artifact.content_json, dict) else {}
    questions = content.get("questions") or []
    if not questions:
        raise ValueError("Quiz artifact has no questions to publish.")

    onboarding = get_v2_onboarding(db, user=user)
    student_count = onboarding.student_count if onboarding and onboarding.student_count else 3
    if student_count < 1:
        student_count = 3

    subject = db.get(EducationSubject, assignment.catalog_subject_id)
    grade = db.get(EducationGrade, assignment.catalog_grade_id)
    objectives = [str(objective_id) for objective_id in assignment.education_objective_ids_json]
    description = build_assignment_description(
        assignment_id=str(assignment.id),
        package_id=str(assignment.instructional_package_id),
        subject_name=subject.display_name if subject else "Subject",
        grade_label=grade.display_name if grade else "Grade",
        objectives=objectives,
    )

    access_token = get_valid_access_token(db, settings=settings, teacher_user_id=user.id)
    created = create_assignment_quiz_form(
        access_token,
        title=assignment.title,
        description=description,
        student_count=student_count,
        questions=questions,
    )

    now = _now()
    row = TeacherAssistV2AssignmentGoogleForm(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        artifact_id=artifact.id,
        instructional_package_id=assignment.instructional_package_id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        education_objective_ids_json=list(assignment.education_objective_ids_json),
        google_form_id=created["google_form_id"],
        google_form_url=created["google_form_url"],
        google_edit_url=created["google_edit_url"],
        google_response_url=created["google_response_url"],
        google_created_at=now,
        google_created_by_user_id=user.id,
        google_sync_status="CREATED",
        question_mapping_json={
            "items": created["question_mapping"],
            "objective_mapping": content.get("objective_mapping"),
        },
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return {
        "google_form": serialize_assignment_google_form(row),
        "message": "Google Form created successfully.",
    }


def _ensure_import_submission(
    db: Session,
    *,
    user: User,
    assignment: TeacherAssistV2Assignment,
    batch: TeacherAssistV2SubmissionBatch,
    student_number: int,
    google_response_id: str,
) -> TeacherAssistV2StudentSubmission:
    existing = db.scalars(
        select(TeacherAssistV2StudentSubmission).where(
            TeacherAssistV2StudentSubmission.assignment_id == assignment.id,
            TeacherAssistV2StudentSubmission.student_number == student_number,
            TeacherAssistV2StudentSubmission.match_method == GOOGLE_FORM_IMPORT_MATCH_METHOD,
        )
    ).first()
    if existing is not None:
        return existing

    now = _now()
    row = TeacherAssistV2StudentSubmission(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        submission_batch_id=batch.id,
        packet_id=None,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        student_number=student_number,
        status="READY_FOR_REVIEW",
        file_key=f"google-forms://{assignment.id}/{google_response_id}",
        original_filename=f"google-form-response-{google_response_id}.json",
        mime_type="application/json",
        file_size=0,
        page_range=None,
        qr_identifier=None,
        match_method=GOOGLE_FORM_IMPORT_MATCH_METHOD,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row


def _upsert_import_grade(
    db: Session,
    *,
    user: User,
    submission: TeacherAssistV2StudentSubmission,
    score: float,
    max_score: float,
    import_source: str,
    google_response_id: str | None,
) -> TeacherAssistV2AssignmentGrade:
    from oziebot_api.services.teacher_assist_v2.grade_reviews import (
        _archive_active_grades,
        _percentage,
    )
    from oziebot_api.services.teacher_assist_v2.mastery_constants import resolve_mastery_level

    _archive_active_grades(db, submission_id=submission.id)
    now = _now()
    percentage = _percentage(score, max_score)
    grade = TeacherAssistV2AssignmentGrade(
        id=uuid.uuid4(),
        tenant_id=submission.tenant_id,
        teacher_user_id=user.id,
        assignment_id=submission.assignment_id,
        student_submission_id=submission.id,
        student_number=submission.student_number,
        grading_draft_id=None,
        score=score,
        max_score=max_score,
        percentage=percentage,
        mastery_level=resolve_mastery_level(percentage),
        rubric_json={
            "import_source": import_source,
            "google_response_id": google_response_id,
        },
        teacher_comment="Imported from Google Forms. Review and confirm before gradebook commit.",
        teacher_override_reason=None,
        review_action="SAVE_DRAFT",
        confirmed_by=None,
        confirmed_at=None,
        status="DRAFT",
        created_at=now,
        updated_at=now,
    )
    db.add(grade)
    db.flush()
    return grade


def import_google_form_results(
    db: Session,
    *,
    settings: Settings,
    user: User,
    assignment_id: uuid.UUID,
) -> dict[str, Any]:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    form_row = get_assignment_google_form(db, assignment_id=assignment.id)
    if form_row is None:
        raise ValueError("Create a Google Form for this assignment before importing results.")

    access_token = get_valid_access_token(db, settings=settings, teacher_user_id=user.id)
    form_payload = get_form_with_questions(access_token, form_id=form_row.google_form_id)
    responses = list_form_responses(access_token, form_id=form_row.google_form_id)
    rows = extract_response_rows(form_payload, responses)

    now = _now()
    batch = TeacherAssistV2SubmissionBatch(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        status="MATCHED",
        uploaded_file_key=f"google-forms-import://{assignment.id}/{now.isoformat()}",
        original_filename="google-forms-api-import.json",
        mime_type="application/json",
        file_size=0,
        created_at=now,
    )
    db.add(batch)
    db.flush()

    imported = 0
    skipped: list[dict[str, Any]] = []
    default_max = float(
        sum(
            int(item.get("points") or 1)
            for item in (form_row.question_mapping_json or {}).get("items", [])
            if item.get("role") == "quiz" and item.get("auto_graded", True) is not False
        )
        or 1
    )

    for row in rows:
        student_number = row.get("student_number")
        if student_number is None:
            skipped.append(
                {"reason": "missing_student_number", "response_id": row.get("google_response_id")}
            )
            continue
        try:
            normalized_student = _validate_student_number(
                db, user=user, student_number=int(student_number)
            )
        except ValueError as exc:
            skipped.append(
                {
                    "reason": str(exc),
                    "student_number": student_number,
                    "response_id": row.get("google_response_id"),
                }
            )
            continue

        score = row.get("score")
        if score is None:
            skipped.append(
                {
                    "reason": "missing_score",
                    "student_number": normalized_student,
                    "response_id": row.get("google_response_id"),
                }
            )
            continue

        max_score = row.get("max_score") or default_max
        submission = _ensure_import_submission(
            db,
            user=user,
            assignment=assignment,
            batch=batch,
            student_number=normalized_student,
            google_response_id=str(row.get("google_response_id") or ""),
        )
        _upsert_import_grade(
            db,
            user=user,
            submission=submission,
            score=float(score),
            max_score=float(max_score) if float(max_score) > 0 else default_max,
            import_source="google_forms_api",
            google_response_id=row.get("google_response_id"),
        )
        imported += 1

    form_row.google_sync_status = "IMPORTED"
    form_row.last_import_at = now
    form_row.last_import_count = imported
    form_row.updated_at = now
    db.flush()

    return {
        "imported_count": imported,
        "skipped": skipped,
        "message": f"Imported {imported} response(s) as draft grades for this assignment.",
    }


def import_google_form_results_csv(
    db: Session,
    *,
    user: User,
    assignment_id: uuid.UUID,
    csv_text: str,
) -> dict[str, Any]:
    assignment = _get_assignment_or_404(db, user=user, assignment_id=assignment_id)
    form_row = get_assignment_google_form(db, assignment_id=assignment.id)
    if form_row is None:
        raise ValueError("Create a Google Form for this assignment before importing results.")

    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        raise ValueError("CSV must include a header row.")

    normalized_headers = {name.strip().lower(): name for name in reader.fieldnames if name}
    student_key = normalized_headers.get("student number") or normalized_headers.get(
        "student_number"
    )
    score_key = normalized_headers.get("score")
    if not student_key or not score_key:
        raise ValueError("CSV must include Student Number and Score columns.")

    max_key = normalized_headers.get("max score") or normalized_headers.get("max_score")
    timestamp_key = normalized_headers.get("timestamp")

    now = _now()
    batch = TeacherAssistV2SubmissionBatch(
        id=uuid.uuid4(),
        tenant_id=assignment.tenant_id,
        teacher_user_id=user.id,
        assignment_id=assignment.id,
        platform_school_year_id=assignment.platform_school_year_id,
        catalog_district_id=assignment.catalog_district_id,
        catalog_school_id=assignment.catalog_school_id,
        catalog_grade_id=assignment.catalog_grade_id,
        catalog_subject_id=assignment.catalog_subject_id,
        status="MATCHED",
        uploaded_file_key=f"google-forms-csv://{assignment.id}/{now.isoformat()}",
        original_filename="google-forms-csv-import.csv",
        mime_type="text/csv",
        file_size=len(csv_text.encode("utf-8")),
        created_at=now,
    )
    db.add(batch)
    db.flush()

    imported = 0
    row_errors: list[dict[str, Any]] = []
    for line_number, row in enumerate(reader, start=2):
        raw_student = (row.get(student_key) or "").strip()
        raw_score = (row.get(score_key) or "").strip()
        if not raw_student and not raw_score:
            continue
        if not raw_student:
            row_errors.append({"line": line_number, "error": "Student Number is required."})
            continue
        if not raw_score:
            row_errors.append({"line": line_number, "error": "Score is required."})
            continue
        try:
            student_number = int(raw_student.lstrip("#").strip())
            normalized_student = _validate_student_number(
                db, user=user, student_number=student_number
            )
            score = float(raw_score)
        except ValueError as exc:
            row_errors.append({"line": line_number, "error": str(exc)})
            continue

        max_score = 100.0
        if max_key and (row.get(max_key) or "").strip():
            try:
                max_score = float((row.get(max_key) or "").strip())
            except ValueError:
                row_errors.append({"line": line_number, "error": "Max Score must be a number."})
                continue

        response_id = f"csv-line-{line_number}"
        if timestamp_key and (row.get(timestamp_key) or "").strip():
            response_id = f"csv-{row[timestamp_key]}-{line_number}"

        submission = _ensure_import_submission(
            db,
            user=user,
            assignment=assignment,
            batch=batch,
            student_number=normalized_student,
            google_response_id=response_id,
        )
        _upsert_import_grade(
            db,
            user=user,
            submission=submission,
            score=score,
            max_score=max_score,
            import_source="google_forms_csv",
            google_response_id=response_id,
        )
        imported += 1

    if imported == 0 and row_errors:
        raise ValueError({"row_errors": row_errors})

    form_row.google_sync_status = "IMPORTED"
    form_row.last_import_at = now
    form_row.last_import_count = imported
    form_row.updated_at = now
    db.flush()

    return {
        "imported_count": imported,
        "row_errors": row_errors,
        "message": f"Imported {imported} CSV row(s) as draft grades for this assignment.",
    }


def enrich_artifact_with_google_form(
    db: Session,
    *,
    artifact: TeacherAssistV2InstructionalPackageArtifact,
    settings: Settings,
    user: User,
) -> dict[str, Any]:
    google_form = None
    assignment_id = artifact.assignment_id
    if assignment_id:
        row = get_assignment_google_form(db, assignment_id=assignment_id)
        google_form = serialize_assignment_google_form(row)
    return {
        "assignment_id": str(assignment_id) if assignment_id else None,
        "google_connection": build_teacher_google_status(db, user=user, settings=settings),
        "google_form": google_form,
    }
