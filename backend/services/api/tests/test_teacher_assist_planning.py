from __future__ import annotations

from datetime import UTC, datetime
import pytest
import uuid
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.membership import TenantMembership
from oziebot_api.models.platform_product import PlatformProduct
from oziebot_api.models.teacher_assist_activity_event import TeacherAssistActivityEvent
from oziebot_api.models.teacher_assist_ai_usage_event import TeacherAssistAIUsageEvent
from oziebot_api.models.teacher_assist_assignment_grading_review import TeacherAssistAssignmentGradingReview
from oziebot_api.models.teacher_assist_assignment_grade_record import TeacherAssistAssignmentGradeRecord
from oziebot_api.models.teacher_assist_assignment_gradebook_audit_event import (
    TeacherAssistAssignmentGradebookAuditEvent,
)
from oziebot_api.models.teacher_assist_assignment_gradebook_commit import (
    TeacherAssistAssignmentGradebookCommit,
)
from oziebot_api.models.teacher_assist_extracted_text_record import TeacherAssistExtractedTextRecord
from oziebot_api.models.teacher_assist_extraction_job import TeacherAssistExtractionJob
from oziebot_api.models.teacher_assist_mastery_audit_event import TeacherAssistMasteryAuditEvent
from oziebot_api.models.teacher_assist_reteach_plan import TeacherAssistReteachPlan
from oziebot_api.models.teacher_assist_reteach_plan_version import TeacherAssistReteachPlanVersion
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.teacher_assist_workflow import TeacherAssistWorkflow
from oziebot_api.models.tenant_product_access import TenantProductAccess
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.ai_provider import TeacherAssistAIProviderResult
from oziebot_api.services.teacher_assist.provider_config import get_teacher_assist_ai_provider
from oziebot_api.services.teacher_assist.prompt_contracts import INSTRUCTIONAL_PLAN_PROMPT_VERSION
from oziebot_api.services.teacher_assist.workflow_service import (
    claim_next_teacher_assist_workflow,
    process_claimed_teacher_assist_workflow_with_engine,
    process_next_teacher_assist_workflow_with_engine,
)
from oziebot_api.services.teacher_assist.extraction_jobs import (
    process_next_teacher_assist_extraction_job_with_engine,
)
from oziebot_api.services.teacher_assist.export_generation import (
    process_next_teacher_assist_export_with_engine,
)
from oziebot_api.services.product_access import TEACHER_ASSIST_PRODUCT_KEY


def _register_user(client, *, email: str, tenant_name: str) -> str:
    response = client.post(
        "/v1/auth/register",
        json={
            "email": email,
            "full_name": email.split("@")[0].title(),
            "password": "password-123",
            "tenant_name": tenant_name,
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["access_token"]


def _grant_teacher_assist_access(db_session: Session, *, email: str, status: str = "active") -> None:
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id)
    )
    assert membership is not None
    product = db_session.scalar(
        select(PlatformProduct).where(PlatformProduct.product_key == TEACHER_ASSIST_PRODUCT_KEY)
    )
    assert product is not None
    existing = db_session.scalar(
        select(TenantProductAccess).where(
            TenantProductAccess.tenant_id == membership.tenant_id,
            TenantProductAccess.product_id == product.id,
        )
    )
    if existing is None:
        db_session.add(
            TenantProductAccess(
                tenant_id=membership.tenant_id,
                product_id=product.id,
                status=status,
                created_at=membership.created_at,
                updated_at=membership.created_at,
            )
        )
    else:
        existing.status = status
    db_session.commit()


def _run_teacher_assist_worker(
    db_session: Session,
    *,
    settings: Settings | None = None,
    workflow_id: str | None = None,
) -> str | None:
    settings = settings or Settings(teacher_assist_worker_max_retries=0)
    claimed_id = process_next_teacher_assist_workflow_with_engine(
        db_session.get_bind(),
        settings=settings,
        workflow_id=uuid.UUID(workflow_id) if workflow_id else None,
        worker_name="teacher-assist-test-worker",
    )
    db_session.expire_all()
    return str(claimed_id) if claimed_id is not None else None


def _run_teacher_assist_extraction_worker(
    db_session: Session,
    *,
    settings: Settings | None = None,
    extraction_job_id: str | None = None,
) -> str | None:
    settings = settings or Settings(teacher_assist_worker_max_retries=0)
    claimed_id = process_next_teacher_assist_extraction_job_with_engine(
        db_session.get_bind(),
        settings=settings,
        extraction_job_id=uuid.UUID(extraction_job_id) if extraction_job_id else None,
        worker_name="teacher-assist-extraction-test-worker",
    )
    db_session.expire_all()
    return str(claimed_id) if claimed_id is not None else None


def _run_teacher_assist_export_worker(
    db_session: Session,
    *,
    settings: Settings | None = None,
    workflow_id: str | None = None,
) -> str | None:
    settings = settings or Settings(teacher_assist_worker_max_retries=0)
    claimed_id = process_next_teacher_assist_export_with_engine(
        db_session.get_bind(),
        settings=settings,
        workflow_id=uuid.UUID(workflow_id) if workflow_id else None,
        worker_name="teacher-assist-export-test-worker",
    )
    db_session.expire_all()
    return str(claimed_id) if claimed_id is not None else None


def _share_teacher_assist_tenant(
    db_session: Session,
    *,
    owner_email: str,
    member_email: str,
    role: str = "user",
) -> None:
    owner = db_session.scalar(select(User).where(User.email == owner_email))
    member = db_session.scalar(select(User).where(User.email == member_email))
    assert owner is not None
    assert member is not None
    owner_membership = db_session.scalar(select(TenantMembership).where(TenantMembership.user_id == owner.id))
    assert owner_membership is not None
    existing = db_session.scalar(
        select(TenantMembership).where(
            TenantMembership.user_id == member.id,
            TenantMembership.tenant_id == owner_membership.tenant_id,
        )
    )
    if existing is None:
        db_session.add(
            TenantMembership(
                user_id=member.id,
                tenant_id=owner_membership.tenant_id,
                role=role,
                created_at=owner_membership.created_at,
            )
        )
    db_session.commit()


def _real_provider_result(*, planning_scope: str = "weekly") -> TeacherAssistAIProviderResult:
    return TeacherAssistAIProviderResult(
        content_json={
            "planning_scope": planning_scope,
            "plan_title": "TeacherAssist Real Provider Plan",
            "module_title": None,
            "duration": {
                "start_date": "2026-08-10",
                "end_date": "2026-08-14",
                "estimated_weeks": 1,
                "instructional_days_count": 5,
                "summary": "1 week / 5 instructional days",
            },
            "overview": "Teacher-ready instructional overview with concrete classroom steps.",
            "instructional_arc": [
                "Launch prior knowledge and objective framing.",
                "Model, guided practice, and checks for understanding.",
                "Independent application and reflection.",
            ],
            "weekly_segments": [
                {
                    "segment_index": 1,
                    "segment_label": "Week 1",
                    "focus": "Close reading and evidence-based response writing.",
                    "objectives": ["Analyze text evidence", "Draft a short written response"],
                    "subjects": [],
                    "daily_breakdown": [
                        {
                            "day": 1,
                            "day_label": "Monday",
                            "focus": "Annotate the anchor text",
                            "teacher_actions": ["Model annotation moves"],
                            "student_activities": ["Annotate a short passage"],
                            "checks_for_understanding": ["Collect annotations"],
                            "materials_needed": ["Anchor text", "Notebook"],
                        }
                    ],
                    "assessment_checkpoints": ["Short written response review"],
                }
            ],
            "standards_progression": [
                {
                    "code": "ELA.5.6A",
                    "description": "Use evidence from text.",
                    "phase": "Apply during guided reading and writing.",
                }
            ],
            "vocabulary": ["annotate", "evidence", "response"],
            "materials_needed": ["Anchor text", "Notebook"],
            "differentiation": {
                "support": ["Sentence stems"],
                "extension": ["Evidence-based paragraph"],
                "intervention": ["Teacher conference"],
            },
            "assessment_checkpoints": ["Entry check", "Exit ticket"],
            "resources_used": [{"id": "resource-1", "title": "Anchor text", "resource_type": "doc"}],
            "teacher_notes_used": "Focus on text evidence and concise writing.",
            "review_notes": "",
        },
        provider="openai",
        model="gpt-4.1-mini",
        input_tokens=1200,
        output_tokens=700,
        estimated_cost_cents=1,
        metadata_json={
            "is_mock": False,
            "provider_mode": "real",
            "prompt_version": INSTRUCTIONAL_PLAN_PROMPT_VERSION,
            "request_id": "resp_test_123",
        },
    )


def _create_ready_planning_draft_context(
    client,
    *,
    token: str,
    subject_name: str = "Math",
    planning_scope: str = "weekly",
    weeks: int = 1,
    module_title: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    estimated_weeks: int | None = None,
    instructional_days_count: int | None = None,
) -> dict[str, dict]:
    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    grading_period = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    ).json()
    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": subject_name[:4].upper(), "name": subject_name},
    ).json()
    teacher_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": f"{subject_name} Block A",
            "grade_level": "5",
            "student_count": 24,
        },
    ).json()
    attach_subject = client.post(
        "/v1/teacher-assist/class-subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"class_id": teacher_class["id"], "subject_id": subject["id"]},
    )
    assert attach_subject.status_code == 201, attach_subject.text
    subject_code_slug = subject_name.upper().replace(" ", "")[:12]
    standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": subject["id"],
            "standard_type": "TEKS",
            "code": f"5.{subject_code_slug}.{uuid.uuid4().hex[:6].upper()}",
            "description": f"Use {subject_name} planning context.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    )
    assert standard.status_code == 201, standard.text
    standard = standard.json()
    resource = client.post(
        "/v1/teacher-assist/resources/link",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": f"{subject_name} Resource",
            "description": "Mock workflow resource",
            "external_url": f"https://example.com/{subject_name.lower()}-resource",
        },
    ).json()
    guide = client.post(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": f"{subject_name} Guide",
            "description": "Workflow-ready guide",
            "grade_level": "5",
            "subject_id": subject["id"],
            "is_shared": False,
        },
    ).json()
    pacing_items = [
        client.post(
            f"/v1/teacher-assist/pacing-guides/{guide['id']}/items",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "grading_period_id": grading_period["id"],
                "subject_id": subject["id"],
                "week_number": week_index,
                "day_number": 1,
                "title": f"{subject_name} Focus Week {week_index}",
                "instructional_focus": f"Model {subject_name.lower()} thinking week {week_index}",
                "objectives": f"Practice {subject_name.lower()} objective week {week_index}",
                "notes": "Use the mock workflow sequence.",
                "sort_order": week_index,
            },
        ).json()
        for week_index in range(1, weeks + 1)
    ]
    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "planning_scope": planning_scope,
            "school_year_id": school_year["id"],
            "grading_period_id": grading_period["id"],
            "class_id": teacher_class["id"],
            "subject_ids": [subject["id"]],
            "pacing_item_ids": [item["id"] for item in pacing_items],
            "standard_ids": [standard["id"]],
            "title": f"{subject_name} Week 1",
            "module_title": module_title,
            "start_date": start_date,
            "end_date": end_date,
            "estimated_weeks": estimated_weeks,
            "instructional_days_count": instructional_days_count,
            "notes": f"Prepare the weekly {subject_name.lower()} mock plan.",
            "status": "draft",
        },
    ).json()
    attach_resource = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/resources",
        headers={"Authorization": f"Bearer {token}"},
        json={"resource_library_item_id": resource["id"]},
    )
    assert attach_resource.status_code == 200, attach_resource.text
    ready = client.patch(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "ready"},
    )
    assert ready.status_code == 200, ready.text
    return {
        "school_year": school_year,
        "grading_period": grading_period,
        "subject": subject,
        "teacher_class": teacher_class,
        "standard": standard,
        "resource": resource,
        "guide": guide,
        "pacing_item": pacing_items[0],
        "pacing_items": pacing_items,
        "draft": ready.json(),
    }


def _generate_weekly_plan(
    client,
    db_session: Session,
    *,
    token: str,
    subject_name: str = "Math",
    planning_scope: str = "weekly",
    weeks: int = 1,
) -> tuple[dict, dict]:
    context = _create_ready_planning_draft_context(
        client,
        token=token,
        subject_name=subject_name,
        planning_scope=planning_scope,
        weeks=weeks,
        estimated_weeks=weeks,
        instructional_days_count=max(5, weeks * 5),
    )
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    weekly_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    return context, weekly_plan


def test_pacing_guide_and_item_round_trip(client, db_session: Session):
    email = "teacher-planning@example.com"
    token = _register_user(client, email=email, tenant_name="Planning Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    grading_period = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    ).json()
    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "MATH", "name": "Math"},
    ).json()
    standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": subject["id"],
            "standard_type": "TEKS",
            "code": "5.3H",
            "description": "Represent and solve addition and subtraction problems.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    ).json()
    resource = client.post(
        "/v1/teacher-assist/resources/link",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "District Curriculum Link",
            "description": "Shared pacing notes",
            "external_url": "https://example.com/curriculum/math",
        },
    ).json()

    pacing_guide = client.post(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "5th Grade Math Pacing",
            "description": "Quarter one pacing foundation",
            "grade_level": "5",
            "subject_id": subject["id"],
            "is_shared": False,
        },
    )
    assert pacing_guide.status_code == 201, pacing_guide.text
    guide_payload = pacing_guide.json()

    pacing_item = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide_payload['id']}/items",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "grading_period_id": grading_period["id"],
            "subject_id": subject["id"],
            "week_number": 1,
            "day_number": 2,
            "instructional_date": "2026-08-11",
            "title": "Place value review",
            "instructional_focus": "Refresh number sense",
            "objectives": "Review whole-number place value",
            "notes": "Use base ten blocks",
            "sort_order": 1,
        },
    )
    assert pacing_item.status_code == 201, pacing_item.text
    pacing_item_payload = pacing_item.json()

    attach_standard = client.post(
        f"/v1/teacher-assist/pacing-items/{pacing_item_payload['id']}/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={"standard_id": standard["id"]},
    )
    assert attach_standard.status_code == 200, attach_standard.text
    assert attach_standard.json()["standard_ids"] == [standard["id"]]

    attach_resource = client.post(
        f"/v1/teacher-assist/pacing-items/{pacing_item_payload['id']}/resources",
        headers={"Authorization": f"Bearer {token}"},
        json={"resource_library_item_id": resource["id"]},
    )
    assert attach_resource.status_code == 200, attach_resource.text
    assert attach_resource.json()["resource_ids"] == [resource["id"]]

    updated_item = client.put(
        f"/v1/teacher-assist/pacing-items/{pacing_item_payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "grading_period_id": grading_period["id"],
            "subject_id": subject["id"],
            "week_number": 1,
            "day_number": 3,
            "instructional_date": "2026-08-12",
            "title": "Place value practice",
            "instructional_focus": "Independent practice",
            "objectives": "Strengthen place value fluency",
            "notes": "Station rotation",
            "sort_order": 2,
        },
    )
    assert updated_item.status_code == 200, updated_item.text
    assert updated_item.json()["day_number"] == 3
    assert updated_item.json()["standard_ids"] == [standard["id"]]
    assert updated_item.json()["resource_ids"] == [resource["id"]]

    guides = client.get(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert guides.status_code == 200, guides.text
    assert guides.json()[0]["item_count"] == 1

    items = client.get(
        f"/v1/teacher-assist/pacing-guides/{guide_payload['id']}/items",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert items.status_code == 200, items.text
    assert items.json()[0]["standard_ids"] == [standard["id"]]
    assert items.json()[0]["resource_ids"] == [resource["id"]]


def test_resource_upload_and_planning_draft_round_trip(client, db_session: Session):
    email = "teacher-resources@example.com"
    token = _register_user(client, email=email, tenant_name="Resources Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    grading_period = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    ).json()
    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"name": "Science"},
    ).json()
    teacher_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": "5th Grade Homeroom",
            "grade_level": "5",
            "student_count": 23,
        },
    ).json()
    attach_subject = client.post(
        "/v1/teacher-assist/class-subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"class_id": teacher_class["id"], "subject_id": subject["id"]},
    )
    assert attach_subject.status_code == 201, attach_subject.text

    upload = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("curriculum-map.pdf", b"%PDF-1.7 planning context", "application/pdf")},
        data={"title": "Curriculum Map", "description": "Quarter one map"},
    )
    assert upload.status_code == 201, upload.text
    resource_payload = upload.json()
    assert resource_payload["resource_type"] == "pdf"
    assert resource_payload["original_filename"] == "curriculum-map.pdf"
    assert resource_payload["storage_key"]
    assert resource_payload["storage_key"].startswith(
        f"teacher-assist/resources/{resource_payload['tenant_id']}/"
    )
    assert resource_payload["linked_pacing_items_count"] == 0

    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "grading_period_id": grading_period["id"],
            "class_id": teacher_class["id"],
            "subject_id": subject["id"],
            "title": "Week 1 Science Context",
            "notes": "Prepare context only. Do not generate yet.",
            "status": "draft",
        },
    )
    assert draft.status_code == 201, draft.text
    draft_payload = draft.json()
    assert draft_payload["status"] == "draft"

    attach_resource = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}/resources",
        headers={"Authorization": f"Bearer {token}"},
        json={"resource_library_item_id": resource_payload["id"]},
    )
    assert attach_resource.status_code == 200, attach_resource.text
    assert attach_resource.json()["resource_ids"] == [resource_payload["id"]]

    updated = client.put(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "grading_period_id": grading_period["id"],
            "class_id": teacher_class["id"],
            "subject_id": subject["id"],
            "title": "Week 1 Science Context",
            "notes": "Context is ready for a later generation phase.",
            "status": "ready",
        },
    )
    assert updated.status_code == 200, updated.text
    assert updated.json()["status"] == "ready"
    assert updated.json()["resource_ids"] == [resource_payload["id"]]

    resources = client.get(
        "/v1/teacher-assist/resources",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resources.status_code == 200, resources.text
    assert resources.json()[0]["linked_planning_drafts_count"] == 1

    drafts = client.get(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert drafts.status_code == 200, drafts.text
    assert drafts.json()[0]["resource_ids"] == [resource_payload["id"]]
    assert drafts.json()[0]["subject_ids"] == [subject["id"]]


def test_teacher_resource_download_url_streams_private_file(client, db_session: Session):
    email = "teacher-resource-download@example.com"
    token = _register_user(client, email=email, tenant_name="Resource Download Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    upload = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("curriculum-map.pdf", b"%PDF-1.7 private resource", "application/pdf")},
        data={"title": "Curriculum Map"},
    )
    assert upload.status_code == 201, upload.text

    download = client.get(
        f"/v1/teacher-assist/resources/{upload.json()['id']}/download-url",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert download.status_code == 200, download.text
    assert download.json()["url"].startswith("/v1/teacher-assist/storage/local-download?token=")

    streamed = client.get(download.json()["url"])
    assert streamed.status_code == 200, streamed.text
    assert streamed.content == b"%PDF-1.7 private resource"
    assert "attachment;" in streamed.headers["content-disposition"].lower()


def test_planning_draft_context_preview_and_ready_status(client, db_session: Session):
    email = "teacher-preview@example.com"
    token = _register_user(client, email=email, tenant_name="Preview Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    grading_period = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    ).json()
    subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "ELA", "name": "ELA"},
    ).json()
    teacher_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": "ELA Block A",
            "grade_level": "5",
            "student_count": 24,
        },
    ).json()
    client.post(
        "/v1/teacher-assist/class-subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"class_id": teacher_class["id"], "subject_id": subject["id"]},
    )
    standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": subject["id"],
            "standard_type": "TEKS",
            "code": "5.6A",
            "description": "Summarize texts with supporting details.",
            "grade_level": "5",
            "school_year_id": school_year["id"],
        },
    ).json()
    resource = client.post(
        "/v1/teacher-assist/resources/link",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "Novel Study Guide",
            "description": "Planning context resource",
            "external_url": "https://example.com/ela-guide",
        },
    ).json()
    guide = client.post(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "ELA Guide",
            "description": "Unit 1",
            "grade_level": "5",
            "subject_id": subject["id"],
            "is_shared": False,
        },
    ).json()
    pacing_item = client.post(
        f"/v1/teacher-assist/pacing-guides/{guide['id']}/items",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "grading_period_id": grading_period["id"],
            "subject_id": subject["id"],
            "week_number": 1,
            "day_number": 1,
            "title": "Launch close reading",
            "instructional_focus": "Model annotations",
            "objectives": "Students identify key details.",
            "notes": "Shared read aloud",
            "sort_order": 1,
        },
    ).json()

    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "grading_period_id": grading_period["id"],
            "class_id": teacher_class["id"],
            "subject_ids": [subject["id"]],
            "pacing_item_ids": [pacing_item["id"]],
            "standard_ids": [standard["id"]],
            "title": "Week 1 ELA Context",
            "notes": "Prioritize vocabulary scaffolds.",
            "status": "draft",
        },
    )
    assert draft.status_code == 201, draft.text
    draft_payload = draft.json()
    assert draft_payload["subject_ids"] == [subject["id"]]
    assert draft_payload["pacing_item_ids"] == [pacing_item["id"]]
    assert draft_payload["standard_ids"] == [standard["id"]]

    attach_resource = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}/resources",
        headers={"Authorization": f"Bearer {token}"},
        json={"resource_library_item_id": resource["id"]},
    )
    assert attach_resource.status_code == 200, attach_resource.text

    preview = client.get(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}/context-preview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preview.status_code == 200, preview.text
    preview_payload = preview.json()
    assert preview_payload["readiness"]["is_ready"] is True
    assert preview_payload["subjects"][0]["id"] == subject["id"]
    assert preview_payload["pacing_items"][0]["id"] == pacing_item["id"]
    assert preview_payload["standards"][0]["id"] == standard["id"]
    assert preview_payload["resources"][0]["id"] == resource["id"]
    assert preview_payload["teacher_notes"] == "Prioritize vocabulary scaffolds."

    ready = client.patch(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "ready"},
    )
    assert ready.status_code == 200, ready.text
    assert ready.json()["status"] == "ready"

    generation_preview = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft_payload['id']}/generation-preview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert generation_preview.status_code == 200, generation_preview.text
    assert generation_preview.json()["ready"] is True


def test_planning_draft_ready_requires_subject_and_context(client, db_session: Session):
    email = "teacher-ready-rules@example.com"
    token = _register_user(client, email=email, tenant_name="Ready Rules Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    grading_period = client.post(
        "/v1/teacher-assist/grading-periods",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "9 Weeks 1",
            "grading_period_type": "nine_weeks",
            "start_date": "2026-08-10",
            "end_date": "2026-10-10",
            "sort_order": 1,
        },
    ).json()
    teacher_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "name": "Ready Rules Class",
            "grade_level": "5",
            "student_count": 18,
        },
    ).json()

    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": school_year["id"],
            "grading_period_id": grading_period["id"],
            "class_id": teacher_class["id"],
            "title": "Incomplete Draft",
            "status": "draft",
        },
    ).json()

    preview = client.get(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/context-preview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["readiness"]["is_ready"] is False
    assert "Add at least one subject." in preview.json()["readiness"]["missing_items"]
    assert (
        "Add at least one pacing item, teacher note, or attached resource."
        in preview.json()["readiness"]["missing_items"]
    )

    ready = client.patch(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "ready"},
    )
    assert ready.status_code == 400
    assert "Planning draft is not ready" in ready.json()["detail"]


def test_planning_draft_preview_is_tenant_isolated_and_invalid_status_is_rejected(
    client, db_session: Session
):
    first_email = "phase4-a@example.com"
    second_email = "phase4-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Phase4 Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Phase4 Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"school_year_id": school_year["id"], "title": "Tenant A Draft", "status": "draft"},
    ).json()

    foreign_preview = client.get(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/context-preview",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_preview.status_code == 404

    invalid_status = client.patch(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/status",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"status": "archived"},
    )
    assert invalid_status.status_code == 422


def test_cannot_start_weekly_plan_workflow_unless_draft_is_ready(client, db_session: Session):
    email = "teacher-workflow-not-ready@example.com"
    token = _register_user(client, email=email, tenant_name="Workflow Not Ready Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token)
    draft_id = context["draft"]["id"]
    revert = client.patch(
        f"/v1/teacher-assist/planning-drafts/{draft_id}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "draft"},
    )
    assert revert.status_code == 200, revert.text

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft_id}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 400
    assert "marked ready" in response.json()["detail"]


def test_teacher_assist_provider_defaults_to_mock_and_real_provider_is_disabled():
    settings = Settings()
    provider = get_teacher_assist_ai_provider(settings)
    assert provider.provider_name == "mock"
    assert settings.teacher_assist_real_provider_enabled is False
    assert settings.teacher_assist_ai_enable_real_provider is False

    with pytest.raises(RuntimeError, match="disabled"):
        get_teacher_assist_ai_provider(
            Settings(
                teacher_assist_ai_provider="openai",
                teacher_assist_real_provider_enabled=False,
                teacher_assist_ai_enable_real_provider=False,
            )
        )


def test_real_provider_requires_api_key_and_allowed_model():
    with pytest.raises(RuntimeError, match="API key"):
        get_teacher_assist_ai_provider(
            Settings(
                teacher_assist_ai_provider="openai",
                teacher_assist_real_provider_enabled=True,
                teacher_assist_real_provider_model="gpt-4.1-mini",
                teacher_assist_allowed_models="gpt-4.1-mini",
                teacher_assist_openai_api_key=None,
            )
        )

    with pytest.raises(RuntimeError, match="not allowed"):
        get_teacher_assist_ai_provider(
            Settings(
                teacher_assist_ai_provider="openai",
                teacher_assist_real_provider_enabled=True,
                teacher_assist_real_provider_model="gpt-4.1-mini",
                teacher_assist_allowed_models="gpt-4.1",
                teacher_assist_openai_api_key="test-key",
            )
        )


def test_worker_claims_queued_teacher_assist_workflow_and_sets_lease_fields(client, db_session: Session):
    email = "teacher-worker-lease@example.com"
    token = _register_user(client, email=email, tenant_name="Worker Lease Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Lease")

    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    workflow_id = start.json()["id"]

    claimed = claim_next_teacher_assist_workflow(
        db_session,
        settings=Settings(),
        worker_name="teacher-assist-test-worker",
        workflow_id=uuid.UUID(workflow_id),
    )
    assert claimed is not None
    db_session.commit()

    detail = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["status"] == "running"
    assert payload["leased_by_worker"] == "teacher-assist-test-worker"
    assert payload["heartbeat_at"] is not None
    assert payload["lease_expires_at"] is not None
    assert payload["prompt_version"] == INSTRUCTIONAL_PLAN_PROMPT_VERSION


def test_weekly_plan_workflow_creates_persisted_output(client, db_session: Session):
    email = "teacher-workflow-success@example.com"
    token = _register_user(client, email=email, tenant_name="Workflow Success Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Science")
    draft_id = context["draft"]["id"]

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft_id}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202, response.text
    workflow_payload = response.json()
    assert workflow_payload["workflow_type"] == "weekly_plan_generation"
    assert workflow_payload["status"] == "queued"
    _run_teacher_assist_worker(db_session)

    workflows = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflows.status_code == 200, workflows.text
    assert workflows.json()[0]["status"] == "completed"
    assert workflows.json()[0]["output_ref_type"] == "weekly_plan"
    assert workflows.json()[0]["output_ref_id"]
    workflow_id = workflows.json()[0]["id"]
    weekly_plan_id = workflows.json()[0]["output_ref_id"]

    workflow_detail = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflow_detail.status_code == 200, workflow_detail.text
    workflow_detail_payload = workflow_detail.json()
    assert workflow_detail_payload["status"] == "completed"
    assert workflow_detail_payload["progress_percent"] == 100
    assert workflow_detail_payload["heartbeat_at"] is not None
    assert workflow_detail_payload["provider_name"] == "mock"
    assert workflow_detail_payload["provider_model"] == "mock"
    assert workflow_detail_payload["prompt_version"] == INSTRUCTIONAL_PLAN_PROMPT_VERSION
    assert workflow_detail_payload["estimated_cost_cents_total"] == 0
    assert [step["status"] for step in workflow_detail_payload["steps"]] == [
        "completed",
        "completed",
        "completed",
        "completed",
    ]
    assert workflow_detail_payload["usage_events"][0]["provider"] == "mock"
    assert workflow_detail_payload["usage_events"][0]["estimated_cost_cents"] == 0

    weekly_plans = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert weekly_plans.status_code == 200, weekly_plans.text
    assert weekly_plans.json()[0]["id"] == weekly_plan_id
    assert weekly_plans.json()[0]["status"] == "in_progress"
    assert "[MOCK OUTPUT]" in weekly_plans.json()[0]["content_json"]["overview"]
    assert weekly_plans.json()[0]["workflow_id"] == workflow_id
    assert weekly_plans.json()[0]["current_version_number"] == 1
    assert weekly_plans.json()[0]["latest_usage_event"]["provider"] == "mock"
    assert weekly_plans.json()[0]["latest_usage_event"]["estimated_cost_cents"] == 0

    weekly_plan = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert weekly_plan.status_code == 200, weekly_plan.text
    weekly_plan_payload = weekly_plan.json()
    assert weekly_plan_payload["source_context_json"]["draft"]["id"] == draft_id
    assert weekly_plan_payload["content_json"]["subjects"][0]["subject_name"] == "Science"
    assert weekly_plan_payload["content_json"]["metadata"]["is_mock"] is True
    assert weekly_plan_payload["content_json"]["metadata"]["generator"] == "mock"
    assert weekly_plan_payload["content_json"]["metadata"]["version"] == 1
    assert weekly_plan_payload["content_json"]["review_required"] is True
    assert "mock-output" in weekly_plan_payload["content_json"]["quality_flags"]
    assert weekly_plan_payload["content_json"]["teacher_review_checklist"]
    assert weekly_plan_payload["content_json"]["weekly_objectives"]
    assert weekly_plan_payload["content_json"]["subjects"][0]["vocabulary"]
    assert weekly_plan_payload["content_json"]["subjects"][0]["daily_breakdown"][0]["day_label"] == "Monday"
    assert weekly_plan_payload["content_json"]["subjects"][0]["daily_breakdown"][0]["materials_needed"]
    assert weekly_plan_payload["content_json"]["subjects"][0]["differentiation"]["support"]
    versions = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan_id}/versions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert versions.status_code == 200, versions.text
    assert versions.json()[0]["version_number"] == 1


def test_real_provider_runs_only_when_enabled_and_configured_and_adds_review_metadata(
    client, db_session: Session, monkeypatch
):
    email = "teacher-real-provider-success@example.com"
    token = _register_user(client, email=email, tenant_name="Real Provider Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="ELA")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.openai_ai_provider.OpenAITeacherAssistAIProvider.generate_instructional_plan",
        lambda self, _context: _real_provider_result(),
    )

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202, response.text
    _run_teacher_assist_worker(
        db_session,
        settings=Settings(
            teacher_assist_ai_provider="openai",
            teacher_assist_real_provider_enabled=True,
            teacher_assist_real_provider_model="gpt-4.1-mini",
            teacher_assist_allowed_models="gpt-4.1-mini",
            teacher_assist_openai_api_key="test-key",
            teacher_assist_worker_max_retries=0,
        ),
        workflow_id=response.json()["id"],
    )

    workflow = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    assert workflow["status"] == "completed"
    assert workflow["provider_name"] == "openai"
    assert workflow["provider_model"] == "gpt-4.1-mini"
    assert workflow["prompt_version"] == INSTRUCTIONAL_PLAN_PROMPT_VERSION
    assert workflow["estimated_cost_cents_total"] == 1

    weekly_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    assert weekly_plan["latest_usage_event"]["provider"] == "openai"
    assert weekly_plan["content_json"]["metadata"]["is_mock"] is False
    assert weekly_plan["content_json"]["metadata"]["provider_mode"] == "real"
    assert weekly_plan["content_json"]["metadata"]["provider_model"] == "gpt-4.1-mini"
    assert weekly_plan["content_json"]["review_required"] is True
    assert weekly_plan["content_json"]["quality_flags"] == []
    assert "Verify standards alignment." in weekly_plan["content_json"]["teacher_review_checklist"]
    assert "Aligned to" in weekly_plan["content_json"]["standards_alignment_summary"]


def test_weekly_plan_workflow_failure_records_error(client, db_session: Session, monkeypatch):
    email = "teacher-workflow-failure@example.com"
    token = _register_user(client, email=email, tenant_name="Workflow Failure Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Reading")

    class _ExplodingProvider:
        provider_name = "mock"

        def generate_instructional_plan(self, _: dict):
            raise RuntimeError("mock generation exploded")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: _ExplodingProvider(),
    )

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202, response.text
    _run_teacher_assist_worker(db_session)

    workflows = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflows.status_code == 200, workflows.text
    assert workflows.json()[0]["status"] == "failed"
    assert workflows.json()[0]["error_message"] == "mock generation exploded"
    workflow_id = workflows.json()[0]["id"]

    workflow_detail = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflow_detail.status_code == 200, workflow_detail.text
    statuses = [step["status"] for step in workflow_detail.json()["steps"]]
    assert statuses[0] == "completed"
    assert statuses[1] == "failed"
    assert statuses[2:] == ["skipped", "skipped"]


def test_planning_draft_defaults_to_weekly_scope(client, db_session: Session):
    email = "teacher-default-scope@example.com"
    token = _register_user(client, email=email, tenant_name="Default Scope Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token)
    assert context["draft"]["planning_scope"] == "weekly"
    assert context["draft"]["plan_title"] == "Math Week 1"


def test_module_planning_draft_can_be_saved(client, db_session: Session):
    email = "teacher-module-draft@example.com"
    token = _register_user(client, email=email, tenant_name="Module Draft Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(
        client,
        token=token,
        subject_name="Science",
        planning_scope="module",
        weeks=2,
        module_title="Ecosystems Module",
        start_date="2026-08-10",
        end_date="2026-08-21",
        estimated_weeks=2,
        instructional_days_count=10,
    )
    assert context["draft"]["planning_scope"] == "module"
    assert context["draft"]["module_title"] == "Ecosystems Module"
    assert context["draft"]["estimated_weeks"] == 2
    assert context["draft"]["instructional_days_count"] == 10


def test_multi_week_context_preview_includes_duration_and_scope(client, db_session: Session):
    email = "teacher-multiweek-preview@example.com"
    token = _register_user(client, email=email, tenant_name="Multiweek Preview Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(
        client,
        token=token,
        subject_name="History",
        planning_scope="multi_week",
        weeks=2,
        start_date="2026-08-10",
        end_date="2026-08-21",
        estimated_weeks=2,
        instructional_days_count=10,
    )
    preview = client.get(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/context-preview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preview.status_code == 200, preview.text
    payload = preview.json()
    assert payload["draft"]["planning_scope"] == "multi_week"
    assert payload["duration_summary"]["estimated_weeks"] == 2
    assert payload["duration_summary"]["instructional_days_count"] == 10
    assert len(payload["pacing_groups"]) == 2
    assert payload["pacing_groups"][0]["label"] == "Week 1"


def test_module_scope_workflow_creates_weekly_segments(client, db_session: Session):
    email = "teacher-module-workflow@example.com"
    token = _register_user(client, email=email, tenant_name="Module Workflow Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(
        client,
        token=token,
        subject_name="Reading",
        planning_scope="module",
        weeks=2,
        module_title="Comprehension Module",
        start_date="2026-08-10",
        end_date="2026-08-21",
        estimated_weeks=2,
        instructional_days_count=10,
    )
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)

    weekly_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    assert weekly_plan["planning_scope"] == "module"
    assert weekly_plan["content_json"]["planning_scope"] == "module"
    assert weekly_plan["content_json"]["duration"]["estimated_weeks"] == 2
    assert len(weekly_plan["content_json"]["weekly_segments"]) == 2


def test_worker_handles_cancellation_before_artifact_persistence(client, db_session: Session):
    email = "teacher-worker-cancelled@example.com"
    token = _register_user(client, email=email, tenant_name="Worker Cancelled Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Cancel")

    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    workflow_id = start.json()["id"]

    claimed = claim_next_teacher_assist_workflow(
        db_session,
        settings=Settings(),
        worker_name="teacher-assist-test-worker",
        workflow_id=uuid.UUID(workflow_id),
    )
    assert claimed is not None
    db_session.commit()

    cancel = client.patch(
        f"/v1/teacher-assist/workflows/{workflow_id}/cancel",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "cancelled"},
    )
    assert cancel.status_code == 200, cancel.text

    process_claimed_teacher_assist_workflow_with_engine(
        db_session.get_bind(),
        uuid.UUID(workflow_id),
        settings=Settings(),
        worker_name="teacher-assist-test-worker",
    )
    db_session.expire_all()

    workflow_detail = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflow_detail.status_code == 200, workflow_detail.text
    assert workflow_detail.json()["status"] == "cancelled"
    assert workflow_detail.json()["output_ref_id"] is None
    assert (
        client.get(
            "/v1/teacher-assist/weekly-plans",
            headers={"Authorization": f"Bearer {token}"},
        ).json()
        == []
    )


def test_copy_weekly_plan_does_not_call_ai(client, db_session: Session, monkeypatch):
    email = "teacher-plan-copy@example.com"
    token = _register_user(client, email=email, tenant_name="Plan Copy Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Art")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    source_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: pytest.fail("copy endpoint should not call provider"),
    )

    copied = client.post(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/copy",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert copied.status_code == 201, copied.text
    copied_payload = copied.json()
    assert copied_payload["derived_from_plan_id"] == source_plan["id"]
    assert copied_payload["source_plan_id"] == source_plan["id"]
    assert copied_payload["workflow_id"] is None
    assert copied_payload["current_version_number"] == 1
    assert copied_payload["latest_usage_event"] is None
    assert copied_payload["content_json"]["metadata"]["copy_mode"] == "personal_copy"


def test_worker_retries_failed_workflow_until_retry_exhaustion(client, db_session: Session, monkeypatch):
    email = "teacher-worker-retry@example.com"
    token = _register_user(client, email=email, tenant_name="Worker Retry Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Retry")
    attempts = {"count": 0}

    class _ExplodingProvider:
        provider_name = "mock"

        def generate_instructional_plan(self, _: dict):
            attempts["count"] += 1
            raise RuntimeError("retryable worker failure")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: _ExplodingProvider(),
    )

    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    workflow_id = start.json()["id"]
    retry_settings = Settings(teacher_assist_worker_max_retries=1)

    _run_teacher_assist_worker(db_session, settings=retry_settings, workflow_id=workflow_id)
    first_attempt = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert first_attempt.status_code == 200, first_attempt.text
    first_payload = first_attempt.json()
    assert first_payload["status"] == "queued"
    assert first_payload["retry_count"] == 1
    assert first_payload["last_error_code"] == "execution_failed"

    _run_teacher_assist_worker(db_session, settings=retry_settings, workflow_id=workflow_id)
    final_attempt = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert final_attempt.status_code == 200, final_attempt.text
    final_payload = final_attempt.json()
    assert final_payload["status"] == "failed"
    assert final_payload["retry_count"] == 2
    assert final_payload["error_message"] == "retryable worker failure"
    assert attempts["count"] == 2


def test_worker_blocks_real_provider_by_default_and_enforces_cost_limit(
    client, db_session: Session, monkeypatch
):
    email = "teacher-worker-guardrails@example.com"
    token = _register_user(client, email=email, tenant_name="Worker Guardrails Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Guardrails")

    openai_start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert openai_start.status_code == 202, openai_start.text
    openai_workflow_id = openai_start.json()["id"]
    _run_teacher_assist_worker(
        db_session,
        settings=Settings(
            teacher_assist_ai_provider="openai",
            teacher_assist_real_provider_enabled=False,
            teacher_assist_worker_max_retries=0,
        ),
        workflow_id=openai_workflow_id,
    )
    openai_workflow = client.get(
        f"/v1/teacher-assist/workflows/{openai_workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert openai_workflow.status_code == 200, openai_workflow.text
    assert openai_workflow.json()["status"] == "failed"
    assert "disabled" in openai_workflow.json()["error_message"]

    cost_start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert cost_start.status_code == 202, cost_start.text
    cost_workflow_id = cost_start.json()["id"]
    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    membership = db_session.scalar(
        select(TenantMembership).where(TenantMembership.user_id == user.id).limit(1)
    )
    assert membership is not None
    db_session.add(
        TeacherAssistAIUsageEvent(
            tenant_id=membership.tenant_id,
            user_id=user.id,
            workflow_id=None,
            provider="mock",
            model="mock",
            feature="weekly_plan_generation",
            input_tokens=0,
            output_tokens=0,
            estimated_cost_cents=5,
            metadata_json={"prompt_version": INSTRUCTIONAL_PLAN_PROMPT_VERSION},
            created_at=datetime.now(UTC),
        )
    )
    db_session.commit()

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: pytest.fail("cost limit should block provider execution"),
    )
    _run_teacher_assist_worker(
        db_session,
        settings=Settings(
            teacher_assist_ai_daily_cost_limit_cents=5,
            teacher_assist_worker_max_retries=0,
        ),
        workflow_id=cost_workflow_id,
    )
    cost_workflow = client.get(
        f"/v1/teacher-assist/workflows/{cost_workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert cost_workflow.status_code == 200, cost_workflow.text
    assert cost_workflow.json()["status"] == "failed"
    assert "daily cost limit" in cost_workflow.json()["error_message"]


def test_malformed_provider_output_fails_gracefully(client, db_session: Session, monkeypatch):
    email = "teacher-malformed-provider@example.com"
    token = _register_user(client, email=email, tenant_name="Malformed Provider Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Music")

    class _MalformedProvider:
        provider_name = "mock"

        def generate_instructional_plan(self, _: dict):
            from oziebot_api.services.teacher_assist.ai_provider import TeacherAssistAIProviderResult

            return TeacherAssistAIProviderResult(
                content_json={"overview": "missing required structure"},
                provider="mock",
                model="mock",
                input_tokens=0,
                output_tokens=0,
                estimated_cost_cents=0,
                metadata_json={"is_mock": True},
            )

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: _MalformedProvider(),
    )

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202, response.text
    _run_teacher_assist_worker(db_session)

    workflow = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    assert workflow["status"] == "failed"
    assert "missing required fields" in workflow["error_message"]


def test_invalid_real_provider_output_fails_without_creating_artifact(
    client, db_session: Session, monkeypatch
):
    email = "teacher-real-provider-invalid@example.com"
    token = _register_user(client, email=email, tenant_name="Real Provider Invalid Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Science")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.openai_ai_provider.OpenAITeacherAssistAIProvider.generate_instructional_plan",
        lambda self, _context: TeacherAssistAIProviderResult(
            content_json={
                "planning_scope": "module",
                "plan_title": "Bad Provider Output",
                "module_title": None,
                "duration": {},
                "overview": "bad",
                "instructional_arc": [],
                "weekly_segments": [],
                "standards_progression": [],
                "vocabulary": [],
                "materials_needed": [],
                "differentiation": {},
                "assessment_checkpoints": [],
                "resources_used": [],
                "teacher_notes_used": "",
                "review_notes": "",
            },
            provider="openai",
            model="gpt-4.1-mini",
            input_tokens=100,
            output_tokens=100,
            estimated_cost_cents=1,
            metadata_json={
                "is_mock": False,
                "provider_mode": "real",
                "prompt_version": INSTRUCTIONAL_PLAN_PROMPT_VERSION,
            },
        ),
    )

    response = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 202, response.text
    workflow_id = response.json()["id"]
    _run_teacher_assist_worker(
        db_session,
        settings=Settings(
            teacher_assist_ai_provider="openai",
            teacher_assist_real_provider_enabled=True,
            teacher_assist_real_provider_model="gpt-4.1-mini",
            teacher_assist_allowed_models="gpt-4.1-mini",
            teacher_assist_openai_api_key="test-key",
            teacher_assist_worker_max_retries=0,
        ),
        workflow_id=workflow_id,
    )

    workflow = client.get(
        f"/v1/teacher-assist/workflows/{workflow_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflow.status_code == 200, workflow.text
    assert workflow.json()["status"] == "failed"
    assert workflow.json()["last_error_code"] == "execution_failed"
    assert "instructional_arc" in workflow.json()["error_message"]
    assert client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json() == []


def test_library_listing_only_returns_tenant_visible_plans_and_private_plans_stay_owner_only(
    client, db_session: Session
):
    owner_email = "teacher-library-owner@example.com"
    teammate_email = "teacher-library-teammate@example.com"
    outsider_email = "teacher-library-outsider@example.com"
    owner_token = _register_user(client, email=owner_email, tenant_name="Library Tenant")
    teammate_token = _register_user(client, email=teammate_email, tenant_name="Teammate Tenant")
    outsider_token = _register_user(client, email=outsider_email, tenant_name="Outsider Tenant")
    _grant_teacher_assist_access(db_session, email=owner_email)
    _grant_teacher_assist_access(db_session, email=outsider_email)
    _share_teacher_assist_tenant(db_session, owner_email=owner_email, member_email=teammate_email)

    owner_context = _create_ready_planning_draft_context(client, token=owner_token, subject_name="Library")
    workflow = client.post(
        f"/v1/teacher-assist/planning-drafts/{owner_context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {owner_token}"},
    )
    assert workflow.status_code == 202, workflow.text
    _run_teacher_assist_worker(db_session)
    owner_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {owner_token}"},
    ).json()[0]

    teammate_private = client.get(
        "/v1/teacher-assist/instructional-plans/library",
        headers={"Authorization": f"Bearer {teammate_token}"},
    )
    assert teammate_private.status_code == 200, teammate_private.text
    assert teammate_private.json() == []

    sharing = client.patch(
        f"/v1/teacher-assist/weekly-plans/{owner_plan['id']}/sharing",
        headers={"Authorization": f"Bearer {owner_token}"},
        json={"visibility_scope": "shared", "reuse_status": "reusable"},
    )
    assert sharing.status_code == 200, sharing.text

    owner_library = client.get(
        "/v1/teacher-assist/instructional-plans/library",
        headers={"Authorization": f"Bearer {owner_token}"},
    )
    assert owner_library.status_code == 200, owner_library.text
    assert owner_library.json()[0]["id"] == owner_plan["id"]

    teammate_library = client.get(
        "/v1/teacher-assist/instructional-plans/library",
        headers={"Authorization": f"Bearer {teammate_token}"},
    )
    assert teammate_library.status_code == 200, teammate_library.text
    assert teammate_library.json()[0]["id"] == owner_plan["id"]

    outsider_library = client.get(
        "/v1/teacher-assist/instructional-plans/library",
        headers={"Authorization": f"Bearer {outsider_token}"},
    )
    assert outsider_library.status_code == 200, outsider_library.text
    assert outsider_library.json() == []


def test_owner_can_update_sharing_fields_and_non_owner_cannot(client, db_session: Session):
    owner_email = "teacher-sharing-owner@example.com"
    teammate_email = "teacher-sharing-teammate@example.com"
    owner_token = _register_user(client, email=owner_email, tenant_name="Sharing Tenant")
    teammate_token = _register_user(client, email=teammate_email, tenant_name="Sharing Member Tenant")
    _grant_teacher_assist_access(db_session, email=owner_email)
    _share_teacher_assist_tenant(db_session, owner_email=owner_email, member_email=teammate_email)

    context = _create_ready_planning_draft_context(client, token=owner_token, subject_name="Sharing")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {owner_token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    plan = client.get("/v1/teacher-assist/weekly-plans", headers={"Authorization": f"Bearer {owner_token}"}).json()[0]

    owner_update = client.patch(
        f"/v1/teacher-assist/weekly-plans/{plan['id']}/sharing",
        headers={"Authorization": f"Bearer {owner_token}"},
        json={"is_template": True, "visibility_scope": "shared", "reuse_status": "reusable"},
    )
    assert owner_update.status_code == 200, owner_update.text
    assert owner_update.json()["is_template"] is True
    assert owner_update.json()["visibility_scope"] == "shared"
    assert owner_update.json()["reuse_status"] == "reusable"

    non_owner_update = client.patch(
        f"/v1/teacher-assist/weekly-plans/{plan['id']}/sharing",
        headers={"Authorization": f"Bearer {teammate_token}"},
        json={"visibility_scope": "district"},
    )
    assert non_owner_update.status_code == 403


def test_copy_endpoint_preserves_lineage_and_can_patch_target_year(client, db_session: Session):
    email = "teacher-copy-lineage@example.com"
    token = _register_user(client, email=email, tenant_name="Copy Lineage Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Lineage")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    source_plan = client.get("/v1/teacher-assist/weekly-plans", headers={"Authorization": f"Bearer {token}"}).json()[0]

    next_school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-10",
            "end_date": "2028-05-28",
            "is_active": False,
        },
    ).json()

    copied = client.post(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/copy",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "target_school_year_id": next_school_year["id"],
            "title_override": "Lineage Personalized Copy",
            "copy_mode": "rollover_copy",
        },
    )
    assert copied.status_code == 201, copied.text
    payload = copied.json()
    assert payload["title"] == "Lineage Personalized Copy"
    assert payload["source_plan_id"] == source_plan["id"]
    assert payload["derived_from_plan_id"] == source_plan["id"]
    assert payload["school_year_origin_id"] == next_school_year["id"]
    assert payload["source_context_json"]["draft"]["school_year_id"] == next_school_year["id"]
    assert payload["latest_usage_event"] is None


def test_rollover_candidates_find_prior_year_reusable_plans(client, db_session: Session):
    email = "teacher-rollover-candidates@example.com"
    token = _register_user(client, email=email, tenant_name="Rollover Candidates Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Candidates")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    source_plan = client.get("/v1/teacher-assist/weekly-plans", headers={"Authorization": f"Bearer {token}"}).json()[0]
    sharing = client.patch(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/sharing",
        headers={"Authorization": f"Bearer {token}"},
        json={"is_template": True, "visibility_scope": "shared", "reuse_status": "reusable"},
    )
    assert sharing.status_code == 200, sharing.text

    target_school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-10",
            "end_date": "2028-05-28",
            "is_active": False,
        },
    ).json()

    candidates = client.get(
        "/v1/teacher-assist/curriculum-rollover/candidates",
        headers={"Authorization": f"Bearer {token}"},
        params={
            "source_school_year_id": context["school_year"]["id"],
            "target_school_year_id": target_school_year["id"],
            "reuse_status": "reusable",
        },
    )
    assert candidates.status_code == 200, candidates.text
    payload = candidates.json()
    assert payload["items"][0]["id"] == source_plan["id"]
    assert payload["items"][0]["already_copied_to_target"] is False
    assert payload["summary_counts_by_planning_scope"]["weekly"] == 1
    assert "Candidates" in payload["subjects_represented"]


def test_rollover_copy_creates_target_year_copies_and_duplicate_rollover_is_warned(
    client, db_session: Session
):
    email = "teacher-rollover-copy@example.com"
    token = _register_user(client, email=email, tenant_name="Rollover Copy Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Rollover")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    source_plan = client.get("/v1/teacher-assist/weekly-plans", headers={"Authorization": f"Bearer {token}"}).json()[0]
    sharing = client.patch(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/sharing",
        headers={"Authorization": f"Bearer {token}"},
        json={"visibility_scope": "shared", "reuse_status": "reusable"},
    )
    assert sharing.status_code == 200, sharing.text

    target_school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-10",
            "end_date": "2028-05-28",
            "is_active": False,
        },
    ).json()

    copied = client.post(
        "/v1/teacher-assist/curriculum-rollover/copy",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "source_school_year_id": context["school_year"]["id"],
            "target_school_year_id": target_school_year["id"],
            "plan_ids": [source_plan["id"]],
            "copy_mode": "rollover_copy",
            "preserve_titles": True,
            "title_suffix": "2027-2028",
        },
    )
    assert copied.status_code == 200, copied.text
    payload = copied.json()
    assert len(payload["copied_plans"]) == 1
    assert payload["copied_plans"][0]["school_year_origin_id"] == target_school_year["id"]
    assert payload["copied_plans"][0]["source_plan_id"] == source_plan["id"]
    assert payload["warnings"] == []

    duplicate = client.post(
        "/v1/teacher-assist/curriculum-rollover/copy",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "source_school_year_id": context["school_year"]["id"],
            "target_school_year_id": target_school_year["id"],
            "plan_ids": [source_plan["id"]],
            "copy_mode": "rollover_copy",
            "preserve_titles": True,
        },
    )
    assert duplicate.status_code == 200, duplicate.text
    assert "already has a rollover copy" in duplicate.json()["warnings"][0]


def test_cross_tenant_rollover_access_is_blocked(client, db_session: Session):
    owner_email = "teacher-rollover-owner@example.com"
    outsider_email = "teacher-rollover-outsider@example.com"
    owner_token = _register_user(client, email=owner_email, tenant_name="Owner Rollover Tenant")
    outsider_token = _register_user(client, email=outsider_email, tenant_name="Outsider Rollover Tenant")
    _grant_teacher_assist_access(db_session, email=owner_email)
    _grant_teacher_assist_access(db_session, email=outsider_email)

    context = _create_ready_planning_draft_context(client, token=owner_token, subject_name="Blocked")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {owner_token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)
    source_plan = client.get("/v1/teacher-assist/weekly-plans", headers={"Authorization": f"Bearer {owner_token}"}).json()[0]
    sharing = client.patch(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/sharing",
        headers={"Authorization": f"Bearer {owner_token}"},
        json={"visibility_scope": "shared", "reuse_status": "reusable"},
    )
    assert sharing.status_code == 200, sharing.text

    outsider_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {outsider_token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-10",
            "end_date": "2028-05-28",
            "is_active": False,
        },
    ).json()

    candidates = client.get(
        "/v1/teacher-assist/curriculum-rollover/candidates",
        headers={"Authorization": f"Bearer {outsider_token}"},
        params={
            "source_school_year_id": context["school_year"]["id"],
            "target_school_year_id": outsider_year["id"],
        },
    )
    assert candidates.status_code == 200, candidates.text
    assert candidates.json()["items"] == []

    copied = client.post(
        "/v1/teacher-assist/curriculum-rollover/copy",
        headers={"Authorization": f"Bearer {outsider_token}"},
        json={
            "source_school_year_id": context["school_year"]["id"],
            "target_school_year_id": outsider_year["id"],
            "plan_ids": [source_plan["id"]],
            "copy_mode": "rollover_copy",
            "preserve_titles": True,
        },
    )
    assert copied.status_code == 200, copied.text
    assert copied.json()["copied_plans"] == []
    assert "not available for rollover" in copied.json()["warnings"][0]


def test_workflow_and_weekly_plan_retrieval_are_tenant_isolated(client, db_session: Session):
    first_email = "workflow-phase5-a@example.com"
    second_email = "workflow-phase5-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Workflow Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Workflow Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    first_context = _create_ready_planning_draft_context(client, token=first_token, subject_name="History")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{first_context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {first_token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)

    first_workflow = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {first_token}"},
    ).json()[0]
    first_weekly_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {first_token}"},
    ).json()[0]

    foreign_workflow = client.get(
        f"/v1/teacher-assist/workflows/{first_workflow['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_workflow.status_code == 404

    foreign_weekly_plan = client.get(
        f"/v1/teacher-assist/weekly-plans/{first_weekly_plan['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_weekly_plan.status_code == 404
    foreign_versions = client.get(
        f"/v1/teacher-assist/weekly-plans/{first_weekly_plan['id']}/versions",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_versions.status_code == 404
    foreign_update = client.put(
        f"/v1/teacher-assist/weekly-plans/{first_weekly_plan['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"title": "Blocked foreign edit", "change_reason": "Should fail"},
    )
    assert foreign_update.status_code == 404

    assert client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {second_token}"},
    ).json() == []
    assert client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {second_token}"},
    ).json() == []


def test_cancel_workflow_rejects_completed_workflow(client, db_session: Session):
    email = "teacher-workflow-cancel@example.com"
    token = _register_user(client, email=email, tenant_name="Workflow Cancel Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Art")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)

    workflow_id = client.get(
        "/v1/teacher-assist/workflows",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]["id"]
    cancel = client.patch(
        f"/v1/teacher-assist/workflows/{workflow_id}/cancel",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "cancelled"},
    )
    assert cancel.status_code == 400
    assert "can no longer be cancelled" in cancel.json()["detail"]


def test_weekly_plan_edit_creates_new_version_and_completed_status(client, db_session: Session):
    email = "teacher-weekly-plan-edit@example.com"
    token = _register_user(client, email=email, tenant_name="Weekly Plan Edit Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="ELA")
    start = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert start.status_code == 202, start.text
    _run_teacher_assist_worker(db_session)

    weekly_plan = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    ).json()[0]
    updated_content = weekly_plan["content_json"]
    updated_content["overview"] = "[MOCK OUTPUT] Teacher-reviewed weekly overview."
    updated_content["review_notes"] = "Tighten the day two transition and mark the plan ready."

    update = client.put(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "Teacher-reviewed ELA Weekly Plan",
            "status": "completed",
            "content_json": updated_content,
            "change_reason": "Teacher review pass",
        },
    )
    assert update.status_code == 200, update.text
    updated_payload = update.json()
    assert updated_payload["title"] == "Teacher-reviewed ELA Weekly Plan"
    assert updated_payload["status"] == "completed"
    assert updated_payload["content_json"]["review_notes"] == updated_content["review_notes"]
    assert updated_payload["content_json"]["metadata"]["version"] == 2
    assert updated_payload["current_version_number"] == 2

    versions = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert versions.status_code == 200, versions.text
    version_payload = versions.json()
    assert [version["version_number"] for version in version_payload] == [2, 1]
    assert version_payload[0]["change_reason"] == "Teacher review pass"

    version_detail = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/versions/{version_payload[0]['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert version_detail.status_code == 200, version_detail.text
    assert version_detail.json()["content_json"]["review_notes"] == updated_content["review_notes"]


def test_regenerate_overview_only_creates_new_version_and_preserves_original(client, db_session: Session):
    email = "teacher-regen-overview@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Overview Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Overview")
    original_overview = weekly_plan["content_json"]["overview"]
    original_vocabulary = list(weekly_plan["content_json"]["vocabulary"])

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "section_key": "overview",
            "teacher_instruction": "Make it more teacher-facing and practical.",
            "provider_mode": "mock",
            "preserve_existing_context": True,
        },
    )
    assert regenerated.status_code == 200, regenerated.text
    payload = regenerated.json()
    assert payload["status"] == "in_progress"
    assert payload["current_version_number"] == 2
    assert payload["content_json"]["overview"] != original_overview
    assert payload["content_json"]["vocabulary"] == original_vocabulary
    assert payload["content_json"]["review_required"] is True
    assert payload["latest_usage_event"]["feature"] == "weekly_plan_section_regeneration"
    assert payload["latest_usage_event"]["estimated_cost_cents"] == 0
    assert payload["latest_usage_event"]["metadata_json"]["section_key"] == "overview"

    versions = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert versions.status_code == 200, versions.text
    version_payload = versions.json()
    assert [version["version_number"] for version in version_payload[:2]] == [2, 1]
    assert version_payload[0]["change_reason"] == "Regenerated overview at overview"
    assert version_payload[1]["content_json"]["overview"] == original_overview


def test_regenerate_specific_weekly_segment_only_updates_target_segment(client, db_session: Session):
    email = "teacher-regen-segment@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Segment Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, weekly_plan = _generate_weekly_plan(
        client,
        db_session,
        token=token,
        subject_name="Segments",
        planning_scope="module",
        weeks=2,
    )
    original_segments = weekly_plan["content_json"]["weekly_segments"]

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "section_key": "weekly_segments",
            "section_path": "weekly_segments.0",
            "teacher_instruction": "Tighten the focus for the first segment.",
            "provider_mode": "mock",
        },
    )
    assert regenerated.status_code == 200, regenerated.text
    payload = regenerated.json()
    assert payload["current_version_number"] == 2
    assert payload["content_json"]["weekly_segments"][0]["focus"] != original_segments[0]["focus"]
    assert payload["content_json"]["weekly_segments"][1] == original_segments[1]
    assert payload["latest_usage_event"]["metadata_json"]["section_path"] == "weekly_segments.0"


def test_regenerate_invalid_section_key_is_rejected(client, db_session: Session):
    email = "teacher-regen-invalid@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Invalid Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Invalid")

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={"section_key": "not_a_section"},
    )
    assert regenerated.status_code == 422, regenerated.text


def test_regenerate_malformed_provider_output_fails_safely(client, db_session: Session, monkeypatch):
    email = "teacher-regen-malformed@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Malformed Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Malformed")
    original_overview = weekly_plan["content_json"]["overview"]

    class _MalformedProvider:
        provider_name = "mock"

        def regenerate_instructional_plan_section(self, **_kwargs):
            return TeacherAssistAIProviderResult(
                content_json={"bad": "payload"},
                provider="mock",
                model="mock",
                input_tokens=0,
                output_tokens=0,
                estimated_cost_cents=0,
                metadata_json={"is_mock": True, "provider_mode": "mock", "prompt_version": "instructional-plan-section-v1"},
            )

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.workflow_service.get_teacher_assist_ai_provider",
        lambda _settings, **_kwargs: _MalformedProvider(),
    )

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={"section_key": "overview", "provider_mode": "mock"},
    )
    assert regenerated.status_code == 400, regenerated.text
    assert "section_content" in regenerated.json()["detail"]

    unchanged = client.get(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert unchanged.status_code == 200, unchanged.text
    assert unchanged.json()["content_json"]["overview"] == original_overview
    assert unchanged.json()["current_version_number"] == 1


def test_regenerate_copied_plan_records_zero_cost_usage_event_without_workflow_id(
    client, db_session: Session
):
    email = "teacher-regen-copy@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Copy Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, source_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Copy Regen")
    copied = client.post(
        f"/v1/teacher-assist/weekly-plans/{source_plan['id']}/copy",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert copied.status_code == 201, copied.text
    copied_payload = copied.json()
    assert copied_payload["workflow_id"] is None

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{copied_payload['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={"section_key": "review_notes", "provider_mode": "mock"},
    )
    assert regenerated.status_code == 200, regenerated.text
    regenerated_payload = regenerated.json()
    assert regenerated_payload["workflow_id"] is None
    assert regenerated_payload["latest_usage_event"]["feature"] == "weekly_plan_section_regeneration"
    assert regenerated_payload["latest_usage_event"]["estimated_cost_cents"] == 0
    assert regenerated_payload["latest_usage_event"]["metadata_json"]["weekly_plan_id"] == copied_payload["id"]


def test_regenerate_real_provider_is_blocked_by_default(client, db_session: Session):
    email = "teacher-regen-real-blocked@example.com"
    token = _register_user(client, email=email, tenant_name="Regenerate Real Blocked Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Blocked Real")

    regenerated = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={"section_key": "overview", "provider_mode": "real"},
    )
    assert regenerated.status_code == 400, regenerated.text
    assert "disabled" in regenerated.json()["detail"]


def test_phase3_tenant_isolation_for_resources_guides_and_drafts(client, db_session: Session):
    first_email = "phase3-a@example.com"
    second_email = "phase3-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Phase3 Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Phase3 Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "title": "2026-2027",
            "start_date": "2026-08-10",
            "end_date": "2027-05-28",
            "is_active": True,
        },
    ).json()
    resource = client.post(
        "/v1/teacher-assist/resources/link",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "title": "Private Curriculum",
            "description": "Tenant A only",
            "external_url": "https://example.com/private-curriculum",
        },
    ).json()
    guide = client.post(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "Tenant A Guide",
            "description": "Private guide",
            "is_shared": False,
        },
    ).json()
    draft = client.post(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"school_year_id": school_year["id"], "title": "Tenant A Draft", "status": "draft"},
    ).json()

    second_resources = client.get(
        "/v1/teacher-assist/resources",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_resources.status_code == 200, second_resources.text
    assert second_resources.json() == []

    second_guides = client.get(
        "/v1/teacher-assist/legacy/pacing-guides",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_guides.status_code == 200, second_guides.text
    assert second_guides.json() == []

    second_drafts = client.get(
        "/v1/teacher-assist/planning-drafts",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_drafts.status_code == 200, second_drafts.text
    assert second_drafts.json() == []

    foreign_resource = client.get(
        f"/v1/teacher-assist/resources/{resource['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_resource.status_code == 404

    foreign_update = client.put(
        f"/v1/teacher-assist/legacy/pacing-guides/{guide['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
        json={
            "school_year_id": school_year["id"],
            "title": "Nope",
            "description": "Should not work",
            "is_shared": False,
        },
    )
    assert foreign_update.status_code == 404

    foreign_attach = client.post(
        f"/v1/teacher-assist/planning-drafts/{draft['id']}/resources",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"resource_library_item_id": resource["id"]},
    )
    assert foreign_attach.status_code == 404


def test_assignment_create_and_read_round_trip(client, db_session: Session):
    email = "teacher-assignments@example.com"
    token = _register_user(client, email=email, tenant_name="Assignments Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Writing")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Evidence-Based Writing Response",
            "description": "Students draft a short constructed response.",
            "assignment_type": "writing",
            "due_date": "2026-08-14",
            "status": "draft",
            "instructions": "Use anonymous STUDENT numbers only.",
            "standard_ids": [context["standard"]["id"]],
            "resource_ids": [context["resource"]["id"]],
        },
    )
    assert assignment.status_code == 201, assignment.text
    payload = assignment.json()
    assert payload["assignment_type"] == "writing"
    assert payload["status"] == "draft"
    assert payload["standard_ids"] == [context["standard"]["id"]]
    assert payload["resource_ids"] == [context["resource"]["id"]]

    fetched = client.get(
        f"/v1/teacher-assist/assignments/{payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert fetched.status_code == 200, fetched.text
    assert fetched.json()["title"] == "Evidence-Based Writing Response"


def test_assignment_list_filters(client, db_session: Session):
    email = "teacher-assignment-filters@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Filters Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Reading")
    first = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Reading Exit Ticket",
            "assignment_type": "exit_ticket",
            "status": "ready",
        },
    )
    assert first.status_code == 201, first.text
    second = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Vocabulary Homework",
            "assignment_type": "homework",
            "status": "draft",
        },
    )
    assert second.status_code == 201, second.text

    filtered = client.get(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        params={"status": "ready", "assignment_type": "exit_ticket", "q": "exit"},
    )
    assert filtered.status_code == 200, filtered.text
    items = filtered.json()
    assert len(items) == 1
    assert items[0]["title"] == "Reading Exit Ticket"


def test_assignment_update_replaces_fields_and_links(client, db_session: Session):
    email = "teacher-assignment-update@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Update Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Science")
    extra_standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": context["subject"]["id"],
            "standard_type": "TEKS",
            "code": "5.2B",
            "description": "Plan and implement classroom investigations.",
            "grade_level": "5",
            "school_year_id": context["school_year"]["id"],
        },
    )
    assert extra_standard.status_code == 201, extra_standard.text

    created = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Science Draft",
            "assignment_type": "other",
            "status": "draft",
            "standard_ids": [context["standard"]["id"]],
        },
    )
    assert created.status_code == 201, created.text
    assignment_id = created.json()["id"]

    updated = client.put(
        f"/v1/teacher-assist/assignments/{assignment_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Science Investigation Write-Up",
            "description": "Students explain the investigation results.",
            "assignment_type": "project",
            "due_date": "2026-08-15",
            "status": "ready",
            "instructions": "Revise with teacher feedback before finalizing.",
            "standard_ids": [extra_standard.json()["id"]],
            "resource_ids": [context["resource"]["id"]],
        },
    )
    assert updated.status_code == 200, updated.text
    payload = updated.json()
    assert payload["title"] == "Science Investigation Write-Up"
    assert payload["assignment_type"] == "project"
    assert payload["status"] == "ready"
    assert payload["standard_ids"] == [extra_standard.json()["id"]]
    assert payload["resource_ids"] == [context["resource"]["id"]]


def test_assignment_status_transition_requires_lifecycle_order(client, db_session: Session):
    email = "teacher-assignment-status@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Status Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Math")
    created = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Math Spiral Review",
            "assignment_type": "homework",
            "status": "draft",
        },
    )
    assert created.status_code == 201, created.text
    assignment_id = created.json()["id"]

    invalid = client.patch(
        f"/v1/teacher-assist/assignments/{assignment_id}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "assigned"},
    )
    assert invalid.status_code == 400, invalid.text
    assert "cannot transition" in invalid.json()["detail"]

    ready = client.patch(
        f"/v1/teacher-assist/assignments/{assignment_id}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "ready"},
    )
    assert ready.status_code == 200, ready.text
    assert ready.json()["status"] == "ready"


def test_assignment_attach_standard_is_idempotent(client, db_session: Session):
    email = "teacher-assignment-standards@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Standards Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="ELA")
    second_standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": context["subject"]["id"],
            "standard_type": "TEKS",
            "code": "5.7C",
            "description": "Compose short responses using text evidence.",
            "grade_level": "5",
            "school_year_id": context["school_year"]["id"],
        },
    )
    assert second_standard.status_code == 201, second_standard.text

    created = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "ELA Response Draft",
            "assignment_type": "short_answer",
            "status": "draft",
            "standard_ids": [context["standard"]["id"]],
        },
    )
    assert created.status_code == 201, created.text
    assignment_id = created.json()["id"]

    attach = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={"standard_id": second_standard.json()["id"]},
    )
    assert attach.status_code == 200, attach.text
    assert attach.json()["standard_ids"] == [context["standard"]["id"], second_standard.json()["id"]]

    reattach = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={"standard_id": second_standard.json()["id"]},
    )
    assert reattach.status_code == 200, reattach.text
    assert reattach.json()["standard_ids"] == [context["standard"]["id"], second_standard.json()["id"]]


def test_assignment_tenant_isolation(client, db_session: Session):
    first_email = "teacher-assignment-a@example.com"
    second_email = "teacher-assignment-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Assignment Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Assignment Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="History")
    created = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "History Reflection",
            "assignment_type": "reading_response",
            "status": "draft",
        },
    )
    assert created.status_code == 201, created.text
    assignment_id = created.json()["id"]

    foreign_get = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_get.status_code == 404

    foreign_update = client.patch(
        f"/v1/teacher-assist/assignments/{assignment_id}/status",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"status": "ready"},
    )
    assert foreign_update.status_code == 404


def test_assignment_rejects_invalid_class_subject_school_year_relationships(client, db_session: Session):
    email = "teacher-assignment-invalid@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Invalid Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Social Studies")
    extra_school_year = client.post(
        "/v1/teacher-assist/school-years",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "2027-2028",
            "start_date": "2027-08-09",
            "end_date": "2028-05-26",
            "is_active": False,
        },
    )
    assert extra_school_year.status_code == 201, extra_school_year.text
    unrelated_subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "ART", "name": "Art"},
    )
    assert unrelated_subject.status_code == 201, unrelated_subject.text

    mismatched_school_year = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": extra_school_year.json()["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Bad School Year",
            "assignment_type": "other",
            "status": "draft",
        },
    )
    assert mismatched_school_year.status_code == 400, mismatched_school_year.text
    assert "school year" in mismatched_school_year.json()["detail"].lower()

    mismatched_subject = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": unrelated_subject.json()["id"],
            "title": "Bad Subject",
            "assignment_type": "other",
            "status": "draft",
        },
    )
    assert mismatched_subject.status_code == 400, mismatched_subject.text
    assert "selected subject" in mismatched_subject.json()["detail"].lower()


def test_weekly_plan_assignment_starter_copies_context_without_ai_usage(client, db_session: Session):
    email = "teacher-assignment-starter@example.com"
    token = _register_user(client, email=email, tenant_name="Assignment Starter Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Starter")
    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    starter = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={"assignment_type": "homework"},
    )
    assert starter.status_code == 201, starter.text
    payload = starter.json()
    assert payload["status"] == "draft"
    assert payload["source_plan_id"] == weekly_plan["id"]
    assert payload["school_year_id"] == context["school_year"]["id"]
    assert payload["class_id"] == context["teacher_class"]["id"]
    assert payload["subject_id"] == context["subject"]["id"]
    assert payload["standard_ids"] == [context["standard"]["id"]]
    assert payload["resource_ids"] == [context["resource"]["id"]]

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage_count == before_usage_count


def test_assignment_print_packet_create_and_page_count(client, db_session: Session):
    email = "teacher-print-packet@example.com"
    token = _register_user(client, email=email, tenant_name="Print Packet Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Packet")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Packet Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    created = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 2, "template_type": "lined_writing_page", "output_format": "html"},
    )
    assert created.status_code == 201, created.text
    packet = created.json()
    assert packet["student_count"] == context["teacher_class"]["student_count"]
    assert packet["total_page_count"] == context["teacher_class"]["student_count"] * 2
    assert packet["template_type"] == "lined_writing_page"

    pages = client.get(
        f"/v1/teacher-assist/print-packets/{packet['id']}/pages",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pages.status_code == 200, pages.text
    assert len(pages.json()) == context["teacher_class"]["student_count"] * 2


def test_assignment_print_packet_qr_payload_is_non_pii(client, db_session: Session):
    email = "teacher-print-packet-pii@example.com"
    token = _register_user(client, email=email, tenant_name="Print Packet PII Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="QR Safety")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "QR Safety Assignment",
            "assignment_type": "short_answer",
            "status": "draft",
        },
    )
    assert assignment.status_code == 201, assignment.text

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 1, "template_type": "blank_writing_page"},
    )
    assert packet.status_code == 201, packet.text

    pages = client.get(
        f"/v1/teacher-assist/print-packets/{packet.json()['id']}/pages",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pages.status_code == 200, pages.text
    first_page = pages.json()[0]
    payload = first_page["qr_payload_json"]
    assert set(payload.keys()) == {
        "qr_version",
        "packet_id",
        "assignment_id",
        "teacher_user_id",
        "tenant_id",
        "school_year_id",
        "grading_period_id",
        "class_id",
        "subject_id",
        "student_number",
        "page_number",
        "qr_token",
    }
    payload_text = str(payload).lower()
    assert "student_name" not in payload_text
    assert "real_student_id" not in payload_text
    assert "@" not in payload_text
    assert first_page["student_number"] == 1
    assert first_page["page_number"] == 1
    assert first_page["qr_svg_data_uri"].startswith("data:image/svg+xml;base64,")


def test_assignment_print_packet_tenant_isolation(client, db_session: Session):
    first_email = "teacher-print-a@example.com"
    second_email = "teacher-print-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Print Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Print Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Isolation")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Tenant Packet",
            "assignment_type": "writing",
            "status": "draft",
        },
    )
    assert assignment.status_code == 201, assignment.text

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/print-packets",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"pages_per_student": 1, "template_type": "short_answer_page"},
    )
    assert packet.status_code == 201, packet.text
    packet_id = packet.json()["id"]

    foreign = client.get(
        f"/v1/teacher-assist/print-packets/{packet_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404

    foreign_pages = client.get(
        f"/v1/teacher-assist/print-packets/{packet_id}/pages",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_pages.status_code == 404


def test_assignment_print_packet_invalid_assignment_is_rejected(client, db_session: Session):
    email = "teacher-print-invalid@example.com"
    token = _register_user(client, email=email, tenant_name="Print Invalid Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    created = client.post(
        f"/v1/teacher-assist/assignments/{uuid.uuid4()}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 1, "template_type": "blank_writing_page"},
    )
    assert created.status_code == 404


def test_assignment_print_packet_does_not_create_ai_usage_event(client, db_session: Session):
    email = "teacher-print-no-ai@example.com"
    token = _register_user(client, email=email, tenant_name="Print No AI Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="No AI Packet")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "No AI Packet Assignment",
            "assignment_type": "project",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 3, "template_type": "blank_writing_page"},
    )
    assert packet.status_code == 201, packet.text
    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage_count == before_usage_count


def test_assignment_student_work_upload_persists_metadata(client, db_session: Session):
    email = "teacher-student-work@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Student Work")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Upload Intake Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 1, "template_type": "blank_writing_page"},
    )
    assert packet.status_code == 201, packet.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-work-02.pdf", b"%PDF-1.7 student work", "application/pdf")},
        data={"student_number": "2", "assignment_print_packet_id": packet.json()["id"]},
    )
    assert uploaded.status_code == 201, uploaded.text
    payload = uploaded.json()
    assert payload["student_number"] == 2
    assert payload["assignment_print_packet_id"] == packet.json()["id"]
    assert payload["assignment_print_page_id"] is None
    assert payload["original_filename"] == "student-work-02.pdf"
    assert payload["mime_type"] == "application/pdf"
    assert payload["file_size"] > 0
    assert payload["storage_key"].startswith(f"teacher-assist/student-work/{payload['tenant_id']}/")
    assert payload["upload_status"] == "uploaded"
    assert payload["processing_status"] == "pending_review"

    listed = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert listed.status_code == 200, listed.text
    assert len(listed.json()) == 1
    assert listed.json()[0]["id"] == payload["id"]

    detail = client.get(
        f"/v1/teacher-assist/student-work/{payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["storage_key"] == payload["storage_key"]


def test_assignment_student_work_link_context_and_update_status(client, db_session: Session):
    email = "teacher-student-work-link@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work Link Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Linking")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Linking Assignment",
            "assignment_type": "short_answer",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 2, "template_type": "short_answer_page"},
    )
    assert packet.status_code == 201, packet.text
    packet_id = packet.json()["id"]

    pages = client.get(
        f"/v1/teacher-assist/print-packets/{packet_id}/pages",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pages.status_code == 200, pages.text
    first_student_page = next(page for page in pages.json() if page["student_number"] == 1)

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-1-response.txt", b"anonymous work", "text/plain")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text
    submission_id = uploaded.json()["id"]

    linked = client.patch(
        f"/v1/teacher-assist/student-work/{submission_id}/packet-context",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "assignment_print_packet_id": packet_id,
            "assignment_print_page_id": first_student_page["id"],
        },
    )
    assert linked.status_code == 200, linked.text
    assert linked.json()["assignment_print_packet_id"] == packet_id
    assert linked.json()["assignment_print_page_id"] == first_student_page["id"]

    ready = client.patch(
        f"/v1/teacher-assist/student-work/{submission_id}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"processing_status": "ready_for_processing"},
    )
    assert ready.status_code == 200, ready.text
    assert ready.json()["processing_status"] == "ready_for_processing"
    assert ready.json()["upload_status"] == "uploaded"


def test_assignment_student_work_rejects_invalid_student_number_and_page_context(client, db_session: Session):
    email = "teacher-student-work-invalid@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work Invalid Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Validation")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Validation Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    invalid_student = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("response.pdf", b"%PDF-1.7 invalid", "application/pdf")},
        data={"student_number": str(context["teacher_class"]["student_count"] + 1)},
    )
    assert invalid_student.status_code == 400, invalid_student.text
    assert "student number" in invalid_student.json()["detail"].lower()

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 1, "template_type": "blank_writing_page"},
    )
    assert packet.status_code == 201, packet.text

    pages = client.get(
        f"/v1/teacher-assist/print-packets/{packet.json()['id']}/pages",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert pages.status_code == 200, pages.text
    second_student_page = next(page for page in pages.json() if page["student_number"] == 2)

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 valid", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text

    mismatched_page = client.patch(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/packet-context",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "assignment_print_packet_id": packet.json()["id"],
            "assignment_print_page_id": second_student_page["id"],
        },
    )
    assert mismatched_page.status_code == 400, mismatched_page.text
    assert "student number" in mismatched_page.json()["detail"].lower()


def test_assignment_student_work_tenant_isolation(client, db_session: Session):
    first_email = "teacher-student-work-a@example.com"
    second_email = "teacher-student-work-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Student Work Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Student Work Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Isolation")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Isolation Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {first_token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 isolate", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text
    submission_id = uploaded.json()["id"]

    foreign_detail = client.get(
        f"/v1/teacher-assist/student-work/{submission_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_detail.status_code == 404

    foreign_status = client.patch(
        f"/v1/teacher-assist/student-work/{submission_id}/status",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"processing_status": "ready_for_processing"},
    )
    assert foreign_status.status_code == 404

    foreign_list = client.get(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_list.status_code == 404


def test_assignment_student_work_download_url_is_tenant_scoped(client, db_session: Session):
    first_email = "teacher-student-work-download-a@example.com"
    second_email = "teacher-student-work-download-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Student Work Download Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Student Work Download Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Downloads")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Download Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {first_token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 download test", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text

    download = client.get(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/download-url",
        headers={"Authorization": f"Bearer {first_token}"},
    )
    assert download.status_code == 200, download.text
    assert download.json()["url"].startswith("/v1/teacher-assist/storage/local-download?token=")

    streamed = client.get(download.json()["url"])
    assert streamed.status_code == 200, streamed.text
    assert streamed.content == b"%PDF-1.7 download test"

    foreign_download = client.get(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/download-url",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_download.status_code == 404


def test_assignment_student_work_does_not_create_ai_usage_event(client, db_session: Session):
    email = "teacher-student-work-no-ai@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work No AI Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="No AI Student Work")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "No AI Student Work Assignment",
            "assignment_type": "project",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-1.txt", b"no ai", "text/plain")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text
    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage_count == before_usage_count


def test_resource_extraction_job_completion_uses_storage_and_persists_preview(
    client, db_session: Session, monkeypatch
):
    email = "teacher-resource-extraction@example.com"
    token = _register_user(client, email=email, tenant_name="Resource Extraction Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("resource-handout.pdf", b"%PDF-1.7 resource content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    opened_keys: list[str] = []
    from oziebot_api.services.teacher_assist import extraction_jobs as extraction_jobs_module

    original_open_stream = extraction_jobs_module.open_teacher_assist_stream

    def _tracking_open_stream(settings: Settings, *, storage_key: str):
        opened_keys.append(storage_key)
        return original_open_stream(settings, storage_key=storage_key)

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.open_teacher_assist_stream",
        _tracking_open_stream,
    )

    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    assert created_job.json()["status"] == "queued"

    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job.json()["id"])

    history = client.get(
        f"/v1/teacher-assist/resources/{resource['id']}/extractions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert history.status_code == 200, history.text
    payload = history.json()
    assert payload[0]["job"]["status"] == "completed"
    assert payload[0]["extracted_text"]["preview_text"].startswith("[MOCK OCR]")
    assert opened_keys == [resource["storage_key"]]

    resource_detail = client.get(
        f"/v1/teacher-assist/resources/{resource['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert resource_detail.status_code == 200, resource_detail.text
    assert resource_detail.json()["latest_extraction_job"]["status"] == "completed"
    assert resource_detail.json()["latest_extracted_text"]["preview_text"].startswith("[MOCK OCR]")

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage_count == before_usage_count
    event_types = db_session.scalars(
        select(TeacherAssistActivityEvent.event_type).where(
            TeacherAssistActivityEvent.entity_id == uuid.UUID(created_job.json()["id"])
        )
    ).all()
    assert "extraction_started" in event_types
    assert "extraction_completed" in event_types


def test_student_work_extraction_completion_updates_workspace_without_ai_or_grading_side_effects(
    client, db_session: Session
):
    email = "teacher-student-work-extraction@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work Extraction Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Extraction")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Extraction Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-3.txt", b"anonymous response body", "text/plain")},
        data={"student_number": "3"},
    )
    assert uploaded.status_code == 201, uploaded.text
    submission = uploaded.json()

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_review_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )

    created_job = client.post(
        f"/v1/teacher-assist/student-work/{submission['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text

    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job.json()["id"])

    detail = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["latest_extraction_job"]["status"] == "completed"
    assert detail.json()["latest_extracted_text"]["preview_text"].startswith("[MOCK OCR]")

    history = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}/extractions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert history.status_code == 200, history.text
    assert history.json()[0]["job"]["status"] == "completed"
    assert history.json()[0]["extracted_text"]["student_number"] == 3

    workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workspace.status_code == 200, workspace.text
    workspace_payload = workspace.json()
    assert workspace_payload["today_summary"]["student_work_ready_for_extraction_count"] == 0
    assert workspace_payload["today_summary"]["extracted_artifacts_ready_for_teacher_review_count"] == 1
    assert "extracted_work_ready_for_teacher_review" in {
        item["type"] for item in workspace_payload["needs_attention"]
    }
    assert any(
        item["entity_type"] == "student_work_submission"
        for item in workspace_payload["review_required_items"]
    )
    assert workspace_payload["class_workspaces"][0]["recent_submissions"][0]["latest_extraction_status"] == "completed"
    assert workspace_payload["class_workspaces"][0]["recent_submissions"][0][
        "extraction_ready_for_teacher_review"
    ] is True

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_review_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAssignmentGradingReview))
    extracted_count = db_session.scalar(select(func.count()).select_from(TeacherAssistExtractedTextRecord))
    assert after_usage_count == before_usage_count
    assert after_review_count == before_review_count
    assert extracted_count >= 1


def test_student_work_extraction_failure_and_cancellation_rules(client, db_session: Session, monkeypatch):
    email = "teacher-student-work-extraction-failure@example.com"
    token = _register_user(client, email=email, tenant_name="Student Work Extraction Failure Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Extraction Failures")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Extraction Failure Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 failure test", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text

    cancelled_job = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert cancelled_job.status_code == 201, cancelled_job.text
    cancelled = client.patch(
        f"/v1/teacher-assist/extraction-jobs/{cancelled_job.json()['id']}/cancel",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "cancelled"},
    )
    assert cancelled.status_code == 200, cancelled.text
    assert cancelled.json()["status"] == "cancelled"
    assert client.patch(
        f"/v1/teacher-assist/extraction-jobs/{cancelled_job.json()['id']}/cancel",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "cancelled"},
    ).status_code == 400

    class _FailingProvider:
        provider_name = "mock"

        def extract_text(self, **_kwargs):
            raise RuntimeError("mock OCR boom")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FailingProvider(),
    )
    failed_job = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert failed_job.status_code == 201, failed_job.text

    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=failed_job.json()["id"])

    job_detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{failed_job.json()['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert job_detail.status_code == 200, job_detail.text
    assert job_detail.json()["job"]["status"] == "failed"
    assert job_detail.json()["job"]["error_code"] == "execution_failed"
    assert job_detail.json()["extracted_text"] is None


def test_extraction_job_creation_is_tenant_scoped(client, db_session: Session):
    first_email = "teacher-extraction-scope-a@example.com"
    second_email = "teacher-extraction-scope-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Extraction Scope A")
    second_token = _register_user(client, email=second_email, tenant_name="Extraction Scope B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    uploaded_resource = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {first_token}"},
        files={"file": ("scope-resource.pdf", b"%PDF-1.7 scope", "application/pdf")},
    )
    assert uploaded_resource.status_code == 201, uploaded_resource.text

    resource_foreign = client.post(
        f"/v1/teacher-assist/resources/{uploaded_resource.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert resource_foreign.status_code == 404

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Scope")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Extraction Scope Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    uploaded_submission = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {first_token}"},
        files={"file": ("scope-student.pdf", b"%PDF-1.7 scope student", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded_submission.status_code == 201, uploaded_submission.text

    submission_foreign = client.post(
        f"/v1/teacher-assist/student-work/{uploaded_submission.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert submission_foreign.status_code == 404


def test_assignment_grading_review_creation_from_student_work(client, db_session: Session):
    email = "teacher-grading-review@example.com"
    token = _register_user(client, email=email, tenant_name="Grading Review Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Grading")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Review Assignment",
            "assignment_type": "short_answer",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-2.txt", b"student response", "text/plain")},
        data={"student_number": "2"},
    )
    assert uploaded.status_code == 201, uploaded.text

    created = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "student_number": 2,
            "max_score": 10,
            "feedback_summary": "Solid reasoning with room to cite stronger evidence.",
            "strengths": ["Clear reasoning"],
            "improvement_areas": ["Use more evidence"],
            "teacher_notes": "Manual review started.",
        },
    )
    assert created.status_code == 201, created.text
    payload = created.json()
    assert payload["assignment_id"] == assignment.json()["id"]
    assert payload["student_work_submission_id"] == uploaded.json()["id"]
    assert payload["student_number"] == 2
    assert payload["status"] == "draft"
    assert payload["review_source"] == "manual"
    assert payload["provider_name"] is None
    assert payload["provider_model"] is None
    assert payload["prompt_version"] is None
    assert payload["ai_usage_event_id"] is None
    assert payload["max_score"] == 10

    listed = client.get(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/grading-reviews",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert listed.status_code == 200, listed.text
    assert len(listed.json()) == 1
    assert listed.json()[0]["id"] == payload["id"]

    detail = client.get(
        f"/v1/teacher-assist/grading-reviews/{payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["feedback_summary"] == payload["feedback_summary"]


def test_assignment_grading_review_teacher_confirmation_validation(client, db_session: Session):
    email = "teacher-grading-confirm@example.com"
    token = _register_user(client, email=email, tenant_name="Grading Confirm Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Confirm")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Confirm Review Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 submission", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text

    created = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={"student_number": 1, "max_score": 20},
    )
    assert created.status_code == 201, created.text

    invalid = client.patch(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "teacher_confirmed"},
    )
    assert invalid.status_code == 400, invalid.text
    assert "teacher confirmed" in invalid.json()["detail"].lower()

    confirmed = client.put(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "status": "teacher_confirmed",
            "max_score": 20,
            "teacher_confirmed_score": 18,
            "teacher_confirmed_feedback": "Teacher confirmed final feedback.",
            "items": [],
        },
    )
    assert confirmed.status_code == 200, confirmed.text
    assert confirmed.json()["status"] == "teacher_confirmed"
    assert confirmed.json()["teacher_confirmed_score"] == 18


def test_assignment_grading_review_rejects_pii_like_content(client, db_session: Session):
    email = "teacher-grading-pii@example.com"
    token = _register_user(client, email=email, tenant_name="Grading PII Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="PII")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "PII Review Assignment",
            "assignment_type": "project",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-3.txt", b"submission", "text/plain")},
        data={"student_number": "3"},
    )
    assert uploaded.status_code == 201, uploaded.text

    rejected = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "student_number": 3,
            "feedback_summary": "Email the family at parent@example.com with this note.",
        },
    )
    assert rejected.status_code == 400, rejected.text
    assert "pii" in rejected.json()["detail"].lower()


def test_assignment_grading_review_tenant_isolation_and_foreign_submission_blocked(client, db_session: Session):
    first_email = "teacher-grading-a@example.com"
    second_email = "teacher-grading-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Grading Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Grading Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Isolation")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Isolation Review Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {first_token}"},
        files={"file": ("student-1.pdf", b"%PDF-1.7 grading", "application/pdf")},
        data={"student_number": "1"},
    )
    assert uploaded.status_code == 201, uploaded.text

    foreign_create = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"student_number": 1},
    )
    assert foreign_create.status_code == 404

    created = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"student_number": 1},
    )
    assert created.status_code == 201, created.text

    foreign_get = client.get(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_get.status_code == 404


def test_assignment_grading_review_does_not_create_ai_usage_or_other_side_effects(client, db_session: Session):
    email = "teacher-grading-no-ai@example.com"
    token = _register_user(client, email=email, tenant_name="Grading No AI Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="No AI Review")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "No AI Review Assignment",
            "assignment_type": "homework",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment.json()['id']}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-4.txt", b"manual review only", "text/plain")},
        data={"student_number": "4"},
    )
    assert uploaded.status_code == 201, uploaded.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    created = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={"student_number": 4, "max_score": 5},
    )
    assert created.status_code == 201, created.text
    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage_count == before_usage_count
    assert created.json()["review_source"] == "manual"
    assert created.json()["ai_usage_event_id"] is None
    assert created.json()["provider_name"] is None


def test_teacher_assist_workspace_aggregates_operational_state_and_activity(client, db_session: Session):
    email = "teacher-workspace@example.com"
    token = _register_user(client, email=email, tenant_name="Workspace Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Workspace")
    second_class = client.post(
        "/v1/teacher-assist/classes",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "name": "Workspace Block B",
            "grade_level": "5",
            "student_count": 18,
        },
    )
    assert second_class.status_code == 201, second_class.text
    attached = client.post(
        "/v1/teacher-assist/class-subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"class_id": second_class.json()["id"], "subject_id": context["subject"]["id"]},
    )
    assert attached.status_code == 201, attached.text

    workflow = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workflow.status_code == 202, workflow.text
    completed_workflow_id = workflow.json()["id"]
    _run_teacher_assist_worker(db_session, workflow_id=completed_workflow_id)

    weekly_plans = client.get(
        "/v1/teacher-assist/weekly-plans",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert weekly_plans.status_code == 200, weekly_plans.text
    weekly_plan_id = weekly_plans.json()[0]["id"]

    failed_workflow = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert failed_workflow.status_code == 202, failed_workflow.text
    failed_workflow_id = uuid.UUID(failed_workflow.json()["id"])

    now = datetime.now(UTC)
    weekly_plan_row = db_session.get(TeacherAssistWeeklyPlan, uuid.UUID(weekly_plan_id))
    assert weekly_plan_row is not None
    weekly_plan_row.content_json = {
        **dict(weekly_plan_row.content_json or {}),
        "quality_flags": ["mock-output", "standards-context-missing"],
        "missing_context_warnings": ["Pacing guide context still needs review."],
    }
    weekly_plan_row.updated_at = now

    failed_workflow_row = db_session.get(TeacherAssistWorkflow, failed_workflow_id)
    assert failed_workflow_row is not None
    failed_workflow_row.status = "failed"
    failed_workflow_row.error_message = "TeacherAssist workflow failed in validation."
    failed_workflow_row.updated_at = now
    db_session.commit()

    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Workspace Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    packet = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/print-packets",
        headers={"Authorization": f"Bearer {token}"},
        json={"pages_per_student": 1, "template_type": "lined_writing_page"},
    )
    assert packet.status_code == 201, packet.text

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-2.pdf", b"%PDF-1.7 workspace", "application/pdf")},
        data={"student_number": "2", "assignment_print_packet_id": packet.json()["id"]},
    )
    assert uploaded.status_code == 201, uploaded.text

    review = client.post(
        f"/v1/teacher-assist/student-work/{uploaded.json()['id']}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={"student_number": 2, "max_score": 10, "feedback_summary": "Teacher review pending."},
    )
    assert review.status_code == 201, review.text

    workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workspace.status_code == 200, workspace.text
    payload = workspace.json()

    assert payload["current_school_year"]["id"] == context["school_year"]["id"]
    assert payload["active_grading_period"]["id"] == context["grading_period"]["id"]
    assert payload["today_summary"]["plans_needing_review_count"] == 1
    assert payload["today_summary"]["grading_reviews_pending_confirmation_count"] == 1
    assert payload["today_summary"]["recent_uploads_count"] == 1
    assert payload["today_summary"]["workflow_failures_count"] == 1
    assert payload["workspace_stats"]["active_plans_count"] == 1
    assert payload["workspace_stats"]["plans_in_review_count"] == 1
    assert payload["workspace_stats"]["pending_grading_reviews_count"] == 1
    assert payload["workspace_stats"]["recent_upload_count"] == 1
    assert payload["workspace_stats"]["workflow_failure_count"] == 1

    attention_types = {item["type"] for item in payload["needs_attention"]}
    assert {
        "workflow_failed",
        "plan_in_progress",
        "missing_standards_alignment",
        "plan_quality_flags",
        "missing_context_warnings",
        "submission_pending_review",
        "grading_review_pending_confirmation",
    }.issubset(attention_types)

    class_workspaces = {item["class"]["id"]: item for item in payload["class_workspaces"]}
    primary_workspace = class_workspaces[context["teacher_class"]["id"]]
    assert len(primary_workspace["active_plans"]) == 1
    assert len(primary_workspace["assignments"]) == 1
    assert len(primary_workspace["pending_grading_reviews"]) == 1
    assert len(primary_workspace["recent_submissions"]) == 1
    assert len(primary_workspace["packet_summaries"]) == 1
    assert primary_workspace["needs_attention_count"] >= 6
    assert class_workspaces[second_class.json()["id"]]["assignments"] == []
    assert class_workspaces[second_class.json()["id"]]["needs_attention_count"] == 0

    activity_event_types = {item["event_type"] for item in payload["recent_activity"]}
    assert {
        "workflow_started",
        "workflow_completed",
        "plan_created",
        "assignment_created",
        "packet_generated",
        "student_work_uploaded",
        "grading_review_created",
    }.issubset(activity_event_types)

    user = db_session.scalar(select(User).where(User.email == email))
    assert user is not None
    recorded_event_types = {
        row.event_type
        for row in db_session.scalars(
            select(TeacherAssistActivityEvent).where(TeacherAssistActivityEvent.user_id == user.id)
        ).all()
    }
    assert activity_event_types.issubset(recorded_event_types)


def test_teacher_assist_workspace_is_tenant_scoped(client, db_session: Session):
    first_email = "teacher-workspace-a@example.com"
    second_email = "teacher-workspace-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Workspace Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Workspace Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Scoped Workspace")
    workflow = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {first_token}"},
    )
    assert workflow.status_code == 202, workflow.text
    _run_teacher_assist_worker(db_session, workflow_id=workflow.json()["id"])

    first_workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {first_token}"},
    )
    assert first_workspace.status_code == 200, first_workspace.text
    assert first_workspace.json()["class_workspaces"]
    assert first_workspace.json()["recent_activity"]

    second_workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_workspace.status_code == 200, second_workspace.text
    second_payload = second_workspace.json()
    assert second_payload["current_school_year"] is None
    assert second_payload["active_grading_period"] is None
    assert second_payload["class_workspaces"] == []
    assert second_payload["needs_attention"] == []
    assert second_payload["recent_activity"] == []
    assert second_payload["active_workflows"] == []
    assert second_payload["review_required_items"] == []


def test_extraction_review_retry_approval_and_history(client, db_session: Session, monkeypatch):
    email = "teacher-extraction-review@example.com"
    token = _register_user(client, email=email, tenant_name="Extraction Review Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("review-handout.pdf", b"%PDF-1.7 review content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()

    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    job_id = created_job.json()["id"]

    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=job_id)

    run_detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert run_detail.status_code == 200, run_detail.text
    extracted = run_detail.json()["extracted_text"]
    assert extracted["review_status"] == "pending_review"
    assert extracted["confidence_level"] in {"low", "medium", "high"}
    assert extracted["provider_confidence_score"] is not None

    detail = client.get(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["record"]["extracted_text"].startswith("[MOCK OCR]")
    assert detail.json()["job"]["attempt_number"] == 1

    started = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert started.status_code == 200, started.text

    corrected = client.put(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/approved-text",
        headers={"Authorization": f"Bearer {token}"},
        json={"teacher_corrected_text": "Teacher corrected extraction text."},
    )
    assert corrected.status_code == 200, corrected.text
    assert corrected.json()["teacher_corrected_text"] == "Teacher corrected extraction text."

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_approved"},
    )
    assert approved.status_code == 200, approved.text

    approved_text = client.put(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/approved-text",
        headers={"Authorization": f"Bearer {token}"},
        json={"approved_text": "Teacher corrected extraction text."},
    )
    assert approved_text.status_code == 200, approved_text.text
    assert approved_text.json()["approved_text"] == "Teacher corrected extraction text."

    history = client.get(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/history",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert history.status_code == 200, history.text
    assert len(history.json()["attempt_jobs"]) == 1
    event_types = {row["event_type"] for row in history.json()["activity_events"]}
    assert "extraction_review_started" in event_types
    assert "extraction_text_corrected" in event_types
    assert "extraction_review_approved" in event_types

    rejected = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "archived"},
    )
    assert rejected.status_code == 200, rejected.text


def test_extraction_retry_creates_new_job_with_lineage(client, db_session: Session, monkeypatch):
    email = "teacher-extraction-retry@example.com"
    token = _register_user(client, email=email, tenant_name="Extraction Retry Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("retry-handout.pdf", b"%PDF-1.7 retry content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()

    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    original_job_id = created_job.json()["id"]

    class _FailingOCR:
        provider_name = "mock"

        def extract_text(self, **kwargs):
            raise RuntimeError("mock extraction failure")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda settings: _FailingOCR(),
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=original_job_id)

    failed = client.get(
        f"/v1/teacher-assist/extraction-jobs/{original_job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert failed.status_code == 200, failed.text
    assert failed.json()["job"]["status"] == "failed"

    retried = client.post(
        f"/v1/teacher-assist/extraction-jobs/{original_job_id}/retry",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert retried.status_code == 201, retried.text
    retry_payload = retried.json()
    assert retry_payload["id"] != original_job_id
    assert retry_payload["parent_extraction_job_id"] == original_job_id
    assert retry_payload["retry_root_job_id"] == original_job_id
    assert retry_payload["attempt_number"] == 2

    summaries = client.get(
        "/v1/teacher-assist/extractions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summaries.status_code == 200, summaries.text
    assert len(summaries.json()) >= 2

    workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workspace.status_code == 200, workspace.text
    assert workspace.json()["today_summary"]["extraction_failures_count"] >= 1


def test_extraction_job_detail_includes_eligibility_and_artifact_metadata(client, db_session: Session):
    email = "teacher-extraction-detail@example.com"
    token = _register_user(client, email=email, tenant_name="Extraction Detail Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("detail-handout.pdf", b"%PDF-1.7 detail content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()

    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    job_id = created_job.json()["id"]

    queued_detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert queued_detail.status_code == 200, queued_detail.text
    queued_payload = queued_detail.json()
    assert queued_payload["cancel_eligible"] is True
    assert queued_payload["retry_eligible"] is False
    assert queued_payload["source_artifact"]["original_filename"] == "detail-handout.pdf"
    assert "storage_key" not in queued_payload["source_artifact"]

    blocked_retry = client.post(
        f"/v1/teacher-assist/extraction-jobs/{job_id}/retry",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert blocked_retry.status_code == 400, blocked_retry.text


def test_extraction_issue_flagging_and_mark_reviewed_without_grading_side_effects(
    client, db_session: Session
):
    email = "teacher-extraction-issue@example.com"
    token = _register_user(client, email=email, tenant_name="Extraction Issue Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("issue-handout.pdf", b"%PDF-1.7 issue content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()

    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job.json()["id"])

    extracted = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job.json()['id']}",
        headers={"Authorization": f"Bearer {token}"},
    ).json()["extracted_text"]

    flagged = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "review_status": "issue_flagged",
            "teacher_issue_reason": "Preview text looks incomplete for classroom use.",
            "teacher_review_notes": "Retry after rescanning the original handout.",
        },
    )
    assert flagged.status_code == 200, flagged.text
    assert flagged.json()["review_status"] == "issue_flagged"
    assert flagged.json()["teacher_issue_reason"] == "Preview text looks incomplete for classroom use."

    blocked_from_issue = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "reviewed"},
    )
    assert blocked_from_issue.status_code == 400, blocked_from_issue.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )

    resumed = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert resumed.status_code == 200, resumed.text

    reviewed_ok = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "reviewed", "teacher_review_notes": "Acceptable after manual check."},
    )
    assert reviewed_ok.status_code == 200, reviewed_ok.text
    assert reviewed_ok.json()["review_status"] == "reviewed"

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )
    assert after_usage_count == before_usage_count
    assert after_grading_count == before_grading_count


def _create_resource_extraction_job(client, token: str, *, filename: str = "ocr-handout.pdf", content: bytes | None = None):
    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": (filename, content or b"%PDF-1.7 ocr content", "application/pdf")},
    )
    assert uploaded.status_code == 201, uploaded.text
    resource = uploaded.json()
    created_job = client.post(
        f"/v1/teacher-assist/resources/{resource['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    return resource, created_job.json()


class _FakeRealOCRProvider:
    provider_name = "textract"

    def __init__(self, *, confidence_score: float = 0.92, text: str = "Real OCR extracted classroom text."):
        self._confidence_score = confidence_score
        self._text = text

    def extract_text(self, **_kwargs):
        from oziebot_api.services.teacher_assist.ocr_provider import TeacherAssistOCRProviderResult
        from oziebot_api.services.teacher_assist.ocr_provider_config import confidence_level_from_score

        confidence_level = confidence_level_from_score(self._confidence_score)
        return TeacherAssistOCRProviderResult(
            extracted_text=self._text,
            provider="textract",
            model="textract-detect-document-text",
            metadata_json={
                "is_mock": False,
                "provider_mode": "real",
                "provider_version": "detect_document_text",
                "provider_confidence_score": self._confidence_score,
                "confidence_level": confidence_level,
                "page_count": 2,
                "estimated_cost_cents": 12,
                "low_confidence_output": confidence_level == "low",
            },
        )


def test_mock_ocr_remains_default_provider(client, db_session: Session):
    from oziebot_api.services.teacher_assist.ocr_provider import get_teacher_assist_ocr_provider

    assert Settings().teacher_assist_ocr_provider == "mock"
    assert get_teacher_assist_ocr_provider(Settings()).provider_name == "mock"

    email = "teacher-ocr-default@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Default Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"])

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["job"]["provider_name"] == "mock"
    assert payload["job"]["provider_mode"] == "mock"
    assert payload["job"]["provider_model"] == "mock-ocr"
    assert payload["extracted_text"]["review_status"] == "pending_review"
    assert payload["extracted_text"]["preview_text"].startswith("[MOCK OCR]")


def test_real_ocr_blocked_without_enable_flag(client, db_session: Session):
    email = "teacher-ocr-blocked@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Blocked Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)

    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=False,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"], settings=settings)

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["job"]["status"] == "failed"
    assert detail.json()["job"]["error_code"] == "provider_disabled"


def test_real_ocr_missing_cost_limit_fails_safe(client, db_session: Session):
    email = "teacher-ocr-cost-limit@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Cost Limit Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)

    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=0,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"], settings=settings)

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["job"]["error_code"] == "provider_disabled"


def test_real_ocr_openai_missing_credentials_fail_safe(client, db_session: Session):
    email = "teacher-ocr-openai-missing@example.com"
    token = _register_user(client, email=email, tenant_name="OCR OpenAI Missing Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("scan.png", b"\x89PNG\r\n", "image/png")},
    )
    assert uploaded.status_code == 201, uploaded.text
    created_job = client.post(
        f"/v1/teacher-assist/resources/{uploaded.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text

    settings = Settings(
        teacher_assist_ocr_provider="openai_vision",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_openai_api_key=None,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(
        db_session,
        extraction_job_id=created_job.json()["id"],
        settings=settings,
    )

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job.json()['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["job"]["error_code"] == "provider_not_configured"


def test_real_ocr_provider_metadata_persists(client, db_session: Session, monkeypatch):
    email = "teacher-ocr-metadata@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Metadata Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FakeRealOCRProvider(),
    )
    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"], settings=settings)

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    job = detail.json()["job"]
    record = detail.json()["extracted_text"]
    assert job["provider_name"] == "textract"
    assert job["provider_mode"] == "real"
    assert job["provider_model"] == "textract-detect-document-text"
    assert job["provider_version"] == "detect_document_text"
    assert job["page_count"] == 2
    assert job["processing_duration_ms"] is not None
    assert job["estimated_cost_cents"] == 12
    assert record["provider_confidence_score"] == pytest.approx(0.92)
    assert record["confidence_level"] == "high"
    assert record["metadata_json"]["provider_mode"] == "real"


def test_low_confidence_real_ocr_stays_pending_review(client, db_session: Session, monkeypatch):
    email = "teacher-ocr-low-confidence@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Low Confidence Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FakeRealOCRProvider(confidence_score=0.12),
    )
    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"], settings=settings)

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    payload = detail.json()
    assert payload["job"]["status"] == "completed"
    assert payload["extracted_text"]["review_status"] == "pending_review"
    assert payload["extracted_text"]["confidence_level"] == "low"
    assert payload["retry_eligible"] is True

    workspace = client.get(
        "/v1/teacher-assist/workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert workspace.status_code == 200, workspace.text
    assert workspace.json()["today_summary"]["low_confidence_extractions_count"] >= 1


def test_ocr_retry_lineage_preserves_provider_attempts(client, db_session: Session, monkeypatch):
    email = "teacher-ocr-lineage@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Lineage Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)
    original_job_id = created_job["id"]

    class _FailingRealOCR:
        provider_name = "textract"

        def extract_text(self, **_kwargs):
            from oziebot_api.services.teacher_assist.ocr_errors import TeacherAssistOCRProviderError

            raise TeacherAssistOCRProviderError(
                "Textract quota exceeded",
                error_code="provider_quota_exceeded",
            )

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FailingRealOCR(),
    )
    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=original_job_id, settings=settings)

    failed = client.get(
        f"/v1/teacher-assist/extraction-jobs/{original_job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert failed.json()["job"]["error_code"] == "provider_quota_exceeded"

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FakeRealOCRProvider(),
    )
    retried = client.post(
        f"/v1/teacher-assist/extraction-jobs/{original_job_id}/retry",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert retried.status_code == 201, retried.text
    retry_job_id = retried.json()["id"]
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=retry_job_id, settings=settings)

    retry_detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{retry_job_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert retry_detail.status_code == 200, retry_detail.text
    retry_payload = retry_detail.json()
    assert retry_payload["job"]["attempt_number"] == 2
    assert retry_payload["job"]["parent_extraction_job_id"] == original_job_id
    assert retry_payload["job"]["provider_mode"] == "real"
    assert len(retry_payload["lineage_jobs"]) == 2


def test_real_ocr_does_not_trigger_grading_mastery_or_ai_usage(client, db_session: Session, monkeypatch):
    email = "teacher-ocr-no-side-effects@example.com"
    token = _register_user(client, email=email, tenant_name="OCR Side Effects Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, created_job = _create_resource_extraction_job(client, token)

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_review_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAssignmentGradingReview))

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.extraction_jobs.get_teacher_assist_ocr_provider",
        lambda _settings: _FakeRealOCRProvider(),
    )
    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job["id"], settings=settings)

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_review_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAssignmentGradingReview))
    assert after_usage_count == before_usage_count
    assert after_review_count == before_review_count


def test_unsupported_mime_type_blocks_ocr(client, db_session: Session):
    email = "teacher-ocr-mime@example.com"
    token = _register_user(client, email=email, tenant_name="OCR MIME Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    uploaded = client.post(
        "/v1/teacher-assist/resources/upload",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("notes.docx", b"PK docx bytes", "application/vnd.openxmlformats-officedocument.wordprocessingml.document")},
    )
    assert uploaded.status_code == 201, uploaded.text
    created_job = client.post(
        f"/v1/teacher-assist/resources/{uploaded.json()['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text

    settings = Settings(
        teacher_assist_ocr_provider="textract",
        teacher_assist_real_ocr_enabled=True,
        teacher_assist_ocr_daily_cost_limit_cents=500,
        teacher_assist_worker_max_retries=0,
    )
    _run_teacher_assist_extraction_worker(
        db_session,
        extraction_job_id=created_job.json()["id"],
        settings=settings,
    )

    detail = client.get(
        f"/v1/teacher-assist/extraction-jobs/{created_job.json()['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["job"]["error_code"] == "unsupported_mime_type"


def _create_student_work_submission_with_extraction(client, db_session: Session, token: str):
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Grading Prep")
    assignment = client.post(
        "/v1/teacher-assist/assignments",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Grading Prep Assignment",
            "assignment_type": "writing",
            "status": "ready",
        },
    )
    assert assignment.status_code == 201, assignment.text
    assignment_id = assignment.json()["id"]

    uploaded = client.post(
        f"/v1/teacher-assist/assignments/{assignment_id}/student-work",
        headers={"Authorization": f"Bearer {token}"},
        files={"file": ("student-5.txt", b"student response for grading prep", "text/plain")},
        data={"student_number": "5"},
    )
    assert uploaded.status_code == 201, uploaded.text
    submission = uploaded.json()

    created_job = client.post(
        f"/v1/teacher-assist/student-work/{submission['id']}/extraction-jobs",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert created_job.status_code == 201, created_job.text
    _run_teacher_assist_extraction_worker(db_session, extraction_job_id=created_job.json()["id"])

    extracted = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}",
        headers={"Authorization": f"Bearer {token}"},
    ).json()["latest_extracted_text"]
    return assignment_id, submission, extracted


def test_grading_prep_approved_text_priority():
    from oziebot_api.services.teacher_assist.grading_prep_service import resolve_approved_text_from_record

    record = TeacherAssistExtractedTextRecord()
    record.id = uuid.uuid4()
    record.extraction_job_id = uuid.uuid4()
    record.review_status = "teacher_approved"
    record.approved_text = "Final approved copy"
    record.teacher_corrected_text = "Teacher corrected copy"
    record.extracted_text = "Raw extracted copy"

    resolution = resolve_approved_text_from_record(record)
    assert resolution is not None
    assert resolution.approved_text == "Final approved copy"
    assert resolution.text_source == "approved_text"

    record.approved_text = None
    resolution = resolve_approved_text_from_record(record)
    assert resolution is not None
    assert resolution.approved_text == "Teacher corrected copy"
    assert resolution.text_source == "teacher_corrected_text"

    record.teacher_corrected_text = None
    resolution = resolve_approved_text_from_record(record)
    assert resolution is not None
    assert resolution.approved_text == "Raw extracted copy"
    assert resolution.text_source == "extracted_text"


def test_grading_prep_blocks_unapproved_review_statuses():
    from oziebot_api.services.teacher_assist.grading_prep_service import (
        grading_prep_blocked_reason,
        resolve_approved_text_from_record,
    )

    record = TeacherAssistExtractedTextRecord()
    record.id = uuid.uuid4()
    record.extraction_job_id = uuid.uuid4()
    record.extracted_text = "Some extracted text"
    record.review_status = "pending_review"
    assert resolve_approved_text_from_record(record) is None
    assert grading_prep_blocked_reason(record) == "review_status:pending_review"

    record.review_status = "issue_flagged"
    assert resolve_approved_text_from_record(record) is None
    assert grading_prep_blocked_reason(record) == "review_status:issue_flagged"

    record.review_status = "teacher_rejected"
    assert resolve_approved_text_from_record(record) is None

    record.review_status = "reviewed"
    resolution = resolve_approved_text_from_record(record)
    assert resolution is not None
    assert resolution.approved_text == "Some extracted text"


def test_student_work_grading_prep_context_ready_after_teacher_approval(client, db_session: Session):
    email = "teacher-grading-prep-ready@example.com"
    token = _register_user(client, email=email, tenant_name="Grading Prep Ready Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    assignment_id, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )

    blocked = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}/grading-prep-context",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert blocked.status_code == 200, blocked.text
    blocked_payload = blocked.json()
    assert blocked_payload["ready_for_grading_prep"] is False
    assert blocked_payload["approved_text"] is None
    assert blocked_payload["ai_grading_enabled"] is False
    assert blocked_payload["blocked_reason"] == "review_status:pending_review"

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert approved.status_code == 200, approved.text

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_approved"},
    )
    assert approved.status_code == 200, approved.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )

    ready = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}/grading-prep-context",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert ready.status_code == 200, ready.text
    ready_payload = ready.json()
    assert ready_payload["ready_for_grading_prep"] is True
    assert ready_payload["text_source"] == "extracted_text"
    assert ready_payload["approved_text"].startswith("[MOCK OCR]")
    assert ready_payload["student_number"] == 5
    assert ready_payload["ai_grading_enabled"] is False

    summary = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/grading-prep-summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary.status_code == 200, summary.text
    summary_payload = summary.json()
    assert summary_payload["total_submissions"] == 1
    assert summary_payload["ready_for_grading_prep_count"] == 1
    assert summary_payload["blocked_count"] == 0
    assert summary_payload["submissions"][0]["ready_for_grading_prep"] is True
    assert summary_payload["ai_grading_enabled"] is False

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )
    assert after_usage_count == before_usage_count
    assert after_grading_count == before_grading_count


def test_student_work_grading_prep_tenant_isolation(client, db_session: Session):
    first_email = "teacher-grading-prep-a@example.com"
    second_email = "teacher-grading-prep-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Grading Prep Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Grading Prep Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    assignment_id, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, first_token
    )
    started = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert started.status_code == 200, started.text

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"review_status": "teacher_approved"},
    )
    assert approved.status_code == 200, approved.text

    blocked_context = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}/grading-prep-context",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert blocked_context.status_code == 404, blocked_context.text

    blocked_summary = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/grading-prep-summary",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert blocked_summary.status_code == 404, blocked_summary.text


def _approve_extraction_for_grading_prep(client, token: str, extracted_id: str) -> None:
    started = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted_id}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert started.status_code == 200, started.text

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted_id}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_approved"},
    )
    assert approved.status_code == 200, approved.text


def _create_grading_review_for_submission(
    client,
    token: str,
    submission_id: str,
    *,
    student_number: int = 5,
    max_score: float = 10,
):
    created = client.post(
        f"/v1/teacher-assist/student-work/{submission_id}/grading-review",
        headers={"Authorization": f"Bearer {token}"},
        json={"student_number": student_number, "max_score": max_score},
    )
    assert created.status_code == 201, created.text
    return created


def test_grading_review_ai_suggestion_blocked_when_extraction_not_approved(client, db_session: Session):
    email = "teacher-ai-suggest-blocked@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest Blocked Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, _extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    created = _create_grading_review_for_submission(client, token, submission["id"])

    blocked = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock"},
    )
    assert blocked.status_code == 400, blocked.text
    assert "grading prep is not ready" in blocked.json()["detail"].lower()


def test_grading_review_ai_suggestion_mock_populates_review_fields(client, db_session: Session):
    email = "teacher-ai-suggest-mock@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest Mock Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))

    suggested = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock", "teacher_instructions": "Focus on evidence quality."},
    )
    assert suggested.status_code == 200, suggested.text
    payload = suggested.json()
    review = payload["review"]
    assert payload["teacher_review_required"] is True
    assert payload["confidence_level"] in {"low", "medium", "high"}
    assert payload["text_source"] == "extracted_text"
    assert review["status"] == "ai_suggested"
    assert review["review_source"] == "ai_placeholder"
    assert review["provider_name"] == "mock"
    assert review["provider_model"] == "mock"
    assert review["prompt_version"] == "grading-assist-v1"
    assert review["ai_usage_event_id"] is not None
    assert review["score_suggestion"] is not None
    assert review["max_score"] == 10
    assert review["feedback_summary"]
    assert review["strengths"]
    assert review["improvement_areas"]
    assert "Teacher focus: Focus on evidence quality." in review["feedback_summary"]
    assert review["status"] != "teacher_confirmed"
    assert review["teacher_confirmed_score"] is None
    assert review["teacher_confirmed_feedback"] is None

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    assert after_usage_count == before_usage_count + 1
    assert after_workflow_count == before_workflow_count


def test_grading_review_ai_suggestion_uses_approved_text_priority(client, db_session: Session):
    email = "teacher-ai-suggest-priority@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest Priority Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )

    started = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_reviewing"},
    )
    assert started.status_code == 200, started.text

    corrected = client.put(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/approved-text",
        headers={"Authorization": f"Bearer {token}"},
        json={"teacher_corrected_text": "Teacher corrected priority text for grading assist."},
    )
    assert corrected.status_code == 200, corrected.text

    approved = client.patch(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/review-status",
        headers={"Authorization": f"Bearer {token}"},
        json={"review_status": "teacher_approved"},
    )
    assert approved.status_code == 200, approved.text

    final_approved = client.put(
        f"/v1/teacher-assist/extracted-text/{extracted['id']}/approved-text",
        headers={"Authorization": f"Bearer {token}"},
        json={"approved_text": "Final approved priority text for grading assist."},
    )
    assert final_approved.status_code == 200, final_approved.text

    created = _create_grading_review_for_submission(client, token, submission["id"])
    suggested = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock"},
    )
    assert suggested.status_code == 200, suggested.text
    assert suggested.json()["text_source"] == "approved_text"

    prep_context = client.get(
        f"/v1/teacher-assist/student-work/{submission['id']}/grading-prep-context",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert prep_context.status_code == 200, prep_context.text
    assert prep_context.json()["approved_text"] == "Final approved priority text for grading assist."


def test_grading_review_ai_suggestion_never_auto_confirms(client, db_session: Session):
    email = "teacher-ai-suggest-no-auto@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest No Auto Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    suggested = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock"},
    )
    assert suggested.status_code == 200, suggested.text
    assert suggested.json()["review"]["status"] == "ai_suggested"

    invalid_confirm = client.patch(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/status",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "teacher_confirmed"},
    )
    assert invalid_confirm.status_code == 400, invalid_confirm.text


def test_grading_review_ai_suggestion_no_mastery_gradebook_parent_side_effects(client, db_session: Session):
    email = "teacher-ai-suggest-side-effects@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest Side Effects Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    before_activity_count = db_session.scalar(select(func.count()).select_from(TeacherAssistActivityEvent))
    before_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))

    suggested = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock"},
    )
    assert suggested.status_code == 200, suggested.text

    after_activity_count = db_session.scalar(select(func.count()).select_from(TeacherAssistActivityEvent))
    after_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    assert after_workflow_count == before_workflow_count

    new_events = db_session.scalars(
        select(TeacherAssistActivityEvent)
        .where(TeacherAssistActivityEvent.event_type == "grading_review_ai_suggested")
        .order_by(TeacherAssistActivityEvent.created_at.desc())
    ).all()
    assert new_events
    assert after_activity_count == before_activity_count + 1
    assert suggested.json()["review"]["status"] != "teacher_confirmed"


def test_grading_review_ai_suggestion_tenant_isolation(client, db_session: Session):
    first_email = "teacher-ai-suggest-a@example.com"
    second_email = "teacher-ai-suggest-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="AI Suggest Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="AI Suggest Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, first_token
    )
    _approve_extraction_for_grading_prep(client, first_token, extracted["id"])
    created = _create_grading_review_for_submission(client, first_token, submission["id"])

    foreign = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"provider_mode": "mock"},
    )
    assert foreign.status_code == 404, foreign.text


def test_grading_review_ai_suggestion_real_provider_guarded(client, db_session: Session, monkeypatch):
    email = "teacher-ai-suggest-real@example.com"
    token = _register_user(client, email=email, tenant_name="AI Suggest Real Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    blocked = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "real"},
    )
    assert blocked.status_code == 400, blocked.text
    assert "disabled" in blocked.json()["detail"].lower()


def _confirm_grading_review(client, token: str, grading_review_id: str, *, score: float = 18, max_score: float = 20):
    confirmed = client.put(
        f"/v1/teacher-assist/grading-reviews/{grading_review_id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "status": "teacher_confirmed",
            "max_score": max_score,
            "teacher_confirmed_score": score,
            "teacher_confirmed_feedback": "Teacher confirmed final feedback.",
            "items": [],
        },
    )
    assert confirmed.status_code == 200, confirmed.text
    return confirmed


def test_gradebook_commit_blocked_until_teacher_confirmed(client, db_session: Session):
    email = "teacher-gradebook-blocked@example.com"
    token = _register_user(client, email=email, tenant_name="Gradebook Blocked Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    blocked = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/gradebook-commit",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert blocked.status_code == 400, blocked.text
    assert "teacher-confirmed" in blocked.json()["detail"].lower()


def test_teacher_confirmed_review_does_not_auto_commit_gradebook(client, db_session: Session):
    email = "teacher-gradebook-no-auto@example.com"
    token = _register_user(client, email=email, tenant_name="Gradebook No Auto Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])

    before_record_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAssignmentGradeRecord))
    before_commit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookCommit)
    )

    _confirm_grading_review(client, token, created.json()["id"])

    after_record_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAssignmentGradeRecord))
    after_commit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookCommit)
    )
    assert after_record_count == before_record_count
    assert after_commit_count == before_commit_count


def test_gradebook_commit_persists_record_history_and_audit(client, db_session: Session):
    email = "teacher-gradebook-commit@example.com"
    token = _register_user(client, email=email, tenant_name="Gradebook Commit Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    assignment_id, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])
    confirmed = _confirm_grading_review(client, token, created.json()["id"])

    committed = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/gradebook-commit",
        headers={"Authorization": f"Bearer {token}"},
        json={"teacher_confirmation_note": "Manual gradebook commit checkpoint."},
    )
    assert committed.status_code == 201, committed.text
    payload = committed.json()
    assert payload["grade_record"]["record_status"] == "active"
    assert payload["grade_record"]["committed_score"] == 18
    assert payload["grade_record"]["max_score"] == 20
    assert payload["commit"]["commit_type"] == "initial_commit"
    assert payload["commit"]["commit_status"] == "active"
    assert payload["commit"]["reason"] == "Manual gradebook commit checkpoint."

    listed = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/gradebook-records",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert listed.status_code == 200, listed.text
    assert len(listed.json()) == 1

    detail = client.get(
        f"/v1/teacher-assist/gradebook/records/{payload['grade_record']['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert len(detail.json()["commits"]) == 1
    assert detail.json()["audit_events"]

    export_view = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/gradebook-export",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert export_view.status_code == 200, export_view.text
    assert export_view.json()["active_record_count"] == 1
    assert export_view.json()["record_count"] == 1
    assert confirmed.json()["status"] == "teacher_confirmed"


def test_gradebook_correction_and_reversal_support(client, db_session: Session):
    email = "teacher-gradebook-correction@example.com"
    token = _register_user(client, email=email, tenant_name="Gradebook Correction Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    assignment_id, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])
    _confirm_grading_review(client, token, created.json()["id"])

    committed = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/gradebook-commit",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert committed.status_code == 201, committed.text
    grade_record_id = committed.json()["grade_record"]["id"]

    corrected = client.post(
        f"/v1/teacher-assist/gradebook/records/{grade_record_id}/corrections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "committed_score": 19,
            "max_score": 20,
            "committed_feedback": "Corrected final feedback.",
            "reason": "Teacher spotted a scoring error.",
        },
    )
    assert corrected.status_code == 200, corrected.text
    assert corrected.json()["grade_record"]["committed_score"] == 19
    assert corrected.json()["commit"]["commit_type"] == "correction"

    detail = client.get(
        f"/v1/teacher-assist/gradebook/records/{grade_record_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert len(detail.json()["commits"]) == 2
    assert detail.json()["commits"][0]["commit_status"] == "superseded"
    assert detail.json()["commits"][1]["commit_status"] == "active"

    reversed_grade = client.post(
        f"/v1/teacher-assist/gradebook/records/{grade_record_id}/reversals",
        headers={"Authorization": f"Bearer {token}"},
        json={"reason": "Submission was rescored outside this assignment."},
    )
    assert reversed_grade.status_code == 200, reversed_grade.text
    assert reversed_grade.json()["grade_record"]["record_status"] == "reversed"
    assert reversed_grade.json()["commit"]["commit_type"] == "reversal"

    blocked_correction = client.post(
        f"/v1/teacher-assist/gradebook/records/{grade_record_id}/corrections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "committed_score": 17,
            "max_score": 20,
            "committed_feedback": "Too late",
            "reason": "Should fail because record reversed",
        },
    )
    assert blocked_correction.status_code == 400, blocked_correction.text


def test_gradebook_commit_tenant_isolation(client, db_session: Session):
    first_email = "teacher-gradebook-a@example.com"
    second_email = "teacher-gradebook-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Gradebook Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Gradebook Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    assignment_id, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, first_token
    )
    _approve_extraction_for_grading_prep(client, first_token, extracted["id"])
    created = _create_grading_review_for_submission(client, first_token, submission["id"])
    _confirm_grading_review(client, first_token, created.json()["id"])
    committed = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/gradebook-commit",
        headers={"Authorization": f"Bearer {first_token}"},
        json={},
    )
    assert committed.status_code == 201, committed.text

    foreign_list = client.get(
        f"/v1/teacher-assist/assignments/{assignment_id}/gradebook-records",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_list.status_code == 404, foreign_list.text

    foreign_detail = client.get(
        f"/v1/teacher-assist/gradebook/records/{committed.json()['grade_record']['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign_detail.status_code == 404, foreign_detail.text


def test_gradebook_commit_does_not_create_mastery_or_parent_side_effects(client, db_session: Session):
    email = "teacher-gradebook-side-effects@example.com"
    token = _register_user(client, email=email, tenant_name="Gradebook Side Effects Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])
    _confirm_grading_review(client, token, created.json()["id"])

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))

    committed = client.post(
        f"/v1/teacher-assist/grading-reviews/{created.json()['id']}/gradebook-commit",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert committed.status_code == 201, committed.text

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    assert after_usage_count == before_usage_count
    assert after_workflow_count == before_workflow_count

    audit_count = db_session.scalar(
        select(func.count())
        .select_from(TeacherAssistAssignmentGradebookAuditEvent)
        .where(TeacherAssistAssignmentGradebookAuditEvent.event_type == "commit_created")
    )
    assert audit_count >= 1


def test_weekly_plan_export_creates_workflow_and_persists_pptx(client, db_session: Session):
    email = "teacher-export-ready@example.com"
    token = _register_user(client, email=email, tenant_name="Export Ready Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Export Science")

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )

    queued = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/exports",
        headers={"Authorization": f"Bearer {token}"},
        json={"artifact_type": "lesson_slides", "export_format": "pptx", "provider_mode": "mock"},
    )
    assert queued.status_code == 202, queued.text
    payload = queued.json()
    assert payload["artifact_status"] == "queued"
    assert payload["workflow_id"] is not None
    assert payload["source_plan_id"] == weekly_plan["id"]

    _run_teacher_assist_export_worker(db_session, workflow_id=payload["workflow_id"])

    detail = client.get(
        f"/v1/teacher-assist/exports/{payload['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    detail_payload = detail.json()
    assert detail_payload["artifact"]["artifact_status"] == "ready"
    assert detail_payload["workflow_status"] == "completed"
    assert detail_payload["artifact"]["preview_json"]["artifact_kind"] == "slides"
    assert len(detail_payload["artifact"]["preview_json"]["slides"]) >= 8
    assert detail_payload["artifact"]["storage_key"].startswith("teacher-assist/exports/")
    assert detail_payload["download_url"]

    download = client.get(
        f"/v1/teacher-assist/exports/{payload['id']}/download",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert download.status_code == 200, download.text
    assert download.json()["download_url"]
    assert download.json()["filename"].endswith(".pptx")

    listed = client.get(
        "/v1/teacher-assist/exports",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert listed.status_code == 200, listed.text
    assert listed.json()[0]["id"] == payload["id"]

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_grading_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradingReview)
    )
    assert after_usage_count == before_usage_count
    assert after_grading_count == before_grading_count


def test_weekly_plan_export_quiz_preview_structure(client, db_session: Session):
    email = "teacher-export-quiz@example.com"
    token = _register_user(client, email=email, tenant_name="Export Quiz Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Export Quiz")

    queued = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/exports",
        headers={"Authorization": f"Bearer {token}"},
        json={"artifact_type": "multiple_choice_quiz", "export_format": "json", "provider_mode": "mock"},
    )
    assert queued.status_code == 202, queued.text
    export_id = queued.json()["id"]
    _run_teacher_assist_export_worker(db_session, workflow_id=queued.json()["workflow_id"])

    detail = client.get(
        f"/v1/teacher-assist/exports/{export_id}",
        headers={"Authorization": f"Bearer {token}"},
    ).json()
    assert detail["artifact"]["preview_json"]["artifact_kind"] == "quiz"
    assert detail["artifact"]["preview_json"]["questions"]
    assert detail["artifact"]["preview_json"]["questions"][0]["question_type"] in {
        "multiple_choice",
        "short_answer",
        "true_false",
    }


def test_weekly_plan_export_tenant_isolation(client, db_session: Session):
    first_email = "teacher-export-a@example.com"
    second_email = "teacher-export-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Export Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Export Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)
    _, weekly_plan = _generate_weekly_plan(client, db_session, token=first_token, subject_name="Export Isolation")

    queued = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/exports",
        headers={"Authorization": f"Bearer {first_token}"},
        json={"artifact_type": "exit_ticket", "provider_mode": "mock"},
    )
    assert queued.status_code == 202, queued.text
    export_id = queued.json()["id"]

    blocked_detail = client.get(
        f"/v1/teacher-assist/exports/{export_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert blocked_detail.status_code == 404, blocked_detail.text

    blocked_list = client.get(
        f"/v1/teacher-assist/exports?source_plan_id={weekly_plan['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert blocked_list.status_code == 200, blocked_list.text
    assert blocked_list.json() == []


def test_weekly_plan_export_failure_persists_error(client, db_session: Session, monkeypatch):
    email = "teacher-export-failure@example.com"
    token = _register_user(client, email=email, tenant_name="Export Failure Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Export Failure")

    queued = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/exports",
        headers={"Authorization": f"Bearer {token}"},
        json={"artifact_type": "lesson_slides", "export_format": "pptx", "provider_mode": "mock"},
    )
    assert queued.status_code == 202, queued.text
    export_id = queued.json()["id"]
    workflow_id = queued.json()["workflow_id"]

    def _boom(**kwargs):
        raise RuntimeError("mock export generation failure")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.export_generation._render_export_file_bytes",
        _boom,
    )
    for _ in range(4):
        _run_teacher_assist_export_worker(
            db_session,
            workflow_id=workflow_id,
            settings=Settings(teacher_assist_worker_max_retries=0),
        )

    detail = client.get(
        f"/v1/teacher-assist/exports/{export_id}",
        headers={"Authorization": f"Bearer {token}"},
    ).json()
    assert detail["artifact"]["artifact_status"] == "failed"
    assert detail["workflow_status"] == "failed"
    assert detail["workflow_error_message"]

    blocked_download = client.get(
        f"/v1/teacher-assist/exports/{export_id}/download",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert blocked_download.status_code == 400, blocked_download.text


def _collect_action_workspace_items(payload: dict) -> list[dict]:
    items = list(payload.get("priority_items") or [])
    for section in payload.get("sections") or []:
        items.extend(section.get("items") or [])
    return items


def _action_workspace_action_types(payload: dict) -> set[str]:
    return {item["action_type"] for item in _collect_action_workspace_items(payload)}


def _action_workspace_navigation_hrefs(payload: dict) -> set[str]:
    return {item["navigation"]["href"] for item in _collect_action_workspace_items(payload)}


def test_teacher_assist_action_workspace_requires_product_access(client):
    token = _register_user(client, email="teacher-action-no-access@example.com", tenant_name="Action No Access")
    response = client.get(
        "/v1/teacher-assist/action-workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "TeacherAssist is not enabled for this user"


def test_teacher_assist_action_workspace_is_tenant_scoped(client, db_session: Session):
    first_email = "teacher-action-a@example.com"
    second_email = "teacher-action-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Action Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Action Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    assignment_id, _submission, _extracted = _create_student_work_submission_with_extraction(
        client, db_session, first_token
    )

    first_payload = client.get(
        "/v1/teacher-assist/action-workspace",
        headers={"Authorization": f"Bearer {first_token}"},
    )
    assert first_payload.status_code == 200, first_payload.text
    first_items = _collect_action_workspace_items(first_payload.json())
    assert any(item.get("assignment_id") == assignment_id for item in first_items)

    second_payload = client.get(
        "/v1/teacher-assist/action-workspace",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_payload.status_code == 200, second_payload.text
    second_items = _collect_action_workspace_items(second_payload.json())
    assert not any(item.get("assignment_id") == assignment_id for item in second_items)
    assert second_payload.json()["summary"]["total_open_actions"] == 0


def test_teacher_assist_action_workspace_aggregates_operational_actions(
    client, db_session: Session, monkeypatch
):
    email = "teacher-action-workspace@example.com"
    token = _register_user(client, email=email, tenant_name="Action Workspace Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    context = _create_ready_planning_draft_context(client, token=token, subject_name="Action Workspace")
    failed_workflow = client.post(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/workflows/weekly-plan",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert failed_workflow.status_code == 202, failed_workflow.text
    failed_workflow_row = db_session.get(TeacherAssistWorkflow, uuid.UUID(failed_workflow.json()["id"]))
    assert failed_workflow_row is not None
    failed_workflow_row.status = "failed"
    failed_workflow_row.error_message = "Action workspace workflow failure."
    db_session.commit()

    assignment_id, pending_submission, pending_extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    failed_job = db_session.scalar(
        select(TeacherAssistExtractionJob).where(
            TeacherAssistExtractionJob.student_work_submission_id == uuid.UUID(pending_submission["id"])
        )
    )
    assert failed_job is not None
    failed_job.status = "failed"
    failed_job.error_message = "Mock extraction failure for action workspace."
    db_session.commit()

    _, ai_submission, ai_extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, ai_extracted["id"])
    ai_review = _create_grading_review_for_submission(client, token, ai_submission["id"])
    suggested = client.post(
        f"/v1/teacher-assist/grading-reviews/{ai_review.json()['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock"},
    )
    assert suggested.status_code == 200, suggested.text

    _, commit_submission, commit_extracted = _create_student_work_submission_with_extraction(
        client, db_session, token
    )
    _approve_extraction_for_grading_prep(client, token, commit_extracted["id"])
    commit_review = _create_grading_review_for_submission(client, token, commit_submission["id"])
    _confirm_grading_review(client, token, commit_review.json()["id"])

    _, weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Action Export")
    queued_export = client.post(
        f"/v1/teacher-assist/weekly-plans/{weekly_plan['id']}/exports",
        headers={"Authorization": f"Bearer {token}"},
        json={"artifact_type": "lesson_slides", "export_format": "pptx", "provider_mode": "mock"},
    )
    assert queued_export.status_code == 202, queued_export.text
    export_workflow_id = queued_export.json()["workflow_id"]

    def _boom(**kwargs):
        raise RuntimeError("mock export generation failure")

    monkeypatch.setattr(
        "oziebot_api.services.teacher_assist.export_generation._render_export_file_bytes",
        _boom,
    )
    for _ in range(4):
        _run_teacher_assist_export_worker(
            db_session,
            workflow_id=export_workflow_id,
            settings=Settings(teacher_assist_worker_max_retries=0),
        )

    response = client.get(
        "/v1/teacher-assist/action-workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    action_types = _action_workspace_action_types(payload)

    assert payload["summary"]["total_open_actions"] >= 4
    assert payload["summary"]["critical_count"] >= 2
    assert "extraction_failed" in action_types
    assert "extraction_pending_review" in action_types
    assert "grading_review_ai_suggested" in action_types
    assert "gradebook_ready_to_commit" in action_types
    assert "workflow_failed" in action_types
    assert "export_failed" in action_types

    extraction_section = next(
        section for section in payload["sections"] if section["section_key"] == "extractions"
    )
    grading_section = next(section for section in payload["sections"] if section["section_key"] == "grading")
    gradebook_section = next(section for section in payload["sections"] if section["section_key"] == "gradebook")
    workflow_section = next(
        section for section in payload["sections"] if section["section_key"] == "workflows_exports"
    )
    assert extraction_section["count"] >= 1
    assert grading_section["count"] >= 1
    assert gradebook_section["count"] >= 1
    assert workflow_section["count"] >= 2

    priority_types = {item["action_type"] for item in payload["priority_items"]}
    assert "extraction_failed" in priority_types or "workflow_failed" in priority_types
    assert len(payload["priority_items"]) <= 10

    hrefs = _action_workspace_navigation_hrefs(payload)
    assert hrefs
    assert all(href.startswith("/teacher-assist/") for href in hrefs)
    assert any(href.startswith("/teacher-assist/extractions") for href in hrefs)
    assert any(href.startswith("/teacher-assist/assignments") for href in hrefs)
    assert any(href.startswith("/teacher-assist/exports") for href in hrefs)

    failed_item = next(item for item in _collect_action_workspace_items(payload) if item["action_type"] == "extraction_failed")
    assert failed_item["severity"] == "critical"
    assert failed_item["extracted_text_id"] == pending_extracted["id"]

    pending_item = next(
        item for item in _collect_action_workspace_items(payload) if item["action_type"] == "extraction_pending_review"
    )
    assert pending_item["severity"] == "review"
    assert pending_item["extracted_text_id"] == pending_extracted["id"]

    ai_item = next(
        item for item in _collect_action_workspace_items(payload) if item["action_type"] == "grading_review_ai_suggested"
    )
    assert ai_item["severity"] == "review"
    assert ai_item["grading_review_id"] == ai_review.json()["id"]

    ready_item = next(
        item for item in _collect_action_workspace_items(payload) if item["action_type"] == "gradebook_ready_to_commit"
    )
    assert ready_item["severity"] == "ready"
    assert ready_item["grading_review_id"] == commit_review.json()["id"]

    export_item = next(item for item in _collect_action_workspace_items(payload) if item["action_type"] == "export_failed")
    assert export_item["severity"] == "critical"


def test_teacher_assist_action_workspace_read_only_no_side_effects(client, db_session: Session):
    email = "teacher-action-readonly@example.com"
    token = _register_user(client, email=email, tenant_name="Action Readonly Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    _, submission, extracted = _create_student_work_submission_with_extraction(client, db_session, token)
    _approve_extraction_for_grading_prep(client, token, extracted["id"])
    created = _create_grading_review_for_submission(client, token, submission["id"])
    _confirm_grading_review(client, token, created.json()["id"])

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    before_grade_record_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradeRecord)
    )
    before_commit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookCommit)
    )
    before_audit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookAuditEvent)
    )
    before_activity_count = db_session.scalar(select(func.count()).select_from(TeacherAssistActivityEvent))

    response = client.get(
        "/v1/teacher-assist/action-workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    assert response.json()["summary"]["ready_count"] >= 1

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    after_grade_record_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradeRecord)
    )
    after_commit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookCommit)
    )
    after_audit_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradebookAuditEvent)
    )
    after_activity_count = db_session.scalar(select(func.count()).select_from(TeacherAssistActivityEvent))

    assert after_usage_count == before_usage_count
    assert after_workflow_count == before_workflow_count
    assert after_grade_record_count == before_grade_record_count
    assert after_commit_count == before_commit_count
    assert after_audit_count == before_audit_count
    assert after_activity_count == before_activity_count


def _create_mastery_matrix(client, token: str, context: dict) -> dict:
    created = client.post(
        "/v1/teacher-assist/mastery-matrices",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Phase 26 Mastery Matrix",
            "standard_ids": [context["standard"]["id"]],
            "target_mastery_level": "mastery",
        },
    )
    assert created.status_code == 201, created.text
    return created.json()


def test_mastery_matrix_requires_product_access(client):
    token = _register_user(client, email="teacher-mastery-no-access@example.com", tenant_name="Mastery No Access")
    response = client.get(
        "/v1/teacher-assist/mastery-matrices",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


def test_mastery_matrix_creation_and_tenant_isolation(client, db_session: Session):
    first_email = "teacher-mastery-a@example.com"
    second_email = "teacher-mastery-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Mastery Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Mastery Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Mastery Science")
    matrix = _create_mastery_matrix(client, first_token, context)
    assert matrix["title"] == "Phase 26 Mastery Matrix"
    assert len(matrix["standards"]) == 1

    foreign = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text

    second_list = client.get(
        "/v1/teacher-assist/mastery-matrices",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert second_list.status_code == 200, second_list.text
    assert second_list.json() == []


def test_mastery_commit_requires_teacher_confirmation(client, db_session: Session):
    email = "teacher-mastery-commit@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Commit Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Commit")
    matrix = _create_mastery_matrix(client, token, context)

    created = client.post(
        "/v1/teacher-assist/mastery-evaluations",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "student_number": 4,
            "standard_id": context["standard"]["id"],
            "mastery_level": "developing",
            "confidence_level": "medium",
            "evidence_source_type": "manual_observation",
            "teacher_notes": "Teacher observed partial mastery during assignment review.",
        },
    )
    assert created.status_code == 201, created.text
    evaluation = created.json()
    assert evaluation["evaluation_status"] == "draft"
    assert evaluation["confirmed_at"] is None

    summary_before = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary_before.status_code == 200, summary_before.text
    assert summary_before.json()["draft_evaluation_count"] == 1
    assert summary_before.json()["active_evaluation_count"] == 0

    committed = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation['id']}/commit",
        headers={"Authorization": f"Bearer {token}"},
        json={"commit_reason": "Teacher confirmed developing mastery."},
    )
    assert committed.status_code == 201, committed.text
    assert committed.json()["evaluation"]["evaluation_status"] == "active"
    assert committed.json()["evaluation"]["confirmed_at"] is not None
    assert committed.json()["commit"]["commit_type"] == "initial_commit"

    summary_after = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary_after.status_code == 200, summary_after.text
    assert summary_after.json()["active_evaluation_count"] == 1
    assert summary_after.json()["draft_evaluation_count"] == 0


def test_mastery_correction_and_reversal_lineage(client, db_session: Session):
    email = "teacher-mastery-lineage@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Lineage Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Lineage")
    matrix = _create_mastery_matrix(client, token, context)

    created = client.post(
        "/v1/teacher-assist/mastery-evaluations",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "student_number": 8,
            "standard_id": context["standard"]["id"],
            "mastery_level": "developing",
            "evidence_source_type": "manual_observation",
        },
    )
    assert created.status_code == 201, created.text
    evaluation_id = created.json()["id"]

    committed = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation_id}/commit",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert committed.status_code == 201, committed.text

    corrected = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation_id}/corrections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_level": "mastery",
            "commit_reason": "Teacher reviewed additional evidence and corrected mastery level.",
        },
    )
    assert corrected.status_code == 200, corrected.text
    assert corrected.json()["evaluation"]["mastery_level"] == "mastery"

    detail = client.get(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert detail.status_code == 200, detail.text
    assert len(detail.json()["commits"]) == 2
    assert detail.json()["commits"][0]["commit_status"] == "superseded"
    assert detail.json()["commits"][1]["commit_status"] == "active"

    reversed_eval = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation_id}/reversals",
        headers={"Authorization": f"Bearer {token}"},
        json={"commit_reason": "Teacher reversed mastery after reassessment."},
    )
    assert reversed_eval.status_code == 200, reversed_eval.text
    assert reversed_eval.json()["evaluation"]["evaluation_status"] == "reversed"
    assert reversed_eval.json()["commit"]["commit_type"] == "reversal"

    blocked_correction = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation_id}/corrections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_level": "advanced",
            "commit_reason": "Should fail because evaluation reversed",
        },
    )
    assert blocked_correction.status_code == 400, blocked_correction.text


def test_mastery_standards_ownership_validation(client, db_session: Session):
    email = "teacher-mastery-standards@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Standards Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Primary")
    other_subject = client.post(
        "/v1/teacher-assist/subjects",
        headers={"Authorization": f"Bearer {token}"},
        json={"code": "READ", "name": "Reading"},
    )
    assert other_subject.status_code == 201, other_subject.text
    foreign_standard = client.post(
        "/v1/teacher-assist/standards",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "subject_id": other_subject.json()["id"],
            "standard_type": "TEKS",
            "code": "5.2B",
            "description": "Foreign subject standard",
            "grade_level": "5",
            "school_year_id": context["school_year"]["id"],
        },
    )
    assert foreign_standard.status_code == 201, foreign_standard.text

    blocked = client.post(
        "/v1/teacher-assist/mastery-matrices",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Invalid Standard Matrix",
            "standard_ids": [foreign_standard.json()["id"]],
        },
    )
    assert blocked.status_code == 400, blocked.text
    assert "subject" in blocked.json()["detail"].lower()


def test_mastery_no_gradebook_ai_parent_side_effects(client, db_session: Session):
    email = "teacher-mastery-side-effects@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Side Effects Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Side Effects")
    matrix = _create_mastery_matrix(client, token, context)

    created = client.post(
        "/v1/teacher-assist/mastery-evaluations",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "student_number": 3,
            "standard_id": context["standard"]["id"],
            "mastery_level": "mastery",
            "evidence_source_type": "manual_observation",
        },
    )
    assert created.status_code == 201, created.text

    before_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_grade_record_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradeRecord)
    )
    before_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))

    committed = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{created.json()['id']}/commit",
        headers={"Authorization": f"Bearer {token}"},
        json={},
    )
    assert committed.status_code == 201, committed.text

    after_usage_count = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_grade_record_count = db_session.scalar(
        select(func.count()).select_from(TeacherAssistAssignmentGradeRecord)
    )
    after_workflow_count = db_session.scalar(select(func.count()).select_from(TeacherAssistWorkflow))
    assert after_usage_count == before_usage_count
    assert after_grade_record_count == before_grade_record_count
    assert after_workflow_count == before_workflow_count

    audit_count = db_session.scalar(
        select(func.count())
        .select_from(TeacherAssistMasteryAuditEvent)
        .where(TeacherAssistMasteryAuditEvent.event_type == "mastery_commit_created")
    )
    assert audit_count >= 1

    reteach = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/reteach-summary",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert reteach.status_code == 200, reteach.text


def _commit_mastery_evaluation(client, token: str, payload: dict) -> dict:
    created = client.post(
        "/v1/teacher-assist/mastery-evaluations",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
    )
    assert created.status_code == 201, created.text
    evaluation = created.json()
    committed = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation['id']}/commit",
        headers={"Authorization": f"Bearer {token}"},
        json={"commit_reason": "Teacher confirmed mastery for analytics test."},
    )
    assert committed.status_code in {200, 201}, committed.text
    return committed.json()["evaluation"]


def test_mastery_heatmap_uses_committed_evaluations_only(client, db_session: Session):
    email = "teacher-mastery-heatmap@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Heatmap Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Heatmap")
    matrix = _create_mastery_matrix(client, token, context)

    draft = client.post(
        "/v1/teacher-assist/mastery-evaluations",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "student_number": 9,
            "standard_id": context["standard"]["id"],
            "mastery_level": "beginning",
            "confidence_level": "medium",
            "evidence_source_type": "manual_observation",
        },
    )
    assert draft.status_code == 201, draft.text

    _commit_mastery_evaluation(
        client,
        token,
        {
            "mastery_matrix_id": matrix["id"],
            "student_number": 3,
            "standard_id": context["standard"]["id"],
            "mastery_level": "mastery",
            "confidence_level": "high",
            "evidence_source_type": "manual_observation",
        },
    )

    heatmap = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/heatmap",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert heatmap.status_code == 200, heatmap.text
    payload = heatmap.json()
    assert payload["active_evaluation_count"] == 1
    assert payload["student_numbers"] == [3]
    assert payload["rows"][0]["cells"][0]["mastery_level"] == "mastery"


def test_mastery_reteach_insights_thresholds(client, db_session: Session):
    email = "teacher-mastery-reteach@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Reteach Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Reteach")
    matrix = _create_mastery_matrix(client, token, context)

    for student_number, level in [(1, "mastery"), (2, "mastery"), (3, "developing"), (4, "beginning")]:
        _commit_mastery_evaluation(
            client,
            token,
            {
                "mastery_matrix_id": matrix["id"],
                "student_number": student_number,
                "standard_id": context["standard"]["id"],
                "mastery_level": level,
                "confidence_level": "medium",
                "evidence_source_type": "manual_observation",
            },
        )

    insights = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/reteach-insights",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert insights.status_code == 200, insights.text
    standard = insights.json()["standard_insights"][0]
    assert standard["mastery_percentage"] == 0.5
    assert standard["operational_status"] == "monitor"


def test_mastery_analytics_tenant_isolation(client, db_session: Session):
    first_email = "teacher-mastery-analytics-a@example.com"
    second_email = "teacher-mastery-analytics-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Mastery Analytics A")
    second_token = _register_user(client, email=second_email, tenant_name="Mastery Analytics B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)
    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Mastery Analytics")
    matrix = _create_mastery_matrix(client, first_token, context)

    foreign = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/heatmap",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text


def test_student_mastery_summary_and_trend(client, db_session: Session):
    email = "teacher-mastery-student@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Student Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Student")
    matrix = _create_mastery_matrix(client, token, context)

    evaluation = _commit_mastery_evaluation(
        client,
        token,
        {
            "mastery_matrix_id": matrix["id"],
            "student_number": 7,
            "standard_id": context["standard"]["id"],
            "mastery_level": "developing",
            "confidence_level": "medium",
            "evidence_source_type": "manual_observation",
        },
    )
    corrected = client.post(
        f"/v1/teacher-assist/mastery-evaluations/{evaluation['id']}/corrections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_level": "mastery",
            "commit_reason": "Teacher corrected mastery after additional evidence.",
        },
    )
    assert corrected.status_code == 200, corrected.text

    summary = client.get(
        f"/v1/teacher-assist/mastery-matrices/{matrix['id']}/student-summary/7",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert summary.status_code == 200, summary.text
    payload = summary.json()
    assert payload["student_number"] == 7
    assert payload["trend"] in {"improving", "stable", "insufficient_data"}
    assert payload["active_evaluation_count"] == 1


def test_mastery_dashboard_and_analytics_no_side_effects(client, db_session: Session):
    email = "teacher-mastery-dashboard@example.com"
    token = _register_user(client, email=email, tenant_name="Mastery Dashboard Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Mastery Dashboard")
    matrix = _create_mastery_matrix(client, token, context)
    _commit_mastery_evaluation(
        client,
        token,
        {
            "mastery_matrix_id": matrix["id"],
            "student_number": 2,
            "standard_id": context["standard"]["id"],
            "mastery_level": "mastery",
            "confidence_level": "high",
            "evidence_source_type": "manual_observation",
        },
    )

    before_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    before_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    dashboard = client.get("/v1/teacher-assist/mastery-dashboard", headers={"Authorization": f"Bearer {token}"})
    workspace = client.get("/v1/teacher-assist/workspace", headers={"Authorization": f"Bearer {token}"})
    actions = client.get("/v1/teacher-assist/action-workspace", headers={"Authorization": f"Bearer {token}"})

    assert dashboard.status_code == 200, dashboard.text
    assert dashboard.json()["matrix_count"] >= 1
    assert workspace.status_code == 200, workspace.text
    assert workspace.json().get("mastery_insights") is not None
    assert actions.status_code == 200, actions.text

    after_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    after_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_audit == before_audit
    assert after_ai == before_ai


def test_today_workspace_read_only_no_side_effects(client, db_session: Session):
    email = "teacher-today-workspace@example.com"
    token = _register_user(client, email=email, tenant_name="Today Workspace Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    before_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    before_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    response = client.get("/v1/teacher-assist/today", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert "priority_items" in payload
    assert "workflow_progress_cards" in payload
    assert "onboarding_checklist" in payload
    assert payload["onboarding_checklist"]["total_count"] == 7

    after_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    after_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_audit == before_audit
    assert after_ai == before_ai


def test_reteach_plan_create_ai_draft_and_teacher_version(client, db_session: Session):
    email = "teacher-reteach-plan@example.com"
    token = _register_user(client, email=email, tenant_name="Reteach Plan Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Reteach Plan")
    matrix = _create_mastery_matrix(client, token, context)
    _commit_mastery_evaluation(
        client,
        token,
        {
            "mastery_matrix_id": matrix["id"],
            "student_number": 3,
            "standard_id": context["standard"]["id"],
            "mastery_level": "beginning",
            "confidence_level": "medium",
            "evidence_source_type": "manual_observation",
        },
    )

    before_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    before_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))

    created = client.post(
        "/v1/teacher-assist/reteach-plans",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "standard_id": context["standard"]["id"],
        },
    )
    assert created.status_code == 201, created.text
    plan = created.json()
    assert plan["status"] == "draft"
    assert plan["standard_id"] == context["standard"]["id"]
    assert plan["current_version_id"] is None

    drafted = client.post(
        f"/v1/teacher-assist/reteach-plans/{plan['id']}/ai-draft",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock", "teacher_instructions": "Focus on vocabulary gaps."},
    )
    assert drafted.status_code == 200, drafted.text
    draft_payload = drafted.json()
    assert draft_payload["teacher_review_required"] is True
    assert draft_payload["provider_mode"] == "mock"
    assert draft_payload["prompt_version"] == "reteach-plan-ai-v1"
    assert draft_payload["plan"]["status"] == "ai_draft"
    assert draft_payload["version"]["version_source"] == "ai_draft"
    assert draft_payload["version"]["version_number"] == 1
    assert draft_payload["version"]["ai_usage_event_id"] is not None
    content = draft_payload["version"]["content_json"]
    assert content["teacher_review_required"] is True
    assert content["is_ai_draft"] is True
    assert content["reteach_objectives"]
    assert content["instructional_strategies"]
    assert content["small_group_recommendations"]
    assert content["intervention_ideas"]
    assert content["vocabulary_focus"]
    assert content["assessment_checks"]
    prompt_context = draft_payload["version"]["prompt_context_json"]
    assert prompt_context["anonymous_only"] is True
    assert prompt_context["pii_policy"] == "STUDENT_NUMBER_ONLY"
    assert all("student_number" in row for row in prompt_context["student_summaries"])
    assert not any("name" in row for row in prompt_context["student_summaries"])

    teacher_saved = client.post(
        f"/v1/teacher-assist/reteach-plans/{plan['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "content_json": {
                **content,
                "reteach_objectives": ["Teacher edited objective."],
                "teacher_review_required": True,
            },
            "change_reason": "Teacher reviewed and edited AI draft.",
        },
    )
    assert teacher_saved.status_code == 201, teacher_saved.text
    teacher_version = teacher_saved.json()
    assert teacher_version["version_source"] == "teacher_edit"
    assert teacher_version["version_number"] == 2

    updated_plan = client.get(
        f"/v1/teacher-assist/reteach-plans/{plan['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert updated_plan.status_code == 200, updated_plan.text
    assert updated_plan.json()["status"] == "teacher_review"
    assert updated_plan.json()["current_version_id"] == teacher_version["id"]

    versions = client.get(
        f"/v1/teacher-assist/reteach-plans/{plan['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert versions.status_code == 200, versions.text
    assert len(versions.json()) == 2

    after_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    after_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    assert after_usage == before_usage + 1
    assert after_audit == before_audit

    usage_event = db_session.scalar(
        select(TeacherAssistAIUsageEvent)
        .where(TeacherAssistAIUsageEvent.feature == "reteach_plan_ai_draft")
        .order_by(TeacherAssistAIUsageEvent.created_at.desc())
    )
    assert usage_event is not None
    assert usage_event.provider == "mock"
    assert usage_event.model == "mock"
    assert usage_event.metadata_json["teacher_review_required"] is True

    activity = db_session.scalar(
        select(TeacherAssistActivityEvent)
        .where(TeacherAssistActivityEvent.event_type == "reteach_plan_ai_drafted")
        .order_by(TeacherAssistActivityEvent.created_at.desc())
    )
    assert activity is not None


def test_reteach_plan_tenant_isolation(client, db_session: Session):
    first_email = "teacher-reteach-a@example.com"
    second_email = "teacher-reteach-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Reteach Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Reteach Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)

    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Reteach A")
    matrix = _create_mastery_matrix(client, first_token, context)
    created = client.post(
        "/v1/teacher-assist/reteach-plans",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "mastery_matrix_id": matrix["id"],
            "standard_id": context["standard"]["id"],
        },
    )
    assert created.status_code == 201, created.text
    plan_id = created.json()["id"]

    foreign = client.get(
        f"/v1/teacher-assist/reteach-plans/{plan_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text

    foreign_draft = client.post(
        f"/v1/teacher-assist/reteach-plans/{plan_id}/ai-draft",
        headers={"Authorization": f"Bearer {second_token}"},
        json={"provider_mode": "mock"},
    )
    assert foreign_draft.status_code == 404, foreign_draft.text


def test_newsletter_create_ai_draft_section_regen_export_and_teacher_version(client, db_session: Session):
    email = "teacher-newsletter@example.com"
    token = _register_user(client, email=email, tenant_name="Newsletter Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context, _weekly_plan = _generate_weekly_plan(client, db_session, token=token, subject_name="Newsletter Science")

    before_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    created = client.post(
        "/v1/teacher-assist/newsletters",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "teacher_notes": "Highlight vocabulary routines this week.",
        },
    )
    assert created.status_code == 201, created.text
    newsletter = created.json()
    assert newsletter["status"] == "draft"

    drafted = client.post(
        f"/v1/teacher-assist/newsletters/{newsletter['id']}/ai-draft",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock", "teacher_instructions": "Keep tone warm and concise."},
    )
    assert drafted.status_code == 200, drafted.text
    draft_payload = drafted.json()
    assert draft_payload["teacher_review_required"] is True
    assert draft_payload["prompt_version"] == "newsletter-ai-v1"
    assert draft_payload["newsletter"]["status"] == "review"
    content = draft_payload["version"]["content_json"]
    assert content["what_we_learned"]
    assert content["standards_covered"]
    assert content["upcoming_topics"]
    assert content["reminders"]
    assert content["celebration_highlights"]
    assert content["teacher_review_required"] is True
    prompt_context = draft_payload["version"]["prompt_context_json"]
    assert prompt_context["pii_policy"] == "NO_STUDENT_NAMES_GRADES_BEHAVIOR"
    assert prompt_context["anonymous_only"] is True
    assert "student_summaries" not in prompt_context

    regen = client.post(
        f"/v1/teacher-assist/newsletters/{newsletter['id']}/regenerate-section",
        headers={"Authorization": f"Bearer {token}"},
        json={"section": "reminders", "provider_mode": "mock"},
    )
    assert regen.status_code == 200, regen.text
    assert regen.json()["section"] == "reminders"
    assert regen.json()["version"]["version_source"] == "ai_section_regen"

    teacher_saved = client.post(
        f"/v1/teacher-assist/newsletters/{newsletter['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "content_json": {
                **regen.json()["version"]["content_json"],
                "teacher_message": "Teacher-approved family message.",
            },
            "change_reason": "Teacher reviewed newsletter draft.",
        },
    )
    assert teacher_saved.status_code == 201, teacher_saved.text
    assert teacher_saved.json()["version_source"] == "teacher_edit"

    approved = client.put(
        f"/v1/teacher-assist/newsletters/{newsletter['id']}",
        headers={"Authorization": f"Bearer {token}"},
        json={"status": "approved"},
    )
    assert approved.status_code == 200, approved.text
    assert approved.json()["status"] == "approved"

    for export_format in ("html", "pdf", "docx"):
        exported = client.post(
            f"/v1/teacher-assist/newsletters/{newsletter['id']}/exports",
            headers={"Authorization": f"Bearer {token}"},
            json={"export_format": export_format},
        )
        assert exported.status_code == 201, exported.text
        export_id = exported.json()["id"]
        download = client.get(
            f"/v1/teacher-assist/newsletters/{newsletter['id']}/exports/{export_id}/download",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert download.status_code == 200, download.text
        assert download.json()["download_url"]

    after_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage == before_usage + 2

    activity = db_session.scalar(
        select(TeacherAssistActivityEvent)
        .where(TeacherAssistActivityEvent.event_type == "newsletter_ai_drafted")
        .order_by(TeacherAssistActivityEvent.created_at.desc())
    )
    assert activity is not None


def test_newsletter_tenant_isolation(client, db_session: Session):
    first_email = "teacher-newsletter-a@example.com"
    second_email = "teacher-newsletter-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Newsletter Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Newsletter Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)
    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Newsletter A")
    created = client.post(
        "/v1/teacher-assist/newsletters",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
        },
    )
    assert created.status_code == 201, created.text
    newsletter_id = created.json()["id"]

    foreign = client.get(
        f"/v1/teacher-assist/newsletters/{newsletter_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text


def test_lesson_reflection_create_ai_suggestions_and_teacher_version(client, db_session: Session):
    email = "teacher-reflection@example.com"
    token = _register_user(client, email=email, tenant_name="Reflection Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Reflection Science")

    before_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    created = client.post(
        "/v1/teacher-assist/reflections",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "grading_period_id": context["grading_period"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
            "title": "Week 1 reflection",
        },
    )
    assert created.status_code == 201, created.text
    reflection = created.json()
    assert reflection["status"] == "draft"

    suggested = client.post(
        f"/v1/teacher-assist/reflections/{reflection['id']}/ai-suggestions",
        headers={"Authorization": f"Bearer {token}"},
        json={"provider_mode": "mock", "teacher_instructions": "Focus on pacing and engagement."},
    )
    assert suggested.status_code == 200, suggested.text
    suggestion_payload = suggested.json()
    assert suggestion_payload["teacher_review_required"] is True
    assert suggestion_payload["prompt_version"] == "lesson-reflection-ai-v1"
    assert suggestion_payload["lesson_reflection"]["status"] == "review"
    content = suggestion_payload["version"]["content_json"]
    assert content["strengths"]
    assert content["weaknesses"]
    assert content["improvements"]
    assert content["teacher_review_required"] is True
    prompt_context = suggestion_payload["version"]["prompt_context_json"]
    assert prompt_context["pii_policy"] == "NO_STUDENT_NAMES_GRADES_BEHAVIOR"
    assert "student_summaries" not in prompt_context

    teacher_saved = client.post(
        f"/v1/teacher-assist/reflections/{reflection['id']}/versions",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "content_json": {
                **content,
                "what_worked": ["Partner practice worked well."],
                "notes_for_next_year": ["Add a shorter warm-up next year."],
            },
            "change_reason": "Teacher reviewed reflection draft.",
        },
    )
    assert teacher_saved.status_code == 201, teacher_saved.text
    assert teacher_saved.json()["version_source"] == "teacher_edit"

    effectiveness = client.get(
        "/v1/teacher-assist/lesson-effectiveness",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert effectiveness.status_code == 200, effectiveness.text

    hints = client.get(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/reflection-hints",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert hints.status_code == 200, hints.text
    assert hints.json()["read_only"] is True

    preview = client.get(
        f"/v1/teacher-assist/planning-drafts/{context['draft']['id']}/context-preview",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["reflection_hints"] is not None

    after_usage = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_usage == before_usage + 1

    activity = db_session.scalar(
        select(TeacherAssistActivityEvent)
        .where(TeacherAssistActivityEvent.event_type == "lesson_reflection_ai_suggested")
        .order_by(TeacherAssistActivityEvent.created_at.desc())
    )
    assert activity is not None


def test_lesson_reflection_tenant_isolation(client, db_session: Session):
    first_email = "teacher-reflection-a@example.com"
    second_email = "teacher-reflection-b@example.com"
    first_token = _register_user(client, email=first_email, tenant_name="Reflection Tenant A")
    second_token = _register_user(client, email=second_email, tenant_name="Reflection Tenant B")
    _grant_teacher_assist_access(db_session, email=first_email)
    _grant_teacher_assist_access(db_session, email=second_email)
    context = _create_ready_planning_draft_context(client, token=first_token, subject_name="Reflection A")
    created = client.post(
        "/v1/teacher-assist/reflections",
        headers={"Authorization": f"Bearer {first_token}"},
        json={
            "school_year_id": context["school_year"]["id"],
            "class_id": context["teacher_class"]["id"],
            "subject_id": context["subject"]["id"],
        },
    )
    assert created.status_code == 201, created.text
    reflection_id = created.json()["id"]

    foreign = client.get(
        f"/v1/teacher-assist/reflections/{reflection_id}",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text


def test_home_workspace_and_work_queue_read_only(client, db_session: Session):
    email = "teacher-home-workspace@example.com"
    token = _register_user(client, email=email, tenant_name="Home Workspace Tenant")
    _grant_teacher_assist_access(db_session, email=email)

    before_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    before_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))

    home = client.get("/v1/teacher-assist/home", headers={"Authorization": f"Bearer {token}"})
    priorities = client.get("/v1/teacher-assist/home/priorities", headers={"Authorization": f"Bearer {token}"})
    classes = client.get("/v1/teacher-assist/home/classes", headers={"Authorization": f"Bearer {token}"})
    timeline = client.get("/v1/teacher-assist/home/timeline", headers={"Authorization": f"Bearer {token}"})
    mastery_alerts = client.get(
        "/v1/teacher-assist/home/mastery-alerts",
        headers={"Authorization": f"Bearer {token}"},
    )
    quick_actions = client.get("/v1/teacher-assist/home/quick-actions", headers={"Authorization": f"Bearer {token}"})
    work_queue = client.get("/v1/teacher-assist/work-queue", headers={"Authorization": f"Bearer {token}"})
    preferences = client.get("/v1/teacher-assist/user-preferences", headers={"Authorization": f"Bearer {token}"})

    assert home.status_code == 200, home.text
    home_payload = home.json()
    assert home_payload["read_only"] is True
    assert "priorities" in home_payload
    assert "onboarding" in home_payload
    assert home_payload["onboarding"]["total_count"] == 10

    assert priorities.status_code == 200, priorities.text
    assert "items" in priorities.json()

    assert classes.status_code == 200, classes.text
    assert isinstance(classes.json(), list)

    assert timeline.status_code == 200, timeline.text
    assert isinstance(timeline.json(), list)

    assert mastery_alerts.status_code == 200, mastery_alerts.text
    assert isinstance(mastery_alerts.json(), list)

    assert quick_actions.status_code == 200, quick_actions.text
    assert len(quick_actions.json()) >= 4

    assert work_queue.status_code == 200, work_queue.text
    queue_payload = work_queue.json()
    assert queue_payload["read_only"] is True
    assert "sections" in queue_payload

    assert preferences.status_code == 200, preferences.text
    assert preferences.json()["preferred_landing"] == "home"

    patched = client.patch(
        "/v1/teacher-assist/user-preferences",
        headers={"Authorization": f"Bearer {token}"},
        json={"preferred_landing": "work_queue"},
    )
    assert patched.status_code == 200, patched.text
    assert patched.json()["preferred_landing"] == "work_queue"

    after_audit = db_session.scalar(select(func.count()).select_from(TeacherAssistMasteryAuditEvent))
    after_ai = db_session.scalar(select(func.count()).select_from(TeacherAssistAIUsageEvent))
    assert after_audit == before_audit
    assert after_ai == before_ai


def test_class_operational_workspace(client, db_session: Session):
    email = "teacher-class-workspace@example.com"
    token = _register_user(client, email=email, tenant_name="Class Workspace Tenant")
    _grant_teacher_assist_access(db_session, email=email)
    context = _create_ready_planning_draft_context(client, token=token, subject_name="Class Workspace")

    response = client.get(
        f"/v1/teacher-assist/classes/{context['teacher_class']['id']}/workspace",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["class_id"] == context["teacher_class"]["id"]
    assert payload["read_only"] is True
    assert "tabs" in payload
    assert "overview" in payload["tabs"]
    assert "assignments" in payload["tabs"]

    second_token = _register_user(client, email="foreign-class@example.com", tenant_name="Foreign Tenant")
    _grant_teacher_assist_access(db_session, email="foreign-class@example.com")
    foreign = client.get(
        f"/v1/teacher-assist/classes/{context['teacher_class']['id']}/workspace",
        headers={"Authorization": f"Bearer {second_token}"},
    )
    assert foreign.status_code == 404, foreign.text

