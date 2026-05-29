"""Product completion review — structured TeacherAssist feature inventory."""

from __future__ import annotations

from typing import Any, Literal


FeatureStatus = Literal["implemented", "partial", "deferred", "deprecated", "blocked"]


def _feature(
    *,
    key: str,
    name: str,
    status: FeatureStatus,
    routes: list[str] | None = None,
    api_prefix: str | None = None,
    notes: str | None = None,
    pilot_ready: bool = False,
) -> dict[str, Any]:
    return {
        "key": key,
        "name": name,
        "status": status,
        "routes": routes or [],
        "api_prefix": api_prefix,
        "notes": notes,
        "pilot_ready": pilot_ready,
    }


FEATURE_AREAS: list[dict[str, Any]] = [
    _feature(
        key="education_catalog",
        name="Education Catalog",
        status="implemented",
        routes=["/teacher-assist/catalog", "/teacher-assist/administration/education-catalog"],
        api_prefix="/v1/teacher-assist/catalog",
        pilot_ready=True,
        notes="Read-only teacher browse; root-admin management.",
    ),
    _feature(
        key="state_district_school_hierarchy",
        name="State / District / School Hierarchy",
        status="implemented",
        api_prefix="/v1/teacher-assist/catalog",
        pilot_ready=True,
        notes="Catalog access resolves teacher school assignments.",
    ),
    _feature(
        key="curriculum_resources",
        name="Curriculum Resources",
        status="implemented",
        routes=["/teacher-assist/resources", "/teacher-assist/catalog"],
        api_prefix="/v1/teacher-assist/resources",
        pilot_ready=True,
    ),
    _feature(
        key="objectives",
        name="Objectives",
        status="implemented",
        routes=["/teacher-assist/catalog", "/teacher-assist/mastery"],
        pilot_ready=True,
        notes="TEKS/objective linkage via pacing guides and mastery matrix.",
    ),
    _feature(
        key="pacing_guides",
        name="Pacing Guides",
        status="implemented",
        routes=[
            "/teacher-assist/pacing-guides",
            "/teacher-assist/planning/pacing-guides/workspace",
        ],
        api_prefix="/v1/teacher-assist/pacing-guides",
        pilot_ready=True,
    ),
    _feature(
        key="instructional_weeks",
        name="Instructional Weeks",
        status="implemented",
        routes=["/teacher-assist/week/[id]", "/teacher-assist/planning/weeks"],
        api_prefix="/v1/teacher-assist/instructional-weeks",
        pilot_ready=True,
    ),
    _feature(
        key="assignments",
        name="Assignments",
        status="implemented",
        routes=["/teacher-assist/assignments"],
        api_prefix="/v1/teacher-assist/assignments",
        pilot_ready=True,
    ),
    _feature(
        key="assessments",
        name="Assessments",
        status="partial",
        routes=["/teacher-assist/assignments", "/teacher-assist/extractions"],
        pilot_ready=True,
        notes="Quiz foundations exist; dedicated assessments hub deferred.",
    ),
    _feature(
        key="gradebook",
        name="Gradebook",
        status="implemented",
        routes=["/teacher-assist/gradebook"],
        api_prefix="/v1/teacher-assist/gradebook",
        pilot_ready=True,
        notes="Teacher-confirmed commits only; gradebook v2 API available.",
    ),
    _feature(
        key="mastery",
        name="Mastery",
        status="partial",
        routes=["/teacher-assist/mastery"],
        api_prefix="/v1/teacher-assist/mastery",
        pilot_ready=True,
        notes="Manual teacher-confirmed commits; v2 dashboard API richer than UI.",
    ),
    _feature(
        key="reteach",
        name="Reteach",
        status="implemented",
        routes=["/teacher-assist/reteach", "/teacher-assist/reteach-plans"],
        api_prefix="/v1/teacher-assist/reteach-workspace",
        pilot_ready=True,
    ),
    _feature(
        key="teacher_copilot",
        name="Teacher Copilot",
        status="implemented",
        routes=["/teacher-assist/copilot"],
        api_prefix="/v1/teacher-assist/copilot",
        pilot_ready=True,
        notes="Mock-first analysis; recommendations only.",
    ),
    _feature(
        key="templates",
        name="Templates",
        status="implemented",
        routes=["/teacher-assist/planning/templates"],
        api_prefix="/v1/teacher-assist/time-savings",
        pilot_ready=True,
    ),
    _feature(
        key="reuse_engine",
        name="Reuse Engine",
        status="implemented",
        routes=["/teacher-assist/planning/weeks", "/teacher-assist/home"],
        api_prefix="/v1/teacher-assist/time-savings",
        pilot_ready=True,
    ),
    _feature(
        key="communication_hub",
        name="Communication Hub",
        status="partial",
        routes=["/teacher-assist/newsletters", "/teacher-assist/communication"],
        pilot_ready=True,
        notes="Newsletter drafts and exports; no outbound send.",
    ),
    _feature(
        key="newsletters",
        name="Newsletters",
        status="implemented",
        routes=["/teacher-assist/newsletters"],
        api_prefix="/v1/teacher-assist/newsletters",
        pilot_ready=True,
    ),
    _feature(
        key="administration",
        name="Administration",
        status="partial",
        routes=["/teacher-assist/settings", "/teacher-assist/administration/education-catalog"],
        pilot_ready=True,
        notes="Catalog admin for root admins; broader district admin deferred.",
    ),
    _feature(
        key="authentication",
        name="Authentication",
        status="implemented",
        api_prefix="/v1/auth",
        pilot_ready=True,
    ),
    _feature(
        key="authorization",
        name="Authorization",
        status="implemented",
        pilot_ready=True,
        notes="Tenant-scoped TeacherAssist; root-admin catalog management.",
    ),
    _feature(
        key="object_storage",
        name="Object Storage",
        status="implemented",
        api_prefix="/v1/teacher-assist/storage",
        pilot_ready=True,
        notes="Local and private S3 backends; temporary download URLs.",
    ),
    _feature(
        key="exports",
        name="Exports",
        status="partial",
        routes=["/teacher-assist/exports"],
        api_prefix="/v1/teacher-assist/exports",
        pilot_ready=True,
        notes="HTML/PDF/DOCX for newsletters; assignment print HTML.",
    ),
    _feature(
        key="background_jobs",
        name="Background Jobs",
        status="partial",
        pilot_ready=True,
        notes="TeacherAssist worker for workflows and extraction; ops wiring environment-dependent.",
    ),
    _feature(
        key="lms_sis_integration",
        name="LMS / SIS Integration",
        status="deferred",
        pilot_ready=False,
    ),
    _feature(
        key="parent_portal",
        name="Parent Portal",
        status="deferred",
        pilot_ready=False,
    ),
    _feature(
        key="district_analytics",
        name="District Analytics",
        status="deferred",
        pilot_ready=False,
    ),
    _feature(
        key="legacy_pacing_items",
        name="Legacy Pacing Items",
        status="deprecated",
        api_prefix="/v1/teacher-assist/legacy/pacing-guides",
        notes="Preserved for backward compatibility.",
    ),
]


def build_product_completion_review() -> dict[str, Any]:
    counts = {status: 0 for status in ("implemented", "partial", "deferred", "deprecated", "blocked")}
    for row in FEATURE_AREAS:
        counts[row["status"]] = counts.get(row["status"], 0) + 1
    pilot_ready_count = sum(1 for row in FEATURE_AREAS if row.get("pilot_ready"))
    return {
        "summary": {
            "total_features": len(FEATURE_AREAS),
            "pilot_ready_count": pilot_ready_count,
            "status_counts": counts,
        },
        "features": FEATURE_AREAS,
        "workflow_gaps": [
            "Parent communication is draft-only; no outbound delivery.",
            "Mastery v2 and gradebook v2 UIs lag behind API richness.",
            "Dedicated assessments hub not implemented; assignments cover most flows.",
            "LMS/SIS import and roster sync deferred.",
            "Real-provider AI disabled by default across copilot and planning.",
        ],
        "navigation_notes": [
            "Primary nav follows teacher workflow: Home → Weeks → Pacing → Assignments → Mastery → Resources → Communication → Copilot.",
            "Work Queue and Catalog remain secondary for operational drill-down.",
            "Legacy routes (/today, /workspace, /actions) preserved for compatibility.",
        ],
    }
