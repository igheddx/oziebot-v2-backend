from __future__ import annotations

from dataclasses import dataclass
import uuid
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.models.teacher_assist_generated_artifact import TeacherAssistGeneratedArtifact
from oziebot_api.models.teacher_assist_pacing_guide import TeacherAssistPacingGuide
from oziebot_api.models.teacher_assist_pacing_guide_period import TeacherAssistPacingGuidePeriod
from oziebot_api.models.teacher_assist_time_savings import (
    TeacherAssistPlanningGroupMember,
    TeacherAssistWeekTemplate,
)
from oziebot_api.models.teacher_assist_weekly_plan import TeacherAssistWeeklyPlan
from oziebot_api.models.user import User
from oziebot_api.services.teacher_assist.week_context_service import WeekContextService


@dataclass
class ReuseScore:
    score: int
    subject_match: bool
    grade_match: bool
    objective_match_ratio: float
    resource_match_ratio: float
    week_similarity: float

    def serialize(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "subject_match": self.subject_match,
            "grade_match": self.grade_match,
            "objective_match_ratio": round(self.objective_match_ratio, 2),
            "resource_match_ratio": round(self.resource_match_ratio, 2),
            "week_similarity": round(self.week_similarity, 2),
        }


def _token_overlap(left: str | None, right: str | None) -> float:
    if not left or not right:
        return 0.0
    left_tokens = {token.lower() for token in left.split() if token.strip()}
    right_tokens = {token.lower() for token in right.split() if token.strip()}
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)


def _objective_codes(period: TeacherAssistPacingGuidePeriod) -> set[str]:
    codes: set[str] = set()
    for row in period.objectives:
        objective = getattr(row, "objective", None)
        code = getattr(objective, "objective_id", None)
        if code:
            codes.add(code)
    return codes


def compute_reuse_score(
    *,
    source_subject: str | None,
    source_grade: str | None,
    source_objectives: set[str],
    source_resources: set[str],
    source_title: str | None,
    target_subject: str | None,
    target_grade: str | None,
    target_objectives: set[str],
    target_resources: set[str],
    target_title: str | None,
) -> ReuseScore:
    subject_match = bool(
        source_subject and target_subject and source_subject.lower() == target_subject.lower()
    )
    grade_match = bool(
        source_grade and target_grade and source_grade.lower() == target_grade.lower()
    )
    objective_ratio = (
        len(source_objectives & target_objectives) / len(source_objectives | target_objectives)
        if source_objectives or target_objectives
        else 0.0
    )
    resource_ratio = (
        len(source_resources & target_resources) / len(source_resources | target_resources)
        if source_resources or target_resources
        else 0.0
    )
    week_similarity = _token_overlap(source_title, target_title)
    score = int(
        (30 if subject_match else 0)
        + (20 if grade_match else 0)
        + objective_ratio * 30
        + resource_ratio * 10
        + week_similarity * 10
    )
    return ReuseScore(
        score=min(score, 100),
        subject_match=subject_match,
        grade_match=grade_match,
        objective_match_ratio=objective_ratio,
        resource_match_ratio=resource_ratio,
        week_similarity=week_similarity,
    )


class InstructionalAssetReuseService:
    @staticmethod
    def search(
        db: Session,
        *,
        tenant_id: uuid.UUID,
        user: User,
        period_id: uuid.UUID,
        limit: int = 12,
    ) -> list[dict[str, Any]]:
        context = WeekContextService.build(db, tenant_id=tenant_id, user=user, period_id=period_id)
        target_objectives = {
            row.get("objective_code") for row in context.objectives if row.get("objective_code")
        }
        target_resources = {
            str(row.get("catalog_resource_id") or row.get("resource_library_item_id"))
            for row in context.resources
            if row.get("catalog_resource_id") or row.get("resource_library_item_id")
        }
        group_ids = [
            row.group_id
            for row in db.scalars(
                select(TeacherAssistPlanningGroupMember).where(
                    TeacherAssistPlanningGroupMember.user_id == user.id
                )
            ).all()
        ]

        candidates: list[dict[str, Any]] = []

        periods = db.scalars(
            select(TeacherAssistPacingGuidePeriod)
            .join(
                TeacherAssistPacingGuide,
                TeacherAssistPacingGuide.id == TeacherAssistPacingGuidePeriod.pacing_guide_id,
            )
            .where(
                TeacherAssistPacingGuide.tenant_id == tenant_id,
                TeacherAssistPacingGuidePeriod.period_type == "WEEK",
                TeacherAssistPacingGuidePeriod.id != period_id,
            )
            .options(
                selectinload(TeacherAssistPacingGuidePeriod.objectives),
                selectinload(TeacherAssistPacingGuidePeriod.resources),
            )
            .limit(50)
        ).all()
        for period in periods:
            guide = db.get(TeacherAssistPacingGuide, period.pacing_guide_id)
            if guide is None:
                continue
            source = "CURRENT_TEACHER" if guide.created_by_user_id == user.id else "SCHOOL"
            if guide.planning_group_id and guide.planning_group_id in group_ids:
                source = "GRADE_TEAM"
            if guide.visibility_scope in {"DISTRICT", "SCHOOL"}:
                source = guide.visibility_scope
            reuse_score = compute_reuse_score(
                source_subject=context.subject_name,
                source_grade=context.grade_level,
                source_objectives=target_objectives,
                source_resources=target_resources,
                source_title=context.period_title,
                target_subject=guide.title,
                target_grade=guide.grade_level,
                target_objectives=_objective_codes(period),
                target_resources={
                    str(row.catalog_resource_id or row.resource_library_item_id)
                    for row in period.resources
                    if row.catalog_resource_id or row.resource_library_item_id
                },
                target_title=period.title,
            )
            candidates.append(
                {
                    "entity_type": "pacing_week",
                    "entity_id": str(period.id),
                    "title": period.title,
                    "source": source,
                    "reuse_score": reuse_score.serialize(),
                    "navigation_href": f"/teacher-assist/planning/weeks?period_id={period.id}",
                }
            )

        artifacts = db.scalars(
            select(TeacherAssistGeneratedArtifact).where(
                TeacherAssistGeneratedArtifact.tenant_id == tenant_id,
                TeacherAssistGeneratedArtifact.pacing_guide_period_id != period_id,
            )
        ).all()
        for artifact in artifacts:
            source = "CURRENT_TEACHER" if artifact.created_by_user_id == user.id else "SCHOOL"
            reuse_score = compute_reuse_score(
                source_subject=context.subject_name,
                source_grade=context.grade_level,
                source_objectives=target_objectives,
                source_resources=target_resources,
                source_title=context.period_title,
                target_subject=context.subject_name,
                target_grade=context.grade_level,
                target_objectives=set((artifact.metadata_json or {}).get("objective_codes") or []),
                target_resources=set(),
                target_title=artifact.title,
            )
            candidates.append(
                {
                    "entity_type": "generated_artifact",
                    "entity_id": str(artifact.id),
                    "title": artifact.title,
                    "artifact_type": artifact.artifact_type,
                    "source": source,
                    "reuse_score": reuse_score.serialize(),
                    "navigation_href": f"/teacher-assist/planning/weeks?period_id={artifact.pacing_guide_period_id}",
                }
            )

        templates = db.scalars(
            select(TeacherAssistWeekTemplate).where(
                TeacherAssistWeekTemplate.tenant_id == tenant_id,
                or_(
                    TeacherAssistWeekTemplate.created_by_user_id == user.id,
                    TeacherAssistWeekTemplate.visibility.in_(("TEAM", "SCHOOL", "DISTRICT")),
                ),
            )
        ).all()
        for template in templates:
            data = template.template_data or {}
            reuse_score = compute_reuse_score(
                source_subject=context.subject_name,
                source_grade=context.grade_level,
                source_objectives=target_objectives,
                source_resources=target_resources,
                source_title=context.period_title,
                target_subject=template.subject,
                target_grade=template.grade_level,
                target_objectives=set(data.get("objective_codes") or []),
                target_resources=set(),
                target_title=template.name,
            )
            candidates.append(
                {
                    "entity_type": "week_template",
                    "entity_id": str(template.id),
                    "title": template.name,
                    "artifact_type": template.artifact_type,
                    "source": "SHARED_TEMPLATE",
                    "reuse_score": reuse_score.serialize(),
                    "navigation_href": f"/teacher-assist/planning/templates?id={template.id}",
                }
            )

        plans = db.scalars(
            select(TeacherAssistWeeklyPlan)
            .where(
                TeacherAssistWeeklyPlan.tenant_id == tenant_id,
                or_(
                    TeacherAssistWeeklyPlan.user_id == user.id,
                    TeacherAssistWeeklyPlan.visibility_scope.in_(
                        ("shared", "grade_team", "school", "district")
                    ),
                ),
            )
            .limit(30)
        ).all()
        for plan in plans:
            reuse_score = compute_reuse_score(
                source_subject=context.subject_name,
                source_grade=context.grade_level,
                source_objectives=target_objectives,
                source_resources=target_resources,
                source_title=context.period_title,
                target_subject=context.subject_name,
                target_grade=context.grade_level,
                target_objectives=set(),
                target_resources=set(),
                target_title=plan.title,
            )
            candidates.append(
                {
                    "entity_type": "instructional_plan",
                    "entity_id": str(plan.id),
                    "title": plan.title,
                    "artifact_type": "LESSON_PLAN",
                    "source": "PRIOR_SCHOOL_YEAR"
                    if plan.school_year_origin_id
                    else "CURRENT_TEACHER",
                    "reuse_score": reuse_score.serialize(),
                    "navigation_href": f"/teacher-assist/weekly-planning/plans?id={plan.id}",
                }
            )

        candidates.sort(key=lambda row: row["reuse_score"]["score"], reverse=True)
        return candidates[:limit]
