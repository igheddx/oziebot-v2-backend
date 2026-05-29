from __future__ import annotations

import uuid

from pydantic import BaseModel, Field


class WeekDuplicateIn(BaseModel):
    target_period_id: uuid.UUID | None = None
    target_guide_id: uuid.UUID | None = None
    target_school_year_id: uuid.UUID | None = None
    copy_objectives: bool = True
    copy_resources: bool = True
    copy_notes: bool = True
    copy_artifacts: bool = False


class WeekTemplateSaveIn(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    description: str | None = None
    template_type: str = "TEACHER"
    visibility: str = "PRIVATE"
    artifact_type: str = "WEEK"


class WeekTemplateApplyIn(BaseModel):
    target_period_id: uuid.UUID


class RolloverV2In(BaseModel):
    source_school_year_id: uuid.UUID
    target_school_year_id: uuid.UUID
    pacing_guide_ids: list[uuid.UUID] = Field(default_factory=list)
    period_ids: list[uuid.UUID] = Field(default_factory=list)
    copy_instructional_plans: bool = True
    copy_assignments: bool = False
    copy_quizzes: bool = False
    copy_rubrics: bool = False
    copy_resources: bool = True


class PlanningGroupCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    description: str | None = None
    subject: str | None = None
    grade_level: str | None = None
    visibility: str = "TEAM"
