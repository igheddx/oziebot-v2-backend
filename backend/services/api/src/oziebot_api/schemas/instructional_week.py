from __future__ import annotations

import uuid

from pydantic import BaseModel, Field


class InstructionalWeekCreateIn(BaseModel):
    status: str = "DRAFT"


class InstructionalWeekUpdateIn(BaseModel):
    status: str | None = None
    notes: str | None = None
    title: str | None = Field(default=None, max_length=160)
    description: str | None = None


class InstructionalWeekObjectiveIn(BaseModel):
    objective_id: uuid.UUID | None = None
    objective_code: str | None = None
    source_type: str = "ADDED"
    is_required: bool = True
    notes: str | None = None


class InstructionalWeekSnapshotIn(BaseModel):
    name: str = Field(min_length=1, max_length=160)


class InstructionalWeekReuseIn(BaseModel):
    source_instructional_week_id: uuid.UUID
    copy_objectives: bool = True
    copy_assignments: bool = False
    copy_assessments: bool = False
    copy_lessons: bool = False
    copy_resources: bool = True
    copy_newsletters: bool = False
