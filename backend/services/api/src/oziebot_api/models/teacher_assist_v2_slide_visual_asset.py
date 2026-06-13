from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, ForeignKey, JSON, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base

if TYPE_CHECKING:
    from oziebot_api.models.teacher_assist_v2_instructional_package import (
        TeacherAssistV2InstructionalPackageArtifact,
    )


class TeacherAssistV2SlideVisualAsset(Base):
    __tablename__ = "teacher_assist_v2_slide_visual_assets"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    artifact_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("teacher_assist_v2_instructional_package_artifacts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    slide_id: Mapped[str] = mapped_column(String(128), nullable=False)
    visual_type: Mapped[str] = mapped_column(String(64), nullable=False)
    title: Mapped[str] = mapped_column(String(256), nullable=False)
    description: Mapped[str] = mapped_column(Text(), nullable=False)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    attribution: Mapped[str | None] = mapped_column(String(512), nullable=True)
    local_asset_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    prompt_hint: Mapped[str | None] = mapped_column(Text(), nullable=True)
    educational_purpose: Mapped[str | None] = mapped_column(Text(), nullable=True)
    suggested_placement: Mapped[str | None] = mapped_column(String(64), nullable=True)
    layout_template: Mapped[str | None] = mapped_column(String(64), nullable=True)
    visual_generation_status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default="recommendation_only",
        server_default="recommendation_only",
    )
    search_terms_json: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    suggested_sources_json: Mapped[list[str]] = mapped_column(JSON(), nullable=False, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    artifact: Mapped["TeacherAssistV2InstructionalPackageArtifact"] = relationship(
        "TeacherAssistV2InstructionalPackageArtifact",
        back_populates="slide_visual_assets",
    )
