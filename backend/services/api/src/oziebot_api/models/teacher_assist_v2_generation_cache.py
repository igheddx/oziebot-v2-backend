from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, Uuid
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from oziebot_api.db.base import Base


class TeacherAssistV2GenerationCache(Base):
    """Tenant-wide, content-addressed cache for expensive AI pipeline stages.

    Layer "curriculum": stores curriculum_sequence_plan + instructional_resource_bank.
      Key = hash(pacing_guide_ids, doc_content_hashes, prompt_version)

    Layer "planning": stores instructional_design_plan + strand_learning_journeys.
      Key = hash(curriculum_key, teacher_notes_hash, delivery_profile_hash, lost_days, prompt_version)

    Entries are never expired by TTL — invalidated only when the cache_key changes
    (new documents, bumped prompt version, changed delivery profile, etc.).
    """

    __tablename__ = "teacher_assist_v2_generation_cache"
    __table_args__ = (
        UniqueConstraint("tenant_id", "cache_layer", "cache_key", name="uq_v2_gen_cache_tenant_layer_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    cache_layer: Mapped[str] = mapped_column(String(32), nullable=False)
    cache_key: Mapped[str] = mapped_column(String(64), nullable=False)
    prompt_version: Mapped[str] = mapped_column(String(64), nullable=False)
    model: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content_json: Mapped[dict[str, Any]] = mapped_column(JSONB(), nullable=False)
    total_cost_cents: Mapped[int | None] = mapped_column(Integer(), nullable=True)
    hit_count: Mapped[int] = mapped_column(Integer(), nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    tenant: Mapped["Tenant"] = relationship("Tenant")


from typing import TYPE_CHECKING  # noqa: E402

if TYPE_CHECKING:
    from oziebot_api.models.tenant import Tenant
