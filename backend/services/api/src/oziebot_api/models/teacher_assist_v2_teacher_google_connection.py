from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, LargeBinary, String, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from oziebot_api.db.base import Base


class TeacherAssistV2TeacherGoogleConnection(Base):
    __tablename__ = "teacher_assist_v2_teacher_google_connections"

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    teacher_user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    google_email: Mapped[str | None] = mapped_column(String(320), nullable=True)
    encrypted_access_token: Mapped[bytes] = mapped_column(LargeBinary(), nullable=False)
    encrypted_refresh_token: Mapped[bytes | None] = mapped_column(LargeBinary(), nullable=True)
    token_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    scopes_json: Mapped[list[Any]] = mapped_column(JSON(), nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
