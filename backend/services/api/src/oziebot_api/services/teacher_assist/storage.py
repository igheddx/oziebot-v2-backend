from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import uuid

from fastapi import UploadFile

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import infer_resource_type


@dataclass(frozen=True)
class StoredTeacherAssistUpload:
    storage_key: str
    original_filename: str
    mime_type: str
    file_size: int
    resource_type: str


async def store_teacher_assist_upload(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    upload: UploadFile,
) -> StoredTeacherAssistUpload:
    if settings.teacher_assist_storage_backend != "local":
        raise ValueError("Unsupported TeacherAssist storage backend")

    original_filename = Path((upload.filename or "upload.bin").strip()).name
    if not original_filename:
        raise ValueError("Uploaded file must include a filename")

    contents = await upload.read()
    await upload.close()

    file_size = len(contents)
    if file_size <= 0:
        raise ValueError("Uploaded file is empty")
    if file_size > settings.teacher_assist_upload_max_bytes:
        raise ValueError(
            f"Uploaded file exceeds the {settings.teacher_assist_upload_max_bytes} byte limit"
        )

    suffix = Path(original_filename).suffix.lower()
    now = datetime.now(UTC)
    storage_key = (
        f"teacher-assist/{tenant_id}/{now:%Y/%m/%d}/{uuid.uuid4().hex}{suffix}"
    )
    storage_root = Path(settings.teacher_assist_storage_root).expanduser()
    storage_path = storage_root / storage_key
    storage_path.parent.mkdir(parents=True, exist_ok=True)
    storage_path.write_bytes(contents)

    mime_type = (upload.content_type or "").strip() or "application/octet-stream"
    resource_type = infer_resource_type(original_filename, mime_type)

    return StoredTeacherAssistUpload(
        storage_key=storage_key.replace(os.sep, "/"),
        original_filename=original_filename,
        mime_type=mime_type,
        file_size=file_size,
        resource_type=resource_type,
    )
