from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
import io
import os
from pathlib import Path
from typing import BinaryIO, Literal
from urllib.parse import quote
import uuid

from botocore.exceptions import ClientError
from fastapi import UploadFile
from jose import JWTError, jwt

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.constants import infer_resource_type

TeacherAssistStorageArea = Literal[
    "resources",
    "student-work",
    "print-packets",
    "exports",
    "temp",
]

STORAGE_BACKENDS = ("local", "s3")
STORAGE_AREAS: tuple[TeacherAssistStorageArea, ...] = (
    "resources",
    "student-work",
    "print-packets",
    "exports",
    "temp",
)
DOWNLOAD_TOKEN_TYPE = "teacher_assist_storage_download"
LOCAL_DOWNLOAD_PATH = "/v1/teacher-assist/storage/local-download"


@dataclass(frozen=True)
class StoredTeacherAssistUpload:
    storage_key: str
    original_filename: str
    mime_type: str
    file_size: int
    resource_type: str


@dataclass(frozen=True)
class TeacherAssistLocalDownloadRequest:
    storage_key: str
    original_filename: str
    mime_type: str


class StorageProvider(ABC):
    @abstractmethod
    def save_file(self, *, storage_key: str, contents: bytes, mime_type: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_file(self, *, storage_key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_download_url(
        self,
        *,
        storage_key: str,
        original_filename: str,
        mime_type: str,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def file_exists(self, *, storage_key: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def open_stream(self, *, storage_key: str) -> BinaryIO:
        raise NotImplementedError


def _normalize_storage_backend(settings: Settings) -> str:
    normalized = settings.teacher_assist_storage_backend.strip().lower() or "local"
    if normalized not in STORAGE_BACKENDS:
        raise ValueError(f"Unsupported TeacherAssist storage backend '{settings.teacher_assist_storage_backend}'")
    return normalized


def _normalize_storage_area(area: str) -> TeacherAssistStorageArea:
    normalized = area.strip().lower()
    if normalized not in STORAGE_AREAS:
        raise ValueError(f"Unsupported TeacherAssist storage area '{area}'")
    return normalized  # type: ignore[return-value]


def _normalize_storage_prefix(settings: Settings) -> str:
    prefix = (settings.teacher_assist_s3_prefix or "teacher-assist").strip().strip("/")
    return prefix or "teacher-assist"


def _normalize_storage_key(storage_key: str) -> str:
    normalized = storage_key.strip().replace("\\", "/")
    if not normalized:
        raise ValueError("TeacherAssist storage key is required")
    if normalized.startswith("/"):
        raise ValueError("TeacherAssist storage keys must be relative")
    parts = [part for part in normalized.split("/") if part]
    if any(part == ".." for part in parts):
        raise ValueError("TeacherAssist storage key cannot traverse parent directories")
    return "/".join(parts)


def _safe_local_path(storage_root: Path, storage_key: str) -> Path:
    normalized_key = _normalize_storage_key(storage_key)
    root = storage_root.expanduser().resolve()
    candidate = (root / normalized_key).resolve()
    if candidate != root and root not in candidate.parents:
        raise ValueError("TeacherAssist storage key resolves outside the configured storage root")
    return candidate


def _normalize_filename(value: str | None) -> str:
    normalized = Path((value or "download.bin").strip()).name
    if not normalized:
        raise ValueError("TeacherAssist download filename is required")
    return normalized


def _normalize_mime_type(value: str | None) -> str:
    normalized = str(value or "").strip()
    return normalized or "application/octet-stream"


def _download_expires_at(settings: Settings) -> datetime:
    return datetime.now(UTC) + timedelta(
        seconds=max(1, int(settings.teacher_assist_s3_presign_expiration_seconds))
    )


def _create_local_download_token(
    settings: Settings,
    *,
    storage_key: str,
    original_filename: str,
    mime_type: str,
) -> str:
    payload = {
        "typ": DOWNLOAD_TOKEN_TYPE,
        "storage_key": _normalize_storage_key(storage_key),
        "original_filename": _normalize_filename(original_filename),
        "mime_type": _normalize_mime_type(mime_type),
        "exp": _download_expires_at(settings),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def resolve_teacher_assist_local_download(
    settings: Settings,
    *,
    token: str,
) -> TeacherAssistLocalDownloadRequest:
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"],
            options={"verify_aud": False},
        )
    except JWTError as exc:
        raise ValueError("Invalid or expired TeacherAssist download token") from exc
    if payload.get("typ") != DOWNLOAD_TOKEN_TYPE:
        raise ValueError("Invalid TeacherAssist download token type")
    return TeacherAssistLocalDownloadRequest(
        storage_key=_normalize_storage_key(str(payload.get("storage_key") or "")),
        original_filename=_normalize_filename(str(payload.get("original_filename") or "")),
        mime_type=_normalize_mime_type(str(payload.get("mime_type") or "")),
    )


class LocalStorageProvider(StorageProvider):
    def __init__(self, *, settings: Settings) -> None:
        self._settings = settings
        self._storage_root = Path(settings.teacher_assist_storage_root)

    def save_file(self, *, storage_key: str, contents: bytes, mime_type: str) -> None:
        storage_path = _safe_local_path(self._storage_root, storage_key)
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        storage_path.write_bytes(contents)

    def delete_file(self, *, storage_key: str) -> None:
        storage_path = _safe_local_path(self._storage_root, storage_key)
        if storage_path.exists():
            storage_path.unlink()

    def get_download_url(
        self,
        *,
        storage_key: str,
        original_filename: str,
        mime_type: str,
    ) -> str:
        token = _create_local_download_token(
            self._settings,
            storage_key=storage_key,
            original_filename=original_filename,
            mime_type=mime_type,
        )
        return f"{LOCAL_DOWNLOAD_PATH}?token={quote(token)}"

    def file_exists(self, *, storage_key: str) -> bool:
        return _safe_local_path(self._storage_root, storage_key).exists()

    def open_stream(self, *, storage_key: str) -> BinaryIO:
        return _safe_local_path(self._storage_root, storage_key).open("rb")


def _build_s3_client(settings: Settings):
    import boto3

    kwargs: dict[str, str] = {}
    region = (settings.teacher_assist_s3_region or "").strip()
    endpoint = (settings.teacher_assist_s3_endpoint or "").strip()
    if region:
        kwargs["region_name"] = region
    if endpoint:
        kwargs["endpoint_url"] = endpoint
    return boto3.client("s3", **kwargs)


class S3StorageProvider(StorageProvider):
    def __init__(self, *, settings: Settings) -> None:
        bucket = (settings.teacher_assist_s3_bucket or "").strip()
        if not bucket:
            raise ValueError("TeacherAssist S3 bucket must be configured when storage backend is s3")
        self._settings = settings
        self._client = _build_s3_client(settings)
        self._bucket = bucket

    def save_file(self, *, storage_key: str, contents: bytes, mime_type: str) -> None:
        self._client.put_object(
            Bucket=self._bucket,
            Key=_normalize_storage_key(storage_key),
            Body=contents,
            ContentType=_normalize_mime_type(mime_type),
            ServerSideEncryption="AES256",
        )

    def delete_file(self, *, storage_key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=_normalize_storage_key(storage_key))

    def get_download_url(
        self,
        *,
        storage_key: str,
        original_filename: str,
        mime_type: str,
    ) -> str:
        return str(
            self._client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self._bucket,
                    "Key": _normalize_storage_key(storage_key),
                    "ResponseContentType": _normalize_mime_type(mime_type),
                    "ResponseContentDisposition": build_content_disposition(
                        _normalize_filename(original_filename)
                    ),
                },
                ExpiresIn=max(1, int(self._settings.teacher_assist_s3_presign_expiration_seconds)),
            )
        )

    def file_exists(self, *, storage_key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=_normalize_storage_key(storage_key))
            return True
        except ClientError as exc:
            code = str((exc.response or {}).get("Error", {}).get("Code") or "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def open_stream(self, *, storage_key: str) -> BinaryIO:
        response = self._client.get_object(Bucket=self._bucket, Key=_normalize_storage_key(storage_key))
        body = response["Body"]
        return io.BytesIO(body.read())


def get_teacher_assist_storage_provider(settings: Settings) -> StorageProvider:
    backend = _normalize_storage_backend(settings)
    if backend == "local":
        return LocalStorageProvider(settings=settings)
    return S3StorageProvider(settings=settings)


def build_teacher_assist_storage_key(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    area: TeacherAssistStorageArea,
    original_filename: str,
) -> str:
    normalized_filename = _normalize_filename(original_filename)
    suffix = Path(normalized_filename).suffix.lower()
    prefix = _normalize_storage_prefix(settings)
    normalized_area = _normalize_storage_area(area)
    return f"{prefix}/{normalized_area}/{tenant_id}/{uuid.uuid4().hex}{suffix}"


def save_teacher_assist_bytes(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    area: TeacherAssistStorageArea,
    original_filename: str,
    contents: bytes,
    mime_type: str,
) -> StoredTeacherAssistUpload:
    file_size = len(contents)
    if file_size <= 0:
        raise ValueError("Uploaded file is empty")
    if file_size > settings.teacher_assist_upload_max_bytes:
        raise ValueError(
            f"Uploaded file exceeds the {settings.teacher_assist_upload_max_bytes} byte limit"
        )

    normalized_filename = _normalize_filename(original_filename)
    normalized_mime_type = _normalize_mime_type(mime_type)
    storage_key = build_teacher_assist_storage_key(
        settings,
        tenant_id=tenant_id,
        area=area,
        original_filename=normalized_filename,
    )
    provider = get_teacher_assist_storage_provider(settings)
    provider.save_file(
        storage_key=storage_key,
        contents=contents,
        mime_type=normalized_mime_type,
    )
    return StoredTeacherAssistUpload(
        storage_key=storage_key.replace(os.sep, "/"),
        original_filename=normalized_filename,
        mime_type=normalized_mime_type,
        file_size=file_size,
        resource_type=infer_resource_type(normalized_filename, normalized_mime_type),
    )


async def store_teacher_assist_upload(
    settings: Settings,
    *,
    tenant_id: uuid.UUID,
    upload: UploadFile,
    area: TeacherAssistStorageArea,
) -> StoredTeacherAssistUpload:
    original_filename = _normalize_filename(upload.filename)
    contents = await upload.read()
    await upload.close()
    return save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area=area,
        original_filename=original_filename,
        contents=contents,
        mime_type=upload.content_type or "application/octet-stream",
    )


def delete_teacher_assist_file(settings: Settings, *, storage_key: str) -> None:
    get_teacher_assist_storage_provider(settings).delete_file(storage_key=storage_key)


def teacher_assist_file_exists(settings: Settings, *, storage_key: str) -> bool:
    return get_teacher_assist_storage_provider(settings).file_exists(storage_key=storage_key)


def open_teacher_assist_stream(settings: Settings, *, storage_key: str) -> BinaryIO:
    return get_teacher_assist_storage_provider(settings).open_stream(storage_key=storage_key)


def get_teacher_assist_download_url(
    settings: Settings,
    *,
    storage_key: str,
    original_filename: str,
    mime_type: str,
) -> str:
    return get_teacher_assist_storage_provider(settings).get_download_url(
        storage_key=storage_key,
        original_filename=original_filename,
        mime_type=mime_type,
    )


def build_content_disposition(filename: str) -> str:
    normalized = _normalize_filename(filename)
    ascii_name = normalized.replace('"', "")
    return f'attachment; filename="{ascii_name}"; filename*=UTF-8\'\'{quote(normalized)}'
