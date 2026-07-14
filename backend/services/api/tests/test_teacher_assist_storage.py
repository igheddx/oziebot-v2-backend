from __future__ import annotations

import io
from urllib.parse import parse_qs, unquote, urlparse
import uuid

from botocore.exceptions import ClientError
import pytest

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.storage import (
    LocalStorageProvider,
    S3StorageProvider,
    build_teacher_assist_storage_key,
    delete_teacher_assist_file,
    get_teacher_assist_download_url,
    get_teacher_assist_storage_provider,
    open_teacher_assist_stream,
    resolve_teacher_assist_local_download,
    save_teacher_assist_bytes,
    teacher_assist_file_exists,
)


class FakeS3Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_calls: list[dict[str, object]] = []
        self.presign_calls: list[dict[str, object]] = []
        self.delete_calls: list[dict[str, object]] = []

    def put_object(self, **kwargs) -> None:
        self.put_calls.append(kwargs)
        self.objects[str(kwargs["Key"])] = bytes(kwargs["Body"])

    def delete_object(self, **kwargs) -> None:
        self.delete_calls.append(kwargs)
        self.objects.pop(str(kwargs["Key"]), None)

    def head_object(self, **kwargs) -> dict[str, object]:
        key = str(kwargs["Key"])
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    def get_object(self, **kwargs) -> dict[str, object]:
        key = str(kwargs["Key"])
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": FakeS3Body(self.objects[key])}

    def generate_presigned_url(self, operation_name: str, *, Params, ExpiresIn: int) -> str:
        self.presign_calls.append(
            {
                "operation_name": operation_name,
                "Params": Params,
                "ExpiresIn": ExpiresIn,
            }
        )
        return "https://signed.example/object"


def test_local_storage_provider_save_read_delete_and_download(tmp_path):
    settings = Settings(
        teacher_assist_storage_backend="local",
        teacher_assist_storage_root=str(tmp_path),
        teacher_assist_s3_presign_expiration_seconds=300,
    )
    tenant_id = uuid.uuid4()

    saved = save_teacher_assist_bytes(
        settings,
        tenant_id=tenant_id,
        area="resources",
        original_filename="lesson-plan.pdf",
        contents=b"%PDF-1.7 lesson plan",
        mime_type="application/pdf",
    )

    assert saved.storage_key.startswith(f"teacher-assist/resources/{tenant_id}/")
    assert saved.resource_type == "pdf"
    assert isinstance(get_teacher_assist_storage_provider(settings), LocalStorageProvider)
    assert teacher_assist_file_exists(settings, storage_key=saved.storage_key) is True

    stream = open_teacher_assist_stream(settings, storage_key=saved.storage_key)
    try:
        assert stream.read() == b"%PDF-1.7 lesson plan"
    finally:
        stream.close()

    download_url = get_teacher_assist_download_url(
        settings,
        storage_key=saved.storage_key,
        original_filename=saved.original_filename,
        mime_type=saved.mime_type,
    )
    parsed = urlparse(download_url)
    token = unquote(parse_qs(parsed.query)["token"][0])
    resolved = resolve_teacher_assist_local_download(settings, token=token)
    assert resolved.storage_key == saved.storage_key
    assert resolved.original_filename == saved.original_filename
    assert resolved.mime_type == saved.mime_type

    delete_teacher_assist_file(settings, storage_key=saved.storage_key)
    assert teacher_assist_file_exists(settings, storage_key=saved.storage_key) is False


def test_teacher_assist_storage_key_is_tenant_scoped():
    settings = Settings()
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()

    key_a = build_teacher_assist_storage_key(
        settings,
        tenant_id=tenant_a,
        area="student-work",
        original_filename="response.pdf",
    )
    key_b = build_teacher_assist_storage_key(
        settings,
        tenant_id=tenant_b,
        area="student-work",
        original_filename="response.pdf",
    )

    assert key_a.startswith(f"teacher-assist/student-work/{tenant_a}/")
    assert key_b.startswith(f"teacher-assist/student-work/{tenant_b}/")
    assert key_a != key_b


def test_s3_storage_provider_save_presign_open_and_delete(monkeypatch):
    fake_client = FakeS3Client()
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: fake_client)

    settings = Settings(
        teacher_assist_storage_backend="s3",
        teacher_assist_s3_bucket="teacherassist-prod-uploads-test",
        teacher_assist_s3_region="us-east-1",
        teacher_assist_s3_endpoint="http://localhost:4566",
        teacher_assist_s3_presign_expiration_seconds=120,
    )
    provider = get_teacher_assist_storage_provider(settings)

    assert isinstance(provider, S3StorageProvider)

    provider.save_file(
        storage_key="teacher-assist/student-work/tenant-a/upload.pdf",
        contents=b"student work bytes",
        mime_type="application/pdf",
    )
    assert fake_client.put_calls[0]["Bucket"] == "teacherassist-prod-uploads-test"
    assert fake_client.put_calls[0]["Key"] == "teacher-assist/student-work/tenant-a/upload.pdf"
    assert fake_client.put_calls[0]["ContentType"] == "application/pdf"
    assert fake_client.put_calls[0]["ServerSideEncryption"] == "AES256"
    assert (
        provider.file_exists(storage_key="teacher-assist/student-work/tenant-a/upload.pdf") is True
    )

    stream = provider.open_stream(storage_key="teacher-assist/student-work/tenant-a/upload.pdf")
    assert isinstance(stream, io.BytesIO)
    assert stream.read() == b"student work bytes"

    download_url = provider.get_download_url(
        storage_key="teacher-assist/student-work/tenant-a/upload.pdf",
        original_filename="upload.pdf",
        mime_type="application/pdf",
    )
    assert download_url == "https://signed.example/object"
    assert fake_client.presign_calls[0]["operation_name"] == "get_object"
    assert fake_client.presign_calls[0]["ExpiresIn"] == 120
    assert fake_client.presign_calls[0]["Params"]["Bucket"] == "teacherassist-prod-uploads-test"

    provider.delete_file(storage_key="teacher-assist/student-work/tenant-a/upload.pdf")
    assert fake_client.delete_calls[0]["Key"] == "teacher-assist/student-work/tenant-a/upload.pdf"


def test_s3_storage_provider_missing_object_returns_false(monkeypatch):
    fake_client = FakeS3Client()
    monkeypatch.setattr("boto3.client", lambda *args, **kwargs: fake_client)
    settings = Settings(
        teacher_assist_storage_backend="s3",
        teacher_assist_s3_bucket="teacherassist-prod-uploads-test",
    )
    provider = get_teacher_assist_storage_provider(settings)
    assert provider.file_exists(storage_key="teacher-assist/temp/missing.bin") is False


def test_local_download_token_rejects_invalid_payload(tmp_path):
    settings = Settings(
        teacher_assist_storage_backend="local",
        teacher_assist_storage_root=str(tmp_path),
    )
    with pytest.raises(ValueError, match="Invalid or expired"):
        resolve_teacher_assist_local_download(settings, token="bad-token")
