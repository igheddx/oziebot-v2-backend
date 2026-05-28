from __future__ import annotations

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ocr_provider import TeacherAssistOCRProviderResult


def _deterministic_mock_confidence(file_bytes: bytes) -> tuple[float, str]:
    sample = file_bytes[:64] if file_bytes else b""
    score_basis = sum(sample) if sample else 42
    score = round((score_basis % 100) / 100.0, 4)
    if score < 0.4:
        return score, "low"
    if score < 0.75:
        return score, "medium"
    return score, "high"


class MockTeacherAssistOCRProvider:
    provider_name = "mock"

    def extract_text(
        self,
        *,
        artifact_type: str,
        mime_type: str,
        original_filename: str,
        file_bytes: bytes,
        settings: Settings,
    ) -> TeacherAssistOCRProviderResult:
        text_limit = max(200, int(settings.teacher_assist_ocr_mock_text_limit))
        artifact_label = "student-work" if artifact_type == "student_work" else "resource"
        placeholder = (
            f"[MOCK OCR] TeacherAssist extracted a {artifact_label} artifact for teacher review.\n"
            f"[MOCK OCR] MIME type: {mime_type or 'application/octet-stream'}\n"
            f"[MOCK OCR] Bytes read through storage abstraction: {len(file_bytes)}\n"
            "[MOCK OCR] Real OCR remains disabled unless explicitly enabled in server configuration."
        )
        confidence_score, confidence_level = _deterministic_mock_confidence(file_bytes)
        return TeacherAssistOCRProviderResult(
            extracted_text=placeholder[:text_limit],
            provider=self.provider_name,
            model="mock-ocr",
            metadata_json={
                "artifact_type": artifact_type,
                "mime_type": mime_type or "application/octet-stream",
                "original_filename_present": bool(original_filename),
                "bytes_read": len(file_bytes),
                "is_mock": True,
                "provider_mode": "mock",
                "provider_version": "mock-ocr",
                "page_count": 1,
                "estimated_cost_cents": 0,
                "provider_confidence_score": confidence_score,
                "confidence_level": confidence_level,
            },
        )
