from __future__ import annotations

from dataclasses import dataclass
import re

from oziebot_api.services.teacher_assist.instructional_plan_validator import contains_pii_like_content

PREVIEW_TEXT_LIMIT = 280
POTENTIAL_PII_RE = re.compile(
    r"\b(?:student\s*name|parent\s*name|guardian|mr\.|mrs\.|ms\.|phone|email)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class SanitizedExtractedText:
    extracted_text: str
    preview_text: str
    text_char_count: int
    pii_flagged: bool
    redaction_applied: bool


def sanitize_extracted_text(raw_text: str) -> SanitizedExtractedText:
    normalized = " ".join((raw_text or "").split())
    pii_flagged = contains_pii_like_content({"extracted_text": normalized}) or bool(
        POTENTIAL_PII_RE.search(normalized)
    )
    redaction_applied = False
    if pii_flagged:
        normalized = (
            "[REDACTED] Potential PII-like content was detected in extracted text. "
            "Review the original artifact through authenticated TeacherAssist flows."
        )
        redaction_applied = True
    preview_text = normalized[:PREVIEW_TEXT_LIMIT]
    return SanitizedExtractedText(
        extracted_text=normalized,
        preview_text=preview_text,
        text_char_count=len(normalized),
        pii_flagged=pii_flagged,
        redaction_applied=redaction_applied,
    )
