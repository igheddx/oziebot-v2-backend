"""Decode TeacherAssist v2 QR payloads from uploaded PDFs and images."""

from __future__ import annotations

import json
import re

QR_TOKEN_CONTENT_PATTERN = re.compile(r'"qr_token"\s*:\s*"([a-f0-9]{32})"', re.IGNORECASE)
QR_HEX_TOKEN_PATTERN = re.compile(r"\b([a-f0-9]{32})\b", re.IGNORECASE)
STUDENT_HEADER_PATTERN = re.compile(r"Student\s+#(\d+)", re.IGNORECASE)


def parse_qr_identifier_from_content(content: str) -> str | None:
    normalized = content.strip()
    if not normalized:
        return None
    try:
        payload = json.loads(normalized)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        token = payload.get("qr_token")
        if isinstance(token, str):
            cleaned = token.strip().lower()
            if len(cleaned) == 32:
                return cleaned
    match = QR_TOKEN_CONTENT_PATTERN.search(normalized)
    if match is not None:
        return match.group(1).lower()
    hex_match = QR_HEX_TOKEN_PATTERN.search(normalized)
    if hex_match is not None:
        return hex_match.group(1).lower()
    return None


def parse_student_number_from_page_text(text: str) -> int | None:
    match = STUDENT_HEADER_PATTERN.search(text)
    if match is None:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def is_pdf_upload(
    *,
    file_bytes: bytes,
    mime_type: str | None = None,
    original_filename: str | None = None,
) -> bool:
    normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()
    filename = (original_filename or "").lower()
    return normalized_mime == "application/pdf" or filename.endswith(".pdf")


def _decode_qr_strings_from_grayscale(gray, *, first_only: bool = False) -> list[str]:
    import cv2

    if gray is None:
        return []

    detector = cv2.QRCodeDetector()
    decoded: list[str] = []
    scales = (4, 8) if max(gray.shape[:2]) <= 250 else (1, 2, 4)
    for scale in scales:
        scaled = (
            gray
            if scale == 1
            else cv2.resize(
                gray,
                (gray.shape[1] * scale, gray.shape[0] * scale),
                interpolation=cv2.INTER_NEAREST,
            )
        )
        candidates = [scaled]
        _, otsu = cv2.threshold(scaled, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        candidates.append(otsu)
        for candidate in candidates:
            try:
                ok, payloads, _, _ = detector.detectAndDecodeMulti(candidate)
                if ok and payloads:
                    for payload in payloads:
                        if isinstance(payload, str) and payload.strip():
                            if first_only:
                                return [payload.strip()]
                            decoded.append(payload.strip())
            except cv2.error:
                pass
            single, _, _ = detector.detectAndDecode(candidate)
            if isinstance(single, str) and single.strip():
                if first_only:
                    return [single.strip()]
                decoded.append(single.strip())
        if first_only and decoded:
            return decoded
    return list(dict.fromkeys(decoded))


def _decode_qr_strings_from_image(image_bytes: bytes) -> list[str]:
    import cv2
    import numpy as np

    array = np.frombuffer(image_bytes, dtype=np.uint8)
    image = cv2.imdecode(array, cv2.IMREAD_COLOR)
    if image is None:
        return []
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return _decode_qr_strings_from_grayscale(gray)


def decode_qr_strings_from_fitz_page(doc, page) -> list[str]:
    import cv2
    import fitz
    import numpy as np

    image_infos = page.get_images(full=True)
    if not image_infos:
        return []

    for image_info in image_infos:
        extracted = doc.extract_image(image_info[0])
        gray = cv2.imdecode(np.frombuffer(extracted["image"], dtype=np.uint8), cv2.IMREAD_GRAYSCALE)
        decoded = _decode_qr_strings_from_grayscale(gray, first_only=True)
        if decoded:
            return decoded

    clip = fitz.Rect(0, 0, min(page.rect.width, 180), min(page.rect.height, 180))
    for scale in (2, 4):
        pixmap = page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False)
        decoded = _decode_qr_strings_from_image(pixmap.tobytes("png"))
        if decoded:
            return decoded
    return []


def decode_qr_strings_from_pdf_page(page_png_bytes: bytes) -> list[str]:
    return _decode_qr_strings_from_image(page_png_bytes)


def _decode_qr_strings_from_pdf(pdf_bytes: bytes) -> list[str]:
    import fitz

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    decoded: list[str] = []
    try:
        for page in doc:
            for payload in decode_qr_strings_from_fitz_page(doc, page):
                _append_unique(decoded, payload)
    finally:
        doc.close()
    return decoded


def decode_qr_contents_from_file(
    *,
    file_bytes: bytes,
    mime_type: str | None = None,
    original_filename: str | None = None,
) -> list[str]:
    if not file_bytes:
        return []

    normalized_mime = (mime_type or "").split(";", 1)[0].strip().lower()
    filename = (original_filename or "").lower()
    is_pdf = normalized_mime == "application/pdf" or filename.endswith(".pdf")
    is_image = normalized_mime.startswith("image/") or any(
        filename.endswith(suffix)
        for suffix in (".png", ".jpg", ".jpeg", ".webp", ".tif", ".tiff", ".bmp")
    )

    raw_contents: list[str] = []
    if is_pdf:
        raw_contents.extend(_decode_qr_strings_from_pdf(file_bytes))
    elif is_image or not normalized_mime:
        raw_contents.extend(_decode_qr_strings_from_image(file_bytes))
    return raw_contents


def extract_qr_payload_assignment_ids(
    *,
    file_bytes: bytes,
    mime_type: str | None = None,
    original_filename: str | None = None,
) -> set[str]:
    assignment_ids: set[str] = set()
    for content in decode_qr_contents_from_file(
        file_bytes=file_bytes,
        mime_type=mime_type,
        original_filename=original_filename,
    ):
        try:
            payload = json.loads(content.strip())
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and payload.get("assignment_id"):
            assignment_ids.add(str(payload["assignment_id"]))
    return assignment_ids


def validate_upload_qr_assignment_match(
    *,
    file_bytes: bytes,
    mime_type: str | None,
    original_filename: str | None,
    assignment_id,
) -> None:
    try:
        assignment_ids = extract_qr_payload_assignment_ids(
            file_bytes=file_bytes,
            mime_type=mime_type,
            original_filename=original_filename,
        )
    except Exception:
        return
    if not assignment_ids:
        return
    target = str(assignment_id)
    if assignment_ids != {target}:
        raise ValueError(
            "QR codes in this upload belong to a different assignment. "
            "Upload the file on the assignment that matches the student packet."
        )


def extract_qr_identifiers_from_file(
    *,
    file_bytes: bytes,
    mime_type: str | None = None,
    original_filename: str | None = None,
) -> list[str]:
    identifiers: list[str] = []
    for content in decode_qr_contents_from_file(
        file_bytes=file_bytes,
        mime_type=mime_type,
        original_filename=original_filename,
    ):
        token = parse_qr_identifier_from_content(content)
        if token is not None:
            _append_unique(identifiers, token)
    return identifiers
