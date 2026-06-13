from __future__ import annotations

from oziebot_api.services.teacher_assist.storage import build_content_disposition


def test_build_content_disposition_uses_ascii_fallback_for_unicode_titles() -> None:
    header = build_content_disposition("Week 1 — ELA — ELA Quiz Preview Answer Key.html")

    assert 'filename="Week 1 _ ELA _ ELA Quiz Preview Answer Key.html"' in header
    assert "filename*=UTF-8''Week%201%20%E2%80%94%20ELA" in header
    assert header.startswith("attachment;")
    header.encode("latin-1")


def test_build_content_disposition_supports_inline_preview() -> None:
    header = build_content_disposition("student-001.pdf", inline=True)
    assert header.startswith('inline; filename="student-001.pdf"')
