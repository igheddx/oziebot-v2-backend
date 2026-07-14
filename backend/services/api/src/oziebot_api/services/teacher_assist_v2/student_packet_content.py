"""Split assessment artifact content into printable pages (before per-student duplication)."""

from __future__ import annotations

from typing import Any


def _paragraphs(*items: tuple[str, bool]) -> list[tuple[str, bool]]:
    return list(items)


def compute_pages_per_student(*, artifact_type: str, content: dict[str, Any]) -> int:
    explicit = content.get("pages_per_student")
    if explicit is not None:
        value = int(explicit)
        return max(1, value)
    return len(split_assessment_content_pages(artifact_type=artifact_type, content=content))


def split_assessment_content_pages(
    *, artifact_type: str, content: dict[str, Any]
) -> list[list[tuple[str, bool]]]:
    if artifact_type == "quiz":
        return _split_quiz_pages(content)
    if artifact_type == "assignment":
        return _split_written_assignment_pages(content)
    if artifact_type == "writing_response":
        return _split_writing_response_pages(content)
    title = str(content.get("title") or "Assessment")
    return [_paragraphs((title, True))]


def _split_quiz_pages(content: dict[str, Any]) -> list[list[tuple[str, bool]]]:
    questions_per_page = max(1, int(content.get("questions_per_page") or 3))
    pages: list[list[tuple[str, bool]]] = []

    intro: list[tuple[str, bool]] = []
    instructions = (
        content.get("instructions") or content.get("summary") or content.get("description")
    )
    if instructions:
        intro.extend([("Instructions", True), (str(instructions), False), ("", False)])
    pages.append(intro if intro else [("", False)])

    questions = list(content.get("questions") or [])
    if not questions:
        return pages

    for chunk_start in range(0, len(questions), questions_per_page):
        chunk = questions[chunk_start : chunk_start + questions_per_page]
        page: list[tuple[str, bool]] = []
        for question in chunk:
            number = question.get("number")
            points = question.get("points", 1)
            page.append(
                (f"Question {number} ({points} point{'s' if int(points or 1) != 1 else ''})", True)
            )
            page.append((str(question.get("prompt") or ""), False))
            q_type = str(question.get("type") or "multiple_choice")
            if q_type == "multiple_choice":
                for index, choice in enumerate(question.get("choices") or [], start=1):
                    letter = chr(64 + index) if index <= 26 else str(index)
                    page.append((f"{letter}. {choice}", False))
            else:
                page.append(("Response:", False))
                for _ in range(int(question.get("response_lines") or 4)):
                    page.append(("", False))
            page.append(("", False))
        pages.append(page)
    return pages


def _split_written_assignment_pages(content: dict[str, Any]) -> list[list[tuple[str, bool]]]:
    lines_per_page = max(1, int(content.get("writing_lines_per_page") or 6))
    writing_lines = max(1, int(content.get("writing_lines") or 12))

    page_one: list[tuple[str, bool]] = []
    instructions = content.get("student_instructions") or []
    if instructions:
        page_one.append(("Student Instructions", True))
        for item in instructions:
            page_one.append((f"• {item}", False))
        page_one.append(("", False))
    criteria = content.get("success_criteria") or []
    if criteria:
        page_one.append(("Success Criteria", True))
        for item in criteria:
            page_one.append((f"• {item}", False))
        page_one.append(("", False))
    if content.get("passage_title") or content.get("passage_text"):
        page_one.append((str(content.get("passage_title") or "Reading Passage"), True))
        if content.get("passage_text"):
            page_one.append((str(content.get("passage_text")), False))
        page_one.append(("", False))
    pages = [page_one if page_one else [("", False)]]

    remaining = writing_lines
    while remaining > 0:
        chunk = min(lines_per_page, remaining)
        writing_page: list[tuple[str, bool]] = [("Your Response", True), ("", False)]
        for _ in range(chunk):
            writing_page.append(("", False))
        pages.append(writing_page)
        remaining -= chunk
    return pages


def _split_writing_response_pages(content: dict[str, Any]) -> list[list[tuple[str, bool]]]:
    lines_per_page = max(1, int(content.get("writing_lines_per_page") or 12))
    response_pages = max(1, int(content.get("response_pages") or 1))
    pages: list[list[tuple[str, bool]]] = []

    intro: list[tuple[str, bool]] = []
    if content.get("prompt"):
        intro.extend([(str(content.get("prompt")), False), ("", False)])
    if content.get("instructions"):
        for item in content.get("instructions") or []:
            intro.append((f"• {item}", False))
        intro.append(("", False))
    pages.append(intro if intro else [("", False)])

    for _ in range(response_pages):
        writing_page: list[tuple[str, bool]] = [("Write your response below.", False), ("", False)]
        for _ in range(lines_per_page):
            writing_page.append(("", False))
        pages.append(writing_page)
    return pages
