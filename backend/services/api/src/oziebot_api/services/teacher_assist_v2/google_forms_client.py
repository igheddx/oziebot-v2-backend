"""Google Forms API client for TeacherAssist v2."""

from __future__ import annotations

import re
from typing import Any

import httpx

FORMS_API = "https://forms.googleapis.com/v1"


def _headers(access_token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {access_token}"}


def _raise_for_status(response: httpx.Response, *, action: str) -> None:
    if response.status_code < 400:
        return
    detail = response.text[:500]
    raise ValueError(f"Google Forms API {action} failed ({response.status_code}): {detail}")


def student_number_label(number: int) -> str:
    return f"Student #{number:03d}"


def parse_student_number_from_text(text: str) -> int | None:
    match = re.search(r"#?\s*0*(\d+)", text or "")
    if not match:
        return None
    value = int(match.group(1))
    return value if value > 0 else None


def build_assignment_description(
    *,
    assignment_id: str,
    package_id: str,
    subject_name: str,
    grade_label: str,
    objectives: list[str],
) -> str:
    objective_lines = "\n".join(f"- {item}" for item in objectives[:5]) or "- See TeacherAssist package"
    return (
        f"TeacherAssist Assignment ID: {assignment_id}\n"
        f"Instructional Package ID: {package_id}\n"
        f"Subject: {subject_name}\n"
        f"Grade: {grade_label}\n"
        f"Objectives:\n{objective_lines}\n\n"
        "Instructions: Select your student number, then complete all quiz questions. "
        "Assign this form through Google Classroom manually if needed."
    )


def create_assignment_quiz_form(
    access_token: str,
    *,
    title: str,
    description: str,
    student_count: int,
    questions: list[dict[str, Any]],
) -> dict[str, Any]:
    create_resp = httpx.post(
        f"{FORMS_API}/forms",
        headers=_headers(access_token),
        json={"info": {"title": title[:200]}},
        timeout=60.0,
    )
    _raise_for_status(create_resp, action="create form")
    form = create_resp.json()
    form_id = form["formId"]
    responder_uri = form.get("responderUri") or f"https://docs.google.com/forms/d/{form_id}/viewform"
    edit_url = f"https://docs.google.com/forms/d/{form_id}/edit"
    responses_url = f"https://docs.google.com/forms/d/{form_id}/viewanalytics"

    requests: list[dict[str, Any]] = [
        {
            "updateFormInfo": {
                "info": {"description": description[:5000]},
                "updateMask": "description",
            }
        },
        {
            "updateSettings": {
                "settings": {"quizSettings": {"isQuiz": True}},
                "updateMask": "quizSettings.isQuiz",
            }
        },
    ]

    student_options = [{"value": student_number_label(number)} for number in range(1, student_count + 1)]
    requests.append(
        {
            "createItem": {
                "item": {
                    "title": "Student Number",
                    "questionItem": {
                        "question": {
                            "required": True,
                            "choiceQuestion": {
                                "type": "RADIO",
                                "options": student_options,
                            },
                        }
                    },
                },
                "location": {"index": 0},
            }
        }
    )

    question_mapping: list[dict[str, Any]] = [
        {"teacher_assist_number": None, "google_item_title": "Student Number", "role": "student_number"}
    ]
    index = 1
    for question in questions:
        prompt = str(question.get("prompt") or f"Question {question.get('number')}")
        q_type = str(question.get("type") or "multiple_choice")
        points = int(question.get("points") or 1)
        mapping_entry = {
            "teacher_assist_number": question.get("number"),
            "google_item_title": prompt[:500],
            "role": "quiz",
            "type": q_type,
        }

        if q_type == "multiple_choice":
            choices = [str(choice) for choice in question.get("choices") or [] if str(choice).strip()]
            if not choices:
                continue
            correct = str(question.get("answer") or choices[0])
            item_body: dict[str, Any] = {
                "title": prompt[:500],
                "questionItem": {
                    "question": {
                        "required": True,
                        "grading": {
                            "pointValue": points,
                            "correctAnswers": {"answers": [{"value": correct}]},
                        },
                        "choiceQuestion": {
                            "type": "RADIO",
                            "options": [{"value": choice[:300]} for choice in choices],
                        },
                    }
                },
            }
            mapping_entry["correct_answer"] = correct
            mapping_entry["points"] = points
        else:
            item_body = {
                "title": prompt[:500],
                "questionItem": {
                    "question": {
                        "required": False,
                        "textQuestion": {"paragraph": q_type in {"short_answer", "evidence_based"}},
                    }
                },
            }
            mapping_entry["auto_graded"] = False

        requests.append({"createItem": {"item": item_body, "location": {"index": index}}})
        question_mapping.append(mapping_entry)
        index += 1

    batch_resp = httpx.post(
        f"{FORMS_API}/forms/{form_id}:batchUpdate",
        headers=_headers(access_token),
        json={"requests": requests},
        timeout=120.0,
    )
    _raise_for_status(batch_resp, action="batch update form")

    return {
        "google_form_id": form_id,
        "google_form_url": responder_uri,
        "google_edit_url": edit_url,
        "google_response_url": responses_url,
        "question_mapping": question_mapping,
    }


def list_form_responses(access_token: str, *, form_id: str) -> list[dict[str, Any]]:
    responses: list[dict[str, Any]] = []
    page_token: str | None = None
    while True:
        params: dict[str, str] = {}
        if page_token:
            params["pageToken"] = page_token
        resp = httpx.get(
            f"{FORMS_API}/forms/{form_id}/responses",
            headers=_headers(access_token),
            params=params,
            timeout=60.0,
        )
        _raise_for_status(resp, action="list responses")
        payload = resp.json()
        responses.extend(payload.get("responses") or [])
        page_token = payload.get("nextPageToken")
        if not page_token:
            break
    return responses


def get_form_with_questions(access_token: str, *, form_id: str) -> dict[str, Any]:
    resp = httpx.get(f"{FORMS_API}/forms/{form_id}", headers=_headers(access_token), timeout=60.0)
    _raise_for_status(resp, action="get form")
    return resp.json()


def extract_response_rows(
    form_payload: dict[str, Any],
    responses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    items = form_payload.get("items") or []
    item_title_by_id = {item.get("itemId"): item.get("title") for item in items if item.get("itemId")}
    student_item_id = None
    for item in items:
        title = str(item.get("title") or "")
        if title.strip().lower() == "student number":
            student_item_id = item.get("itemId")
            break

    rows: list[dict[str, Any]] = []
    for response in responses:
        answers = response.get("answers") or {}
        student_number = None
        if student_item_id and student_item_id in answers:
            text_answers = answers[student_item_id].get("textAnswers", {}).get("answers") or []
            if text_answers:
                student_number = parse_student_number_from_text(str(text_answers[0].get("value") or ""))

        score = response.get("totalScore")
        if score is None:
            score = response.get("grade")
        if isinstance(score, dict):
            score = score.get("score")

        max_score = response.get("maxScore")
        if max_score is None and isinstance(response.get("grade"), dict):
            max_score = response["grade"].get("maxScore")

        rows.append(
            {
                "google_response_id": response.get("responseId"),
                "student_number": student_number,
                "score": float(score) if score is not None else None,
                "max_score": float(max_score) if max_score is not None else None,
                "submitted_at": response.get("lastSubmittedTime"),
                "answers": {
                    item_title_by_id.get(question_id, question_id): answer
                    for question_id, answer in answers.items()
                },
            }
        )
    return rows
