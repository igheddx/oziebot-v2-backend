from __future__ import annotations

from typing import Any

from oziebot_api.services.teacher_assist.ai_provider import (
    TeacherAssistAIProvider,
    TeacherAssistAIProviderResult,
)
from oziebot_api.services.teacher_assist.instructional_plan_prompt_builder import (
    build_instructional_plan_prompt,
    build_instructional_plan_section_regeneration_prompt,
)
from oziebot_api.services.teacher_assist.prompt_contracts import (
    INSTRUCTIONAL_PLAN_ARTIFACT_SUGGESTIONS,
    INSTRUCTIONAL_PLAN_DAY_LABELS,
    INSTRUCTIONAL_PLAN_PROMPT_VERSION,
    INSTRUCTIONAL_PLAN_SECTION_REGEN_PROMPT_VERSION,
)


def _non_empty(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        if value is None:
            continue
        stripped = value.strip()
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        normalized.append(stripped)
    return normalized


def _day_label(day_number: int) -> str:
    return INSTRUCTIONAL_PLAN_DAY_LABELS.get(day_number, f"Day {day_number}")


def _subject_vocabulary(subject_name: str) -> list[str]:
    return [
        f"[MOCK OUTPUT] {subject_name} anchor term",
        f"[MOCK OUTPUT] {subject_name} academic language",
        "[MOCK OUTPUT] evidence-based explanation",
    ]


def _build_daily_breakdown(
    *,
    subject_name: str,
    pacing_items: list[dict[str, Any]],
    teacher_notes: str | None,
) -> list[dict[str, Any]]:
    if pacing_items:
        breakdown: list[dict[str, Any]] = []
        for index, item in enumerate(pacing_items[:5], start=1):
            day_number = item.get("day_number") or index
            focus = item.get("title") or f"Mock {subject_name} focus {index}"
            objective = item.get("objectives") or item.get("instructional_focus") or "Reinforce weekly goals"
            note = item.get("notes") or teacher_notes or "No additional teacher notes provided."
            breakdown.append(
                {
                    "day": day_number,
                    "day_label": _day_label(day_number),
                    "focus": f"[MOCK OUTPUT] {focus}",
                    "teacher_actions": [
                        f"Model the {subject_name} focus for '{focus}'.",
                        f"Connect instruction to the weekly objective: {objective}.",
                    ],
                    "student_activities": [
                        f"Complete a guided {subject_name.lower()} task aligned to '{focus}'.",
                        f"Capture learning evidence connected to: {note}",
                    ],
                    "checks_for_understanding": [
                        f"Exit ticket on {focus}.",
                        f"Teacher observation notes for {subject_name}.",
                    ],
                    "materials_needed": [
                        f"{subject_name} teacher exemplar",
                        "Student notebook",
                        "Resource library support material",
                    ],
                }
            )
        return breakdown

    return [
        {
            "day": day,
            "day_label": _day_label(day),
            "focus": f"[MOCK OUTPUT] {subject_name} weekly focus day {day}",
            "teacher_actions": [
                f"Preview the mock weekly objective for {subject_name}.",
                "Model one key concept before guided practice.",
            ],
            "student_activities": [
                "Complete a short warm-up tied to the selected standards.",
                "Work through a mock independent practice task.",
            ],
            "checks_for_understanding": [
                "Quick turn-and-talk summary.",
                "Short written check for understanding.",
            ],
            "materials_needed": [
                "Anchor chart",
                "Student notebook",
                "Mock practice resource",
            ],
        }
        for day in range(1, 6)
    ]


def _mock_regenerated_section(
    *,
    current_plan_content: dict[str, Any],
    section_key: str,
    section_path: str | None,
    current_section_content: Any,
    teacher_instruction: str | None,
) -> Any:
    instruction_suffix = (
        f" Teacher note: {teacher_instruction.strip()}."
        if isinstance(teacher_instruction, str) and teacher_instruction.strip()
        else ""
    )
    if section_key == "overview":
        return (
            "[MOCK OUTPUT] Regenerated overview with stronger teacher-facing clarity."
            + instruction_suffix
        )
    if section_key == "review_notes":
        return "[MOCK OUTPUT] Review this regenerated section before use." + instruction_suffix
    if section_key == "instructional_arc":
        return [
            "[MOCK OUTPUT] Re-launch prior knowledge with a concise teacher model.",
            "[MOCK OUTPUT] Guide practice with checks for understanding and classroom pacing.",
            "[MOCK OUTPUT] Close with independent evidence and teacher reflection."
            + instruction_suffix,
        ]
    if section_key == "vocabulary":
        return [
            "[MOCK OUTPUT] anchor vocabulary",
            "[MOCK OUTPUT] academic language",
            "[MOCK OUTPUT] content-specific discussion term",
        ]
    if section_key == "materials_needed":
        return [
            "[MOCK OUTPUT] Teacher exemplar",
            "[MOCK OUTPUT] Student practice resource",
            "[MOCK OUTPUT] Visual scaffold",
        ]
    if section_key == "assessment_checkpoints":
        return [
            "[MOCK OUTPUT] Entry check aligned to lesson goals.",
            "[MOCK OUTPUT] Mid-lesson teacher observation checkpoint.",
            "[MOCK OUTPUT] Exit ticket or short demonstration task.",
        ]
    if section_key == "standards_progression":
        return [
            {
                "code": "MOCK-1",
                "description": "[MOCK OUTPUT] Core standard emphasis.",
                "phase": "Introduce and model.",
            },
            {
                "code": "MOCK-2",
                "description": "[MOCK OUTPUT] Application emphasis.",
                "phase": "Guided practice to independent transfer.",
            },
        ]
    if section_key == "differentiation":
        return {
            "support": ["[MOCK OUTPUT] Use guided supports before independent work."],
            "extension": ["[MOCK OUTPUT] Add a deeper application task with justification."],
            "intervention": ["[MOCK OUTPUT] Pull a short reteach group using formative evidence."],
        }
    if section_key == "weekly_segments" and section_path:
        current_segment = dict(current_section_content or {})
        segment_label = current_segment.get("segment_label") or "Week Segment"
        return {
            **current_segment,
            "focus": f"[MOCK OUTPUT] Refined focus for {segment_label}.{instruction_suffix}".strip(),
            "objectives": [
                "[MOCK OUTPUT] Clarify the segment goal.",
                "[MOCK OUTPUT] Practice and demonstrate mastery.",
            ],
            "assessment_checkpoints": [
                f"[MOCK OUTPUT] Checkpoint for {segment_label}.",
            ],
        }
    if section_key == "weekly_segments":
        current_segments = list(current_section_content or current_plan_content.get("weekly_segments") or [])
        if not current_segments:
            return [
                {
                    "segment_index": 1,
                    "segment_label": "Week 1",
                    "focus": "[MOCK OUTPUT] Regenerated weekly segment focus.",
                    "objectives": ["[MOCK OUTPUT] Weekly objective"],
                    "subjects": [],
                    "daily_breakdown": [],
                    "assessment_checkpoints": ["[MOCK OUTPUT] Weekly checkpoint"],
                }
            ]
        regenerated = []
        for index, segment in enumerate(current_segments, start=1):
            updated_segment = dict(segment)
            updated_segment["focus"] = (
                f"[MOCK OUTPUT] Regenerated {segment.get('segment_label') or f'Week {index}'} focus."
            )
            regenerated.append(updated_segment)
        return regenerated
    if section_key == "daily_breakdown":
        current_day = dict(current_section_content or {})
        day_label = current_day.get("day_label") or "Day"
        return {
            **current_day,
            "focus": f"[MOCK OUTPUT] Refined {day_label} focus.{instruction_suffix}".strip(),
            "teacher_actions": [
                "[MOCK OUTPUT] Model the concept clearly.",
                "[MOCK OUTPUT] Check for understanding before release.",
            ],
            "student_activities": [
                "[MOCK OUTPUT] Complete guided practice.",
                "[MOCK OUTPUT] Show independent evidence of learning.",
            ],
            "checks_for_understanding": [
                "[MOCK OUTPUT] Quick verbal check.",
                "[MOCK OUTPUT] Written exit check.",
            ],
            "materials_needed": [
                "[MOCK OUTPUT] Student notebook",
                "[MOCK OUTPUT] Teacher exemplar",
            ],
        }
    raise ValueError(f"Unsupported mock section regeneration key '{section_key}'")


class MockTeacherAssistAIProvider(TeacherAssistAIProvider):
    provider_name = "mock"

    def generate_instructional_plan(self, context_preview: dict[str, Any]) -> TeacherAssistAIProviderResult:
        draft = context_preview["draft"]
        subjects = context_preview.get("subjects", [])
        pacing_items = context_preview.get("pacing_items", [])
        pacing_groups = context_preview.get("pacing_groups", [])
        standards = context_preview.get("standards", [])
        resources = context_preview.get("resources", [])
        teacher_notes = context_preview.get("teacher_notes")
        planning_scope = draft.get("planning_scope", "weekly")
        duration_summary = context_preview.get("duration_summary", {})
        prompt_payload = build_instructional_plan_prompt(context_preview)

        subject_sections: list[dict[str, Any]] = []
        weekly_objectives = _non_empty(
            [
                item.get("objectives") or item.get("instructional_focus") or item.get("title")
                for item in pacing_items
            ]
        )
        for subject in subjects:
            subject_pacing_items = [
                item for item in pacing_items if item.get("subject_id") in (subject["id"], None)
            ]
            subject_standards = [
                standard
                for standard in standards
                if standard.get("subject_id") in (subject["id"], None)
            ]
            subject_sections.append(
                {
                    "subject_id": subject["id"],
                    "subject_name": subject["name"],
                    "standards": [
                        {
                            "id": standard["id"],
                            "code": standard["code"],
                            "description": standard["description"],
                        }
                        for standard in subject_standards
                    ],
                    "objectives": _non_empty(
                        [
                            item.get("objectives") or item.get("instructional_focus") or item.get("title")
                            for item in subject_pacing_items
                        ]
                    )
                    or [f"[MOCK OUTPUT] Reinforce weekly {subject['name']} goals."],
                    "vocabulary": _subject_vocabulary(subject["name"]),
                    "daily_breakdown": _build_daily_breakdown(
                        subject_name=subject["name"],
                        pacing_items=subject_pacing_items,
                        teacher_notes=teacher_notes,
                    ),
                    "differentiation": {
                        "support": [
                            f"[MOCK OUTPUT] Provide sentence stems during {subject['name']} discussion.",
                            "[MOCK OUTPUT] Use guided small-group reteach before independent work.",
                        ],
                        "extension": [
                            f"[MOCK OUTPUT] Add a higher-order application task for {subject['name']}.",
                            "[MOCK OUTPUT] Invite students to justify reasoning with evidence.",
                        ],
                        "visual_supports": [
                            "[MOCK OUTPUT] Anchor chart",
                            "[MOCK OUTPUT] Modeled exemplar",
                        ],
                    },
                    "suggested_artifacts": list(INSTRUCTIONAL_PLAN_ARTIFACT_SUGGESTIONS),
                }
            )

        subject_names = ", ".join(subject["name"] for subject in subjects) or "selected subjects"
        draft_title = draft.get("plan_title") or draft.get("title") or "TeacherAssist Instructional Plan"
        standards_progression = [
            {
                "code": standard["code"],
                "description": standard["description"],
                "phase": f"Reinforce during {planning_scope.replace('_', ' ')} planning",
            }
            for standard in standards[:8]
        ]
        materials_needed = _non_empty(
            [
                material
                for section in subject_sections
                for day in section.get("daily_breakdown", [])
                for material in day.get("materials_needed", [])
            ]
        )[:10]
        weekly_segments = []
        grouped_items = pacing_groups or [{"label": "Week 1", "pacing_items": pacing_items}]
        for index, group in enumerate(grouped_items, start=1):
            group_items = group.get("pacing_items", [])
            segment_objectives = _non_empty(
                [
                    item.get("objectives") or item.get("instructional_focus") or item.get("title")
                    for item in group_items
                ]
            ) or weekly_objectives[:2]
            segment_subjects = []
            for section in subject_sections:
                segment_subjects.append(
                    {
                        "subject_id": section.get("subject_id"),
                        "subject_name": section.get("subject_name"),
                        "objectives": section.get("objectives", [])[:3],
                        "daily_breakdown": section.get("daily_breakdown", [])[:5],
                    }
                )
            weekly_segments.append(
                {
                    "segment_index": index,
                    "segment_label": group.get("label") or f"Week {index}",
                    "focus": f"[MOCK OUTPUT] {group.get('label') or f'Week {index}'} focus for {draft_title}.",
                    "objectives": segment_objectives,
                    "subjects": segment_subjects,
                    "daily_breakdown": [
                        day
                        for section in segment_subjects
                        for day in section.get("daily_breakdown", [])[:2]
                    ][:5],
                    "assessment_checkpoints": [
                        f"[MOCK OUTPUT] Teacher checkpoint for {group.get('label') or f'Week {index}'}."
                    ],
                }
            )

        content_json = {
            "planning_scope": planning_scope,
            "plan_title": draft_title,
            "module_title": draft.get("module_title"),
            "duration": {
                "start_date": draft.get("start_date"),
                "end_date": draft.get("end_date"),
                "estimated_weeks": draft.get("estimated_weeks") or duration_summary.get("estimated_weeks"),
                "instructional_days_count": draft.get("instructional_days_count")
                or duration_summary.get("instructional_days_count"),
                "summary": duration_summary.get("summary"),
            },
            "overview": (
                f"[MOCK OUTPUT] Instructional plan for '{draft_title}' covering {subject_names}. "
                "This is deterministic mock generation content for teacher review only."
            ),
            "instructional_arc": [
                "[MOCK OUTPUT] Launch prior knowledge and align expectations.",
                "[MOCK OUTPUT] Guided instruction and collaborative practice.",
                "[MOCK OUTPUT] Independent demonstration and review.",
            ],
            "weekly_objectives": weekly_objectives
            or ["[MOCK OUTPUT] Reinforce the week with a clear teacher-facing objective sequence."],
            "subjects": subject_sections,
            "weekly_segments": weekly_segments,
            "standards_progression": standards_progression,
            "vocabulary": _non_empty(
                [entry for section in subject_sections for entry in section.get("vocabulary", [])]
            )[:10],
            "materials_needed": materials_needed,
            "differentiation": {
                "support": [
                    "[MOCK OUTPUT] Use a small-group reteach checkpoint before independent practice."
                ],
                "extension": [
                    "[MOCK OUTPUT] Ask students to justify reasoning with evidence and examples."
                ],
                "intervention": [
                    "[MOCK OUTPUT] Pull a focused intervention group using formative evidence."
                ],
            },
            "assessment_checkpoints": [
                "[MOCK OUTPUT] Entry check aligned to selected standards.",
                "[MOCK OUTPUT] Mid-plan teacher conference note.",
                "[MOCK OUTPUT] End-of-segment exit ticket placeholder.",
            ],
            "resources_used": [
                {
                    "id": resource["id"],
                    "title": resource["title"],
                    "resource_type": resource["resource_type"],
                }
                for resource in resources
            ],
            "teacher_notes_used": teacher_notes
            or "[MOCK OUTPUT] No teacher notes were supplied; this placeholder reflects the saved draft context.",
            "review_notes": "",
            "daily_breakdown": weekly_segments[0]["daily_breakdown"] if weekly_segments else [],
        }
        return TeacherAssistAIProviderResult(
            content_json=content_json,
            provider=self.provider_name,
            model="mock",
            input_tokens=0,
            output_tokens=0,
            estimated_cost_cents=0,
            metadata_json={
                "is_mock": True,
                "provider_mode": "mock",
                "prompt_version": INSTRUCTIONAL_PLAN_PROMPT_VERSION,
                "prompt_summary": {
                    "planning_scope": prompt_payload["planning_scope"],
                    "standards_count": len(prompt_payload.get("standards", [])),
                    "resource_count": len(prompt_payload.get("resources", [])),
                },
            },
        )

    def regenerate_instructional_plan_section(
        self,
        *,
        context_preview: dict[str, Any],
        current_plan_content: dict[str, Any],
        section_key: str,
        section_path: str | None = None,
        current_section_content: Any = None,
        teacher_instruction: str | None = None,
        preserve_existing_context: bool = True,
    ) -> TeacherAssistAIProviderResult:
        prompt_payload = build_instructional_plan_section_regeneration_prompt(
            context_preview=context_preview,
            current_plan_content=current_plan_content,
            section_key=section_key,
            section_path=section_path,
            current_section_content=current_section_content if preserve_existing_context else None,
            teacher_instruction=teacher_instruction,
            preserve_existing_context=preserve_existing_context,
        )
        section_content = _mock_regenerated_section(
            current_plan_content=current_plan_content,
            section_key=section_key,
            section_path=section_path,
            current_section_content=current_section_content if preserve_existing_context else None,
            teacher_instruction=teacher_instruction,
        )
        return TeacherAssistAIProviderResult(
            content_json={"section_content": section_content},
            provider=self.provider_name,
            model="mock",
            input_tokens=0,
            output_tokens=0,
            estimated_cost_cents=0,
            metadata_json={
                "is_mock": True,
                "provider_mode": "mock",
                "prompt_version": INSTRUCTIONAL_PLAN_SECTION_REGEN_PROMPT_VERSION,
                "prompt_summary": {
                    "section_key": section_key,
                    "section_path": section_path,
                    "preserve_existing_context": preserve_existing_context,
                    "planning_scope": prompt_payload["planning_scope"],
                },
            },
        )
