"""Seed Phase 39 Teacher Copilot demo sessions for Texas Grade 5."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import get_settings
from oziebot_api.models.teacher_copilot_session import TeacherCopilotMessage, TeacherCopilotSession
from oziebot_api.scripts.seed_instructional_loop import seed_instructional_loop
from oziebot_api.scripts.seed_pacing_guides import _seed_actor
from oziebot_api.services.teacher_assist.constants import COPILOT_FEATURE
from oziebot_api.services.teacher_assist.teacher_context_engine import build_teacher_context
from oziebot_api.services.teacher_assist.teacher_copilot_intents import analyze_copilot_question
from oziebot_api.services.teacher_assist.teacher_copilot_service import _json_safe


SAMPLE_QUESTIONS = [
    "What objectives need reteaching?",
    "Which students need support?",
    "Summarize this week.",
    "What resources should I use?",
    "Create small groups.",
]


def seed_teacher_copilot(db: Session) -> dict[str, int]:
    counts = {"sessions": 0, "messages": 0}
    seed_instructional_loop(db)
    actor, tenant_id = _seed_actor(db)
    settings = get_settings()
    context = build_teacher_context(
        db,
        settings=settings,
        tenant_id=tenant_id,
        user=actor,
    )
    now = datetime.now(UTC)

    existing = db.scalars(
        select(TeacherCopilotSession).where(
            TeacherCopilotSession.tenant_id == tenant_id,
            TeacherCopilotSession.teacher_id == actor.id,
            TeacherCopilotSession.title == "Mason Elementary — Week 1 review",
        )
    ).first()
    if existing is not None:
        return counts

    session = TeacherCopilotSession(
        tenant_id=tenant_id,
        teacher_id=actor.id,
        title="Mason Elementary — Week 1 review",
        created_at=now,
        updated_at=now,
    )
    db.add(session)
    db.flush()
    counts["sessions"] += 1

    for question in SAMPLE_QUESTIONS:
        analysis = analyze_copilot_question(question=question, context=context)
        teacher_at = now
        db.add(
            TeacherCopilotMessage(
                tenant_id=tenant_id,
                session_id=session.id,
                role="teacher",
                content=question,
                context_snapshot={
                    "context_packet_keys": list((context.get("context_packets") or {}).keys())
                },
                created_at=teacher_at,
            )
        )
        counts["messages"] += 1
        db.add(
            TeacherCopilotMessage(
                tenant_id=tenant_id,
                session_id=session.id,
                role="assistant",
                content=analysis.get("answer") or "",
                context_snapshot=_json_safe(
                    {
                        "analysis": analysis,
                        "context_packets": context.get("context_packets"),
                        "audit": {
                            "prompt": question,
                            "intent": analysis.get("intent"),
                            "provider": "mock",
                            "feature": COPILOT_FEATURE,
                            "timestamp": teacher_at.isoformat(),
                        },
                    }
                ),
                created_at=teacher_at,
            )
        )
        counts["messages"] += 1

    session.updated_at = now
    db.flush()
    return counts


def main() -> None:
    from oziebot_api.db.session import SessionLocal

    with SessionLocal() as session:
        counts = seed_teacher_copilot(session)
        session.commit()
        print(f"Teacher Copilot seed complete: {counts}")


if __name__ == "__main__":
    main()
