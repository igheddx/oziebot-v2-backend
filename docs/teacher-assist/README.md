# TeacherAssist AI

TeacherAssist is Oziebot's instructional planning and classroom operations product for teachers.

## Quick links

| Doc | Purpose |
|-----|---------|
| [PHASE_STATUS.md](./PHASE_STATUS.md) | Implemented phases 1–41 |
| [FEATURE_INVENTORY.md](./FEATURE_INVENTORY.md) | Product completion review |
| [PILOT_READINESS.md](./PILOT_READINESS.md) | Teacher pilot checklist |
| [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md) | AWS / env / migrations |
| [PRODUCTION_CHECKLIST.md](./PRODUCTION_CHECKLIST.md) | Pre-production validation |
| [KNOWN_LIMITATIONS.md](./KNOWN_LIMITATIONS.md) | Deferred capabilities |
| [ARCHITECTURE.md](./ARCHITECTURE.md) | System design |

## Local development

```bash
# API (from backend/services/api)
export DATABASE_URL="postgresql+psycopg://..."
alembic upgrade head
python3 -m uvicorn oziebot_api.main:app --host 127.0.0.1 --port 8000 --app-dir src --reload

# Frontend (from frontend/apps/web)
npm run dev -- --port 3000
```

Open http://localhost:3000/teacher-assist/home

## Pilot seed stack

```bash
python3 -m oziebot_api.scripts.seed_education_catalog
python3 -m oziebot_api.scripts.seed_pacing_guides
python3 -m oziebot_api.scripts.seed_instructional_weeks
python3 -m oziebot_api.scripts.seed_instructional_loop
python3 -m oziebot_api.scripts.seed_teacher_copilot
python3 -m oziebot_api.scripts.seed_teacher_assist_access
```

## Primary teacher workflow

Home → Instructional Weeks → Pacing Guides → Assignments → Mastery → Resources → Communication → Copilot

Feedback during pilot: `/teacher-assist/feedback`
