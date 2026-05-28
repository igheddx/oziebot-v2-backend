# TeacherAssist AI Product Requirements

## Scope

TeacherAssist AI is a **new product module** inside the existing Oziebot platform. It must reuse the current login/session/user foundation while remaining operationally and visually separate from crypto trading.

This document is planning-only. It does **not** authorize implementation changes to trading systems.

## Repo-aware platform constraints

- Frontend is a **separate Next.js static-export app** in `frontend/apps/web` deployed from the frontend repo to S3/CloudFront (`frontend/.github/workflows/frontend-deploy.yml`).
- Backend is a **FastAPI + PostgreSQL** service in `backend/services/api` with JWT auth and Alembic migrations (`backend/services/api/src/oziebot_api/main.py`, `backend/services/api/alembic/env.py`).
- Current user identity lives in `users`, with tenancy via `tenant_memberships` (`backend/services/api/src/oziebot_api/models/user.py`, `.../membership.py`).
- Current commercial access is **tenant + trading entitlement** oriented, not multi-product oriented (`.../tenant_entitlement.py`, `.../services/entitlements.py`).
- Current UI is **globally dark/trading-oriented** (`frontend/apps/web/app/layout.tsx`, `frontend/apps/web/app/globals.css`).

## Product requirements

### 1. Multi-product platform

- One login across products.
- Reuse existing user/profile foundation.
- User may access multiple products/modules.
- Known products:
  - `trading`
  - `teacher_assist`
  - future modules
- User can switch products if access exists.
- First product association becomes default app.
- User can later change default app in settings.
- TeacherAssist defaults to a **light educator-focused UI**.
- Trading keeps the existing dark/trading UI.

## 2. TeacherAssist core purpose

TeacherAssist reduces teacher planning and grading workload while keeping teachers in control.

Primary outcomes:

- weekly planning across all subjects
- standards/pacing-guide grounded lesson generation
- PPTX deck generation for Google Slides upload
- quizzes, assignments, differentiated artifacts
- newsletters and communication support
- grading assistance with teacher confirmation
- TEKS mastery tracking without student PII

## 3. Privacy rules

TeacherAssist must not store:

- student names
- parent names
- district student IDs
- diagnoses

Allowed pattern:

- anonymous class identifiers using `STUDENT #`
- classroom-specific ranges such as `1..23`
- placeholder names only in generated communications

Teacher keeps the real-world mapping outside the platform.

## 4. Setup requirements

One-time or infrequent setup should capture:

- school year
- grade level
- subjects
- class roster counts
- grading period type and dates
- TEKS/standards for year and grading period
- class/subject relationships
- teaching preferences
- support/accommodation-style instructional preferences without medical or diagnosis fields

## 5. Pacing guide requirements

- Pacing guide is a first-class planning source.
- Current likely import source is Excel.
- Data must be normalized into structured planning records.
- Curriculum/resource files belong in a separate resource library linked to pacing items.
- Shared district or campus pacing/resource data should support later teacher-specific customization.

## 6. Input flexibility

Teacher inputs must support:

- drag/drop uploads
- PDFs
- Excel
- PowerPoint/PPTX
- exported Google Slides
- images
- worksheets
- URLs
- teacher notes

Important UX rule:

- save draft first
- generate only on explicit teacher action
- avoid premature LLM calls

## 7. Planning workflow requirements

- weekly planning by subject
- full weekly decks per subject
- daily classroom-ready decks combining subjects
- exported PPTX for manual Google upload in MVP

## 8. Slide generation requirements

- no AI-generated images
- use royalty-free assets or teacher-provided curriculum media
- AI generates content and visual suggestions only
- template engine renders kid-friendly decks
- PPTX is the first export target

## 9. Lesson lifecycle

Required statuses:

- `in_progress`
- `completed`

Completion should later drive:

- lesson effectiveness
- mastery rollups
- dashboard updates
- newsletter eligibility
- reteach recommendations

## 10. TEKS mastery matrix

Matrix requirements:

- per subject
- per grading period
- rows = `STUDENT #`
- columns = TEKS + assessment attempts
- values:
  - `beginning`
  - `developing`
  - `mastery`
- color mapping:
  - green / orange / red

## 11. Assessments and grading support

- each question maps to TEKS
- Google Forms quiz MVP includes required `STUDENT #`
- printable written assignments include QR code metadata
- scanned/uploaded written work resolves by QR + assignment metadata
- AI suggests grading; teacher confirms before commit

## 12. Learning plans, insights, newsletters

TeacherAssist must support:

- mastery trends
- student view by `STUDENT #`
- lesson effectiveness
- reteach recommendations
- weekly newsletter generation
- communication assistant with placeholders only

## 13. Differentiation

Artifacts should support:

- on-level
- simplified support
- advanced extension
- visual support
- guided notes
- small-group / reteach

## 14. UX/UI

TeacherAssist UX should be:

- separate from Oziebot trading identity
- light mode first
- desktop first
- workflow oriented, not admin-heavy
- inline alert oriented, not toast-heavy
- Tailwind-first with lightweight component reuse only

Target navigation:

- Dashboard
- Weekly Planning
- Daily Teaching
- Assessments
- Students
- Insights
- Newsletters
- Communication
- Settings

## 15. LLM and cost strategy

- backend-only LLM calls
- `TeacherAssistLLMService` abstraction
- structured JSON outputs
- workflow input snapshots
- async background jobs
- mock mode for development
- fixture capture for real responses
- section-level regeneration
- reuse extracted document context
- usage tracking per teacher/workflow

## 16. Workflow jobs

Required statuses:

- `queued`
- `running`
- `completed`
- `failed`
- `cancelled`

Jobs must run asynchronously while the teacher continues using the app.

## 17. File/storage strategy

- original uploads in S3-compatible object storage
- Postgres stores metadata, extracted text, summaries, references
- no base64 blobs in DB
- generated exports expire
- structured lesson/mastery/grading records persist

## 18. Hallucination controls

- ground outputs in imported/extracted context
- validate TEKS and mastery values in code
- flag uncertain outputs as review-needed
- require teacher approval before any final grade/matrix commit

## Repo-aware implications

1. The current platform does **not** yet have a product-access model such as `user_products`.
2. The current tenant/billing model is trading-centric and should **not** be overloaded for TeacherAssist product access.
3. The frontend is static-export only, so TeacherAssist uploads/generation flows must use backend APIs rather than Next server actions.
4. The backend already contains a backend-only OpenAI-compatible precedent in AI diagnostics (`backend/services/api/src/oziebot_api/services/admin_ai_diagnostics.py`), which is a good pattern reference for a future `TeacherAssistLLMService`.
