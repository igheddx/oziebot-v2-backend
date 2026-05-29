# TeacherAssist AI Proposed Architecture

## Architectural goal

Add TeacherAssist as a **separate product module** within Oziebot while preserving:

- current trading routes
- current trading workers
- current Coinbase integrations
- current strategy engines and trading diagnostics

TeacherAssist should share identity, tenancy, Postgres, and deployment foundations, but own its UI shell, service layer, tables, workflow jobs, and storage boundaries.

## Current repo shape

### Frontend

- `frontend/apps/web` is a **Next.js App Router** application with static export enabled (`frontend/apps/web/next.config.ts`).
- All existing routes are trading-oriented and live directly under `frontend/apps/web/app/*`.
- Global root layout wraps the entire app in `AuthProvider` and `TradingModeProvider`, and hard-codes `<html className="dark">` (`frontend/apps/web/app/layout.tsx`).
- Styling uses Tailwind v4 plus custom CSS variables in `frontend/apps/web/app/globals.css`.
- Main shell/navigation is `frontend/apps/web/components/layout/app-shell.tsx`.

### Backend

- API is FastAPI in `backend/services/api/src/oziebot_api`.
- JWT auth, refresh sessions, and current-user resolution live in:
  - `api/v1/auth.py`
  - `deps/auth.py`
  - `services/tokens.py`
  - `models/auth_session.py`
- Database access uses SQLAlchemy sessions from `db/session.py`.
- Migrations are Alembic in `backend/services/api/alembic/versions`.
- Current background processing uses dedicated services plus Postgres outbox leasing:
  - `backend/packages/py-common/src/oziebot_common/worker_outbox.py`
  - `backend/services/alerts-worker/...`

### Deployment

- Backend repo deploys to a **Lightsail lean Docker Compose host** on push (`.github/workflows/backend-ci-deploy.yml`, `docker-compose.lean.yml`, `infrastructure/lean/deploy-lean-host.sh`).
- Frontend repo deploys **static export output** to S3/CloudFront (`frontend/.github/workflows/frontend-deploy.yml`).
- ECS/Fargate reference artifacts still exist under `infrastructure/aws/**`, but `README_LEAN_MODE.md` documents the current validation-phase live shape as Lightsail/Compose backend plus S3/CloudFront frontend.

## Key architectural decisions

## 1. Keep TeacherAssist physically separate inside the repo

### Frontend

Recommended additions:

```text
frontend/apps/web/app/teacher-assist/
frontend/apps/web/components/teacher-assist/
frontend/apps/web/lib/teacher-assist-api.ts
frontend/apps/web/lib/teacher-assist-types.ts
```

TeacherAssist routes should live under:

- `/teacher-assist`
- `/teacher-assist/weekly-planning`
- `/teacher-assist/daily-teaching`
- `/teacher-assist/assessments`
- `/teacher-assist/students`
- `/teacher-assist/insights`
- `/teacher-assist/newsletters`
- `/teacher-assist/communication`
- `/teacher-assist/settings`

### Backend

Recommended additions:

```text
backend/services/api/src/oziebot_api/api/v1/teacher_assist.py
backend/services/api/src/oziebot_api/schemas/teacher_assist/
backend/services/api/src/oziebot_api/services/teacher_assist/
backend/services/api/src/oziebot_api/models/teacher_assist_*.py
backend/services/api/alembic/versions/0xx_teacher_assist_foundation.py
backend/services/teacher-assist-worker/
backend/packages/py-common/src/oziebot_common/teacher_assist_*.py   # only if shared helpers are truly cross-service
```

Do **not** place TeacherAssist logic in:

- strategy-engine
- risk-engine
- execution-engine
- Coinbase service modules
- trading strategy models

## 2. Do not overload trading entitlements for multi-product access

Current commercial access is trading-specific:

- `tenant_entitlements` links tenants to `platform_strategies`
- `subscription_plans.plan_kind` is `all_strategies` / `per_strategy`
- billing routes and entitlement checks assume trading behavior

TeacherAssist should introduce **product access** instead of reusing strategy entitlements.

Recommended new model set:

```text
platform_products
tenant_product_access
user_product_preferences
```

### Suggested semantics

- `platform_products`
  - canonical product catalog (`trading`, `teacher_assist`, future)
- `tenant_product_access`
  - whether a tenant is provisioned for a product
  - optional billing/source metadata
- `user_product_preferences`
  - per-user default product
  - last-selected product

This preserves existing `tenant_entitlements` strictly for trading.

## 3. Reuse auth and identity, not trading UX assumptions

Safe shared foundations:

- `users`
- `tenant_memberships`
- JWT/session flow
- `/v1/auth/*`
- `/v1/me`

Needed extension points:

- include available products + default product in bootstrap/profile responses
- root `/` redirect should eventually choose the default product instead of always `/dashboard`
- product switcher UI should be access-aware

## 4. TeacherAssist should use a route-scoped light theme, not a global trading rewrite

The safest repo-aware theming approach is:

1. keep current global trading shell untouched
2. add a nested `app/teacher-assist/layout.tsx`
3. wrap TeacherAssist pages in a `TeacherAssistShell`
4. define a scoped theme container such as `.teacherassist-theme` that overrides CSS variables locally

Why this is safer than global theme rewrites:

- current root layout hard-codes dark mode
- current trading pages assume dark variables
- route-scoped theme variables reduce contamination risk

Recommended frontend additions:

```text
frontend/apps/web/app/teacher-assist/layout.tsx
frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx
frontend/apps/web/components/teacher-assist/teacher-assist-nav.tsx
```

## 5. TeacherAssist jobs should follow the existing outbox-worker pattern

Current repo precedent:

- enqueue work into `worker_message_outbox`
- claim with leases
- run dedicated process workers
- keep API responsive

TeacherAssist should follow the same model:

- API stores workflow input snapshot
- API enqueues job message
- TeacherAssist worker performs extraction/generation/export
- worker updates workflow state rows

Recommended future queue names:

- `oziebot:queue:teacher_assist:workflow`
- `oziebot:queue:teacher_assist:workflow_retry`

Recommended future worker service:

```text
backend/services/teacher-assist-worker/
```

Avoid putting TeacherAssist jobs into trading workers.

## 6. TeacherAssist needs its own storage model

Current repo has:

- Postgres for app state
- Redis for coordination/runtime hot paths
- S3 usage for observability and frontend hosting
- no existing general-purpose user-upload subsystem

Recommended TeacherAssist storage split:

- **Object storage**: original uploads, extracted artifacts, generated PPTX, temporary exports
- **Postgres**: metadata, structured document extraction, workflow snapshots, planning entities, mastery data

Recommended TeacherAssist data domains:

```text
teacher_assist_profiles
teacher_assist_school_years
teacher_assist_grading_periods
teacher_assist_classes
teacher_assist_class_subjects
teacher_assist_subjects
teacher_assist_standards
teacher_assist_pacing_guides
teacher_assist_pacing_items
teacher_assist_resources
teacher_assist_resource_links
teacher_assist_uploads
teacher_assist_workflows
teacher_assist_workflow_steps
teacher_assist_weekly_plans
teacher_assist_daily_decks
teacher_assist_assessments
teacher_assist_assessment_items
teacher_assist_student_aliases
teacher_assist_submission_artifacts
teacher_assist_grading_reviews
teacher_assist_teks_mastery
teacher_assist_newsletters
teacher_assist_communication_drafts
teacher_assist_ai_usage
```

All TeacherAssist tables should be clearly prefixed to preserve module isolation.

## 7. LLM architecture should mirror existing backend-only AI precedent

Current backend precedent:

- `OpenAICompatibleDiagnosticProvider` in `services/admin_ai_diagnostics.py`
- API key/config stored only in backend settings

Recommended TeacherAssist abstraction:

```text
backend/services/api/src/oziebot_api/services/teacher_assist/llm_service.py
```

With interfaces like:

- `TeacherAssistLLMService`
- `MockTeacherAssistLLMService`
- `OpenAICompatibleTeacherAssistLLMService`

Required design rules:

- frontend never calls OpenAI directly
- inputs saved before generation
- prompts built from extracted structured context
- JSON schema validation on outputs
- section-level regeneration only

## 8. PPTX export should be a worker responsibility

Because the frontend is static-export only and the Lightsail backend host is resource constrained, PPTX generation should:

- run asynchronously
- write temporary export files to object storage
- store metadata/expiry in Postgres
- avoid blocking API requests

## Recommended implementation boundaries

### Safe shared code to reuse

- auth/session providers
- `/v1/auth/*`
- `/v1/me`
- SQLAlchemy session pattern
- Alembic migration pattern
- Postgres outbox worker pattern
- S3-style config approach

### Code that must remain untouched or isolated

- `backend/services/strategy-engine`
- `backend/services/risk-engine`
- `backend/services/execution-engine`
- Coinbase exchange/integration logic
- trading strategy registries and runtime state
- trading entitlement rules

## Open architecture questions before implementation

1. Should TeacherAssist product access be billed per tenant, per teacher user, or both?
2. Should TeacherAssist setup data be mostly user-scoped (single teacher) or tenant-scoped (future school admin model)?
3. Is TeacherAssist expected to use the existing frontend repo and S3/CloudFront deployment, or move behind the Lightsail host later?
4. Should file uploads go through backend multipart endpoints first, or directly to object storage with presigned URLs?
5. Is Google Drive/Slides integration explicitly post-MVP, or needed in phase 1.5?

## Pilot readiness (Phase 41)

TeacherAssist pilot operations add:

- **Product completion review** — `GET /v1/teacher-assist/pilot/completion-review` mirrors `FEATURE_INVENTORY.md`
- **Pilot feedback** — teachers submit issues at `/teacher-assist/feedback`; stored in `teacher_assist_pilot_feedback`
- **Usage metrics foundation** — `teacher_assist_usage_metrics` daily rollups; login recorded on home load
- **System health** — root admin dashboard at `/teacher-assist/administration/system-health`
- **Seed validation** — Texas / LISD / Mason Elementary checks for demo readiness

Deployment and production checklists: `DEPLOYMENT_GUIDE.md`, `PRODUCTION_CHECKLIST.md`, `PILOT_READINESS.md`.
