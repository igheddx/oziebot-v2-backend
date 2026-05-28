# TeacherAssist AI Repo Inventory

## Frontend inventory

### Framework and build

- **Next.js 15.5.15**, React 19.1.0, TypeScript, Tailwind CSS v4  
  `frontend/apps/web/package.json`
- Static export enabled with `output: "export"` and `trailingSlash: true`  
  `frontend/apps/web/next.config.ts`

### App/router structure

- App Router root: `frontend/apps/web/app`
- Current pages:
  - `/` -> redirect to `/dashboard` (`app/page.tsx`)
  - `/login`
  - `/dashboard`
  - `/strategies`
  - `/tokens`
  - `/allocation`
  - `/strategic-allocation`
  - `/volatility-harvest`
  - `/alerts`
  - `/analytics`
  - `/trade-log`
  - `/trading-performance-export`
  - `/onboarding`
  - `/subscription`
  - `/coinbase`
  - admin routes under `/admin/*`
- Only current layout file is the root layout: `frontend/apps/web/app/layout.tsx`

### Existing layout/theme/auth structure

- Global root layout wraps all pages with:
  - `AuthProvider`
  - `TradingModeProvider`
- Root layout hard-codes `<html className="dark">`  
  `frontend/apps/web/app/layout.tsx`
- Global theme variables are defined in `frontend/apps/web/app/globals.css`
- Main app shell is `frontend/apps/web/components/layout/app-shell.tsx`
- Current navigation config is in `frontend/apps/web/components/nav/app-nav-links.ts`

### Auth/session handling

- Auth provider: `frontend/apps/web/components/providers/auth-provider.tsx`
- Auth fetch/login/logout/token refresh: `frontend/apps/web/lib/auth-service.ts`
- Session uses access/refresh tokens in browser storage and sends bearer tokens to backend.
- Auth redirects:
  - unauthenticated -> `/login`
  - authenticated login page -> `/dashboard`
  - non-admin user blocked from `/admin/*`

### Current theme and UI library inventory

- Tailwind CSS + custom components only
- No existing heavy UI framework
- No existing ShadCN dependency package
- Current visual language is trading-oriented dark mode with custom CSS variables and `oz-panel` utilities

### Where TeacherAssist routes should live

Recommended:

```text
frontend/apps/web/app/teacher-assist/
```

Recommended subareas:

```text
frontend/apps/web/app/teacher-assist/page.tsx
frontend/apps/web/app/teacher-assist/layout.tsx
frontend/apps/web/app/teacher-assist/weekly-planning/page.tsx
frontend/apps/web/app/teacher-assist/daily-teaching/page.tsx
frontend/apps/web/app/teacher-assist/assessments/page.tsx
frontend/apps/web/app/teacher-assist/students/page.tsx
frontend/apps/web/app/teacher-assist/insights/page.tsx
frontend/apps/web/app/teacher-assist/newsletters/page.tsx
frontend/apps/web/app/teacher-assist/communication/page.tsx
frontend/apps/web/app/teacher-assist/settings/page.tsx
```

### How to create a separate TeacherAssist layout/theme without restructuring everything

Safest approach:

1. add nested `app/teacher-assist/layout.tsx`
2. add `TeacherAssistShell` in `components/teacher-assist/`
3. scope light-mode CSS variables to a TeacherAssist wrapper class
4. keep trading pages on the current dark shell

This avoids a risky global dark/light rewrite.

## Backend inventory

### Framework and API structure

- **FastAPI 0.115+**, Python 3.12, SQLAlchemy 2.0+, psycopg 3.1+  
  `backend/services/api/pyproject.toml`
- application entrypoint in `backend/services/api/src/oziebot_api/main.py`
- API router root at `backend/services/api/src/oziebot_api/api/v1/router.py`
- Current route groups include:
  - `auth`
  - `me`
  - `billing`
  - `tenants`
  - `tokens`
  - `strategies`
  - `allocations`
  - admin routes
  - dedicated strategy modules

### Auth/session/JWT handling

- JWT bearer dependency: `deps/auth.py`
- Login/register/refresh/logout: `api/v1/auth.py`
- Access token and refresh session logic: `services/tokens.py`
- Refresh sessions stored in `user_sessions` via `models/auth_session.py`

### Existing user/profile/tenant foundation

- user identity: `models/user.py`
- tenant membership: `models/membership.py`
- tenant record: `models/tenant.py`
- primary tenant resolution currently picks the earliest membership: `services/tenant_scope.py`

### Existing DB/session pattern

- SQLAlchemy engine/session factory in `db/session.py`
- `DbSession` dependency in `deps/__init__.py`
- request-scoped commit/rollback handling via generator dependency

### Existing migration pattern

- Alembic env: `backend/services/api/alembic/env.py`
- revisions: `backend/services/api/alembic/versions`
- current chain ends at `035_volatility_harvest.py`

### Existing background worker / async patterns

- dedicated service processes:
  - `backend/services/strategy-engine`
  - `backend/services/risk-engine`
  - `backend/services/execution-engine`
  - `backend/services/alerts-worker`
  - `backend/services/market-data-ingestor`
- Postgres outbox queue table introduced in revision `030_no_redis_outbox_kv_trade_logs.py`
- queue helper utilities in `backend/packages/py-common/src/oziebot_common/worker_outbox.py`
- example worker bootstrap in `backend/services/alerts-worker/src/oziebot_alerts_worker/__main__.py`

### Existing file/storage approach

- no current generic user-upload subsystem found
- existing S3 integration is for observability snapshots via `oziebot_common/s3_observability.py`
- frontend static hosting also uses S3/CloudFront in the separate frontend repo workflow

### Where TeacherAssist backend modules should live

Recommended:

```text
backend/services/api/src/oziebot_api/api/v1/teacher_assist.py
backend/services/api/src/oziebot_api/schemas/teacher_assist/
backend/services/api/src/oziebot_api/services/teacher_assist/
backend/services/api/src/oziebot_api/models/teacher_assist_*.py
backend/services/teacher-assist-worker/
```

## Database inventory

### Current access/commercialization tables

- `users`
- `tenant_memberships`
- `tenants`
- `subscription_plans`
- `stripe_subscriptions`
- `stripe_subscription_items`
- `tenant_entitlements`

### What exists today

- strategy-level trading access exists through `tenant_entitlements`
- per-user default product does **not** exist
- `user_products` does **not** exist
- `default_app` does **not** exist

### Recommended multi-product additions

Recommended new tables:

- `platform_products`
- `tenant_product_access`
- `user_product_preferences`

Recommended TeacherAssist schema areas:

- setup/profile
- school years and grading periods
- classes and subjects
- standards/TEKS
- pacing guides and pacing items
- resource library
- uploads and extracted documents
- workflows and workflow steps
- generated plans/decks
- assessments and submissions
- mastery matrix and grading reviews
- newsletters and communication drafts
- AI usage and cost telemetry

## Deployment inventory

### Backend deploy shape

- push-to-main backend workflow deploys to a Lightsail host over SSH  
  `.github/workflows/backend-ci-deploy.yml`
- host deploy runs `docker compose` using:
  - `docker-compose.lean.yml`
  - `docker-compose.lean.edge.yml`
- compose services currently include:
  - postgres
  - redis
  - api
  - strategy-engine
  - risk-engine
  - execution-engine
  - alerts-worker
  - market-data-ingestor
  - optional caddy edge
- `README_LEAN_MODE.md` explicitly documents the current validation-phase live pattern as **single Lightsail/small-EC2 backend host + S3/CloudFront frontend**, while `infrastructure/aws/**` remains as future scale-up reference.

### Frontend deploy shape

- frontend is a separate repo deployment
- static export uploaded to S3 bucket and invalidated via CloudFront  
  `frontend/.github/workflows/frontend-deploy.yml`

### Environment variables / startup

- example env: `env.example`
- backend app settings: `backend/services/api/src/oziebot_api/config.py`
- compose startup expects `.env.lean` on host

### How to add OpenAI API key safely

Recommended:

- add backend-only env variable to API/worker service config
- keep secret in Lightsail host `.env.lean` and/or AWS secret management path used by deploy workflows
- never expose from frontend build vars

### How to add a background worker safely

Recommended:

1. add new service directory `backend/services/teacher-assist-worker`
2. add compose service entry to `docker-compose.lean.yml`
3. use dedicated queue names
4. keep memory limits explicit
5. avoid reusing trading workers

### Lightsail capacity risks

Current Lightsail lean host already runs:

- API
- Postgres
- Redis
- 5 background/process services
- optional Caddy

TeacherAssist risks:

- document extraction CPU spikes
- OCR memory usage
- PPTX generation memory/disk pressure
- concurrent workflow jobs affecting API latency

TeacherAssist likely needs strict concurrency limits and job sizing before production use on the same instance.

## Risks inventory

### Trading code that must not be touched

- `backend/services/strategy-engine/**`
- `backend/services/risk-engine/**`
- `backend/services/execution-engine/**`
- trading strategy models and runtime state tables
- Coinbase integration and exchange connection flows

### Coupling risks

- current entitlement model is trading-specific
- current frontend root UX assumes trading is the default app
- current theme is globally dark by default

### Auth risks

- product-aware bootstrap is not implemented yet
- root redirect currently always points to trading dashboard

### Migration risks

- avoid modifying existing trading tables for TeacherAssist-specific state
- keep TeacherAssist migrations additive and prefixed

### Deployment risks

- frontend/backend are split repos and split deploy paths
- “same Lightsail deployment” is only true for the backend repo, not current frontend hosting

### UI theme contamination risks

- global `html.dark` assumption
- shared CSS variable names currently tuned for trading

### Cost risks

- no TeacherAssist LLM throttling/usage subsystem exists yet
- large planning prompts and repeated deck generation could become expensive quickly

### Privacy risks

- uploads and grading workflows can accidentally capture PII unless the product design enforces alias-only student handling from the start
