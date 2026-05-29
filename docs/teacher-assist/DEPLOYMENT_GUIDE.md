# TeacherAssist Deployment Guide

Phase 41 production deployment reference for pilot and lean production hosts.

## Architecture Overview

| Component | Technology | Notes |
|-----------|------------|-------|
| API | FastAPI (`backend/services/api`) | Runs migrations on startup when enabled |
| Frontend | Next.js static export (`frontend/apps/web`) | S3 + CloudFront or local dev |
| Database | PostgreSQL 16 | Required |
| Cache / queue | Redis 7 | Worker coordination |
| Worker | `teacher-assist-worker` | Workflows, extraction jobs |
| Storage | Local filesystem or private S3 | No public bucket access |

## AWS / Lean Host Requirements

- **Compute:** Lightsail or EC2 with Docker Compose (see `docker-compose.lean.yml`)
- **Postgres:** Managed or containerized; daily backups recommended
- **Redis:** Container or ElastiCache
- **S3 (optional):** Private bucket with IAM user/role for presigned URLs
- **TLS:** Caddy edge (`docker-compose.lean.edge.yml`) or CloudFront

## Environment Variables

### Core

| Variable | Required | Example |
|----------|----------|---------|
| `DATABASE_URL` | Yes | `postgresql+psycopg://user:pass@host:5432/oziebot` |
| `REDIS_URL` | Yes | `redis://redis:6379/0` |
| `JWT_SECRET` | Yes | 32+ char secret |
| `CORS_ORIGINS` | Yes | `https://app.example.com` |

### TeacherAssist Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `TEACHER_ASSIST_STORAGE_BACKEND` | `local` | `local` or `s3` |
| `TEACHER_ASSIST_STORAGE_ROOT` | `/tmp/oziebot-teacher-assist` | Local root path |
| `TEACHER_ASSIST_S3_BUCKET` | — | Required for S3 backend |
| `TEACHER_ASSIST_S3_REGION` | — | AWS region |
| `TEACHER_ASSIST_S3_PREFIX` | `teacher-assist` | Key prefix |
| `TEACHER_ASSIST_S3_PRESIGN_EXPIRATION_SECONDS` | `900` | Download URL TTL |

### TeacherAssist AI (guarded)

| Variable | Default | Description |
|----------|---------|-------------|
| `TEACHER_ASSIST_REAL_PROVIDER_ENABLED` | `false` | Enable real LLM calls |
| `TEACHER_ASSIST_AI_DAILY_COST_LIMIT_CENTS` | `0` | `0` = mock unlimited; set >0 for real provider budget |
| `TEACHER_ASSIST_OCR_PROVIDER` | `mock` | OCR provider selection |

### Worker

| Variable | Default |
|----------|---------|
| `TEACHER_ASSIST_WORKER_ENABLED` | `true` |
| `TEACHER_ASSIST_WORKER_POLL_INTERVAL_SECONDS` | `1.0` |

## Database Setup

```bash
cd backend/services/api
export DATABASE_URL="postgresql+psycopg://..."
alembic upgrade head
```

Migrations through `067_teacher_assist_pilot_readiness_foundation` include copilot and pilot feedback tables.

## Seed Data (Pilot)

Run in order on a fresh or dev database:

```bash
python3 -m oziebot_api.scripts.seed_education_catalog
python3 -m oziebot_api.scripts.seed_pacing_guides
python3 -m oziebot_api.scripts.seed_instructional_weeks
python3 -m oziebot_api.scripts.seed_instructional_loop
python3 -m oziebot_api.scripts.seed_teacher_copilot
python3 -m oziebot_api.scripts.seed_teacher_assist_access
```

Validate: `GET /v1/teacher-assist/pilot/seed-validation` (root admin).

## Background Jobs

1. Start `teacher-assist-worker` container/process with same `DATABASE_URL` and storage env as API.
2. Worker handles: instructional plan workflows, extraction jobs, OCR processing.
3. Monitor failed workflows via `GET /v1/teacher-assist/pilot/system-health` (root admin).

## Deployment Steps (Lean Compose)

```bash
docker compose -f docker-compose.lean.yml --env-file .env.lean up -d --build
```

1. Configure `.env.lean` from `.env.lean.example`
2. Run migrations (API entrypoint or manual `alembic upgrade head`)
3. Seed pilot data
4. Deploy frontend static export to S3/CloudFront
5. Verify `/v1/ready` and TeacherAssist login

## Secrets

- Store `JWT_SECRET`, DB credentials, Stripe keys, OpenAI keys in host secrets manager or `.env.lean` (never commit)
- S3 IAM: least privilege — `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on bucket prefix only

## Rollback

- Database: restore Postgres snapshot; downgrade Alembic only if revision downgrade scripts exist
- App: redeploy previous container image / static export

## Support Contacts

Document internal on-call and pilot support channel in your deployment runbook (not stored in repo).
