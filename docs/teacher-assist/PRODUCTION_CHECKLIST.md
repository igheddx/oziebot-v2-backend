# TeacherAssist Production Checklist

Use before enabling real-teacher pilot traffic.

## Database

- [ ] Postgres 16+ provisioned with automated backups
- [ ] `alembic upgrade head` succeeds on staging
- [ ] Connection pooling configured for API + worker
- [ ] Tenant isolation verified (no cross-tenant queries in tests)

## Storage

- [ ] `TEACHER_ASSIST_STORAGE_BACKEND` set (`local` for dev, `s3` for production)
- [ ] S3 bucket private; block public access enabled
- [ ] IAM credentials rotated; not embedded in frontend
- [ ] Download URLs are temporary presigned or backend-mediated only

## Security

- [ ] `JWT_SECRET` is strong and unique per environment
- [ ] CORS restricted to production frontend origin
- [ ] Root admin accounts limited and documented
- [ ] TeacherAssist file download tokens expire per config
- [ ] No PII in AI prompts (STUDENT # anonymous pattern enforced)

## Backups

- [ ] Daily Postgres snapshots with tested restore procedure
- [ ] S3 lifecycle rules for `temp/` and `exports/` prefixes (if using S3)

## Monitoring & Logging

- [ ] API `/v1/ready` health check in load balancer
- [ ] Structured request logs enabled
- [ ] Failed workflow count monitored (`/pilot/system-health`)
- [ ] Open pilot feedback reviewed weekly

## Alerts

- [ ] Alert on API readiness failure
- [ ] Alert on worker container restart loop
- [ ] Alert on extraction job failure spike

## Admin Accounts

- [ ] Root admin seeded via `seed_root_admin.py` or existing process
- [ ] Pilot teachers seeded via `seed_teacher_assist_access.py`
- [ ] Default passwords rotated after first login

## Seed Data

- [ ] Texas / LISD / Mason Elementary catalog seeded
- [ ] Grade 5 pacing guide + instructional weeks present
- [ ] Seed validation endpoint passes (`/pilot/seed-validation`)

## Validation

- [ ] `pytest tests/test_teacher_assist_pilot_foundation.py` passes
- [ ] End-to-end teacher journey: Home → Week → Assignment → Mastery → Copilot
- [ ] Export smoke test (newsletter HTML/PDF)
- [ ] No toast notifications in UI (inline alerts only)

## Post-Deploy

- [ ] `PRODUCTION_CHECKLIST` signed off by engineering owner
- [ ] `PILOT_READINESS.md` shared with pilot coordinators
