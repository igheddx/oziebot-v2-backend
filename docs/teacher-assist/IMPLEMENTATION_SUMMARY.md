# TeacherAssist AI Implementation Summary

## Purpose

This document summarizes the current Oziebot repo findings and the proposed implementation foundation for adding **TeacherAssist AI** as a separate product module without disrupting the existing trading platform.

## Current implemented baseline

**Phases 1–31.5** are implemented. Latest completed work:

- **Phase 29** — AI-assisted reteach plan drafting (mock AI drafts, versioning, mastery integration)
- **Phase 30** — Weekly newsletter generation (mock AI drafts, section regen, export, no auto-send)
- **Phase 31** — Lesson effectiveness + teacher reflection (read-only scores, reflection workspace, mock AI suggestions, historical comparison, planning hints)
- **Phase 31.5** — Teacher experience completion (onboarding, Home, Work Queue, class workspace, nav redesign, quick create, shortcuts, user preferences)

**Next recommended:** Phase 32 — real-provider reflection AI, assignment effectiveness UI on Assignments, or reteach plan publish into daily teaching (no automatic outbound communication, mastery, or gradebook side effects).

See also: `docs/teacher-assist/PHASE_STATUS.md`, `docs/teacher-assist/KNOWN_LIMITATIONS.md`.

## Operational Access Setup - TeacherAssist AI Seed / Admin Script

### What was implemented

- Added an idempotent admin script at:
  - `backend/services/api/src/oziebot_api/scripts/seed_teacher_assist_access.py`
- Added reusable service-layer seed helpers at:
  - `backend/services/api/src/oziebot_api/services/teacher_assist/access_seed.py`
- The seed/setup flow now:
  - ensures `platform_products` contains `teacher_assist`
  - keeps the canonical display name as `TeacherAssist AI`
  - locates Dominic by email case-insensitively
  - uses Dominic's existing primary tenant membership
  - grants TeacherAssist product access to Dominic's tenant
  - sets Dominic's `user_product_preferences.default_product` to `teacher_assist`
  - creates Awele only if she does not already exist
  - creates a minimal tenant + membership for Awele only if needed
  - grants TeacherAssist product access to Awele's tenant
  - sets Awele's default product to `teacher_assist`
- Existing trading access is preserved:
  - no trading product access is removed
  - no trading entitlements are removed
- If Awele is created without an explicit password environment variable, the script stores a generated temporary password hash without printing a password value.

### Validation coverage added

- `backend/services/api/tests/test_teacher_assist_access_seed.py`
  - validates Dominic receives TeacherAssist access and default-product routing
  - validates Awele is created with TeacherAssist access when missing
  - validates TeacherAssist display-name repair on `platform_products`
  - validates idempotency across repeated runs
  - validates trading product access remains intact for existing users

### Usage

- Run from `backend/services/api`:
  - `python -m oziebot_api.scripts.seed_teacher_assist_access`
- Optional environment variables:
  - `TEACHER_ASSIST_DOMINIC_EMAIL`
  - `TEACHER_ASSIST_AWELE_EMAIL`
  - `TEACHER_ASSIST_AWELE_FULL_NAME`
- `TEACHER_ASSIST_AWELE_TENANT_NAME`
- `TEACHER_ASSIST_AWELE_PASSWORD`

## Phase 9 - Dedicated TeacherAssist Worker + Guarded Real Provider Activation Foundation

### What was implemented

- Moved TeacherAssist instructional-plan execution off API-process background tasks and into a dedicated worker foundation under:
  - `backend/services/teacher-assist-worker/`
- Changed workflow start behavior so the API now queues TeacherAssist workflows and returns persisted workflow metadata without attempting inline execution.
- Added leased workflow processing foundations on `teacher_assist_workflows` for:
  - `leased_by_worker`
  - `lease_expires_at`
  - `heartbeat_at`
  - `retry_count`
  - `max_retries`
  - `timeout_at`
  - `last_error_code`
  - `execution_log_json`
- Added provider/accounting metadata on workflows for:
  - `provider_name`
  - `provider_model`
  - `prompt_version`
  - `input_tokens_total`
  - `output_tokens_total`
  - `estimated_cost_cents_total`
- Added worker-side execution handling for:
  - queue claiming
  - stale-lease reclaim
  - heartbeat/progress updates
  - cancellation polling
  - timeout enforcement
  - retry requeue vs terminal failure persistence
  - malformed provider-output failure handling
- Kept the provider seam guarded:
  - mock provider remains the default
  - real provider stays disabled by default
  - allowed-model parsing exists as a seam
  - daily-cost-limit enforcement blocks execution before provider calls
- Added a dedicated worker runtime entrypoint that loops on TeacherAssist workflows only and does not touch trading workers or trading execution services.
- Updated the instructional-planning workspace to better surface worker-managed status:
  - queued / running / completed / failed / cancelled states
  - retry counts
  - heartbeat / worker metadata
  - provider / model / prompt-version metadata
  - token/cost metadata placeholders backed by workflow fields

### Migration added

- `043_teacher_assist_worker_foundation.py`
  - adds worker lease, retry, timeout, provider, accounting, and execution-log columns to `teacher_assist_workflows`
  - adds indexes needed for worker claiming and stale-lease recovery

### Backend files added or updated

- `backend/services/api/alembic/versions/043_teacher_assist_worker_foundation.py`
- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_workflow.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/provider_config.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`
- `backend/services/teacher-assist-worker/pyproject.toml`
- `backend/services/teacher-assist-worker/Dockerfile`
- `backend/services/teacher-assist-worker/src/oziebot_teacher_assist_worker/__main__.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`

### User-visible behavior after Phase 9

1. Starting an instructional-plan workflow now leaves the workflow queued for worker pickup instead of assuming immediate API-process completion.
2. The planning workspace keeps polling while workflows are queued or running and shows worker-oriented status metadata.
3. Failed workflows now preserve retry/error metadata instead of silently losing worker context.
4. Mock generation still completes as before once the worker processes the queued job.

### Remaining gaps after Phase 9

- Real provider calls are still disabled by default and remain a guarded seam only.
- Provider circuit breaking exists as a foundation, not a mature distributed breaker.
- The frontend still surfaces workflow metadata from list responses; it does not yet expose a richer dedicated workflow-detail workspace.
- TeacherAssist worker deployment/orchestration is a service foundation and still needs environment-level rollout wiring where applicable.

## Phase 10 - Controlled Real Provider Activation + Instructional Plan Quality Review

### What was implemented

- Added a guarded real-provider path for instructional-plan generation through the existing backend-only TeacherAssist provider seam.
- Kept real-provider execution disabled unless all required conditions are satisfied:
  - `teacher_assist_real_provider_enabled = true`
  - `teacher_assist_ai_provider` selects a real provider
  - the configured model is allowlisted
  - an API key is configured
  - daily cost limits permit execution
  - the workflow is instructional-plan generation
- Added a backend real-provider implementation for the OpenAI-compatible path using worker-only execution.
- Kept provider calls out of:
  - frontend code
  - request handlers
  - non-worker workflow startup paths
- Hardened the instructional-plan prompt builder with:
  - planning scope
  - duration
  - pacing groups
  - standards and resources
  - teacher notes
  - structured-output contract
  - teacher-review-required language
  - privacy / no-PII rules
  - anonymous `STUDENT #` rule
  - no grade commitment rule
  - no parent communication generation rule
- Strengthened instructional-plan validation to reject:
  - malformed/non-object output
  - missing required sections
  - empty instructional arc
  - missing or insufficient weekly segments for longer scopes
  - missing standards progression when standards were supplied
  - unsupported planning scope
  - detectable PII-like keys or values
- Preserved graceful failure behavior:
  - failed workflows persist `error_message`
  - failed workflows persist `last_error_code`
  - invalid output does not create a plan artifact
- Added plan quality-review metadata directly to generated `content_json`:
  - `review_required`
  - `quality_flags`
  - `missing_context_warnings`
  - `standards_alignment_summary`
  - `teacher_review_checklist`
- Updated the plan viewer to show:
  - provider mode
  - provider
  - model
  - prompt version
  - token and estimated cost metadata
  - review-required banner
  - quality flags
  - missing-context warnings
  - standards alignment summary
  - teacher review checklist
- Preserved teacher control:
  - generated plans still start in `in_progress`
  - teacher must explicitly mark a plan `completed`
  - no automatic publish/commit behavior was added

### Backend files added or updated

- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/provider_config.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/openai_ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_prompt_builder.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_validator.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/prompt_contracts.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-plan-viewer.tsx`

### User-visible behavior after Phase 10

1. Mock remains the default generation mode and still works without real-provider configuration.
2. Real-provider execution is available only when explicitly enabled on the backend.
3. Generated plans surface review-required metadata and remain teacher-review-first.
4. Plan viewing now exposes provider/model/prompt/tokens/cost metadata plus plan quality-review cues.

### Remaining gaps after Phase 10

- Real-provider support is currently limited to the guarded OpenAI-compatible path.
- Daily cost control is enforced at the workflow seam using accumulated usage events; broader budgeting/alerting workflows are not implemented yet.
- Section-level regeneration and richer teacher review workspaces remain future phases.

## Phase 11 - Section-Level Regeneration + Teacher Edit Workflow

### What was implemented

- Added targeted instructional-plan section regeneration through:
  - `POST /v1/teacher-assist/weekly-plans/{id}/regenerate-section`
- Supported section-level regeneration for:
  - `overview`
  - `instructional_arc`
  - `weekly_segments`
  - a targeted `weekly_segments.{index}`
  - a targeted daily breakdown path such as `subjects.{index}.daily_breakdown.{index}`
  - `vocabulary`
  - `materials_needed`
  - `differentiation`
  - `assessment_checkpoints`
  - `standards_progression`
  - `review_notes`
- Kept regeneration teacher-review-first:
  - regenerated plans stay `in_progress`
  - regeneration never auto-completes or publishes a plan
  - original versions remain accessible through `weekly_plan_versions`
- Preserved software behavior around versioning:
  - every regeneration writes a new version snapshot
  - only the targeted section is replaced
  - existing content remains intact outside the requested section/path
- Extended the provider seam with targeted section-regeneration methods for mock and guarded real-provider paths.
- Added section-regeneration prompt scaffolding and structured-output validation:
  - `instructional-plan-section-v1`
  - required `section_content` wrapper output
  - section-key/path-specific validation
  - malformed output fails safely without mutating the saved plan
- Preserved mock-first behavior:
  - mock regeneration remains deterministic
  - mock regeneration records zero cost and zero tokens
- Preserved guarded real-provider behavior:
  - real regeneration remains blocked unless the existing backend provider guardrails are satisfied
  - provider/model/prompt metadata is recorded on regeneration usage events
- Added plan-scoped latest-usage resolution so copied plans without a workflow id can still surface regeneration metadata in the viewer.
- Updated the plan viewer to expose:
  - targeted regeneration actions for overview, instructional arc, weekly segments, daily breakdown items, vocabulary, differentiation, materials, assessment checkpoints, standards progression, and review notes
  - teacher instruction input
  - optional provider-mode override (`mock` or `real`)
  - preserve-existing-context toggle
  - automatic refresh to the new version after successful regeneration

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/openai_ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_prompt_builder.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_validator.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/prompt_contracts.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-plan-viewer.tsx`

### User-visible behavior after Phase 11

1. Teachers can regenerate selected plan sections instead of rerunning the full instructional plan.
2. Regeneration creates a new version and keeps earlier versions recoverable.
3. Copied plans now surface regeneration provider/cost metadata even when they do not have an originating workflow id.
4. Mock regeneration remains zero-cost, deterministic, and available without real-provider configuration.

### Remaining gaps after Phase 11

- Section regeneration currently saves directly to a new version; preview-before-save was deferred.
- Real-provider regeneration still depends on the existing guarded backend configuration and remains disabled by default.
- Deeper collaborative approval/governance workflows for shared-plan edits are still not implemented.

## Phase 17 - TeacherAssist Storage Hardening + S3 Migration Foundation

### What was implemented

- Hardened TeacherAssist storage behind a provider abstraction in `services/teacher_assist/storage.py`.
- Added provider support for:
  - `local`
  - `s3`
- Added provider operations for:
  - `save_file()`
  - `delete_file()`
  - `get_download_url()`
  - `file_exists()`
  - `open_stream()`
- Kept the API layer storage-provider agnostic by routing TeacherAssist file operations through the storage service instead of hardcoding S3 inside routes.
- Preserved local storage as the default development backend while enabling S3-backed private object storage for production.
- Refactored TeacherAssist upload key generation to use provider-safe object keys such as:
  - `teacher-assist/resources/{tenant_id}/{uuid}.pdf`
  - `teacher-assist/student-work/{tenant_id}/{uuid}.pdf`
- Added private download-url foundations for:
  - uploaded resource-library files
  - uploaded student-work submissions
- Added local signed-download fallback for development and presigned S3 URL support for production.
- Updated TeacherAssist frontend surfaces to support download actions for stored resources and student-work files without rewriting the broader upload UX.
- Added AWS delivery artifacts for bucket bootstrap and least-privilege IAM guidance without introducing Terraform or CDK.

### Migration notes

- No database migration was required.
- Existing metadata fields remain the source of truth:
  - `storage_key`
  - `mime_type`
  - `original_filename`
  - `file_size`
- Existing TeacherAssist upload APIs continue to work with the new provider abstraction.

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/storage.py`
- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/pyproject.toml`
- `backend/services/api/tests/test_teacher_assist_storage.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`
- `docker-compose.yml`
- `docker-compose.lean.yml`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-resources-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`

### Infrastructure artifacts added

- `infrastructure/aws/teacher-assist/bootstrap-private-uploads-bucket.sh`
- `infrastructure/aws/teacher-assist/teacher-assist-uploads-iam-policy.json`

### AWS CLI bucket bootstrap script

- Use:
  - `bash infrastructure/aws/teacher-assist/bootstrap-private-uploads-bucket.sh`
- Behavior:
  - creates or reuses a private bucket named like `teacherassist-prod-uploads-<account>-<region>`
  - blocks all public access
  - enforces bucket-owner object ownership and disables ACL-based object control
  - enables SSE-S3 default encryption
  - creates `teacher-assist/resources/`, `student-work/`, `print-packets/`, `exports/`, and `temp/` prefixes
  - applies lifecycle expiration for:
    - `teacher-assist/temp/`
    - `teacher-assist/exports/`

### Example IAM policy JSON

- Path:
  - `infrastructure/aws/teacher-assist/teacher-assist-uploads-iam-policy.json`
- Scope:
  - bucket-level list/location access limited to `teacher-assist/*`
  - object-level `GetObject`, `PutObject`, `DeleteObject`, and multipart-upload permissions limited to `teacher-assist/*`
- Important:
  - replace `teacherassist-prod-uploads-ACCOUNT-REGION` with the real bucket name before attaching the policy

### Environment-variable setup

- New backend configuration:
  - `TEACHER_ASSIST_STORAGE_BACKEND=local|s3`
  - `TEACHER_ASSIST_STORAGE_ROOT=/tmp/oziebot-teacher-assist`
  - `TEACHER_ASSIST_S3_BUCKET=teacherassist-prod-uploads-<account>-<region>`
  - `TEACHER_ASSIST_S3_REGION=<aws-region>`
  - `TEACHER_ASSIST_S3_PREFIX=teacher-assist`
  - `TEACHER_ASSIST_S3_ENDPOINT=` optional override for S3-compatible endpoints
  - `TEACHER_ASSIST_S3_PRESIGN_EXPIRATION_SECONDS=900`
- Runtime dependencies added:
  - `boto3`
  - `botocore`

### Local-dev instructions

1. Keep `TEACHER_ASSIST_STORAGE_BACKEND=local`.
2. Optionally override `TEACHER_ASSIST_STORAGE_ROOT` to a writable local directory.
3. Run the existing API/frontend stacks normally; resource uploads and student-work uploads continue to use the existing routes.
4. Local file downloads now use short-lived backend-signed download URLs instead of exposing filesystem paths.

### Lightsail deployment notes

1. Bootstrap the private S3 bucket with `bootstrap-private-uploads-bucket.sh`.
2. Attach the least-privilege S3 policy to the IAM principal used by the backend runtime.
3. Set the new `TEACHER_ASSIST_S3_*` variables in the Lightsail Docker Compose environment.
4. Set `TEACHER_ASSIST_STORAGE_BACKEND=s3` for the backend container.
5. Rebuild/redeploy the API container so the new `boto3` dependency and configuration are present.
6. Keep the bucket private; do not add public bucket policies, website hosting, CDN exposure, or public ACLs.

### Test coverage summary

- Added unit coverage for:
  - local provider save/read/delete behavior
  - tenant-safe key generation
  - storage provider selection
  - S3 presigned URL generation
  - S3 file existence/open/delete behavior with mocked clients
  - signed local download-token validation
- Added integration coverage for:
  - resource upload key prefixes
  - resource download-url generation and local download streaming
  - student-work upload key prefixes
  - student-work download-url tenant isolation
  - existing upload metadata persistence

### User-visible behavior after Phase 17

1. Resource uploads and student-work uploads keep the same TeacherAssist workflows, but stored object keys are now provider-safe and S3-ready.
2. Teachers can download stored resources and uploaded student-work files through backend-generated temporary download URLs.
3. Frontend screens no longer need to expose local filesystem concepts; downloads stay backend-controlled and private.

### Remaining gaps after Phase 17

- Printable packet HTML views still exist as the current export path; packet-file persistence in object storage is only a foundation for later phases.
- Direct-browser uploads are not implemented yet.
- OCR, extraction queues, grading automation, embeddings, and public artifact delivery remain intentionally out of scope.
- Retention cleanup jobs are not yet implemented; only S3 lifecycle recommendations and bootstrap wiring were added in this phase.

### Next recommended phase

- Phase 18 - OCR intake and artifact-processing foundation
  - build async OCR/extraction orchestration on top of the new storage abstraction and private object storage without introducing grading automation or public file delivery

## Phase 18 - OCR Intake + Artifact Processing Foundation

### What was implemented

- Added async TeacherAssist extraction foundations on top of the private storage abstraction.
- Added durable persistence for:
  - `teacher_assist_extraction_jobs`
  - `teacher_assist_extracted_text_records`
- Added a mock-first OCR provider seam through:
  - `services/teacher_assist/ocr_provider.py`
  - `services/teacher_assist/mock_ocr_provider.py`
- Added artifact-processing sanitization through:
  - `services/teacher_assist/artifact_processing.py`
- Added worker-managed extraction execution through:
  - `services/teacher_assist/extraction_jobs.py`
  - the existing dedicated `teacher-assist-worker` loop
- Kept extraction tenant-safe and backend-controlled:
  - files are read only through `services/teacher_assist/storage.py`
  - no public S3/object URLs are exposed
  - no base64 blobs are stored in Postgres
- Added sensitive extracted-text handling:
  - persisted preview text
  - text length metadata
  - PII-like flagging/redaction seam
- Added TeacherAssist activity events for:
  - `extraction_started`
  - `extraction_completed`
  - `extraction_failed`
  - `extraction_cancelled`
- Extended the unified workspace to surface:
  - failed extraction jobs
  - student work ready for extraction
  - extracted work ready for teacher review
  - extraction counts in summary/stats
- Updated TeacherAssist resource and student-work screens to show:
  - extraction status
  - start extraction actions
  - cancel extraction for queued/running student-work jobs
  - extracted text previews
  - processing errors
  - disabled “AI grading coming later” messaging

### Migration summary

- Added `049_teacher_assist_extraction_foundation.py`
- New tables:
  - `teacher_assist_extraction_jobs`
  - `teacher_assist_extracted_text_records`
- New indexed metadata includes:
  - artifact type and source references
  - school-year / grading-period / class / subject context
  - status / retry / lease / heartbeat / timeout fields
  - error metadata and execution logs
- Rollback concern:
  - downgrading this migration removes extraction-job history and extracted-text preview records

### Backend files added or updated

- `backend/services/api/alembic/versions/049_teacher_assist_extraction_foundation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_extraction_job.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_extracted_text_record.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_resource_library_item.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_student_work_submission.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/extraction_jobs.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/artifact_processing.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workspace_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`
- `backend/services/teacher-assist-worker/src/oziebot_teacher_assist_worker/__main__.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-resources-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`

### API routes added

- `POST /v1/teacher-assist/resources/{id}/extraction-jobs`
- `GET /v1/teacher-assist/resources/{id}/extractions`
- `POST /v1/teacher-assist/student-work/{id}/extraction-jobs`
- `GET /v1/teacher-assist/student-work/{id}/extractions`
- `GET /v1/teacher-assist/extraction-jobs/{id}`
- `PATCH /v1/teacher-assist/extraction-jobs/{id}/cancel`

### Activity-event types implemented

- `extraction_started`
- `extraction_completed`
- `extraction_failed`
- `extraction_cancelled`

### Workspace aggregation behavior

- Workspace summaries now include extraction failure counts, student-work-ready-for-extraction counts, and extracted-artifact-ready-for-review counts.
- Class workspaces now include recent submissions with latest extraction status and teacher-review-ready flags.
- Needs-attention aggregation now surfaces extraction failures, unextracted student work, and extracted work awaiting teacher review.
- Review-required items now include extracted student-work artifacts when no grading review exists yet.

### Needs-attention rules implemented

- failed resource extraction job -> `critical`
- failed student-work extraction job -> `critical`
- student work uploaded with no completed extraction and no queued/running extraction job -> `warning`
- extracted student work with no non-archived grading review yet -> `warning`

### Storage/OCR provider behavior

- extraction workers read file bytes only through the TeacherAssist storage abstraction
- mock OCR is the default and only implemented provider in this phase
- mock extraction persists safe preview text and provider metadata without creating AI usage events
- PII-like content may be flagged/redacted before persistence

### Frontend behavior

1. Resource-library cards now show extraction status, preview text, queue actions, and extraction errors.
2. Student-work rows and detail panels now show extraction status, queue/cancel actions, preview text, and a disabled AI-grading placeholder.
3. Workspace summary cards, review queue, class submission cards, and needs-attention panels now reflect extraction operational state.

### Tests added

- tenant-safe extraction job creation for resources and student-work
- storage-backed resource extraction completion and preview persistence
- student-work extraction completion with no AI usage or grading-review side effects
- extraction failure persistence
- extraction cancellation rules
- workspace extraction attention/review aggregation

### Manual validation checklist

1. Upload a TeacherAssist resource file and confirm `Start extraction` queues a job.
2. Refresh the resource screen and confirm status changes to completed with a preview.
3. Upload anonymous student work and confirm extraction can be queued from the student-work detail panel.
4. Confirm queued/running student-work extraction can be cancelled.
5. Confirm completed extraction does not auto-create a grading review or AI usage event.
6. Open `/teacher-assist/workspace` and verify extraction failures / extraction-ready / review-ready states appear in summary cards and attention panels.
7. Confirm extraction routes remain tenant-scoped by attempting cross-tenant access.

### Known limitations

- real OCR providers are not implemented yet; mock OCR remains the only provider
- external-link resources still cannot be extracted because they do not have a stored file body
- extraction completion does not auto-create grading reviews or mastery updates

### Next recommended phase

- Phase 19 - extraction remediation and teacher-review drill-down
  - add richer retry tooling, extraction history/detail screens, teacher review actions on extracted artifacts, and guarded evaluation of real OCR providers without introducing grading automation

## Phase 19 - Extraction Remediation + Teacher Review Drill-Down

### What was implemented

- Added extraction review and remediation foundations on top of Phase 18 mock OCR/extraction jobs.
- Added `extraction_review_service.py` for:
  - extraction detail aggregation
  - retry eligibility checks
  - cancel eligibility checks
  - review-status transitions
  - corrected/approved text persistence
  - issue flagging with teacher notes/reason stored in record metadata
  - attempt lineage/history loading
  - stale extraction job detection support
- Extended extracted-text persistence with teacher-review metadata:
  - `review_status`
  - `provider_confidence_score`
  - `confidence_level`
  - `teacher_corrected_text`
  - `approved_text`
  - `reviewed_at`
  - `reviewed_by_user_id`
  - `source_extraction_job_id`
  - teacher review notes / issue reason via `metadata_json`
- Extended extraction-job persistence with retry lineage:
  - `parent_extraction_job_id`
  - `retry_root_job_id`
  - `attempt_number`
- Added review statuses:
  - `pending_review`
  - `teacher_reviewing`
  - `teacher_approved`
  - `teacher_rejected`
  - `reviewed`
  - `issue_flagged`
  - `needs_retry`
  - `archived`
- Added TeacherAssist activity events for:
  - `extraction_retry_requested`
  - `extraction_review_started`
  - `extraction_review_approved`
  - `extraction_review_rejected`
  - `extraction_text_corrected`
  - `extraction_issue_flagged`
- Added worker stale-job recovery through `recover_stale_extraction_jobs()` in the dedicated TeacherAssist worker loop.
- Extended workspace aggregation to surface:
  - low-confidence extractions
  - teacher-rejected extractions
  - retry-required extractions
  - stale extraction jobs
  - awaiting-teacher-review counts
  - recently approved extractions
- Added the `/teacher-assist/extractions` operational workspace with drill-down detail for both extracted-text review and job-only remediation views.

### Migration summary

- Added `050_teacher_assist_extraction_review_workspace.py`
- Extended tables:
  - `teacher_assist_extracted_text_records`
  - `teacher_assist_extraction_jobs`
- New review/lineage fields and queue indexes for review/remediation lookups
- Rollback concern:
  - downgrading this migration removes review metadata and retry lineage needed for remediation history

### Backend files added or updated

- `backend/services/api/alembic/versions/050_teacher_assist_extraction_review_workspace.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_extracted_text_record.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_extraction_job.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/extraction_review_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/extraction_jobs.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workspace_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`
- `backend/services/teacher-assist-worker/src/oziebot_teacher_assist_worker/__main__.py`

### Frontend files added or updated

- `frontend/apps/web/app/teacher-assist/extractions/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-extraction-detail-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-resources-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/lib/teacher-assist-types.ts`

### API routes added or changed

- `GET /v1/teacher-assist/extractions`
- `GET /v1/teacher-assist/extraction-jobs/{id}` — enriched job detail with timeline, artifact metadata, retry/cancel eligibility
- `POST /v1/teacher-assist/extraction-jobs/{id}/retry`
- `POST /v1/teacher-assist/resources/{id}/extraction-jobs/retry`
- `POST /v1/teacher-assist/student-work/{id}/extraction-jobs/retry`
- `GET /v1/teacher-assist/extracted-text/{id}`
- `GET /v1/teacher-assist/extracted-text/{id}/history`
- `PATCH /v1/teacher-assist/extracted-text/{id}/review-status`
- `PUT /v1/teacher-assist/extracted-text/{id}/approved-text`

### Activity-event types implemented

- `extraction_retry_requested`
- `extraction_review_started`
- `extraction_review_approved`
- `extraction_review_rejected`
- `extraction_text_corrected`
- `extraction_issue_flagged`

### Workspace aggregation behavior

- Workspace summaries now include low-confidence, rejected, retry-required, awaiting-review, stale-job, and recently-approved extraction counts.
- Needs-attention aggregation now surfaces:
  - `low_confidence_extraction` -> `warning`
  - `teacher_rejected_extraction` -> `critical`
  - `stale_extraction_job` -> `critical`
  - `multiple_failed_retries` -> `critical`
  - extraction retry queue visibility
- Review-required items now include extracted-text records awaiting teacher approval before downstream use.

### Retry/remediation behavior

- Retry creates a new immutable extraction-job row; prior jobs and extracted-text records remain unchanged.
- Retry lineage is preserved through `parent_extraction_job_id`, `retry_root_job_id`, and `attempt_number`.
- Retry is blocked for queued/running jobs and only allowed for terminal or remediation-eligible states such as failed, cancelled, low-confidence completed jobs, or issue-flagged/rejected review states.
- Stale running jobs are recovered through lease/heartbeat timeout handling in the worker loop.

### Frontend behavior

1. `/teacher-assist/extractions` now provides an operational extraction list with dashboard cards for failures, low confidence, awaiting review, retry required, stale jobs, and recently approved items.
2. Extracted-text drill-down supports side-by-side original vs corrected text, confidence warnings, review actions, retry/cancel controls, and activity/history visibility.
3. Job-only drill-down via `?jobId=` supports failed or in-progress extraction remediation before extracted text exists.
4. Workspace, Resources, and Assignments now route review-required extraction items into the extraction drill-down experience.
5. AI grading remains visibly disabled; extraction review does not trigger grading or AI usage.

### Tests added

- extraction job detail with retry/cancel eligibility and artifact metadata
- retry blocked for queued/running jobs
- teacher review approval/history flow
- issue flagging with teacher notes/reason
- mark reviewed without grading-review or AI-usage side effects
- retry lineage creation for failed jobs
- tenant-safe extraction remediation behavior

### Manual validation checklist

1. Upload a resource or student-work artifact and queue extraction.
2. Open `/teacher-assist/extractions` and confirm the job appears with status/confidence metadata.
3. Open extraction detail and confirm original text, preview, review status, and confidence appear.
4. Start review, save corrected text, approve or mark reviewed, and confirm approved/corrected text persists separately from original extracted text.
5. Flag an issue with a teacher reason and confirm it appears in detail/workspace attention surfaces.
6. Retry a failed or remediation-eligible job and confirm a new extraction-job row is created with lineage fields populated.
7. Confirm queued/running jobs cannot be retried.
8. Confirm workspace attention panels surface low-confidence, rejected, stale, and retry-required extraction items.
9. Confirm no grading review, AI usage event, or mastery workflow side effects are created by extraction review actions.

### Known limitations

- teacher review notes and issue reasons are currently stored in `metadata_json`, not dedicated columns
- artifact-level retry buttons are centered in the Extractions workspace; Resources/Assignments primarily link into drill-down rather than exposing every remediation action inline
- approved extracted text is consumable for grading-prep readiness checks (Phase 21), but AI grading and analytics workflows are not wired yet

### Next recommended phase

- Phase 20 - guarded real OCR provider integration

## Phase 20 - Guarded Real OCR Provider Integration

### What already existed (Phases 18–19)

- Mock-first OCR provider seam via `ocr_provider.py` and `mock_ocr_provider.py`
- Extraction jobs, extracted-text records, worker-managed storage-backed execution
- Teacher-review-first persistence (`pending_review` default, corrected/approved text fields)
- Phase 19 retry lineage (`parent_extraction_job_id`, `retry_root_job_id`, `attempt_number`)
- Extraction detail/history APIs and `/teacher-assist/extractions` drill-down UI
- Confidence metadata and low-confidence workspace attention rules

### What was added

- Config-guarded real OCR provider integration with mock remaining the default
- Provider-neutral OCR config/circuit breaker in `ocr_provider_config.py`
- Structured OCR provider errors in `ocr_errors.py`
- Real provider implementations:
  - `textract_ocr_provider.py` (AWS Textract `detect_document_text`)
  - `openai_vision_ocr_provider.py` (OpenAI vision JSON extraction)
- OCR provider metadata persistence on extraction jobs:
  - `provider_model`
  - `provider_version`
  - `provider_mode` (`mock` / `real`)
  - `page_count`
  - `processing_duration_ms`
  - `estimated_cost_cents` placeholder
- Preflight OCR guards for file size and MIME allowlists (real providers only)
- Graceful failure codes:
  - `provider_disabled`
  - `provider_not_configured`
  - `unsupported_mime_type`
  - `provider_timeout`
  - `provider_malformed_response`
  - `provider_quota_exceeded`
- Extraction UI updates showing provider mode/model/version/attempt/confidence/duration/cost placeholder
- Real OCR messaging that output still requires teacher review

### Migration added

- `051_teacher_assist_ocr_provider_metadata.py`

### Config / env variables added

- `TEACHER_ASSIST_REAL_OCR_ENABLED` (default `false`)
- `TEACHER_ASSIST_OCR_ALLOWED_PROVIDERS` (default `mock,textract,openai_vision`)
- `TEACHER_ASSIST_OCR_ALLOWED_MODELS` (default `mock-ocr,textract-detect-document-text,gpt-4o-mini`)
- `TEACHER_ASSIST_OCR_MAX_FILE_BYTES` (default `26214400`)
- `TEACHER_ASSIST_OCR_MAX_PAGES` (default `15`)
- `TEACHER_ASSIST_OCR_PROVIDER_TIMEOUT_SECONDS` (default `120`)
- `TEACHER_ASSIST_OCR_DAILY_COST_LIMIT_CENTS` (default `0`, keeps real OCR disabled by policy)
- `TEACHER_ASSIST_OCR_OPENAI_VISION_MODEL` (default `gpt-4o-mini`)
- `TEACHER_ASSIST_OCR_AWS_REGION` (optional; defaults to TeacherAssist S3 region)

Existing settings reused without frontend exposure:

- `TEACHER_ASSIST_OCR_PROVIDER` (default `mock`)
- `TEACHER_ASSIST_OPENAI_API_KEY` (OpenAI vision only, backend-only)

### Backend files added or updated

- `backend/services/api/alembic/versions/051_teacher_assist_ocr_provider_metadata.py`
- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_extraction_job.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/ocr_errors.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/ocr_provider_config.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/textract_ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/openai_vision_ocr_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/extraction_jobs.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-extraction-detail-screen.tsx`

### Tests added / run

- mock OCR remains default provider
- real OCR blocked without enable flag / zero daily cost limit
- missing OpenAI credentials fail safely
- real OCR provider metadata persists on jobs/records
- low-confidence real OCR stays `pending_review` and remains retry-eligible
- retry lineage preserves provider attempts across failed/successful real OCR runs
- real OCR does not create grading reviews or AI usage events
- unsupported MIME types fail safely for real OCR providers
- full TeacherAssist planning suite: **82 passed**
- ruff check clean on changed backend OCR files
- frontend lint/build successful

### Remaining gaps

- no persisted daily OCR usage accounting yet (cost limit is a config placeholder only)
- OpenAI vision OCR does not create AI usage events even though it uses an LLM
- Azure OCR / Google Vision adapters are not implemented yet (seam supports adding them)
- handwriting-heavy and multi-page PDF async OCR remain limited
- approved extracted text is consumable for grading-prep readiness checks; guarded AI grading suggestions (Phase 23) and manual gradebook commits (Phase 24) are implemented — mastery analytics remain deferred

### Next recommended phase

- Phase 21 - teacher-approved extraction downstream consumption
  - read-only grading-prep context and assignment summary APIs with teacher-review gating before downstream grading workflows

## Phase 21 - Teacher-Approved Extraction Downstream Consumption

### What was implemented

- Added read-only grading-prep resolution so only teacher-approved or teacher-corrected extracted text can be used as downstream input for future grading-prep and analytics workflows
- Approved-text priority:
  1. `approved_text`
  2. `teacher_corrected_text`
  3. `extracted_text` only when `review_status` is `teacher_approved` or `reviewed`
- Blocked downstream use for `pending_review`, `teacher_reviewing`, `teacher_rejected`, `issue_flagged`, `needs_retry`, and `archived`
- Added read-only APIs:
  - `GET /v1/teacher-assist/student-work/{id}/grading-prep-context`
  - `GET /v1/teacher-assist/assignments/{id}/grading-prep-summary`
- Added frontend grading-prep readiness UI in Assignments and Extractions showing **Ready for grading prep** only after teacher approval
- Preserved tenant isolation and STUDENT # privacy (no new PII exposure beyond existing submission scoping)

### Explicit non-goals preserved

- AI grading remains disabled (`ai_grading_enabled: false` on all grading-prep responses)
- No mastery updates
- No parent communication
- No gradebook commits from grading-prep read endpoints (manual teacher-confirmed commits added in Phase 24)
- No AI usage events created by grading-prep read endpoints
- No OpenAI or OCR provider calls from grading-prep logic
- No trading-system changes

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/grading_prep_service.py` (new)
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-extraction-detail-screen.tsx`

### Tests added / run

- approved-text priority selection (`approved_text` > `teacher_corrected_text` > eligible `extracted_text`)
- unapproved review statuses blocked from downstream resolution
- grading-prep context ready after teacher approval workflow
- assignment grading-prep summary counts ready vs blocked submissions
- tenant isolation on grading-prep context and summary endpoints
- grading-prep GET endpoints do not create AI usage events or grading reviews
- ruff check clean on changed backend files
- frontend lint/build successful

### Manual validation checklist

1. Upload student work, run extraction, and confirm grading-prep context is blocked while review status is `pending_review`.
2. Start review, approve or mark reviewed, and confirm grading-prep context returns `ready_for_grading_prep: true` with resolved approved text.
3. Open assignment grading-prep summary and confirm ready/blocked counts match submission review states.
4. Confirm Assignments and Extractions show **Ready for grading prep** only after teacher approval.
5. Confirm another tenant cannot read grading-prep context or summary for foreign submissions/assignments.
6. Confirm no grading review, AI usage event, mastery workflow, or parent-communication side effects occur from grading-prep reads.

### Remaining gaps

- guarded AI grading suggestions now consume approved grading-prep text (Phase 23)
- manual teacher-confirmed gradebook commits now available (Phase 24)
- analytics/insights workflows do not yet consume approved text
- extractions list uses review status for lightweight badges; detail/assignments use full API readiness checks
- mastery updates and parent communication remain unimplemented by design

### Next recommended phase

- Phase 22 - artifact export foundation
  - async slide/quiz export workflows with teacher download/review flows and no Google API integration

## Phase 22 - Artifact Export Foundation (Google Slides + Quiz Export)

### What was implemented

- Added persisted export artifact foundation through `teacher_assist_export_artifacts`
- Added async `artifact_export` workflow type processed by the TeacherAssist worker (never synchronous in API handlers)
- Added mock-first export generation services:
  - `export_templates.py` — deterministic slide/quiz preview JSON from weekly plan content
  - `export_generation.py` — workflow claim/process, PPTX/JSON/HTML rendering, storage upload
  - `export_artifacts.py` — persistence, listing, detail, signed download URLs
- Supported export artifact types:
  - Slides: `lesson_slides`, `guided_notes`
  - Quiz: `multiple_choice_quiz`, `exit_ticket`, `short_answer_quiz`
- Supported export formats: `pptx`, `json`, `printable_html`
- Added worker-managed PPTX generation via `python-pptx` stored in private `exports` storage area
- Added APIs:
  - `POST /v1/teacher-assist/weekly-plans/{id}/exports` (202 queued)
  - `GET /v1/teacher-assist/exports`
  - `GET /v1/teacher-assist/exports/{id}`
  - `GET /v1/teacher-assist/exports/{id}/download`
- Added frontend weekly-plan export actions and `/teacher-assist/exports` workspace
- Added export activity events (`export_queued`, `export_completed`, `export_failed`)

### Explicit non-goals preserved

- No Google OAuth, Google Slides API, Google Forms API, or Google Drive sync
- No auto publishing, collaborative editing, or LMS integration
- No AI grading, mastery updates, parent communication, or gradebook commits from export flows
- No AI usage events from export generation (mock-first)
- No trading-system changes

### Migration added

- `052_teacher_assist_export_artifacts.py`

### Backend files added or updated

- `backend/services/api/alembic/versions/052_teacher_assist_export_artifacts.py`
- `backend/services/api/pyproject.toml` (`python-pptx`)
- `backend/services/api/src/oziebot_api/models/teacher_assist_export_artifact.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/export_templates.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/export_artifacts.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/export_generation.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`
- `backend/services/teacher-assist-worker/src/oziebot_teacher_assist_worker/__main__.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-plan-viewer.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-exports-screen.tsx`
- `frontend/apps/web/app/teacher-assist/exports/page.tsx`

### Tests added / run

- tenant-safe export creation and listing isolation
- workflow + export artifact persistence
- PPTX storage upload and signed download URL generation
- quiz mock preview structure (multiple choice / short answer / true-false)
- failed export persistence with safe error metadata
- export worker retry exhaustion to `failed`
- no AI usage or grading review side effects from export generation
- ruff check clean on export backend files
- frontend lint/build successful

### Manual validation checklist

1. Generate lesson slides from a weekly plan.
2. Confirm workflow row is created (`artifact_export`, `queued` → `completed`).
3. Confirm export artifact row persists with preview JSON.
4. Confirm PPTX uploads through private `exports` storage.
5. Confirm signed download URL works.
6. Confirm `/teacher-assist/exports` renders history and detail.
7. Confirm failed exports persist errors safely.
8. Confirm no Google APIs are called.
9. Confirm no grading workflows are triggered.
10. Confirm no trading systems are touched.

### Remaining gaps

- export retry UI is placeholder only (teachers re-queue from plan viewer)
- real provider export generation remains guarded/disabled (mock-only templates today)
- no Google Slides import automation — teachers import PPTX manually
- assignment-scoped exports (`source_assignment_id`) not exposed in UI yet
- workspace dashboard does not yet surface export attention counts

### Next recommended phase

- Phase 23 - guarded AI grading prep assist
  - consume teacher-approved grading-prep context for teacher-confirmed AI scoring suggestions without automatic gradebook commits, mastery updates, or parent communication

## Phase 24 - Gradebook Commit Foundation (Teacher-Confirmed Only)

### What was implemented

- gradebook commit tables for assignment grade records, commit history, and dedicated audit events
- explicit teacher-only commit seam: `POST /grading-reviews/{id}/gradebook-commit` after `teacher_confirmed` review status
- no automatic commits on review confirmation, AI suggestion, or extraction approval
- grade correction and reversal flows with superseded/reversed commit lineage
- export-ready assignment gradebook JSON view
- `/teacher-assist/gradebook` workspace with commit history, audit trail, correction/reversal controls
- Assignments UI **Commit to Gradebook** action for teacher-confirmed reviews

### Backend files added or updated

- `backend/services/api/alembic/versions/053_teacher_assist_gradebook_commit_foundation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_grade_record.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_gradebook_commit.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_gradebook_audit_event.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/gradebook_commits.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/grading_prep_service.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-gradebook-screen.tsx`
- `frontend/apps/web/app/teacher-assist/gradebook/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`

### API routes added

- `POST /v1/teacher-assist/grading-reviews/{id}/gradebook-commit`
- `GET /v1/teacher-assist/assignments/{id}/gradebook-records`
- `GET /v1/teacher-assist/gradebook/records/{id}`
- `POST /v1/teacher-assist/gradebook/records/{id}/corrections`
- `POST /v1/teacher-assist/gradebook/records/{id}/reversals`
- `GET /v1/teacher-assist/assignments/{id}/gradebook-export`
- `GET /v1/teacher-assist/gradebook/audit-events`

### Tests added / run

- gradebook commit blocked until review is teacher-confirmed
- teacher-confirmed review does not auto-commit
- initial commit persists record, commit history, and audit events
- correction supersedes prior commit; reversal blocks further corrections
- tenant isolation on gradebook endpoints
- no AI usage, workflow, mastery, or parent communication side effects from commits
- ruff check clean on gradebook backend files

### Manual validation checklist

1. Confirm a grading review manually.
2. Verify no gradebook record appears until **Commit to Gradebook** is clicked.
3. Commit grade and confirm active grade record appears.
4. Open `/teacher-assist/gradebook` and inspect commit history + audit trail.
5. Commit a correction with reason and verify superseded prior commit.
6. Reverse a grade with reason and verify record status becomes reversed.
7. Generate export-ready gradebook JSON for the assignment.
8. Confirm no mastery, parent communication, LMS, or SIS side effects occur.
9. Confirm trading system is untouched.

### Remaining gaps

- no CSV/PDF export download yet (JSON export view only)
- class-wide or grading-period gradebook rollups not implemented
- LMS sync and SIS integration intentionally deferred

### Next recommended phase

- Phase 26 - mastery matrix foundation
  - teacher-confirmed mastery tracking without automatic gradebook or parent communication side effects

## Phase 25 - Operational UX Cohesion + Teacher Action Workspace

### What was implemented

- backend-composed read model at `GET /v1/teacher-assist/action-workspace`
- unified operational action workspace aggregating extractions, grading, gradebook, workflows/exports, and planning/assignments
- summary counts, prioritized action items, grouped sections, class rollups, and recent activity feed
- safe TeacherAssist-only navigation targets for each action item
- `/teacher-assist/actions` frontend route with summary cards, priority panel, grouped sections, and class rollups
- Workspace summary page now links prominently to the Actions workspace
- assignment and gradebook deep links via `?assignment_id=` query params

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/action_workspace.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-action-workspace-screen.tsx`
- `frontend/apps/web/app/teacher-assist/actions/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-gradebook-screen.tsx`

### API routes added

- `GET /v1/teacher-assist/action-workspace`

### Tests added / run

- action workspace requires TeacherAssist product access
- tenant isolation prevents cross-tenant action visibility
- failed extraction jobs appear as critical action items
- pending extracted text appears as review action items
- AI-suggested grading reviews appear as review action items
- teacher-confirmed uncommitted reviews appear as ready gradebook action items
- failed export/workflow items appear as critical or warning items
- navigation hrefs are safe TeacherAssist routes only
- read endpoint creates no AI usage events, workflows, gradebook commits, or activity side effects

### Manual validation checklist

1. Open `/teacher-assist/actions`.
2. Confirm summary cards show open action counts.
3. Confirm failed extraction jobs appear under Extractions.
4. Confirm extracted text pending teacher approval appears as review work.
5. Confirm AI-suggested grading reviews appear as teacher-confirmation work.
6. Confirm teacher-confirmed but uncommitted reviews appear as gradebook-ready work.
7. Confirm failed exports or workflows appear in Workflows / Exports.
8. Confirm each action routes to the correct existing detail/workspace page.
9. Confirm viewing the action workspace does not mutate data.
10. Confirm no AI usage events, gradebook commits, mastery updates, parent communication, Google API calls, LMS/SIS calls, or trading changes occur.

### Remaining gaps

- no live push notifications or websocket refresh beyond polling
- no dedicated workflow-detail screen; failed/stale workflows route to weekly planning
- action workspace does not yet support bulk remediation actions
- mastery automation, parent communication, LMS/SIS sync, and trading changes intentionally deferred

### Next recommended phase

- Phase 30 - teacher publish workflow for reviewed reteach plans, assignment effectiveness UI on Assignments screen, or real-provider reteach AI (teacher-confirmed only)

## Phase 28.5 - Teacher Workflow UX Polish + Workflow Cohesion

### What was implemented

- `GET /v1/teacher-assist/today` read-only Today workspace aggregating action queue, review items, mastery alerts, workflow progress cards, onboarding checklist, and recent activity
- `/teacher-assist/today` preferred landing page (`/teacher-assist` redirects here)
- Grouped navigation: Planning, Instruction, Assessment, Mastery, Operations, Settings
- Workflow progress cards: Lesson Plan → Assignment → Student Work → Grading Review → Gradebook → Mastery
- Reusable empty states, cross-link panels, onboarding checklist UI
- Tablet/Chromebook-friendly responsive layouts (collapsible mobile nav, stacked cards)

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/today_workspace.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/app/teacher-assist/today/page.tsx`
- `frontend/apps/web/app/teacher-assist/page.tsx` (redirect to Today)
- `frontend/apps/web/components/teacher-assist/teacher-assist-today-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-empty-state.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workflow-progress-card.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-onboarding-checklist.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-cross-links.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-dashboard-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### Migration notes

- No database migrations required (read-model composition only).

### Manual validation checklist

1. Open `/teacher-assist` and confirm redirect to `/teacher-assist/today`.
2. Verify Today shows prioritized queue categories and counts.
3. Confirm workflow progress cards render for assignments with pipeline steps.
4. Complete onboarding checklist items and verify progress updates.
5. Test grouped navigation on desktop and collapsible nav on tablet/mobile widths.
6. Follow cross-links from Mastery to Today, Actions, and Assignments.
7. Confirm viewing Today creates no mastery commits, AI usage, or workflow mutations.

### Next recommended phase

- Phase 30 - teacher publish workflow for reviewed reteach plans, assignment effectiveness UI on Assignments screen, or real-provider reteach AI behind existing guardrails (teacher-confirmed only)

## Phase 29 - AI-Assisted Reteach Plan Drafting

### What was implemented

- Reteach plan foundation tied to mastery matrices and standards (`draft`, `ai_draft`, `teacher_review`, `archived`)
- Version history for AI drafts and teacher edits (`initial`, `ai_draft`, `teacher_edit`)
- Mock AI reteach draft generation with standards-focused prompting (mastery levels, class summaries, reteach insights; STUDENT # only — no student names)
- AI usage event tracking (`reteach_plan_ai_draft`, provider/model/tokens/cost metadata)
- Activity events: `reteach_plan_created`, `reteach_plan_ai_drafted`, `reteach_plan_version_created`
- Mastery dashboard integration: weak / reteach-recommended standards → **Create reteach plan** → **Generate draft**
- `/teacher-assist/reteach-plans` review workspace with version history and teacher save flow

### Migration summary

- **055** `teacher_assist_reteach_plans` — plan header, status, matrix/standard/class/subject linkage, current version pointer, latest AI usage pointer
- **055** `teacher_assist_reteach_plan_versions` — versioned content JSON, prompt context JSON, provider metadata, AI usage FK
- **Rollback:** drop version table then plan table (no cross-product FKs)

### Backend files added

- `backend/services/api/alembic/versions/055_teacher_assist_reteach_plan_foundation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_reteach_plan.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_reteach_plan_version.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/reteach_plans.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/reteach_plan_ai_assist.py`

### Backend files modified

- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/prompt_contracts.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added

- `frontend/apps/web/app/teacher-assist/reteach-plans/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-reteach-plans-screen.tsx`

### Frontend files modified

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-heatmap.tsx`

### API routes added

- `GET /v1/teacher-assist/reteach-plans`
- `POST /v1/teacher-assist/reteach-plans`
- `GET /v1/teacher-assist/reteach-plans/{id}`
- `PUT /v1/teacher-assist/reteach-plans/{id}`
- `GET /v1/teacher-assist/reteach-plans/{id}/versions`
- `POST /v1/teacher-assist/reteach-plans/{id}/versions`
- `POST /v1/teacher-assist/reteach-plans/{id}/ai-draft`

### AI prompt contracts

- Feature: `reteach_plan_ai_draft` (`RETEACH_PLAN_AI_FEATURE`)
- Prompt version: `reteach-plan-ai-v1` (`RETEACH_PLAN_AI_PROMPT_VERSION`)
- Input context: standard insight, mastery distribution, anonymous STUDENT # summaries, reteach insight counts, optional teacher instructions
- Output fields: `reteach_objectives`, `instructional_strategies`, `small_group_recommendations`, `intervention_ideas`, `vocabulary_focus`, `assessment_checks`, `teacher_review_required`
- Provider: mock-only in this phase; real provider path guarded and disabled

### Tests added / run

- reteach plan create + AI draft creates version + AI usage event
- teacher edit creates second version and moves plan to `teacher_review`
- prompt context is anonymous-only (STUDENT # summaries, no names)
- tenant isolation on plan read and AI draft
- AI draft creates no mastery audit side effects
- full TeacherAssist planning suite: **121 passed**

### Manual validation checklist

1. Open `/teacher-assist/mastery`, select a matrix with committed evaluations.
2. On **Standards needing reteach** or **Weakest standards**, click **Create reteach plan**.
3. Confirm redirect to `/teacher-assist/reteach-plans?id=...` with plan status `draft`.
4. Click **Generate draft** and confirm status becomes `ai_draft` with version `v1` (`ai_draft`).
5. Review AI sections (objectives, strategies, small groups, interventions, vocabulary, assessment checks).
6. Save a **teacher-reviewed version** and confirm status becomes `teacher_review` with `v2` (`teacher_edit`).
7. Confirm no mastery commits, gradebook commits, or parent communication occur.
8. Confirm AI usage event recorded with feature `reteach_plan_ai_draft`.
9. Confirm another tenant cannot read or draft the plan (404).
10. Archive a plan and confirm AI draft is blocked.

### Remaining gaps

- no real LLM provider execution for reteach drafts (mock only)
- no publish/link workflow into daily teaching or weekly plans yet
- no collaborative reteach plan sharing or admin moderation
- no automatic mastery updates, parent communication, gradebook commits, LMS/SIS sync, or trading changes (intentionally deferred)

### Next recommended phase

- Phase 31 - teacher-controlled send handoff metadata, assignment effectiveness UI on Assignments, or real-provider newsletter AI (no automatic outbound communication)

## Phase 30 - Weekly Newsletter Generation

### What was implemented

- `/teacher-assist/newsletters` workspace with statuses: `draft`, `review`, `approved`, `archived`
- Mock AI newsletter drafts from instructional activity (weekly plans, assignments, teacher notes, grading-period context)
- PII-safe prompting: no student names, grades, behavior comments, or PII in AI context
- Section regeneration: overview, upcoming learning, teacher message, reminders
- Version history (`ai_draft`, `ai_section_regen`, `teacher_edit`)
- Export HTML, PDF, DOCX for teacher-controlled distribution — **no email/SMS sending**

### Migration summary

- **056** `teacher_assist_newsletters`, `teacher_assist_newsletter_versions`, `teacher_assist_newsletter_exports`

### API routes added

- `GET/POST /v1/teacher-assist/newsletters`
- `GET/PUT /v1/teacher-assist/newsletters/{id}`
- `GET/POST /v1/teacher-assist/newsletters/{id}/versions`
- `POST /v1/teacher-assist/newsletters/{id}/ai-draft`
- `POST /v1/teacher-assist/newsletters/{id}/regenerate-section`
- `POST /v1/teacher-assist/newsletters/{id}/exports`
- `GET /v1/teacher-assist/newsletters/{id}/exports/{export_id}/download`

### AI prompt contracts

- Feature: `newsletter_generation` (`NEWSLETTER_AI_FEATURE`), prompt `newsletter-ai-v1`
- Section regen: `newsletter_section_regeneration`, prompt `newsletter-section-v1`
- Output: overview, what_we_learned, standards_covered, upcoming_topics, reminders, celebration_highlights, teacher_message

### Tests added / run

- newsletter create + AI draft + section regen + teacher version + approve + export (html/pdf/docx)
- tenant isolation
- full TeacherAssist planning suite: **123 passed**

### Manual validation checklist

1. Open `/teacher-assist/newsletters` and create a draft for a class/subject.
2. Add teacher notes and generate an AI draft.
3. Confirm status moves to `review` and sections populate without student PII.
4. Regenerate reminders or teacher message section.
5. Save a teacher-reviewed version and mark `approved`.
6. Export HTML, PDF, and DOCX and download each file.
7. Confirm TeacherAssist does not send any messages automatically.

### Recommended Phase 31

- Teacher-controlled send handoff metadata (export + copy workflow only)
- Assignment effectiveness UI on Assignments screen
- Real-provider newsletter AI behind existing guardrails
- Still **no outbound communication**, automatic mastery updates, or gradebook side effects

## Phase 27 - Mastery Visualization + Reteach Insights

### What was implemented

- read-only mastery heatmap aggregation (standards × anonymous STUDENT # rows, committed evaluations only)
- rules-based reteach insight aggregation with configurable thresholds (`healthy`, `monitor`, `reteach_recommended`, `critical_attention`)
- student mastery drill-down summaries with deterministic trend visibility
- assignment effectiveness read model (assignment-linked evidence only)
- operational mastery dashboard with class/subject/grading-period filters
- workspace and action workspace mastery insight panels (read-only, no side effects)
- `/teacher-assist/mastery` heatmap UI with reteach insight cards and drill-down hover metadata

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_analytics_helpers.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_heatmaps.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/reteach_insights.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/assignment_effectiveness.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_dashboard.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_workspace_insights.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workspace_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/action_workspace.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/config.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-heatmap.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-action-workspace-screen.tsx`

### API routes added

- `GET /v1/teacher-assist/mastery-matrices/{id}/heatmap`
- `GET /v1/teacher-assist/mastery-matrices/{id}/reteach-insights`
- `GET /v1/teacher-assist/mastery-matrices/{id}/student-summary/{student_number}`
- `GET /v1/teacher-assist/assignments/{id}/effectiveness`
- `GET /v1/teacher-assist/mastery-dashboard`

### Migration notes

- **No new tables** in Phase 27 — all analytics are computed read models over Phase 26 mastery tables.
- **No new indexes** required for initial rollout; large-matrix performance tuning may add composite indexes later.
- **Rollback:** safe to revert API/service/frontend changes without database rollback.

### Tests added / run

- heatmap excludes draft evaluations (committed-only)
- reteach threshold classification (`monitor` at 50% mastery)
- tenant isolation on analytics endpoints
- student summary + deterministic trend after correction lineage
- dashboard/workspace/action analytics create no audit/AI side effects
- full TeacherAssist planning suite: **118 passed**

### Manual validation checklist

1. Commit mastery evaluations for multiple STUDENT # rows.
2. Open `/teacher-assist/mastery` and verify heatmap cells reflect committed levels only.
3. Confirm draft evaluations do not appear in heatmap analytics.
4. Review reteach insight cards (strongest/weakest/improving/declining/unassessed).
5. Click a STUDENT # row and verify student drill-down summary loads.
6. Open `/teacher-assist/workspace` and verify mastery insight counts appear.
7. Open `/teacher-assist/actions` and verify mastery alert items link back to mastery workspace.
8. Filter mastery dashboard by class, grading period, and subject.
9. Confirm viewing analytics creates no new mastery commits, AI usage, workflows, or exports.

### Remaining gaps

- assignment effectiveness UI on Assignments screen not yet wired (API available)
- no persisted reteach recommendation rows (by design — computed read models only)
- standard trend accuracy limited without longer commit history windows
- district analytics, predictive forecasting, and AI reteach plans remain deferred

### Next recommended phase

- Phase 28 - assignment effectiveness UI, reteach planning drafts, or guarded AI mastery suggestion drafts (teacher-confirmed only)

## Phase 26 - Mastery Matrix Foundation + Standards Progress Tracking

### What was implemented

- persisted mastery matrices, matrix standards, evaluations, commit history, and audit events
- teacher-confirmed-only mastery lifecycle: draft evaluation → explicit commit → correction/reversal lineage
- evidence linkage via `evidence_source_type` / `evidence_source_id` for assignments, grading reviews, gradebook commits, and manual observations
- class/standard/student read-model summaries and reteach visibility endpoints
- `/teacher-assist/mastery` workspace with standards cards, student-number matrix, commit drill-down, and teacher-confirmed actions
- no automatic mastery updates from grading confirmation, gradebook commits, or AI suggestions

### Backend files added or updated

- `backend/services/api/alembic/versions/054_teacher_assist_mastery_matrix_foundation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_mastery_matrix.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_mastery_matrix_standard.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_mastery_evaluation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_mastery_commit.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_mastery_audit_event.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_matrix.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_commit_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mastery_visualization.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-mastery-screen.tsx`
- `frontend/apps/web/app/teacher-assist/mastery/page.tsx`

### API routes added

- `GET /v1/teacher-assist/mastery-matrices`
- `POST /v1/teacher-assist/mastery-matrices`
- `GET /v1/teacher-assist/mastery-matrices/{id}`
- `PUT /v1/teacher-assist/mastery-matrices/{id}`
- `POST /v1/teacher-assist/mastery-evaluations`
- `PUT /v1/teacher-assist/mastery-evaluations/{id}`
- `GET /v1/teacher-assist/mastery-evaluations/{id}`
- `POST /v1/teacher-assist/mastery-evaluations/{id}/commit`
- `POST /v1/teacher-assist/mastery-evaluations/{id}/corrections`
- `POST /v1/teacher-assist/mastery-evaluations/{id}/reversals`
- `GET /v1/teacher-assist/mastery-matrices/{id}/summary`
- `GET /v1/teacher-assist/mastery-matrices/{id}/standards`
- `GET /v1/teacher-assist/mastery-matrices/{id}/students`
- `GET /v1/teacher-assist/mastery-matrices/{id}/reteach-summary`

### Tests added / run

- mastery matrix requires TeacherAssist product access
- tenant isolation on mastery matrix endpoints
- teacher commit required before active mastery counts in summaries
- correction/reversal lineage persistence and reversed-state blocking
- standards ownership validation against matrix subject/class context
- no AI usage, gradebook, workflow, or parent communication side effects from mastery commits

### Manual validation checklist

1. Create a mastery matrix for a class and grading period.
2. Add standards to the matrix.
3. Create draft mastery evaluations for anonymous student numbers.
4. Confirm teacher commit is required before mastery counts as active.
5. Commit a correction and verify lineage persistence.
6. Reverse a mastery commit and confirm reversal state visibility.
7. Open mastery summaries and verify standards distributions and reteach visibility.
8. Confirm no parent communication, AI auto-commit, LMS/SIS sync, or trading changes occur.

### Remaining gaps

- AI mastery suggestions remain deferred
- reteach-plan generation remains deferred
- district-level analytics and mastery forecasting remain deferred
- parent communication and LMS/SIS sync intentionally deferred

### Next recommended phase

- Phase 28 - assignment effectiveness UI, reteach planning drafts, or guarded AI mastery suggestion drafts (teacher-confirmed only)

## Phase 26 - Mastery Matrix Foundation + Standards Progress Tracking

### What was implemented

- guarded mock-first AI grading suggestion service that consumes only teacher-approved grading-prep context
- `POST /v1/teacher-assist/grading-reviews/{id}/ai-suggestions` with draft suggestion fields persisted on grading reviews
- teacher-review-required semantics: suggestions set `ai_suggested` status and never auto-confirm
- Assignments grading review UI: **Generate AI Suggestion** when prep is ready, blocked reason when not
- AI usage event + activity event tracking for suggestion generation
- real provider mode remains guarded/disabled for this phase

### Backend files added or updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/grading_ai_assist.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/prompt_contracts.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`

### API routes added

- `POST /v1/teacher-assist/grading-reviews/{id}/ai-suggestions`

### Tests added / run

- AI suggestion blocked when extraction is not teacher-approved
- mock suggestion populates grading review fields and AI usage event
- approved-text priority respected via grading-prep context
- `teacher_confirmed` is never automatic
- no workflow side effects from suggestion generation
- tenant isolation on AI suggestion endpoint
- real provider mode remains guarded/disabled

### Manual validation checklist

1. Upload student work.
2. Run extraction.
3. Confirm AI suggestion is blocked before teacher approval.
4. Approve extracted text.
5. Generate AI grading suggestion.
6. Confirm suggestion appears as draft / teacher-review-required.
7. Edit suggestion.
8. Manually confirm grading review.
9. Confirm AI suggestions do not auto-commit to gradebook (manual commit is a separate Phase 24 action).
10. Confirm no mastery or parent communication side effects occur.
11. Confirm trading system is untouched.

### Remaining gaps

- real provider grading assist execution remains disabled (mock-only MVP)
- async worker path for long-running real provider suggestions not implemented
- workspace dashboard does not yet surface AI-suggested reviews needing confirmation
- no rubric-item-level AI suggestions yet (review-level fields only)

### Next recommended phase

- Phase 24 - gradebook commit foundation (teacher-confirmed only)
  - persist teacher-confirmed grading review outcomes into a guarded gradebook commit seam without mastery automation or parent communication

## Phase 16 - Unified Teacher Workspace + Workflow Cohesion Layer

### What was implemented

- Added a backend-composed TeacherAssist operational workspace through:
  - `GET /v1/teacher-assist/workspace`
  - `workspace_service.py` read-model aggregation
  - a new frontend workspace landing screen at `/teacher-assist/workspace`
- Added durable append-only TeacherAssist activity events through:
  - `teacher_assist_activity_events`
  - reusable `record_activity_event(...)` helpers
  - recent activity feed support for the last 50 events
- Added workspace orchestration that aggregates:
  - current school year and active grading period
  - today summary counts
  - class-centric workspaces
  - needs-attention alerts
  - recent activity
  - active workflows
  - teacher review-required items
  - workspace stats
- Added class-centric grouping so teachers can see, per class:
  - active plans
  - assignments
  - pending grading reviews
  - recent submissions
  - workflow summaries
  - packet summaries
  - needs-attention count
- Added needs-attention detection for:
  - failed workflows
  - retrying queued workflows
  - cancelled workflows
  - stale workflow heartbeats
  - in-progress plans
  - standards-alignment gaps
  - general plan quality flags
  - missing-context warnings
  - student-work submissions pending review
  - grading reviews awaiting teacher confirmation
- Added activity-event recording hooks across existing TeacherAssist services for:
  - workflow lifecycle changes
  - plan creation/update/completion and section regeneration
  - assignment create/update/status changes
  - printable packet generation
  - student-work upload/status changes
  - grading-review creation/update/confirmation
- Updated the TeacherAssist frontend so the workspace becomes the operational landing experience and shows:
  - today summary cards
  - grouped needs-attention panel
  - class workspace cards
  - review-required queue
  - active workflow list
  - recent activity timeline
  - workspace stats

### Migrations added

- `048_teacher_assist_activity_events.py`
  - creates the append-only `teacher_assist_activity_events` table plus supporting indexes

### Backend files added or updated

- `backend/services/api/alembic/versions/048_teacher_assist_activity_events.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_activity_event.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/activity_events.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workspace_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/assignments.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/print_packets.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/student_work.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/grading_reviews.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files added or updated

- `frontend/apps/web/components/teacher-assist/teacher-assist-workspace-screen.tsx`
- `frontend/apps/web/app/teacher-assist/workspace/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-dashboard-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### Activity-event types implemented

- `workflow_started`
- `workflow_completed`
- `workflow_failed`
- `workflow_cancelled`
- `plan_created`
- `plan_updated`
- `plan_regenerated`
- `plan_completed`
- `assignment_created`
- `assignment_updated`
- `assignment_status_changed`
- `packet_generated`
- `student_work_uploaded`
- `student_work_status_changed`
- `grading_review_created`
- `grading_review_confirmed`
- `grading_review_updated`
- `section_regenerated`

### User-visible behavior after Phase 16

1. Teachers can land in one operational TeacherAssist workspace instead of stitching together plans, assignments, workflows, uploads, and reviews manually.
2. Teachers can immediately see what needs attention, which classes are busiest, what was recently changed, and which reviews still need confirmation.
3. Workflow failures, pending submissions, plan-quality warnings, and unconfirmed grading reviews are now operationally visible without enabling OCR, AI grading, mastery updates, or parent communication.

### Manual validation checklist

- Backend:
  - `python3 -m ruff check src tests`
  - `python3 -m pytest -q tests/test_teacher_assist_planning.py`
- Frontend:
  - `npm run lint`
  - `npm run build`
- Result notes:
  - backend TeacherAssist suite passes
  - frontend lint/build pass
  - one pre-existing frontend warning remains outside TeacherAssist scope: `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js` unused `handler`

### Known limitations after Phase 16

- Workspace polling is periodic and lightweight; live push notifications/websocket infrastructure is not implemented.
- The workspace is intentionally summary-oriented and does not yet provide a dedicated workflow-remediation detail screen.
- Activity events are relational and append-only foundations, not a broader event-streaming platform.
- Frontend validation is currently lint/build only because this repo does not yet have a wired TeacherAssist frontend test harness.

### Next recommended phase

- Phase 17 - Workspace action drill-down and remediation flows
  - add deeper workflow failure detail, retry/remediation affordances, richer review-required routing, and teacher action shortcuts on top of the unified workspace without introducing OCR, grading automation, or mastery auto-commit

## Phase 15 - Grading Review Foundation + Teacher Confirmation

### What was implemented

- Added persisted grading-review foundations through:
  - `assignment_grading_reviews`
  - `assignment_grading_review_items`
- Added tenant-safe backend grading-review APIs:
  - `GET /v1/teacher-assist/assignments/{id}/grading-reviews`
  - `POST /v1/teacher-assist/student-work/{id}/grading-review`
  - `GET /v1/teacher-assist/grading-reviews/{id}`
  - `PUT /v1/teacher-assist/grading-reviews/{id}`
  - `PATCH /v1/teacher-assist/grading-reviews/{id}/status`
- Added software-only grading review creation that:
  - requires a visible teacher-owned assignment and student-work submission inside the current TeacherAssist tenant
  - stores anonymous `student_number` only and rejects student-name/email-like feedback content
  - copies assignment, class, subject, school-year, and grading-period context onto each review row
  - preserves safe placeholder metadata for later AI phases: provider name/model, prompt version, usage id, and `review_source`
  - avoids all OCR/provider usage, AI usage events, mastery updates, gradebook commits, and parent communication output
- Added grading-review lifecycle support for:
  - `draft`
  - `ai_suggested`
  - `teacher_reviewing`
  - `teacher_confirmed`
  - `returned_for_revision`
  - `archived`
- Added teacher-confirmation validation so `teacher_confirmed` requires a confirmed score or confirmed feedback.
- Updated the assignments workspace to support:
  - starting a grading review from an anonymous student-work submission
  - grading review list/history per assignment
  - manual review editing for score suggestion, max score, feedback summary, strengths, improvement areas, and teacher notes
  - teacher-confirmed score/feedback entry and explicit status updates
  - disabled placeholders for AI grading, mastery commit, and parent communication

### Migration added

- `047_teacher_assist_grading_review_foundation.py`
  - creates grading-review and grading-review-item tables plus supporting indexes and relationships

### Backend files added or updated

- `backend/services/api/alembic/versions/047_teacher_assist_grading_review_foundation.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_grading_review.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_grading_review_item.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_student_work_submission.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/grading_reviews.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### User-visible behavior after Phase 15

1. Teachers can start a manual grading review from an anonymous student-work submission using `STUDENT #` only.
2. The assignments workspace now shows grading review history, editable review details, and explicit teacher-confirmation fields.
3. Provider/model/usage metadata remains visible as placeholders, while AI grading, mastery commit, and parent communication remain disabled.

### Remaining gaps after Phase 15

- OCR and scan-content extraction are still deferred (implemented in Phases 18–20).
- AI/provider-assisted grading suggestions and manual gradebook commits are implemented in Phases 23–24.
- Mastery updates and parent communication generation are still deferred.
- Criterion-level item editing is persisted in the backend foundation, but the current frontend keeps review editing at the summary level.

## Phase 14 - Uploaded Student Work Intake Foundation

### What was implemented

- Added persisted student-work intake foundations through:
  - `assignment_student_work_submissions`
- Added tenant-safe backend student-work APIs:
  - `GET /v1/teacher-assist/assignments/{id}/student-work`
  - `POST /v1/teacher-assist/assignments/{id}/student-work`
  - `GET /v1/teacher-assist/student-work/{id}`
  - `PATCH /v1/teacher-assist/student-work/{id}/status`
  - `PATCH /v1/teacher-assist/student-work/{id}/packet-context`
- Added software-only submission intake that:
  - requires a visible teacher-owned assignment inside the current TeacherAssist tenant
  - stores anonymous `student_number` instead of student names
  - copies assignment, class, subject, school-year, and grading-period context onto each submission row
  - optionally links submissions to printable packet and packet-page context when student numbers match
  - persists upload metadata only: original filename, mime type, file size, storage key, upload status, and processing status
  - avoids all AI/provider usage, OCR, grading outputs, and mastery updates
- Added submission lifecycle support for:
  - upload status: `uploaded`, `archived`
  - processing status: `pending_review`, `ready_for_processing`, `processing_deferred`, `archived`
- Updated the assignments workspace to support:
  - anonymous student-work uploads
  - submission list by `STUDENT #`
  - upload metadata display
  - manual review-status updates
  - optional packet/page linking after upload
  - continued disabled placeholders for later grading automation and mastery updates

### Migration added

- `046_teacher_assist_student_work_intake.py`
  - creates the student-work submission table plus supporting indexes

### Backend files added or updated

- `backend/services/api/alembic/versions/046_teacher_assist_student_work_intake.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_print_packet.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_print_page.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_student_work_submission.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/print_packets.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/student_work.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### User-visible behavior after Phase 14

1. Teachers can upload completed student work against an assignment using anonymous STUDENT # values.
2. Submissions keep packet/page context optional, so uploads can happen before or after QR packet generation.
3. The assignments workspace now shows upload metadata, review status, and packet/page linking without running OCR or grading.

### Remaining gaps after Phase 14

- OCR and any scan-content extraction are still deferred.
- Grading-review automation and mastery updates are still deferred.
- Submission intake currently stores upload metadata and linkage only; there is not yet a richer teacher annotation workflow.

## Phase 13 - QR-Coded Printable Assignment Packets

### What was implemented

- Added persisted printable packet foundations through:
  - `assignment_print_packets`
  - `assignment_print_pages`
- Added tenant-safe backend print-packet APIs:
  - `POST /v1/teacher-assist/assignments/{id}/print-packets`
  - `GET /v1/teacher-assist/assignments/{id}/print-packets`
  - `GET /v1/teacher-assist/print-packets/{id}`
  - `GET /v1/teacher-assist/print-packets/{id}/pages`
- Added software-only packet generation that:
  - requires a visible teacher-owned assignment inside the current TeacherAssist tenant
  - derives packet size from `class.student_count` and `pages_per_student`
  - creates anonymous `student_number` page rows without storing names, emails, or real student ids
  - generates compact QR payload JSON plus SVG QR images for each page
  - avoids all AI/provider usage and does not create any usage event
- Added supported printable template types for:
  - `blank_writing_page`
  - `lined_writing_page`
  - `short_answer_page`
- Added a printable frontend workflow at:
  - `/teacher-assist/assignments`
  - `/teacher-assist/assignments/print-packets?id={packet_id}`
- Updated the assignments workspace to support:
  - print-packet creation
  - packet history per assignment
  - packet summary and total page counts
  - QR payload preview without sensitive data
  - first-page QR previews
  - a browser print view with packet pages and QR codes
- Kept upload, grading review, and mastery actions disabled with “coming later” messaging.

### Migration added

- `045_teacher_assist_print_packets.py`
  - creates printable packet and printable page tables plus supporting indexes

### Backend files added or updated

- `backend/services/api/pyproject.toml`
- `backend/services/api/alembic/versions/045_teacher_assist_print_packets.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_print_packet.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_print_page.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/print_packets.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-print-packet-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx`
- `frontend/apps/web/app/teacher-assist/assignments/print-packets/page.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### User-visible behavior after Phase 13

1. Teachers can generate printable assignment packets directly from the assignments workspace without invoking AI.
2. Packet size is created from anonymous STUDENT # values and the chosen pages-per-student count.
3. Packet history, QR payload previews, and first-page previews are visible before printing.
4. A dedicated print view renders QR-coded packet pages for blank writing, lined writing, and short-answer templates.

### Remaining gaps after Phase 13

- Scan/upload ingestion, OCR, and grading-review execution are still deferred.
- QR packets currently render through the HTML print view only; there is not yet a PDF/export pipeline.
- Printable packets remain teacher-owned and assignment-scoped; broader collaborative packet governance is still deferred.
- Mastery-matrix workflows are still deferred.

## Phase 12 - Assignment Model Foundation

### What was implemented

- Added a persisted TeacherAssist assignment foundation through:
  - `assignments`
  - `assignment_standards`
  - `assignment_resources`
- Added tenant-safe backend assignment APIs:
  - `GET /v1/teacher-assist/assignments`
  - `POST /v1/teacher-assist/assignments`
  - `GET /v1/teacher-assist/assignments/{id}`
  - `PUT /v1/teacher-assist/assignments/{id}`
  - `PATCH /v1/teacher-assist/assignments/{id}/status`
  - `POST /v1/teacher-assist/assignments/{id}/standards`
  - `POST /v1/teacher-assist/assignments/{id}/resources`
  - `POST /v1/teacher-assist/weekly-plans/{id}/assignments`
- Enforced assignment validation for:
  - TeacherAssist product access
  - tenant isolation
  - school-year ownership
  - grading-period-to-school-year alignment
  - class-to-school-year alignment
  - subject-to-class alignment when class-subject mappings exist
  - subject-safe standards attachment
  - disallowed PII-like/student-identifying content patterns
- Added assignment lifecycle support for:
  - `draft`
  - `ready`
  - `assigned`
  - `collected`
  - `review_in_progress`
  - `reviewed`
  - `archived`
- Added supported assignment types for:
  - `writing`
  - `reading_response`
  - `short_answer`
  - `quiz`
  - `exit_ticket`
  - `project`
  - `homework`
  - `other`
- Added a software-only weekly-plan-to-assignment starter that:
  - creates a draft assignment from a visible instructional plan
  - links `source_plan_id`
  - copies class / subject / grading-period / standards / resource context when available
  - does not create any new AI usage event
  - does not call the provider seam
- Added a new TeacherAssist assignments workspace at:
  - `/teacher-assist/assignments`
- Updated TeacherAssist navigation to include **Assignments**.
- Added a basic assignment UI for:
  - assignment list and filters
  - create/edit form
  - lifecycle status updates
  - standards selection
  - disabled placeholders for QR packets, student-work upload, grading review, and mastery updates

### Migration added

- `044_teacher_assist_assignments.py`
  - creates assignment, assignment-standards, and assignment-resources tables plus supporting indexes

### Backend files added or updated

- `backend/services/api/alembic/versions/044_teacher_assist_assignments.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_standard.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_assignment_resource.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/constants.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_validator.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/assignments.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-assignments-screen.tsx`
- `frontend/apps/web/app/teacher-assist/assignments/page.tsx`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`

### User-visible behavior after Phase 12

1. Teachers now have a dedicated assignments workspace inside TeacherAssist.
2. Assignments can be created and edited with class, subject, grading period, standards, and due-date context.
3. Assignment status can move through a guarded teacher-owned lifecycle instead of using ad hoc freeform labels.
4. A visible instructional plan can now seed a draft assignment without invoking AI or changing trading behavior.

### Remaining gaps after Phase 12

- Uploaded student-work handling, OCR, and grading-review execution are still deferred.
- Mastery-matrix workflows are still deferred.
- The frontend assignment workspace does not yet expose richer assignment-resource editing beyond the backend starter/link foundation.

## Phase 8 - Shared Plan Library + Annual Curriculum Rollover Foundation

### What was implemented

- Added a shared instructional-plan library foundation on top of the existing `weekly_plans` artifact store instead of introducing a new generalized persistence table.
- Added backend visibility-aware plan discovery for reusable instructional plans through:
  - `GET /v1/teacher-assist/instructional-plans/library`
- Added filter support for the library endpoint across:
  - school year
  - subject
  - planning scope
  - visibility scope
  - reuse status
  - template flag
  - text search
- Kept plan-library visibility tenant-safe:
  - private plans remain owner-only
  - non-private plans are limited to the current TeacherAssist tenant context
- Added sharing/template control support through:
  - `PATCH /v1/teacher-assist/weekly-plans/{id}/sharing`
- Restricted sharing mutation to `owner_user_id` for now rather than adding incomplete tenant-admin mutation rules.
- Enhanced software-only plan copy through:
  - `POST /v1/teacher-assist/weekly-plans/{id}/copy`
- Extended copy payload support for:
  - `target_school_year_id`
  - `target_grading_period_id`
  - `target_class_id`
  - `title_override`
  - `copy_mode`
- Preserved source and derived lineage across personal copies and rollover copies.
- Kept copy behavior deterministic and AI-free:
  - no provider call
  - no AI usage event
  - copied plans default to teacher-owned/private artifacts
- Added annual curriculum rollover foundation through:
  - `GET /v1/teacher-assist/curriculum-rollover/candidates`
  - `POST /v1/teacher-assist/curriculum-rollover/copy`
- Added rollover candidate summaries for:
  - planning-scope counts
  - subjects represented
  - grading periods represented
  - duplicate-target detection when lineage already exists in the target year
- Added duplicate rollover warnings instead of silently duplicating artifacts.
- Added TeacherAssist frontend routes for:
  - `/teacher-assist/plans`
  - `/teacher-assist/curriculum-rollover`
- Added a plan library UI with:
  - My Plans
  - Shared / Reusable Plans
  - Templates
  - Prior-Year Plans
  - Open / Copy / sharing actions
- Added a curriculum rollover UI with:
  - source-year and target-year selection
  - reusable candidate loading
  - duplicate-aware selection
  - software-only rollover copy actions

### Backend files updated

- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-plan-library-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-curriculum-rollover-screen.tsx`
- `frontend/apps/web/app/teacher-assist/plans/page.tsx`
- `frontend/apps/web/app/teacher-assist/curriculum-rollover/page.tsx`

### User-visible behavior after Phase 8

1. Teachers can browse visible instructional plans in a dedicated library route.
2. Private plans remain owner-only, while shared plans stay tenant-scoped.
3. Owners can mark plans reusable/templates or return them to private status.
4. Copying a plan remains software-only and does not invoke AI.
5. Teachers can load prior-year reusable candidates and copy selected plans into a target school year with duplicate warnings.

### Remaining gaps after Phase 8

- Sharing mutation is owner-only; tenant-admin/team moderation workflows are not implemented yet.
- Collaborative editing and branch/version management beyond copy + manual edit are not implemented yet.
- Rollover currently focuses on instructional plans, not broader asset-library artifact families.
- Target grading-period remapping UI is not implemented yet.
- Real provider calls remain disabled by default.

## Phase 7 - Instructional Planning Scope + Reusable Plan Architecture Foundation + Guarded Provider Seam

### What was implemented

- Generalized TeacherAssist planning from weekly-only assumptions toward a broader instructional-planning model while keeping the existing `/teacher-assist/weekly-planning` route and `weekly_plans` artifact table stable.
- Added planning-scope and duration fields to planning drafts:
  - `planning_scope`
  - `module_title`
  - `start_date`
  - `end_date`
  - `estimated_weeks`
  - `instructional_days_count`
- Defaulted existing draft behavior to `weekly`.
- Expanded planning context previews and workflow snapshots to include:
  - scope-aware draft metadata
  - duration summary
  - grouped pacing segments
  - selected subjects, standards, and resources
  - readiness details that work for weekly and longer-duration plans
- Generalized mock generation into a flexible instructional-plan output shape with:
  - metadata
  - planning scope
  - duration
  - instructional arc
  - weekly segments
  - daily breakdown
  - standards progression
  - vocabulary
  - materials
  - differentiation
  - assessment checkpoint placeholders
- Preserved weekly behavior while allowing module and multi-week outputs to generate multiple weekly segments.
- Added reusable-plan architecture foundation on persisted plan artifacts:
  - `owner_user_id`
  - `source_plan_id`
  - `derived_from_plan_id`
  - `is_template`
  - `visibility_scope`
  - `reuse_status`
  - `school_year_origin_id`
- Added a safe software-only copy flow at:
  - `POST /v1/teacher-assist/weekly-plans/{id}/copy`
- Ensured copy/version/manual edit flows do not invoke AI and do not create AI usage events.
- Strengthened the provider seam with guarded config fields for:
  - `teacher_assist_ai_provider`
  - `teacher_assist_real_provider_enabled`
  - `teacher_assist_real_provider_model`
  - `teacher_assist_ai_max_input_tokens`
  - `teacher_assist_ai_max_output_tokens`
  - `teacher_assist_ai_daily_cost_limit_cents`
- Kept the real provider disabled by default and left mock mode as the only active path.
- Added generalized instructional-plan prompt-building metadata and structured provider-output validation so malformed output fails the workflow cleanly instead of persisting invalid artifacts.
- Updated the planning workspace UI language to **Instructional Planning** while keeping the route unchanged.
- Added planning UI fields for scope, title, module title, dates, and estimated duration.
- Added planning-screen provider metadata placeholders and kept future actions disabled.

### Migrations added

- `042_teacher_assist_instructional_planning.py`
  - adds planning-scope and duration columns to `planning_input_drafts`
  - adds planning-scope, duration, and reusable-plan lineage columns to `weekly_plans`
  - backfills `planning_scope = weekly`
  - backfills `owner_user_id = user_id`

### Backend files added or updated

- `backend/services/api/alembic/versions/042_teacher_assist_instructional_planning.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_weekly_plan.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/planning.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/planning_context_service.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_prompt_builder.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/instructional_plan_validator.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/mock_ai_provider.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/provider_config.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist/workflow_service.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/tests/test_teacher_assist_planning.py`

### Frontend files updated

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-plan-viewer.tsx`

### User-visible behavior after Phase 7

1. Teachers can still use the existing weekly-planning route.
2. Planning drafts now support weekly, multi-week, module, unit, and grading-period scopes.
3. Context preview shows duration and grouped pacing context instead of only flat weekly inputs.
4. Generated mock plan artifacts now look like generalized instructional plans rather than only weekly-only payloads.
5. Teachers can create teacher-owned copies of plan artifacts without triggering AI usage.

### Remaining gaps after Phase 7

- Shared/team visibility behavior is not exposed in the UI yet.
- Template-management workflows are not implemented yet.
- Selective section regeneration is not implemented yet.
- Real provider calls remain disabled by default.

## Repo inventory findings

### Frontend

- Frontend lives in the separate `frontend` repo under `frontend/apps/web`.
- Stack: **Next.js 15.5.15**, React 19.1.0, TypeScript, Tailwind CSS v4.
- Uses **App Router** with routes directly under `frontend/apps/web/app/*`.
- Current root layout is global and trading-oriented:
  - wraps the app in `AuthProvider`
  - wraps the app in `TradingModeProvider`
  - hard-codes dark mode via `<html className="dark">`
- Current UI is custom Tailwind-based, with no heavy UI framework.
- Existing navigation and shell are centralized in:
  - `frontend/apps/web/components/layout/app-shell.tsx`
  - `frontend/apps/web/components/nav/app-nav-links.ts`

### Backend

- Backend lives in `backend/services/api`.
- Stack: **FastAPI 0.115+**, Python 3.12, SQLAlchemy 2.0+, Alembic, PostgreSQL, psycopg 3.1+.
- JWT auth and DB-backed refresh sessions are already implemented.
- Current API is modular under `backend/services/api/src/oziebot_api/api/v1/*`.
- Existing long-running/background infrastructure already exists through dedicated services:
  - strategy-engine
  - risk-engine
  - execution-engine
  - alerts-worker
  - market-data-ingestor
- Async queue/workflow precedent exists through a **Postgres outbox / leased worker** pattern.

### Deployment

- Backend currently deploys to a **Lightsail lean Docker Compose host**.
- Frontend currently deploys separately as a **static export to S3/CloudFront**.
- ECS/Fargate artifacts still exist in `infrastructure/aws/**`, but current validation-phase live shape is Lightsail backend + S3/CloudFront frontend.

## Architecture findings

1. **TeacherAssist should be a separate product module, not a trading extension.**
2. **Auth, tenancy, user identity, Postgres, and deployment foundations can be reused safely.**
3. **Trading entitlements should not be reused for TeacherAssist product access.**
4. **TeacherAssist should get its own route tree, shell, theme, backend modules, tables, and worker.**
5. **Large generation/export work should use the existing async outbox-worker pattern, not request-thread processing.**
6. **LLM access should stay backend-only and follow the existing OpenAI-compatible service precedent already used in AI diagnostics.**

## Documentation foundation added

### Boundaries added

- `BOUNDARIES.md` now defines strict product, frontend, backend, entitlement, utility, data, and privacy boundaries.
- TeacherAssist is explicitly separated from trading engines, Coinbase services, trading entitlements, and trading worker responsibilities.
- Route-scoped frontend isolation and backend namespace isolation are now documented as non-negotiable architectural constraints.

### Glossary added

- `GLOSSARY.md` now defines the core TeacherAssist domain language used across planning and future implementation.
- This includes product-access terms, academic setup terms, instructional planning terms, mastery terminology, workflow terminology, and privacy language such as `STUDENT #` and `No PII`.

### Implementation rules added

- `IMPLEMENTATION_RULES.md` now documents the build discipline for future phases.
- It locks in phase-by-phase delivery, migration-note expectations, access-control documentation expectations, workflow status/error documentation, mock-AI-first requirements, and teacher-confirmation requirements for grading-related AI output.

## Phase 1 implementation

### What was implemented

- Added a new platform-level product access foundation in the backend:
  - `platform_products`
  - `tenant_product_access`
  - `user_product_preferences`
- Seeded `platform_products` with:
  - `trading`
  - `teacher_assist`
- Extended `/v1/me` so the bootstrap response now includes:
  - `products`
  - `default_product`
- Added product preference endpoints:
  - `GET /v1/me/products`
  - `PATCH /v1/me/default-product`
- Added product-aware frontend routing so login and `/` redirect into the user’s accessible default product.
- Added a shared app switcher that:
  - shows available apps
  - navigates between products
  - can explicitly set the default app
- Added an isolated TeacherAssist frontend subtree with placeholder pages only:
  - `/teacher-assist`
  - `/teacher-assist/weekly-planning`
  - `/teacher-assist/daily-teaching`
  - `/teacher-assist/assessments`
  - `/teacher-assist/students`
  - `/teacher-assist/insights`
  - `/teacher-assist/newsletters`
  - `/teacher-assist/communication`
  - `/teacher-assist/settings`
- Added a dedicated TeacherAssist shell with scoped light-theme styling and separate navigation.

### Files added or changed

Representative backend additions:

- `backend/services/api/alembic/versions/036_platform_products.py`
- `backend/services/api/src/oziebot_api/models/platform_product.py`
- `backend/services/api/src/oziebot_api/models/tenant_product_access.py`
- `backend/services/api/src/oziebot_api/models/user_product_preference.py`
- `backend/services/api/src/oziebot_api/services/product_access.py`
- `backend/services/api/tests/test_me_products.py`

Representative backend updates:

- `backend/services/api/src/oziebot_api/api/v1/auth.py`
- `backend/services/api/src/oziebot_api/api/v1/me.py`
- `backend/services/api/src/oziebot_api/schemas/me.py`
- `backend/services/api/src/oziebot_api/models/__init__.py`
- `backend/services/api/alembic/env.py`

Representative frontend additions:

- `frontend/apps/web/lib/products.ts`
- `frontend/apps/web/components/platform/app-switcher.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/app/teacher-assist/**`

Representative frontend updates:

- `frontend/apps/web/components/providers/auth-provider.tsx`
- `frontend/apps/web/components/layout/app-shell.tsx`
- `frontend/apps/web/lib/auth-service.ts`
- `frontend/apps/web/lib/auth-types.ts`
- `frontend/apps/web/app/page.tsx`
- `frontend/apps/web/app/globals.css`

### Migrations added

- `036_platform_products.py`
  - creates `platform_products`
  - creates `tenant_product_access`
  - creates `user_product_preferences`
  - seeds `trading` and `teacher_assist`
  - backfills trading product access for existing tenants
  - backfills trading as the default app for existing users with tenant memberships

### API routes added

- `GET /v1/me/products`
- `PATCH /v1/me/default-product`

### Frontend routes added

- `/teacher-assist`
- `/teacher-assist/weekly-planning`
- `/teacher-assist/daily-teaching`
- `/teacher-assist/assessments`
- `/teacher-assist/students`
- `/teacher-assist/insights`
- `/teacher-assist/newsletters`
- `/teacher-assist/communication`
- `/teacher-assist/settings`

### How product and default app behavior works

1. Product access is granted at the tenant level through `tenant_product_access`.
2. A signed-in user sees the union of selectable products available through the user’s tenant memberships.
3. `active` and `trial` products are selectable.
4. `disabled` products are not selectable and cannot become the default app.
5. If a user has an explicit default in `user_product_preferences`, that default is used.
6. If the user does not have an explicit default, the first accessible product association is used as the fallback default.
7. Product routing currently maps:
   - `trading` -> `/dashboard`
   - `teacher_assist` -> `/teacher-assist`

### How TeacherAssist stays isolated from trading

- No trading worker or engine service files were modified.
- No Coinbase execution or integration logic was changed for TeacherAssist.
- Product access is modeled separately from `tenant_entitlements`.
- TeacherAssist UI lives under its own `/teacher-assist` route subtree.
- TeacherAssist uses its own light-mode shell and navigation.
- Shared auth/bootstrap logic remains platform-level rather than TeacherAssist depending on trading-specific modules.

### Known risks

- The migration backfills trading product access to all existing tenants to preserve current login and dashboard behavior; future billing/access refinement may need to narrow that model.
- Root layout still hard-codes dark mode globally, so TeacherAssist depends on scoped CSS variable overrides rather than a platform-wide theme switch.
- The current frontend remains static-exported, so product switching and bootstrap behavior continue to rely on client-side routing and backend APIs.
- Product access is currently tenant-based and aggregated across memberships; if TeacherAssist eventually needs user-level licensing rules, the model will need extension rather than replacement.

### Manual test steps

1. Log in as an existing trading user and confirm the app still lands on the trading experience.
2. Call `GET /v1/me` and confirm `products` and `default_product` are present.
3. Confirm a trading-only user lands on `/dashboard`.
4. Grant `teacher_assist` access to a tenant and confirm the user can open `/teacher-assist`.
5. Confirm a multi-product user sees the app switcher and can navigate between trading and TeacherAssist.
6. Use the switcher to set `teacher_assist` as default and confirm the next bootstrap/login redirects there.
7. Confirm TeacherAssist renders in the light shell while `/dashboard` keeps the trading dark shell.
8. Confirm no trading worker/service files were touched in the change set.

### Next recommended phase

**Teacher and academic setup foundation**

The next phase should focus on teacher profile/setup, school year, grading period, subject, and class scaffolding before any planning, uploads, or AI generation features are introduced.

## Phase 2 - Teacher Foundation + School-Year Setup

### What was implemented

- Added the foundational TeacherAssist educational setup model in the backend for:
  - teacher profiles
  - school years
  - grading periods
  - subjects
  - classes
  - class-to-subject assignments
  - standards / TEKS entries
- Added protected TeacherAssist setup APIs under `/v1/teacher-assist/*`.
- Added canonical setup option metadata for:
  - grading period types
  - standards types
  - supported grade levels
- Added tenant-aware validation for:
  - TeacherAssist product access
  - school-year ownership
  - grading-period date windows
  - grading-period overlap checks
  - class student count
  - subject/class/school-year relationship ownership
- Replaced the Phase 1 TeacherAssist dashboard/settings placeholders with:
  - a setup-first TeacherAssist dashboard checklist
  - a real settings workspace for profile, school years, grading periods, classes, subjects, assignments, and standards
- Kept TeacherAssist isolated under its own backend namespace and frontend shell with no trading worker or execution changes.

### Tables and models added

Added tables and SQLAlchemy models:

- `teacher_profiles`
- `school_years`
- `grading_periods`
- `subjects`
- `classes`
- `class_subjects`
- `standards`

Representative model files:

- `backend/services/api/src/oziebot_api/models/teacher_assist_profile.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_school_year.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_grading_period.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_subject.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_class.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_class_subject.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_standard.py`

### Migrations added

- `037_teacher_assist_foundation.py`
  - creates `teacher_profiles`
  - creates `school_years`
  - creates `grading_periods`
  - creates `subjects`
  - creates `classes`
  - creates `class_subjects`
  - creates `standards`
  - adds indexes and uniqueness for setup ownership and class-subject pairing

### APIs added

Added TeacherAssist APIs under `/v1/teacher-assist`:

- `GET /v1/teacher-assist/options`
- `GET /v1/teacher-assist/profile`
- `PUT /v1/teacher-assist/profile`
- `GET /v1/teacher-assist/school-years`
- `POST /v1/teacher-assist/school-years`
- `PUT /v1/teacher-assist/school-years/{id}`
- `GET /v1/teacher-assist/grading-periods`
- `POST /v1/teacher-assist/grading-periods`
- `PUT /v1/teacher-assist/grading-periods/{id}`
- `GET /v1/teacher-assist/subjects`
- `POST /v1/teacher-assist/subjects`
- `GET /v1/teacher-assist/classes`
- `POST /v1/teacher-assist/classes`
- `PUT /v1/teacher-assist/classes/{id}`
- `POST /v1/teacher-assist/class-subjects`
- `GET /v1/teacher-assist/standards`
- `POST /v1/teacher-assist/standards`

### Frontend pages and components added or updated

Representative frontend additions:

- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/components/teacher-assist/teacher-assist-dashboard-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-settings-screen.tsx`

Representative frontend updates:

- `frontend/apps/web/app/teacher-assist/page.tsx`
- `frontend/apps/web/app/teacher-assist/settings/page.tsx`
- `frontend/apps/web/app/globals.css`

### Validation rules added

- TeacherAssist endpoints require authenticated access to the `teacher_assist` product.
- TeacherAssist setup resolves to the first tenant membership with active or trial TeacherAssist access.
- `preferred_grade_level` and class `grade_level` are validated against supported grade levels.
- `preferred_grading_period_type` and grading-period rows are validated against canonical grading-period types.
- `standard_type` is validated against canonical standard types.
- timezone values are validated through Python `ZoneInfo`.
- school-year start date must be on or before end date.
- grading-period dates must stay inside the school year window.
- grading periods cannot overlap within the same school year.
- `student_count` must be greater than zero.
- no student identity tables were added; anonymous `STUDENT #` ranges are derived from `student_count`.

### Architectural decisions during implementation

- TeacherAssist setup data is tenant-scoped except for the teacher profile, which is user-scoped.
- TeacherAssist access resolution uses the first tenant membership that has selectable TeacherAssist product access.
- The setup domain uses the user-requested table names (`teacher_profiles`, `school_years`, `grading_periods`, `subjects`, `classes`, `class_subjects`, `standards`) while still keeping model files and routes clearly TeacherAssist-namespaced.
- The frontend continues to use backend APIs only; no uploads, workflows, AI generation, or external integrations were introduced.

### Known issues

- TeacherAssist setup currently assumes one active TeacherAssist tenant context per signed-in user path, using the first accessible tenant membership.
- Subject and standards management is create/list focused in this phase; delete flows and richer editing can be added later if needed.
- The frontend lint run still reports one pre-existing warning in `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js`, outside TeacherAssist scope.

### Manual test checklist

1. Confirm existing trading login and dashboard behavior still work unchanged.
2. Grant TeacherAssist access to a tenant and confirm `/teacher-assist` loads the setup checklist.
3. Open `/teacher-assist/settings` and save a teacher profile.
4. Create a school year and mark it active.
5. Add grading periods whose dates fit within the school year.
6. Attempt an overlapping or out-of-range grading period and confirm validation blocks it.
7. Create a class and confirm the anonymous STUDENT # range preview matches `1-N`.
8. Create subjects and attach them to classes.
9. Add standards / TEKS entries and confirm they appear in the settings table.
10. Confirm no student names are required anywhere in the setup flow.
11. Confirm TeacherAssist remains light mode while trading remains in the dark shell.
12. Confirm no trading worker/service files were modified.

### Screenshots

- No screenshots were captured in this CLI session.

### Next recommended phase

**Pacing guide import and resource library**

Now that the teacher foundation and school-year setup model exist, the next phase should add pacing-guide import and resource-library foundations without starting AI generation or workflow jobs yet.

## Phase 3 - Pacing Guide Import + Resource Library Foundation

### What was implemented

- Added the TeacherAssist planning-context foundation in the backend for:
  - pacing guides
  - pacing items
  - pacing-item standards
  - resource library items
  - pacing-item resource mappings
  - planning input drafts
  - planning-draft resource mappings
- Added metadata-first TeacherAssist upload handling with a clean local storage abstraction.
- Added protected TeacherAssist APIs for:
  - pacing guide CRUD
  - pacing item CRUD
  - pacing-item standards/resource linking
  - resource upload and link creation
  - planning draft CRUD and resource linking
- Added TeacherAssist frontend workspaces for:
  - `/teacher-assist/resources`
  - `/teacher-assist/pacing-guides`
  - `/teacher-assist/weekly-planning`
- Updated the TeacherAssist dashboard to guide teachers through setup, resources, pacing guides, and explicit draft preparation without adding any generation trigger.

### Tables and models added

Added tables and SQLAlchemy models:

- `pacing_guides`
- `pacing_items`
- `pacing_item_standards`
- `resource_library_items`
- `pacing_item_resources`
- `planning_input_drafts`
- `planning_input_draft_resources`

Representative model files:

- `backend/services/api/src/oziebot_api/models/teacher_assist_pacing_guide.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_pacing_item.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_pacing_item_standard.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_resource_library_item.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_pacing_item_resource.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft_resource.py`

### Migrations added

- `038_teacher_assist_planning_context.py`
  - creates `pacing_guides`
  - creates `pacing_items`
  - creates `pacing_item_standards`
  - creates `resource_library_items`
  - creates `pacing_item_resources`
  - creates `planning_input_drafts`
  - creates `planning_input_draft_resources`
  - adds tenant, ownership, and mapping indexes plus uniqueness for reusable-link relationships

### APIs added

Added TeacherAssist APIs under `/v1/teacher-assist`:

- `GET /v1/teacher-assist/pacing-guides`
- `POST /v1/teacher-assist/pacing-guides`
- `PUT /v1/teacher-assist/pacing-guides/{id}`
- `GET /v1/teacher-assist/pacing-guides/{id}/items`
- `POST /v1/teacher-assist/pacing-guides/{id}/items`
- `PUT /v1/teacher-assist/pacing-items/{id}`
- `POST /v1/teacher-assist/pacing-items/{id}/standards`
- `POST /v1/teacher-assist/pacing-items/{id}/resources`
- `GET /v1/teacher-assist/resources`
- `POST /v1/teacher-assist/resources/upload`
- `POST /v1/teacher-assist/resources/link`
- `GET /v1/teacher-assist/resources/{id}`
- `GET /v1/teacher-assist/planning-drafts`
- `POST /v1/teacher-assist/planning-drafts`
- `PUT /v1/teacher-assist/planning-drafts/{id}`
- `POST /v1/teacher-assist/planning-drafts/{id}/resources`

### Upload and storage approach

- Resource uploads are metadata-first in this phase.
- Postgres stores resource metadata:
  - `resource_type`
  - `storage_key`
  - `original_filename`
  - `mime_type`
  - `file_size`
  - `external_url`
- TeacherAssist file writes go through `services/teacher_assist/storage.py` instead of hardcoding file I/O in routes.
- The initial storage backend is local and configurable through:
  - `teacher_assist_storage_backend`
  - `teacher_assist_storage_root`
  - `teacher_assist_upload_max_bytes`
- No base64 blobs are stored in Postgres.
- No OCR, parsing, extraction, embeddings, or AI generation runs in this phase.

### Frontend pages and components added or updated

Representative frontend additions:

- `frontend/apps/web/components/teacher-assist/teacher-assist-resources-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-pacing-guides-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`

Representative frontend route additions:

- `frontend/apps/web/app/teacher-assist/resources/page.tsx`
- `frontend/apps/web/app/teacher-assist/pacing-guides/page.tsx`
- `frontend/apps/web/app/teacher-assist/weekly-planning/page.tsx`

Representative frontend updates:

- `frontend/apps/web/components/teacher-assist/teacher-assist-dashboard-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.ts`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/lib/teacher-assist-types.ts`
- `frontend/apps/web/lib/auth-service.ts`

### Pacing-guide architecture decisions

- Pacing guides are tenant-scoped structured timelines with:
  - required school year
  - optional grade level
  - optional subject
  - creator tracking
  - future-oriented `is_shared` support
- Pacing items remain reusable structured records inside a guide rather than embedding resource blobs or freeform uploads.
- Standards and resources are linked through separate mapping tables so pacing content stays normalized.
- Pacing items validate:
  - grading period ownership
  - subject ownership
  - instructional dates inside the school year
  - grading period alignment with the guide’s school year

### Resource-library architecture decisions

- Resources are independent reusable assets and are never embedded directly into pacing items or drafts.
- File uploads and URL resources both land in `resource_library_items`, with links represented as resource rows using `resource_type = link`.
- Resource-library usage counts are surfaced to the frontend so teachers can see whether resources have already been linked into pacing items or planning drafts.

### Validation rules added

- TeacherAssist product access is required for all Phase 3 APIs.
- Tenant isolation is enforced across guides, pacing items, resources, and planning drafts.
- Pacing guide school-year and subject references must belong to the TeacherAssist tenant.
- Pacing-item grading periods must belong to the same school year as the selected pacing guide.
- Planning draft school year, grading period, and class references must stay internally consistent.
- If a class already has attached subjects, planning drafts validate that the selected subject is attached to that class.
- Uploads reject empty files and files above the configured byte limit.

### Known issues

- The current upload backend is local-storage-first and suitable for development/foundation work, but production object storage will still be needed later.
- Linking flows are additive only in this phase; unlink/delete flows can be added later if needed.
- “Import” currently means structured manual entry plus metadata upload/link preparation, not Excel parsing or extraction.
- The frontend lint run still reports one pre-existing warning in `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js`, outside TeacherAssist scope.

### Manual test checklist

1. Confirm trading login and dashboard behavior still work unchanged.
2. Open `/teacher-assist/resources` and upload curriculum files.
3. Add an external URL resource and confirm it appears in the resource library.
4. Open `/teacher-assist/pacing-guides` and create a pacing guide.
5. Add pacing items by grading period, week, day, or instructional date.
6. Attach standards to a pacing item.
7. Attach resources to a pacing item.
8. Open `/teacher-assist/weekly-planning` and save a planning draft.
9. Attach resources to the planning draft.
10. Confirm no Generate button exists and no AI, OCR, or workflow processing is triggered.
11. Confirm TeacherAssist remains in the light shell and trading remains unchanged.
12. Confirm no trading worker/service files were modified.

### Screenshots

- No screenshots were captured in this CLI session.

### Next recommended phase

**Workflow-ready planning refinement**

The next phase should build on the saved context foundation by refining planning-draft preparation and introducing workflow-safe orchestration seams, still without jumping straight into unbounded synchronous generation.

## Phase 4 - Workflow-Ready Planning Refinement

### What was implemented

- Extended TeacherAssist planning drafts so a teacher can save a fuller weekly planning context with:
  - one-or-more subjects
  - draft-linked pacing items
  - draft-linked standards / TEKS
  - draft-linked resources
  - teacher notes
  - explicit `draft` / `ready` status
- Added a backend readiness-validation seam that blocks `ready` unless the saved draft has the minimum planning context required for future workflows.
- Added a structured context-preview endpoint that returns the exact saved planning payload future generation builders will consume later.
- Added a generation placeholder endpoint that confirms readiness only and does not trigger AI, OCR, grading, or workflow jobs.
- Reworked `/teacher-assist/weekly-planning` into a true planning workspace with:
  - draft create/select flow
  - multi-subject selection
  - pacing-item selection
  - standard selection
  - resource selection
  - persistent readiness alerts
  - saved context preview
  - disabled generation placeholder

### Schema additions

Added tables and SQLAlchemy models:

- `planning_input_draft_subjects`
- `planning_input_draft_pacing_items`
- `planning_input_draft_standards`

Representative model files:

- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft_subject.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft_pacing_item.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_planning_input_draft_standard.py`

### Migrations added

- `039_teacher_assist_planning_refinement.py`
  - creates `planning_input_draft_subjects`
  - creates `planning_input_draft_pacing_items`
  - creates `planning_input_draft_standards`
  - adds uniqueness and ownership indexes for draft-to-subject, draft-to-pacing-item, and draft-to-standard mappings

### APIs added

Added or expanded TeacherAssist APIs under `/v1/teacher-assist`:

- `GET /v1/teacher-assist/planning-drafts`
- `POST /v1/teacher-assist/planning-drafts`
- `PUT /v1/teacher-assist/planning-drafts/{id}`
- `POST /v1/teacher-assist/planning-drafts/{id}/resources`
- `GET /v1/teacher-assist/planning-drafts/{id}/context-preview`
- `PATCH /v1/teacher-assist/planning-drafts/{id}/status`
- `POST /v1/teacher-assist/planning-drafts/{id}/generation-preview`

Draft create/update payloads now support:

- `subject_ids`
- `pacing_item_ids`
- `standard_ids`

### Frontend planning workspace changes

Representative frontend updates:

- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/lib/teacher-assist-types.ts`

The weekly-planning workspace now:

- guides teachers through explicit saved-context preparation
- uses dropdowns for school year, grading period, and class
- uses checkbox-based multi-select flows for subjects, pacing items, standards, and resources
- displays persistent informational, warning, and ready-state alerts instead of toast-only guidance
- keeps “Generate Weekly Plan” visible but disabled with an explanation that later phases will enable it

### Validation and readiness rules

Readiness now requires:

- school year selected
- grading period selected
- class selected
- at least one subject selected
- at least one pacing item, teacher note, or attached resource

Additional draft validation now enforces:

- tenant ownership for all referenced planning records
- grading period alignment with the selected school year
- class alignment with the selected school year
- selected subjects must belong to the class if the class already has class-subject assignments
- draft status can only be `draft` or `ready`
- `draft -> ready` is blocked until readiness validation passes

### Context preview design

- `GET /context-preview` returns a structured payload containing:
  - the draft row
  - school year
  - grading period
  - class
  - selected subjects
  - selected pacing items
  - selected standards
  - attached resources
  - teacher notes
  - readiness summary with `is_ready`, `missing_items`, and `warnings`
- This preview acts as the future LLM/workflow input contract without introducing any real generation in Phase 4.

### Known issues

- Resource attachments remain additive-only in this phase; unlink/remove flows are still deferred.
- The weekly-planning screen aggregates pacing items by loading guide items client-side, which is acceptable for the current foundation but may want a dedicated list endpoint later.
- Generation preview is intentionally placeholder-only and does not create jobs, queue work, or call any model provider.
- The frontend lint run still reports one pre-existing warning in `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js`, outside TeacherAssist scope.

### Manual test checklist

1. Confirm trading login and dashboard behavior still work unchanged.
2. Open `/teacher-assist/weekly-planning` and create a planning draft.
3. Select school year, grading period, class, and one or more subjects.
4. Attach pacing items, standards, resources, and teacher notes.
5. Save the draft and confirm the blue saved-context alert appears.
6. Review the context-preview panel and confirm missing items are shown when the draft is incomplete.
7. Mark the draft ready once the readiness requirements are met.
8. Confirm the green ready-state alert appears and the draft status changes to `ready`.
9. Confirm the Generate button remains disabled.
10. Confirm no AI, OCR, workflow job, grading flow, or export flow is triggered.
11. Confirm TeacherAssist settings, pacing guides, and resource library still work.
12. Confirm no trading worker/service files were modified.

### Next recommended phase

**Workflow execution + mock generation seam**

The next phase should introduce persisted workflow/job orchestration plus a mock generation layer that consumes the Phase 4 context-preview contract, while still avoiding real LLM calls until the mock-output path is proven end to end.

## Phase 5 - Workflow Execution + Mock Generation Seam

### What was implemented

- Added persisted TeacherAssist workflow rows, workflow-step rows, and weekly-plan artifact rows.
- Added an isolated TeacherAssist workflow runner that:
  - saves workflow state first
  - uses the Phase 4 context-preview contract as the saved input snapshot
  - processes mock weekly-plan generation in a background task with a fresh DB session bound to the current API engine
- Added deterministic mock weekly-plan generation that produces structured JSON without calling OpenAI or any external AI provider.
- Added weekly-planning frontend workflow controls so teachers can start generation only from a ready draft, monitor workflow status, and open the generated weekly-plan artifact.

### Workflow tables and models added

Added tables and SQLAlchemy models:

- `teacher_assist_workflows`
- `teacher_assist_workflow_steps`
- `weekly_plans`

Representative model files:

- `backend/services/api/src/oziebot_api/models/teacher_assist_workflow.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_workflow_step.py`
- `backend/services/api/src/oziebot_api/models/teacher_assist_weekly_plan.py`

### Migrations added

- `040_teacher_assist_workflows.py`
  - creates `teacher_assist_workflows`
  - creates `teacher_assist_workflow_steps`
  - creates `weekly_plans`
  - adds ownership, draft-reference, output-reference, and workflow-reference indexes

### Weekly plan artifact table added

- `weekly_plans` stores structured JSON artifacts for generated weekly plans.
- Weekly-plan product status language in this phase is:
  - `in_progress`
  - `completed`
- Each weekly plan persists:
  - `content_json`
  - `source_context_json`
  - draft/workflow linkage
  - teacher + tenant ownership

### APIs added

Added TeacherAssist APIs under `/v1/teacher-assist`:

- `POST /v1/teacher-assist/planning-drafts/{id}/workflows/weekly-plan`
- `GET /v1/teacher-assist/workflows`
- `GET /v1/teacher-assist/workflows/{id}`
- `PATCH /v1/teacher-assist/workflows/{id}/cancel`
- `GET /v1/teacher-assist/weekly-plans`
- `GET /v1/teacher-assist/weekly-plans/{id}`

### Workflow lifecycle

TeacherAssist workflow status now supports:

- `queued`
- `running`
- `completed`
- `failed`
- `cancelled`

Workflow-step status now supports:

- `queued`
- `running`
- `completed`
- `failed`
- `skipped`

For Phase 5 mock weekly-plan generation:

1. Start endpoint validates the draft exists, belongs to the current TeacherAssist tenant/user, is marked `ready`, and still passes readiness rules.
2. The backend saves `input_snapshot_json` from the structured context-preview contract.
3. The workflow row is persisted with `queued` status before background processing starts.
4. Background processing moves the workflow through:
   - `queued`
   - `running`
   - `completed`
5. On success:
   - a `weekly_plans` row is created
   - `output_ref_type = weekly_plan`
   - `output_ref_id` is saved
   - `progress_percent = 100`
6. On failure:
   - the workflow moves to `failed`
   - `error_message` is persisted
   - step history records where the failure occurred

### Mock generation design

- Mock generation lives in:
  - `services/teacher_assist/planning_context_service.py`
  - `services/teacher_assist/mock_generation_service.py`
  - `services/teacher_assist/workflow_service.py`
- The mock generator consumes the Phase 4 context-preview snapshot and produces structured JSON containing:
  - overview
  - subject sections
  - objectives
  - standards
  - daily breakdown
  - suggested artifacts
  - resources used
  - teacher notes used
- Output is deterministic and clearly labeled as mock output.
- No OpenAI calls, no OCR, no extraction, no embeddings, and no external AI calls were added.

### Frontend workflow and status changes

Representative frontend additions and updates:

- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-planning-screen.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-weekly-plan-viewer.tsx`
- `frontend/apps/web/app/teacher-assist/weekly-planning/plans/page.tsx`
- `frontend/apps/web/lib/teacher-assist-api.ts`
- `frontend/apps/web/lib/teacher-assist-types.ts`

The weekly-planning workspace now:

- enables **Generate Weekly Plan** only when the active draft is saved and ready
- starts a persisted workflow instead of pretending generation is unavailable
- shows persistent workflow status banners/cards for queued, running, completed, failed, and cancelled states
- shows recent TeacherAssist workflows with progress and output links

The weekly-plan viewer now:

- displays the generated overview
- shows subject sections, objectives, standards, daily breakdown, resources used, and teacher notes used
- labels the output as mock generation
- keeps future artifact actions visible but disabled:
  - Generate Daily Deck
  - Generate Quiz
  - Generate Guided Notes

### Tests added or updated

Backend tests now cover:

- generation cannot start unless the draft is ready
- workflow start creates a persisted workflow row
- mock generation creates a persisted weekly plan row
- workflow completion stores `output_ref_id`
- workflow failure stores `error_message`
- tenant isolation for workflow and weekly-plan retrieval
- completed workflows reject cancellation

### Known issues

- Phase 5 uses an isolated in-API background-task runner instead of a dedicated TeacherAssist worker service; this is intentional for the mock-generation seam and should be revisited before real LLM workloads.
- Cancellation is implemented at the API/state layer, but the current mock workflow is so short-lived that queued/running cancellation windows are narrow in practice.
- Weekly-plan viewer routing uses a static-export-safe query-string route (`/teacher-assist/weekly-planning/plans?id=...`) instead of a runtime dynamic segment because the frontend is statically exported.
- Resource unlink/remove flows are still deferred.
- The frontend lint run still reports one pre-existing warning in `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js`, outside TeacherAssist scope.

### Manual test checklist

1. Confirm trading login and dashboard behavior still work unchanged.
2. Confirm TeacherAssist settings, resources, pacing guides, and weekly-planning draft preparation still work.
3. Create or open a ready planning draft.
4. Click **Generate Weekly Plan**.
5. Confirm a workflow status card appears.
6. Confirm the mock workflow completes.
7. Confirm a weekly-plan output row is created.
8. Open the weekly-plan viewer.
9. Confirm the content is structured and clearly labeled as mock output.
10. Confirm no OpenAI or external AI call happens.
11. Confirm no OCR, grading, PPTX, QR, newsletter, or mastery-matrix feature was introduced.
12. Confirm TeacherAssist remains isolated from trading workers/services.

### Next recommended phase

**Mock output refinement + real provider seam preparation**

The next phase should refine the mock artifact shapes, add stronger teacher review/edit seams around generated artifacts, and prepare the backend-only provider abstraction for real LLM integration without enabling production model calls by default.

## Proposed structure

### Frontend

Recommended new structure:

```text
frontend/apps/web/app/teacher-assist/
frontend/apps/web/components/teacher-assist/
frontend/apps/web/lib/teacher-assist-api.ts
frontend/apps/web/lib/teacher-assist-types.ts
```

Recommended route tree:

```text
/teacher-assist
/teacher-assist/weekly-planning
/teacher-assist/daily-teaching
/teacher-assist/assessments
/teacher-assist/students
/teacher-assist/insights
/teacher-assist/newsletters
/teacher-assist/communication
/teacher-assist/settings
```

### Backend

Recommended new structure:

```text
backend/services/api/src/oziebot_api/api/v1/teacher_assist.py
backend/services/api/src/oziebot_api/schemas/teacher_assist/
backend/services/api/src/oziebot_api/services/teacher_assist/
backend/services/api/src/oziebot_api/models/teacher_assist_*.py
backend/services/api/alembic/versions/0xx_teacher_assist_foundation.py
backend/services/teacher-assist-worker/
```

## Recommended file/folder additions

### Frontend additions

- `frontend/apps/web/app/teacher-assist/layout.tsx`
- `frontend/apps/web/app/teacher-assist/page.tsx`
- `frontend/apps/web/app/teacher-assist/weekly-planning/page.tsx`
- `frontend/apps/web/app/teacher-assist/daily-teaching/page.tsx`
- `frontend/apps/web/app/teacher-assist/assessments/page.tsx`
- `frontend/apps/web/app/teacher-assist/students/page.tsx`
- `frontend/apps/web/app/teacher-assist/insights/page.tsx`
- `frontend/apps/web/app/teacher-assist/newsletters/page.tsx`
- `frontend/apps/web/app/teacher-assist/communication/page.tsx`
- `frontend/apps/web/app/teacher-assist/settings/page.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-shell.tsx`
- `frontend/apps/web/components/teacher-assist/teacher-assist-nav.tsx`

### Backend additions

- `backend/services/api/src/oziebot_api/api/v1/teacher_assist.py`
- `backend/services/api/src/oziebot_api/schemas/teacher_assist/`
- `backend/services/api/src/oziebot_api/services/teacher_assist/`
- `backend/services/api/src/oziebot_api/models/teacher_assist_*.py`
- `backend/services/teacher-assist-worker/`
- future queue names for TeacherAssist workflow processing

## Database findings

### Existing identity/access shape

Current relevant tables:

- `users`
- `tenant_memberships`
- `tenants`
- `subscription_plans`
- `stripe_subscriptions`
- `stripe_subscription_items`
- `tenant_entitlements`

### Important gaps

- no `user_products`
- no `default_app`
- no multi-product product switcher model
- no TeacherAssist tables

### Recommended multi-product additions

- `platform_products`
- `tenant_product_access`
- `user_product_preferences`

### Recommended TeacherAssist schema areas

- teacher profile/setup
- school years
- grading periods
- subjects/classes
- standards / TEKS
- pacing guides and pacing items
- resource library
- uploads and extracted text
- workflow runs and workflow steps
- weekly plans / daily decks
- assessments / submissions
- grading reviews
- TEKS mastery matrix
- newsletters / communication drafts
- AI usage tracking

## Auth findings

- Existing JWT + refresh-session model is reusable.
- Existing user identity lives in `users`; there is no separate profile table today.
- Existing tenant resolution uses the earliest `tenant_membership` as the primary tenant.
- `/v1/auth/*` and `/v1/me` are safe extension points for future product-aware bootstrap/app-switch behavior.
- Future work should extend auth/bootstrap responses to include:
  - available products
  - default product
  - current/selected product

## Risks

### Isolation risks

- TeacherAssist must not be woven into:
  - strategy-engine
  - risk-engine
  - execution-engine
  - Coinbase execution/integration logic

### Theme contamination risks

- current frontend theme is globally dark
- current root layout is shared by the whole app
- TeacherAssist needs a light-mode shell without destabilizing trading UI

### Access model risks

- current entitlement system is strategy/trading-centric
- overloading it for TeacherAssist would create long-term coupling

### Deployment risks

- frontend/backend deploy independently today
- “same Lightsail deployment” is only partially true under the current split-repo setup

### Capacity/cost risks

- current Lightsail lean host already runs several backend services
- document extraction, OCR, PPTX generation, and LLM jobs can pressure CPU, RAM, and disk
- repeated full-plan generation could become expensive without workflow controls

### Privacy risks

- TeacherAssist must avoid student/parent names and student IDs
- uploaded materials may contain accidental PII
- AI outputs must not commit grades automatically

## Proposed phased plan

1. Product access, default app, app switcher
2. TeacherAssist UI shell and separate theme
3. Teacher/school year/class/grading period setup
4. Pacing guide import and resource library
5. Flexible planning inputs and draft save
6. Workflow job engine
7. Mock LLM weekly planning generation
8. Real LLM generation with structured outputs
9. PPTX / Google Slides-compatible deck export
10. Assessment / TEKS question mapping
11. TEKS mastery matrix
12. QR-coded printable assignment templates
13. Uploaded written work review workspace
14. Grading assistant and teacher confirmation
15. Insights / lesson effectiveness / learning plans
16. Newsletter and communication assistant
17. File retention and cleanup
18. Hardening / security / privacy / cost review

## Unresolved questions

1. Should TeacherAssist be billed per tenant, per user, or bundled?
2. Should TeacherAssist setup data be user-scoped, tenant-scoped, or mixed?
3. Should TeacherAssist remain in the current static-export frontend architecture long term?
4. Should uploads use backend multipart handling first or presigned object storage?
5. Which bucket/prefix should hold TeacherAssist uploads and expiring exports?
6. Is OCR part of MVP or a later phase?
7. Is one-teacher-per-tenant still a valid MVP assumption?
8. Is manual PPTX upload the full MVP for Google compatibility, or is Google integration needed soon after?

## Bottom line

TeacherAssist fits the current Oziebot repo best as a **new, isolated product module** that:

- reuses auth, tenancy, Postgres, and deployment foundations
- introduces a new product-access model
- lives in its own frontend route subtree and backend namespace
- uses the existing worker/outbox pattern for async generation
- keeps trading systems untouched

## Phase 6 - Mock Output Refinement + Provider Seam Preparation

### What was implemented

- Refined persisted weekly-plan artifacts into richer structured JSON with:
  - metadata
  - overview
  - weekly objectives
  - vocabulary
  - day labels
  - materials needed
  - differentiation
  - review notes
- Added teacher review/edit capability for weekly plans.
- Added versioning so weekly-plan creation writes version 1 and every edit writes a new version.
- Added a backend-only TeacherAssist AI provider abstraction with mock as the only active provider.
- Added AI usage tracking scaffolding for workflow generation.
- Added optional dev-only fixture record/replay support for structured provider payloads.
- Kept all real provider calls disabled and did not require any OpenAI key.

### Workflow tables/models added

- `weekly_plan_versions`
- `teacher_assist_ai_usage_events`

### Weekly plan artifact table usage

- `weekly_plans` remains the persisted artifact row introduced in Phase 5.
- Phase 6 refines `content_json` to carry the richer reviewable structure and preserves edit history in `weekly_plan_versions`.

### APIs added or changed

- `PUT /v1/teacher-assist/weekly-plans/{id}`
- `GET /v1/teacher-assist/weekly-plans/{id}/versions`
- `GET /v1/teacher-assist/weekly-plans/{id}/versions/{version_id}`
- `GET /v1/teacher-assist/workflows/{id}` now includes usage events
- weekly-plan responses now include current version and latest usage summary

### Workflow lifecycle

- Workflow lifecycle remains:
  - `queued`
  - `running`
  - `completed`
  - `failed`
  - `cancelled`
- Weekly-plan lesson lifecycle remains:
  - `in_progress`
  - `completed`
- Generated weekly plans now start in `in_progress` so teachers can review and explicitly mark them completed.

### Mock generation design

- Weekly-plan generation now routes through a backend-only provider seam.
- The active provider is still deterministic mock output only.
- The saved Phase 4 context-preview snapshot remains the generation input contract.
- Mock usage writes `provider=mock`, `model=mock`, and zero-cost usage data.
- Fixture record/replay support exists for dev testing only and is not required in production.

### Frontend workflow/status changes

- The weekly-plan viewer now renders:
  - mock label
  - overview
  - weekly objectives
  - standards
  - vocabulary
  - daily breakdown with materials
  - differentiation
  - resources used
  - teacher notes used
  - review notes
  - version history
  - provider/model/cost placeholders
- Teachers can now edit title, overview, weekly objectives, review notes, and plan status, then save a new version or mark the plan completed.
- The viewer also shows disabled placeholders for:
  - Regenerate Section
  - Generate Daily Deck
  - Generate Quiz
  - Generate Guided Notes

### Tests/manual checklist

Added or updated coverage for:

- provider defaulting to mock
- real provider remaining disabled by default
- weekly plan creation creating version 1
- weekly plan edit creating a new version
- completed weekly plan status persistence
- mock AI usage event exposure with zero cost
- tenant-protected version retrieval
- tenant-protected weekly plan updates

Manual validation checklist:

1. Trading still works unchanged.
2. Generate a mock weekly plan from a ready draft.
3. Open the weekly-plan viewer.
4. Confirm refined content sections render.
5. Edit title/review fields and save.
6. Confirm version history records a new version.
7. Mark the plan Completed.
8. Confirm provider is mock and cost is zero.
9. Confirm no OpenAI key is required and no real AI call happens.
10. Confirm no OCR, grading, PPTX/Slides, QR, newsletter, or mastery feature was introduced.

### Known issues

- Real provider integration is still intentionally disabled.
- Section-level regeneration remains a disabled placeholder.
- Fixture record/replay is backend-only and dev-oriented for now.
- The existing frontend lint warning in `frontend/apps/web/deploy/aws/cloudfront-viewer-request.js` remains outside TeacherAssist scope.

### Next recommended phase

**Guarded real-provider integration planning**

The next phase should focus on provider-enablement boundaries, prompt-shaping, and regeneration rules while still keeping OCR, grading, export, QR, and mastery work deferred.
