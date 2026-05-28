# TeacherAssist Prompt History

Use this file to record each major Copilot implementation prompt.

For each prompt, append:

## Phase X - Title

Date:
Purpose:
Prompt summary:
Files/areas expected to change:
Non-goals:
Result summary:

Initial entry:

## Phase 7 - Instructional Planning Scope + Guarded Provider Seam

Date:
Purpose:
Evolve TeacherAssist planning from weekly-only to flexible instructional planning while preserving mock-first provider safety.

Prompt summary:
Add planning scope, module/unit/multi-week support, generalized instructional-plan context preview, guarded provider seam, prompt builder, structured output validation, frontend language updates, and usage metadata visibility.

Non-goals:
No OCR, grading, QR implementation, slides, newsletters, mastery matrix, embeddings, or default real provider calls.

Files/areas expected to change:
- planning draft models, schemas, and services
- workflow/provider seam
- mock generation output shape
- weekly-plans persistence model
- instructional-planning frontend workspace
- TeacherAssist docs

Result summary:
- delivered planning scope and duration fields
- generalized context preview and instructional-plan mock output
- added reusable-plan lineage foundation and software-only copy endpoint
- kept real provider disabled by default
- added malformed-output workflow failure handling

## Product Evolution - Shared Planning + Curriculum Reuse

Date:
Purpose:
Capture the evolving TeacherAssist architecture direction around reusable instructional planning, teacher personalization branching, annual curriculum rollover, reusable instructional asset libraries, and AI cost optimization.

Summary:
The product direction evolved from isolated weekly-plan generation toward:
- reusable shared instructional plans
- teacher-owned personalization copies
- controlled AI invocation
- annual curriculum rollover
- reusable instructional asset libraries
- multi-year instructional continuity

Key architectural principles added:
- copy/edit without AI calls
- AI invoked only for adaptation/regeneration
- reusable curriculum artifacts across school years
- shared instructional templates
- instructional asset durability

Non-goals:
No implementation code changes in this documentation update task.

## Phase 8 - Shared Plan Library + Annual Curriculum Rollover Foundation

Date:
Purpose:
Add reusable-plan discovery, owner-managed sharing controls, teacher-owned copy enhancements, and annual curriculum rollover foundations on top of the existing instructional-plan architecture.

Prompt summary:
Add a shared instructional-plan library API and UI, enhance software-only plan copying, add owner-managed sharing/template controls, add prior-year rollover candidate discovery and duplicate-aware rollover copy, and keep all flows mock-safe with no real provider usage.

Non-goals:
No real provider enablement, OCR, uploaded-file parsing, grading, QR implementation, slides, newsletters, TEKS mastery matrix, or trading-system modifications.

Files/areas expected to change:
- TeacherAssist workflow service, schemas, and API routes
- plan-library and curriculum-rollover frontend screens
- TeacherAssist nav, API helpers, and types
- TeacherAssist docs

Result summary:
- delivered tenant-safe plan-library listing and owner-only sharing controls
- expanded software-only copy behavior with lineage-preserving rollover metadata
- added duplicate-aware curriculum rollover candidate and copy endpoints
- added `/teacher-assist/plans` and `/teacher-assist/curriculum-rollover` foundations
- kept copy and rollover flows provider-free and usage-free

## Phase 9 - Dedicated TeacherAssist Worker + Guarded Real Provider Activation Foundation

Date:
Purpose:
Move TeacherAssist workflow execution out of API-process background tasks into a dedicated worker foundation while keeping real provider calls disabled by default.

Prompt summary:
Add a TeacherAssist-only worker service, leased workflow processing, retries, cancellation polling, timeout handling, heartbeat/progress persistence, guarded provider activation seams, cost-limit enforcement seams, prompt/model/accounting metadata, and frontend workflow-status visibility improvements.

Non-goals:
No default real provider activation, OCR, grading, QR implementation, slides, newsletters, mastery matrix, embeddings, SIS/Google Classroom integration, or trading-system changes.

Files/areas expected to change:
- TeacherAssist workflow model, config, provider config, service logic, and API schemas/routes
- dedicated `backend/services/teacher-assist-worker/` package
- instructional-planning frontend workflow metadata display
- TeacherAssist docs

Result summary:
- delivered a dedicated TeacherAssist worker service foundation and queue-only API start behavior
- added lease, retry, timeout, heartbeat, provider, and cost metadata on workflows
- added graceful malformed-output, cancellation, retry, and cost-limit handling in worker execution
- improved planning-screen workflow visibility for queued/running/completed/failed/cancelled states
- kept real provider execution disabled by default

## Phase 10 - Controlled Real Provider Activation + Instructional Plan Quality Review

Date:
Purpose:
Enable a guarded real-provider path for instructional-plan generation while preserving mock-first safety and adding explicit teacher-facing quality review controls.

Prompt summary:
Add worker-only real-provider execution behind explicit config, model allowlisting, API-key checks, cost controls, prompt hardening, stronger structured-output validation, review-required metadata, and plan-viewer quality-review UI updates.

Non-goals:
No OCR, grading assistant, quizzes, slides, newsletters, QR assignment workflows, scan/upload review, embeddings, SIS/Google Classroom integration, or trading-system modifications.

Files/areas expected to change:
- TeacherAssist provider config and real-provider implementation
- instructional-plan prompt builder and validator
- worker execution and persisted workflow/plan metadata
- TeacherAssist plan viewer and related frontend types
- TeacherAssist docs

Result summary:
- delivered guarded real-provider execution for instructional-plan generation only
- kept mock as default and blocked real-provider use without enablement, API key, allowlisted model, and cost headroom
- added review-required plan metadata, quality flags, missing-context warnings, standards-alignment summary, and teacher review checklist
- improved teacher-facing plan review UI with provider/model/prompt/tokens/cost visibility

## Phase 11 - Section-Level Regeneration + Teacher Edit Workflow

Date:
Purpose:
Allow teachers to regenerate only selected sections of an instructional plan while preserving version history, teacher review control, and provider/cost metadata.

Prompt summary:
Add a targeted regeneration endpoint, section/path-aware prompt and validation logic, version-preserving section replacement, mock-first regeneration behavior, guarded real-provider reuse, plan-scoped usage metadata, and plan-viewer regeneration actions for common instructional sections.

Non-goals:
No OCR, grading, quizzes, QR workflows, slides, newsletters, mastery matrix, SIS/Google Classroom integration, or trading-system changes.

Files/areas expected to change:
- TeacherAssist provider interface and mock/real implementations
- instructional-plan prompt builder and validator
- weekly-plan service logic, schemas, and API routes
- plan viewer, API helpers, and frontend types
- TeacherAssist docs

Result summary:
- delivered section-level regeneration with version history preservation and targeted section replacement
- added plan-scoped usage-event lookup so copied plans surface regeneration metadata too
- kept mock regeneration deterministic and zero-cost
- kept real-provider regeneration behind the existing explicit guardrails and blocked by default

## Phase 12 - Assignment Model Foundation

Date:
Purpose:
Add the assignment persistence and workflow foundation that future QR templates, uploaded student work, grading review, and mastery tracking will build on.

Prompt summary:
Add TeacherAssist assignment models and migration, tenant-safe CRUD/status/standards APIs, a software-only weekly-plan-to-assignment starter, assignment lifecycle validation, a basic `/teacher-assist/assignments` workspace, and synced docs.

Non-goals:
No QR packet generation, printable templates, OCR, uploaded student-work review, grading assistant, mastery matrix, quiz generation, slides, newsletters, SIS/Google Classroom integration, real-provider assignment flows, or trading-system changes.

Files/areas expected to change:
- TeacherAssist assignment models, schemas, services, and API routes
- assignment migration and backend validation tests
- TeacherAssist nav, assignment screen, API helpers, and frontend types
- TeacherAssist docs

Result summary:
- delivered tenant-safe assignment persistence with standards/resource links and guarded lifecycle transitions
- added a software-only weekly-plan-to-assignment starter that copies context without creating AI usage
- added the `/teacher-assist/assignments` workspace with list/create/edit/status/standards flows
- kept QR, upload, grading, mastery, and trading behavior out of scope

## Phase 13 - QR-Coded Printable Assignment Packets

Date:
Purpose:
Add the software-only printable packet foundation that future scan/upload, grading review, and mastery workflows can depend on without introducing AI, OCR, or PII-heavy packet metadata.

Prompt summary:
Add printable packet/page models and migration, QR-safe packet generation APIs, assignment packet UI, a printable packet view, packet/page tests, and synced docs while keeping upload/grading/mastery/trading behavior untouched.

Non-goals:
No OCR, grading assistant, scanned upload review, mastery matrix, newsletters, slides, quizzes, SIS/Google Classroom integration, AI/provider packet generation, or trading-system changes.

Files/areas expected to change:
- printable packet models, schemas, services, and API routes
- packet migration, dependency wiring, and backend validation tests
- TeacherAssist assignments screen, printable packet route, API helpers, and frontend types
- TeacherAssist docs

Result summary:
- delivered tenant-safe assignment print-packet persistence with per-page QR payloads and SVG QR rendering
- added software-only packet generation from anonymous STUDENT # values with zero AI/provider usage
- added packet history, payload preview, and print-view UX inside TeacherAssist assignments
- kept upload, OCR, grading, mastery, and trading behavior out of scope

## Phase 14 - Uploaded Student Work Intake Foundation

Date:
Purpose:
Add the first student-work intake layer so teachers can upload anonymous assignment submissions and associate them with printable packet context without enabling OCR, grading, or AI.

Prompt summary:
Add student-work submission persistence, upload/status/packet-link APIs, assignment-workspace upload and review controls, privacy-safe validation, backend tests, and the related docs updates while keeping OCR/grading/mastery/trading behavior out of scope.

Non-goals:
No OCR, AI/provider calls, grading results, mastery-matrix updates, SIS/Google Classroom integration, or trading-system changes.

Files/areas expected to change:
- student-work models, migration, services, schemas, and API routes
- assignment packet/page relationships and backend validation tests
- TeacherAssist assignments screen plus student-work API helpers and frontend types
- TeacherAssist docs

Result summary:
- delivered tenant-safe student-work submission persistence using anonymous STUDENT # values and upload metadata only
- added optional packet/page linking and manual review-status controls without OCR or grading
- extended the assignments workspace with upload, list, and context-link flows while keeping grading and mastery actions disabled

## Phase 15 - Grading Review Foundation + Teacher Confirmation

Date:
Purpose:
Add the first teacher-confirmation-first grading review layer for uploaded student work without committing grades, updating mastery, exposing student names, or invoking OCR/AI.

Prompt summary:
Add grading review models and migration, tenant-safe grading review APIs, manual review lifecycle/status handling, PII-safe feedback validation, teacher-confirmation rules, assignment-workspace grading review UI, and disabled placeholders for future AI/mastery/parent actions.

Non-goals:
No OCR, AI/provider grading, gradebook commit, mastery update, parent communication generation, student-name storage, SIS/Google Classroom integration, or trading-system changes.

Files/areas expected to change:
- grading review models, schemas, services, and API routes
- grading review migration and backend validation tests
- TeacherAssist assignments screen, API helpers, and frontend types
- TeacherAssist docs

Result summary:
- delivered persisted grading-review records and item rows linked to anonymous student-work submissions
- kept review creation software-only with `manual` review source and zero provider/usage metadata
- added teacher-confirmed validation plus PII rejection across review feedback fields
- added assignment-workspace grading review creation, list, edit, and status controls with disabled future automation placeholders

## Phase 16 - Unified Teacher Workspace + Workflow Cohesion Layer

Date:
Purpose:
Consolidate TeacherAssist planning, assignments, workflows, packets, uploads, grading reviews, alerts, and recent activity into a single backend-composed operational workspace.

Prompt summary:
Add an append-only activity-event foundation, backend workspace read-model/orchestration service, `/v1/teacher-assist/workspace` API, class-centric grouping, needs-attention aggregation, recent activity feed, workspace stats, and a new frontend workspace route/screen while keeping OCR, grading automation, parent communication, mastery auto-commit, and trading behavior out of scope.

Non-goals:
No OCR, embeddings/vector DBs, autonomous grading, parent messaging, newsletter generation, mastery auto-commit, Google/SIS integration, Slides/PPTX export, or trading-system changes.

Files/areas expected to change:
- activity-event migration, model, helper, and TeacherAssist service hooks
- workspace read-model service, schemas, and API route
- TeacherAssist dashboard/nav/shell, workspace screen, and frontend API/types
- backend validation tests and TeacherAssist docs

Result summary:
- delivered append-only tenant-safe activity events for core TeacherAssist lifecycle changes
- added a backend-composed unified workspace endpoint with class grouping, attention detection, review queue, activity feed, and stats
- made the TeacherAssist landing experience workspace-first in the frontend
- kept the phase operational and software-centric without adding OCR, grading automation, mastery updates, or provider-side review flows

## Phase 17 - TeacherAssist Storage Hardening + S3 Migration Foundation

Date:
Purpose:
Move TeacherAssist file handling from local-only persistence toward a private S3-ready storage foundation without changing trading infrastructure or introducing OCR/grading behavior.

Prompt summary:
Add a provider-based TeacherAssist storage abstraction, local and S3 backends, private download-url support, S3-safe key generation, runtime/dependency wiring, AWS bucket bootstrap artifacts, IAM guidance, backend tests, and light frontend download actions while keeping storage private and backend-controlled.

Non-goals:
No OCR, AI grading, embeddings/vector DB, public file access, CDN exposure, Google Drive integration, PDF generation rewrite, mastery automation, or trading-system changes.

Files/areas expected to change:
- TeacherAssist storage service, config, routes, schemas, and tests
- resource/student-work frontend API/types/screens
- backend runtime dependencies and docker-compose env wiring
- AWS bootstrap/policy artifacts and TeacherAssist docs

Result summary:
- delivered `local` and `s3` TeacherAssist storage providers behind a shared abstraction
- added backend-generated temporary download URLs for stored resource and student-work files
- preserved local development storage while preparing private S3-backed production storage
- added AWS CLI bucket bootstrap and least-privilege IAM artifacts without introducing Terraform/CDK or public object access

## Phase 18 - OCR Intake + Artifact Processing Foundation

Date:
Purpose:
Add TeacherAssist extraction-job and extracted-text foundations on top of the new private storage layer while preserving teacher-review-first behavior and keeping grading automation disabled.

Prompt summary:
Add extraction job persistence, extracted-text records, mock-first OCR provider seams, worker-managed storage-backed extraction, tenant-safe extraction APIs, activity events, workspace attention updates, frontend extraction status/preview controls, and backend validation while keeping OCR providers mocked and grading/mastery side effects out of scope.

Non-goals:
No AI grading, no mastery updates, no gradebook commits, no parent communication, no public file access, no embeddings/vector DB, no real OCR calls by default, and no trading-system changes.

Files/areas expected to change:
- TeacherAssist extraction models, migration, services, worker loop, schemas, and API routes
- resource/student-work frontend API helpers, types, workspace, and extraction UI
- TeacherAssist docs and validation coverage

Result summary:
- delivered tenant-safe extraction jobs and extracted-text persistence for uploaded resources and student-work submissions
- added a mock-first OCR seam and worker-managed extraction flow that reads artifacts only through the private TeacherAssist storage abstraction
- surfaced extraction status, previews, failures, and teacher-review-ready items in TeacherAssist workspace/resource/student-work screens
- kept AI usage, grading review creation, mastery updates, and parent communication out of the extraction flow
