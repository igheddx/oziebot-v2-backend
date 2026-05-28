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
