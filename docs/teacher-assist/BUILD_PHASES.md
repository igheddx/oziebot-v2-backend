# TeacherAssist AI Build Phases

This phase plan is designed to protect the existing trading platform while introducing TeacherAssist incrementally.

## Guiding rules

- no trading engine modifications
- no Coinbase workflow changes
- no strategy worker coupling
- migrations must be additive
- all TeacherAssist tables/routes/services stay namespaced
- async generation/export work stays off request paths

## Phase 1. Product access, default app, app switcher

Deliver:

- product catalog model
- tenant/user product access model
- user default app preference
- bootstrap response extended with allowed products/default product
- root redirect logic planned for default product

Repo targets:

- backend models + Alembic
- `/v1/me` or adjacent profile/bootstrap route
- frontend top-level product switcher and redirect rules

## Phase 2. TeacherAssist UI shell and separate theme

Deliver:

- `/teacher-assist/*` route tree
- TeacherAssist shell/navigation
- scoped light-mode theme
- isolation from trading nav/layout

Repo targets:

- `frontend/apps/web/app/teacher-assist/`
- `frontend/apps/web/components/teacher-assist/`

## Phase 3. Teacher/school year/class/grading period setup

Deliver:

- teacher profile/setup flow
- school year, grading periods, subjects, classes
- class student counts using anonymous numbering

Repo targets:

- TeacherAssist setup models
- setup APIs
- setup pages/forms

## Phase 4. Pacing guide import and resource library

Deliver:

- pacing guide upload/import pipeline
- Excel parsing
- normalized pacing items
- resource library with link/file references

## Phase 5. Flexible planning inputs and draft save

Deliver:

- upload/link/note inputs
- draft records
- explicit Generate trigger
- no early LLM invocation

## Phase 6. Workflow job engine

Deliver:

- TeacherAssist workflow tables
- queued/running/completed/failed/cancelled states
- input snapshot persistence
- dedicated worker service using outbox/lease pattern

## Phase 7. Mock LLM weekly planning generation

Deliver:

- deterministic/mock generation
- end-to-end workflow wiring
- UI progress/status surfaces
- sectioned outputs

## Phase 8. Real LLM generation with structured outputs

Deliver:

- backend-only OpenAI integration
- JSON schema validation
- usage logging
- retry/review handling

## Phase 9. PPTX / Google Slides-compatible deck export

Deliver:

- template-based PPTX generation
- export artifact storage
- expiry metadata

## Phase 10. Assessment / TEKS question mapping

Deliver:

- TEKS-linked assessment definitions
- question-to-standard mapping
- quiz/assignment metadata model

## Phase 11. TEKS mastery matrix

Deliver:

- student alias rows by `STUDENT #`
- mastery status model
- assessment rollup logic
- teacher review surfaces

## Phase 12. QR-coded printable assignment templates

Deliver:

- printable per-student artifacts
- QR payload format
- assignment/class/student alias binding

## Phase 13. Uploaded written work review workspace

Deliver:

- upload review workspace
- OCR/extracted text persistence
- source viewer + extracted content + AI recommendation side-by-side

## Phase 14. Grading assistant and teacher confirmation

Deliver:

- teacher approval workflow
- score/mastery/feedback evidence review
- commit only after teacher confirmation

## Phase 15. Insights / lesson effectiveness / learning plans

Deliver:

- TEKS mastery trends
- lesson effectiveness scoring
- reteach recommendations
- student alias insight pages

## Phase 16. Newsletter and communication assistant

Deliver:

- weekly newsletters
- parent/behavior drafts with placeholders only
- teacher review/edit before use

## Phase 17. File retention and cleanup

Deliver:

- temporary export expiry
- stale upload cleanup
- retention policies for extracted artifacts

## Phase 18. Hardening / security / privacy / cost review

Deliver:

- prompt-size controls
- usage budgeting
- privacy review
- capacity review on Lightsail worker/API host
- operational alerts/metrics for TeacherAssist jobs

## Repo-aware order rationale

1. Product access must come first because current Oziebot only understands trading access.
2. TeacherAssist shell/theme comes next so new work lands in a visibly separate surface.
3. Structured setup and pacing data must exist before any planning or grading workflows.
4. Workflow engine must precede real LLM/PPTX generation because the current platform already favors background processing.
5. Grading/matrix/insights should come after planning and assessment metadata exist.

## Suggested milestone gates

- **Gate A**: TeacherAssist route tree, product switcher, and setup shell exist with no trading regressions.
- **Gate B**: Draft-save and workflow job engine work end to end using mock generation.
- **Gate C**: Real LLM planning and PPTX export are cost-controlled and observable.
- **Gate D**: Assessment, mastery, and grading review flows are teacher-confirmed only.
- **Gate E**: Privacy, retention, and capacity controls are in place before broad rollout.
