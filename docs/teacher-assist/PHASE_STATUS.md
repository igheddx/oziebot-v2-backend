# TeacherAssist Phase Status

## Current Implemented Baseline

Phases 1 through 19 have been implemented according to the latest implementation summary.

Operational TeacherAssist access setup is also in place through an idempotent admin seed script for the requested Dominic and Awele user access/bootstrap flow.

## Phase 1

Product access, default app behavior, app switcher, TeacherAssist placeholder routes, and isolated shell.

## Phase 2

Teacher profile, school years, grading periods, subjects, classes, class-subjects, and standards/TEKS foundation.

## Phase 3

Pacing guide import foundation, resource library, planning drafts, and metadata-first upload handling.

## Phase 4

Workflow-ready planning refinement with context preview, readiness validation, and disabled generation placeholder.

## Phase 5

Persisted workflow execution and deterministic mock weekly-plan generation.

## Phase 6

Mock output refinement, weekly-plan editing/versioning, backend-only provider seam, AI usage tracking, and fixture support.

## Phase 7

Instructional planning scope support, generalized context preview, flexible mock instructional-plan output, reusable-plan lineage fields, copy-without-AI support, and guarded provider validation/config.

## Phase 8

Shared plan library foundation, owner-managed sharing/template controls, teacher-owned software-only copy enhancements, prior-year candidate discovery, annual curriculum rollover copy foundation, and new plans/rollover UI routes.

## Phase 9

Dedicated TeacherAssist worker foundation, queued workflow execution, lease/retry/heartbeat metadata, timeout/cancellation handling, guarded provider activation seams, cost-limit enforcement seam, and workflow-status metadata improvements in the instructional-planning workspace.

## Phase 10

Controlled real-provider activation for instructional-plan generation only, worker-only provider execution, prompt/validation hardening, quality-review metadata on generated plans, and plan-viewer review UI updates.

## Phase 11

Section-level regeneration, teacher-instruction-guided rewrite controls, version-preserving targeted plan updates, plan-scoped regeneration usage metadata, and plan-viewer regeneration actions.

## Phase 12

Assignment model foundation, assignment lifecycle/status APIs, tenant-safe assignment validation, a software-only weekly-plan-to-assignment starter, and the `/teacher-assist/assignments` workspace.

## Phase 13

QR-coded printable assignment packet foundation, tenant-safe packet/page persistence, software-only QR generation, assignment packet history, and the printable packet browser view.

## Phase 14

Uploaded student-work intake foundation, anonymous STUDENT # submission persistence, software-only upload metadata capture, optional packet/page linkage, and assignment-workspace submission review controls.

## Phase 15

Grading review foundation, anonymous STUDENT # teacher-review persistence, software-only/manual review creation, teacher confirmation validation, and assignment-workspace grading review controls.

## Phase 16

Unified TeacherAssist workspace, append-only activity-event foundation, class-centric operational grouping, needs-attention aggregation, recent activity feed, and workspace dashboard routing.

## Phase 17

Storage-provider hardening, private S3-ready object storage abstraction, backend-generated temporary download URLs, Lightsail/Docker runtime configuration, and AWS bucket/IAM bootstrap guidance.

## Phase 18

OCR intake and artifact-processing foundation, extraction-job persistence, extracted-text records, mock-first OCR provider seam, worker-managed storage-backed extraction, extraction status APIs, activity-event expansion, and workspace/frontend extraction visibility.

## Phase 19

Extraction remediation and teacher-review drill-down, extraction review statuses, confidence metadata, retry/lineage tracking, stale-job recovery, extraction detail/history APIs, issue flagging with teacher notes, workspace remediation attention rules, and the `/teacher-assist/extractions` operational review workspace.

## Next Phase

The next recommended phase is guarded real OCR provider integration so TeacherAssist can evaluate real extraction providers behind config guards while preserving teacher-approved text, retry lineage, and the no-grading-automation boundary.
