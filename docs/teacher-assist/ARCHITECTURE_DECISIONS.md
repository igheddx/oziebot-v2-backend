# TeacherAssist Architecture Decisions

## ADR-001: TeacherAssist remains isolated from trading

TeacherAssist must remain a separate product module and must not be woven into trading strategy engines, Coinbase integrations, trading entitlements, or trading worker responsibilities.

Reason:
Avoid long-term coupling between education workflows and trading infrastructure.

## ADR-002: Reuse platform foundations safely

TeacherAssist may reuse auth, tenants, users, Postgres, deployment foundations, and shared app switching.

Reason:
These are platform-level capabilities and do not create trading coupling.

## ADR-003: Product access is separate from trading entitlements

TeacherAssist access should use platform product access, not trading strategy entitlements.

Reason:
Trading entitlements are strategy-specific and should not define education product access.

## ADR-004: Backend-only AI provider access

All LLM/provider calls must happen in the backend.

Reason:
Protect provider keys, centralize usage tracking, enforce workflow controls, and prevent client-side leakage.

## ADR-005: Mock-first AI development

Mock provider remains the default until real provider guardrails are proven.

Reason:
Controls cost, protects development speed, and avoids accidental external calls.

## ADR-006: Async workflow execution

Generation, parsing, OCR, exports, grading assistance, and other long-running operations should use persisted workflows/jobs.

Reason:
Avoid request-thread blocking and provide reliable status/error tracking.

## ADR-007: Teacher confirmation required

AI-generated grading, feedback, plans, newsletters, and communications require teacher review/confirmation before final commit.

Reason:
Teacher remains accountable and final authority.

## ADR-008: Anonymous student identity model

Use anonymous STUDENT # identifiers instead of student names or real student IDs wherever possible.

Reason:
Minimize PII and support privacy-conscious classroom workflows.

## ADR-009: Instructional planning is scope-flexible

TeacherAssist should not be architected as only a weekly-plan generator.

Reason:
Teachers plan by week, module, unit, grading period, and instructional sequence.

## ADR-010: QR code assignment-page identity

QR codes should resolve assignment-page context through signed tokens or secure references.

Reason:
Scanned work must reconnect to assignment/student/page context without exposing sensitive identity data.

## ADR-011: Shared instructional plan model

TeacherAssist should support reusable shared instructional plans that may be copied and personalized by individual teachers.

Reason:
Multiple teachers often teach the same curriculum and should not need to regenerate duplicate plans independently.

Implications:
- plans may be shared
- plans may be copied
- plans may branch
- plans may preserve lineage
- plans may support visibility scopes such as:
  - private
  - shared
  - grade-level
  - school
  - district

## ADR-012: Teacher personalization branching

Teacher personalization should create teacher-owned versions/copies derived from reusable source plans.

Reason:
Teachers require autonomy without destroying reusable curriculum assets.

Implications:
- preserve source plan lineage
- support derived_from_plan_id relationships
- support teacher-owned modifications
- preserve reusable master/template plans

## ADR-013: AI invocation boundaries

AI should only be called when a teacher explicitly requests generation, adaptation, regeneration, or personalization.

Reason:
Reduce unnecessary cost and preserve fast deterministic workflows.

Implications:
- copying plans is not an AI operation
- manual editing is not an AI operation
- branching/versioning is not an AI operation
- regeneration/adaptation may invoke AI

## ADR-014: Annual curriculum rollover

Instructional artifacts should persist across school years and support selective reuse.

Reason:
Teachers frequently reuse and refine curriculum year over year.

Implications:
- preserve prior-year artifacts
- support copying artifacts into a new school year
- support selective regeneration
- support reusable instructional libraries

## ADR-015: Instructional asset library

TeacherAssist should evolve toward a reusable instructional asset library model.

Reason:
Generated instructional content becomes more valuable over time and should not be disposable.

Implications:
Artifacts may include:
- instructional plans
- quizzes
- rubrics
- templates
- pacing structures
- resources
- newsletters
- assessments

Potential future metadata:
- source_artifact_id
- derived_from_artifact_id
- visibility_scope
- reusable_status
- version
- school_year_origin

## ADR-016: Reuse weekly plan persistence for plan-library foundations

TeacherAssist should continue using the existing `weekly_plans` artifact store as the persisted instructional-plan record during the shared-library and rollover foundation phase.

Reason:
Avoid migration churn while the product is still stabilizing around generalized instructional planning and reusable-plan lineage.

Implications:
- generalized plan behavior can ship without a new table rename
- library and rollover flows read from the existing artifact model
- future persistence renaming remains possible after workflows mature

## ADR-017: Owner-only sharing mutation for phase-eight safety

TeacherAssist should restrict plan sharing/template mutation to the current plan owner until tenant-admin role behavior is mature enough for broader shared-plan governance.

Reason:
Avoid shipping ambiguous authorization rules for shared instructional assets during the first reusable-plan library phase.

Implications:
- owners can manage template/visibility/reuse state now
- non-owner shared viewers can copy but not mutate source-plan sharing settings
- future admin/team governance can extend this rule without breaking current copy semantics

## ADR-018: Dedicated TeacherAssist worker isolation

TeacherAssist workflow execution should run in its own worker service foundation instead of reusing API-process background tasks or trading workers.

Reason:
Avoid coupling educational AI workflow behavior to request lifecycles or trading execution infrastructure.

Implications:
- TeacherAssist workflows are queued by the API and processed by a dedicated worker
- worker lease/retry/heartbeat behavior stays scoped to TeacherAssist workflows only
- trading workers remain unchanged
- future provider rollout can harden around this isolated seam without impacting trading services

## ADR-019: Controlled real-provider activation

TeacherAssist real-provider execution should remain disabled by default and activate only through explicit backend environment configuration, model allowlisting, workflow-type gating, and daily cost controls.

Reason:
Protect cost, privacy, and reliability while preserving mock-first development and safe rollout behavior.

Implications:
- mock remains the default provider
- request handlers and frontend never call providers directly
- worker execution enforces provider enablement, configured API key presence, allowed model selection, and daily cost limits
- only instructional-plan generation workflows may use the real provider seam

## ADR-020: Teacher-review metadata is part of generated plan quality

TeacherAssist should persist review-required metadata with generated instructional plans so teacher review is visible, explicit, and durable.

Reason:
Teachers need fast, repeatable quality checks before using AI-assisted plans in the classroom.

Implications:
- generated plans include review_required metadata
- plans may include quality flags and missing-context warnings
- standards alignment summary and teacher review checklist travel with the artifact
- teacher review remains a software-layer completion step, not an automatic publish event

## ADR-021: Section regeneration is versioned and plan-scoped

TeacherAssist should treat section regeneration as a versioned mutation of a persisted instructional-plan artifact rather than as an untracked transient rewrite.

Reason:
Teachers need targeted AI-assisted refinement without losing recoverability, lineage, review status, or provider/cost metadata.

Implications:
- section regeneration writes a new `weekly_plan_versions` snapshot
- only the targeted section/path is replaced
- regeneration usage events carry `weekly_plan_id` metadata so copied plans without workflow ids can still surface provider/cost details
- regeneration keeps plans in `in_progress` and preserves teacher review as the final commit step

## ADR-022: Assignment foundation is teacher-owned and software-first

TeacherAssist assignments should start as a tenant-safe, teacher-owned software workflow before QR, upload, grading, or mastery automation layers are added.

Reason:
Assignment context must be reliable and privacy-aware before downstream scan/review workflows can safely depend on it.

Implications:
- assignments are scoped to TeacherAssist tenant access and the current teacher user
- assignment validation reuses school-year, grading-period, class, subject, and standard ownership rules
- weekly-plan-to-assignment creation is a software-only starter, not an AI workflow
- printable packets, student uploads, grading review, and mastery automation remain later phases

## ADR-023: Printable packets use anonymous page identity and software-only QR generation

TeacherAssist printable assignment packets should be generated as a tenant-safe software workflow that encodes anonymous page identity without introducing OCR or provider dependencies.

Reason:
QR-coded packet generation needs to be useful before scan/upload ingestion exists, while still guaranteeing that later ingestion workflows inherit non-PII assignment, class, subject, and page context.

Implications:
- packet/page persistence lives alongside TeacherAssist assignments instead of a separate scan subsystem
- packet generation derives student pages from `class.student_count` and anonymous `STUDENT #` values only
- QR payloads carry compact ids plus a per-page token and must exclude names, emails, real student ids, and freeform sensitive content
- the first printable implementation is HTML/browser-print oriented; OCR, scan upload, and grading review remain later phases

## ADR-024: Student-work intake stores anonymous upload metadata before OCR or grading

TeacherAssist student-work submissions should be introduced as a privacy-safe upload and linkage layer before any OCR, grading, or mastery automation is considered.

Reason:
Teachers need a durable way to associate completed work with assignments and printable packet identity without prematurely introducing document parsing, rubric scoring, or other higher-risk workflows.

Implications:
- submissions store anonymous `student_number` plus upload metadata and assignment/class/subject context
- packet/page linkage is optional and can be added after upload when QR packet context exists
- intake remains software-only and must not create AI usage, OCR output, grading artifacts, or mastery updates
- review status is teacher-controlled and intentionally lightweight until later grading phases exist
