# TeacherAssist AI Product Principles

## Core Philosophy

- TeacherAssist exists to reduce teacher after-hours workload, not to replace teachers.
- The teacher remains the final authority.
- AI suggests; teachers review, edit, approve, and commit.
- No AI-generated grades, feedback, plans, or communications should be automatically committed without teacher confirmation.
- Teacher workflow value matters more than flashy AI demos.
- The platform should solve real teacher pain: planning, grading, feedback, standards alignment, documentation, parent communication, and instructional continuity.
- TeacherAssist should support how teachers actually work, not force teachers into rigid software workflows.
- Teachers do not only plan weekly; they also plan by module, unit, grading period, instructional sequence, and pacing guide.
- Instructional continuity matters. Plans should connect across days, weeks, modules, standards, resources, assessments, and interventions.
- Privacy-first design is non-negotiable.
- Use anonymous STUDENT # identifiers instead of student names or real student IDs.
- Avoid storing unnecessary student personally identifiable information.
- Uploaded or scanned student work must be handled carefully and reviewed by the teacher.
- AI workflows should be asynchronous, persisted, explainable, and recoverable.
- Structured outputs are preferred over freeform blobs.
- Mock-first development should remain the default until real-provider guardrails are proven.
- Real-provider activation must stay explicit, environment-gated, cost-limited, and reversible.
- Backend-only AI provider access is required.
- TeacherAssist must remain isolated from trading/Coinbase/Oziebot trading logic.

## Grading Philosophy

- AI may assist with rubric interpretation, feedback drafting, pattern detection, and provisional scoring.
- The teacher must confirm before grades are committed.
- The system should help reduce repetitive feedback writing.
- The system should support teacher judgment, not override it.
- Assignment foundations should remain teacher-owned, context-rich, and software-first until scan/upload/grading workflows are mature.
- Grading workflows should preserve assignment, student number, class, subject, school year, grading period, standard, and page context.

## Planning Philosophy

- Planning is not only weekly.
- Planning should support weekly plans, multi-week plans, modules, units, and grading-period plans.
- Pacing guides are central to planning.
- Generated plans should preserve instructional arc, standards progression, resources, vocabulary, assessments/checkpoints, differentiation, and teacher notes.
- Weekly plans should be treated as one view of a larger instructional plan.
- Generated plans should surface teacher-review metadata such as quality flags, missing-context warnings, standards-alignment summary, and a review checklist.
- A generated plan should stay in progress until a teacher reviews and marks it complete.
- Teachers should be able to regenerate targeted sections without rerunning the full plan when only one part needs improvement.

## QR / Printable Assignment Philosophy

- QR codes should identify assignment-page context, not expose student identity.
- QR payloads should use signed tokens or compact secure references.
- Printable packet generation should stay software-only until later scan/upload phases need additional processing layers.
- QR resolution should connect scanned pages to the correct teacher, class, school year, grading period, subject, assignment, anonymous student number, page number, total pages, and template version.
- Printable assignment templates should support per-student, per-page QR identity.
- Multi-page student work must preserve page ordering after scan/upload.

## Shared Planning + Reusability Philosophy

- TeacherAssist should maximize instructional-plan reuse instead of regenerating duplicate plans unnecessarily.
- Shared instructional plans should support team-level curriculum alignment while still allowing teacher personalization.
- A generated instructional plan may become:
  - a reusable template
  - a shared grade-level plan
  - a teacher-owned personalized copy
- Teacher personalization should not destroy plan reusability.
- Copying and manual editing should not require AI calls.
- AI calls should only happen when a teacher explicitly requests:
  - regeneration
  - adaptation
  - personalization
  - rewriting
  - standards alignment updates
  - pacing adjustments
- The platform should preserve a canonical reusable instructional plan while allowing teacher-specific branching/versioning.

## Instructional Asset Durability Philosophy

- Generated artifacts should be durable and reusable across school years.
- TeacherAssist should become more valuable each year a teacher uses it.
- The platform should support annual curriculum rollover.
- Teachers should be able to:
  - reuse prior-year plans
  - reuse prior-year quizzes
  - reuse prior-year rubrics
  - reuse printable templates
  - reuse newsletters/communication drafts
  - reuse pacing-aligned instructional assets
- Reuse should not require regeneration unless requested.
- Teachers should be able to selectively regenerate only portions impacted by:
  - pacing-guide changes
  - curriculum updates
  - TEKS/standards changes
  - instructional strategy changes

## AI Cost Optimization Philosophy

- AI should not be invoked when deterministic software workflows can satisfy the request.
- Copying, versioning, branching, and manual editing are software-layer operations.
- AI generation should be incremental and targeted whenever possible.
- Section-level regeneration is preferred over full-plan regeneration.
- Shared reusable instructional plans should reduce repeated AI generation across teacher teams.
