# TeacherAssist AI Implementation Summary

## Purpose

This document summarizes the current Oziebot repo findings and the proposed implementation foundation for adding **TeacherAssist AI** as a separate product module without disrupting the existing trading platform.

## Current implemented baseline

**Phases 1–38** are implemented. Latest completed work:

- **Phase 37** — Week-centric instructional workspace (`instructional_weeks`, tabbed `/teacher-assist/week/[id]`, action center, snapshots, pacing integration)
- **Phase 38** — Assignment → gradebook → mastery → reteach instructional loop (instructional evidence, objective performance, reteach workspace, week closure, health reporting)
- **Phase 39** — Teacher Copilot (context engine, explainable analysis intents, sessions/messages audit trail, copilot UI, home integration)
- **Phase 41** — Pilot readiness (feature inventory, feedback workspace, usage metrics, system health, deployment checklists, nav audit)
- **Grade mastery levels on v2 grades** — Mastery / Developing / Beginning stored on assignment grades and gradebook records; shown in gradebook, review, and grading UI
- **Subject gradebook grid** — 9-week subject matrix with TEKS rollups, Missing level, grade-entry assignments, CSV export
- **Teacher-created assignments + QR cover sheets** — Manual assignment create (week, subject, TEKS) with downloadable Word cover sheets (one page per student) for external work

**Next recommended:** Phase 42 — Mastery v2 / gradebook v2 UI completion, parent communication send, or LMS/SIS import (no auto-grade or auto-mastery commits).

## Class rubric score report DOCX export (2026-06-12)

### What was changed

- The assignment-level **Class rubric score report** now downloads as a **Word DOCX** instead of opening printable HTML.
- Each student gets a dedicated page with **Student #**, total score, mastery label, rubric criteria scores/feedback, and teacher comment.
- DOCX output inserts a **page break between students** so the document prints cleanly one student per page.
- Assignment view now shows the download button only when the backend marks the report as available, preventing false-positive clicks that could return a 400 blocker.
- Readiness now treats a submission as resolved when that exact submission already has an **official grade** (`CONFIRMED` or `REVISED`), even if the submission row itself is stuck on an older non-terminal status.

### Files changed

- `backend/services/api/src/oziebot_api/services/teacher_assist_v2/rubric_score_exports.py`
- `backend/services/api/src/oziebot_api/api/v1/teacher_assist_v2.py`
- `frontend/apps/web/lib/teacher-assist-v2-api.ts`
- `frontend/apps/web/components/teacher-assist-v2/teacher-assist-v2-assignment-viewer-screen.tsx`
- `backend/services/api/tests/test_grading_rubric.py`

### Remaining gaps

- Per-student rubric score cards still open as printable HTML; only the class-wide report was converted to DOCX in this pass.

## Teacher onboarding duplicate-preferences race (2026-06-02)

### What was fixed

- New-teacher onboarding could fail with a browser **CORS / “Could not reach API”** message when two parallel requests both tried to create `teacher_assist_user_preferences` for the same user.
- Root cause: `get_user_preferences_or_create` caught the duplicate-key `IntegrityError` but then called `db.expunge(row)` after the nested savepoint rollback had already detached the failed insert, raising `InvalidRequestError` → HTTP 500 without a usable response (browser surfaces this as CORS + `net::ERR_FAILED`).
- Same safe-expunge pattern applied to `get_or_create_v2_onboarding`.

### Files changed

- `backend/services/api/src/oziebot_api/services/teacher_assist/user_preferences.py`
- `backend/services/api/src/oziebot_api/services/teacher_assist_v2/teacher_onboarding.py`
- `backend/services/api/tests/test_teacher_assist_access_seed.py` — `test_user_preferences_or_create_recovers_from_duplicate_insert`

### Tests run

- `pytest tests/test_teacher_assist_access_seed.py::test_user_preferences_or_create_recovers_from_duplicate_insert`
- `pytest tests/test_teacher_assist_access_seed.py::test_user_preferences_or_create_is_idempotent`

### Remaining gaps

- `Unchecked runtime.lastError: Could not establish connection. Receiving end does not exist` is **browser extension noise** (not TeacherAssist); safe to ignore.

## Pacing guide → lesson plan carryover (2026-06-02)

### What was implemented

- Instructional package generation now reads **per-day pacing guide plans** (`pacing_context.days`): daily topics, objective focus, materials, teacher notes, and assessment checks.
- **All pacing materials** (guide/week/day/objective supporting materials + catalog book/resources) flatten into generation context and appear in lesson plan materials unless removed by the teacher before generate.
- Planning review `daily_topic` and `district_materials` now come from pacing context instead of legacy week description blobs.
- AI prompts instruct models to use only listed pacing materials (no invented textbook names).
- Demo ELA backfill no longer triggers from package title heuristics — only explicit `content_profile=ela_week1_main_idea` metadata.

### Backend files changed

- `pacing_plan_resolver.py` (new), `instructional_package_generation.py`, `planning_workflow.py`, `planning_context.py`, `supporting_materials.py`, `deterministic_package_content.py`, `package_artifact_refresh.py`, `instructional_package_ai.py`
- Tests: `test_pacing_plan_resolver.py`

### Manual validation checklist

1. Build a 1-week pacing guide with distinct daily topics + book reference
2. Assign guide to teacher subject
3. Open planning review — verify daily topics/materials appear
4. Generate package — Monday vs Tuesday daily plans should differ and include your materials
5. Old seed/demo curriculum should not appear on new custom guides

---

## Planning supplemental materials scoped per package (2026-06-02)

### What was implemented

- Teacher supplemental materials uploaded during planning are now **session-scoped**: planning review and the supplemental-materials list return only rows with `package_id IS NULL` (not yet linked to a generated package).
- On package generate, only **unlinked** supplemental rows for that week range are attached to the new package (`package_id` set).
- Package detail still shows supplemental materials linked to that specific package via `package_id`.

### Backend files changed

- `planning_workflow.py` — `list_planning_supplemental_materials(unlinked_only=..., package_id=...)`
- `planning_context.py`, `teacher_assist_v2.py` — planning flows use `unlinked_only=True`
- `instructional_package_generation.py` — link only unlinked rows on generate
- `package_dashboard.py` — package detail lists supplemental by `package_id`
- Tests: `test_teacher_assist_v2_planning.py` (`test_v2_planning_supplemental_materials_scoped_to_current_session`)

### Manual validation checklist

1. Add PDF/link supplemental materials and generate a lesson plan package
2. Start a new plan for the same week — supplemental step should be empty
3. Add new supplemental materials and generate again — only new items attach to the new package
4. Open the prior package viewer — prior supplemental materials still visible there

---

## Planning pacing materials + daily teaching plan focus (2026-06-02)

### What was implemented

- **Step 3 (materials):** Shows pacing guide template materials grouped by subject with **Remove** (confirm dialog). Removed items are excluded from package generation via `excluded_pacing_material_ids` (not deleted from the pacing guide).
- **Step 2 (pacing review):** Shows each day's **daily topic** and **objective focus** from the pacing guide instead of repeating weekly TEKS codes.
- **Daily teaching plans:** Plan-level and per-subject content now uses pacing day `daily_topic` and `objective_focus`. Package viewer shows "Today's focus" and day objective; TEKS code appears separately as "Standard:" when present.

### Backend files changed

- `pacing_plan_resolver.py`, `deterministic_package_content.py`, `package_export.py`
- `planning_workflow.py`, `planning_context.py`, `instructional_package_generation.py`, `package_artifact_refresh.py`
- `teacher_assist_v2.py` schema — `excluded_pacing_material_ids` on generate
- Frontend: `teacher-assist-v2-planning-screen.tsx`, `teacher-assist-v2-package-viewer-screen.tsx`, types/api
- Tests: `test_pacing_plan_resolver.py`

### Manual validation checklist

1. Open planning step 2 — verify Mon/Tue/Wed show topic + objective from pacing guide
2. Step 3 — verify pacing guide PDFs/links listed with Remove; confirm dialog appears; removed items stay out after generate
3. Generate package — daily teaching plans show different focus/objective per day, not repeated TEKS text

---

## Writing response rubric auto-generation + teacher edit (2026-06-02)

### What was implemented

- Selecting **Writing Response Pages** now automatically generates a linked **rubric per subject** based on that writing assignment's prompt and instructions (no need to separately check Rubrics).
- Standalone rubric output is skipped when writing response is selected to avoid duplicates.
- Teachers can **edit and save** rubric criteria, points, and performance levels from the package viewer (`Edit rubric` → `Save rubric`).
- Saved rubrics refresh preview/export HTML and are used in grading context for linked writing assignments.

### Backend files changed

- `deterministic_package_content.py` — `build_rubric_for_writing_response()`
- `instructional_package_generation.py` — auto-link rubric to writing response artifacts
- `package_artifact_update.py` (new), `grading_context.py`
- `teacher_assist_v2.py` — `PUT .../artifacts/{artifact_id}/rubric`
- Frontend: `teacher-assist-v2-rubric-editor.tsx`, package viewer, planning note, API/types
- Tests: `test_package_rubric.py`

### Manual validation checklist

1. Generate a package with **Writing Response Pages** selected
2. Open package viewer — verify a rubric appears for each writing response subject
3. Edit rubric criteria/points, save, refresh — changes persist in preview
4. Optional: generate grading draft for a writing submission and confirm rubric context is present

---

## Additional package assignments after generation (2026-06-02)

### What was implemented

- Teachers can add **new assignments** to an existing instructional package from the package viewer without removing prior assignments.
- Form supports subject, assignment type (quiz / written assignment / writing response), required guidance notes, and optional title hint.
- Generation uses the existing v2 AI artifact pipeline with `generation_mode=package_additional_assignment`, existing-assignment context, and teacher notes so output differs from the original plan.
- Each new artifact is appended with its own sequence number, mapped to TEKS/objectives/pacing/package, and creates a linked gradebook assignment record.
- Writing response additions also auto-generate a linked rubric (same behavior as initial package generation).

### Backend files changed

- `package_additional_assignments.py` (new)
- `instructional_package_ai.py`, `planning_workflow.py`, `teacher_assist_v2.py`, schemas
- Frontend: `teacher-assist-v2-add-assignment-panel.tsx`, package viewer, API/types
- Tests: `test_package_additional_assignments.py`

### Manual validation checklist

1. Open a generated package → **Assessments** → use **Add another assignment**
2. Pick subject + type, add notes describing how it should differ, generate
3. Verify original assignments remain and the new one appears with **Additional assignment** label
4. Confirm new assignment appears in assignments list / gradebook with correct subject and TEKS mapping

### Bug fix (close-out error, 2026-06-02)

- **Symptom:** Closing a plan surfaced `Closed packages cannot receive additional assignments.` (raw JSON in UI).
- **Cause:** Package viewer showed **Add another assignment** when `can_close_out` was **false** (inverted conditional). After close-out, the panel mounted and called the additional-assignment form API on a completed package.
- **Fix:** Show the panel only when `can_close_out` is true; gate generate (not form read) on open packages in `package_additional_assignments.py`; add missing `db.commit()` on close-out endpoint.
- **Files:** `teacher-assist-v2-package-viewer-screen.tsx`, `package_additional_assignments.py`, `teacher_assist_v2.py`

---

## Optional pacing guide per subject (2026-06-02)

### What was implemented

- Pacing guide setup dropdown includes **No option** (default) for each subject with available district guides.
- Teachers may save pacing choices without assigning a guide to every onboarded subject.
- Subjects left on **No option** have their pacing assignment deactivated and are **excluded from planning**, package generation, and teaching-order defaults.
- At least one subject must still have a pacing guide selected before setup can be saved.

### Backend / frontend files changed

- `pacing_guide_setup.py` — partial save, deactivate omitted subjects, validation message
- `teacher-assist-v2-pacing-guide-setup-screen.tsx` — No option default, UI copy, client validation
- Tests: `test_teacher_assist_v2_planning.py` (`test_v2_pacing_guide_setup_allows_no_option_and_excludes_from_planning`)

### Manual validation checklist

1. Open **Pacing Guides** — each subject defaults to **No option**
2. Select a guide for one subject only, save — planning form lists only that subject
3. Re-open pacing setup, switch to **No option** for that subject and pick a guide for another subject — planning follows the new selection

---

## Rubric-linked grading and score reports (2026-06-02)

### Package viewer rubric pairing (2026-06-02)

- Writing responses and written assignments show their **linked rubric in the same card** (preview, edit, print) instead of a separate Rubrics section.
- Orphan rubrics (no linked assignment) still appear under **Unlinked Rubrics**.
- **Files:** `teacher-assist-v2-package-viewer-screen.tsx`, `planning_workflow.py`

### Class rubric score report availability fix (2026-06-02)

- Class report button now appears when **every submission row** on the assignment is resolved (confirmed or incomplete), matching the Submissions table teachers see — not hidden roster slots with no submission row.
- Stopped creating `NOT_UPLOADED` placeholder rows when opening the assignment page (that could block the report).
- Assignment view always shows the class report section for writing/written assignments (green when ready, amber with reason when not).
- **Files:** `submission_workflow.py`, `rubric_score_exports.py`, `assignments.py`, `teacher-assist-v2-assignment-viewer-screen.tsx`

### What was implemented

- **Written assignment** and **writing response** artifacts now auto-generate a **linked rubric** (same pattern as writing responses; standalone rubric output skipped when assignment is selected).
- AI grading uses package rubric **criteria names and point values** instead of a generic 100-point template.
- Grade confirmation shows an **editable rubric score card**; criterion scores sync to the total grade on confirm.
- **Per-student rubric score card** (printable HTML) after grade confirmation.
- **Class rubric score report** (Word DOCX with one student per page) when every roster student is resolved.

### Backend / frontend files changed

- `grading_rubric.py`, `rubric_score_exports.py`, `grading_context.py`, `grading_ai.py`, `grade_reviews.py`
- `deterministic_package_content.py` — `build_rubric_for_written_assignment()`
- `instructional_package_generation.py`, `package_additional_assignments.py`, `assignments.py`, `submission_intake.py`
- API: accept grade with rubric body, `GET .../rubric-scorecard`, `GET .../rubric-score-report`
- Frontend: `teacher-assist-v2-rubric-score-editor.tsx`, assignment review/viewer, submission viewer
- Tests: `test_grading_rubric.py`

### Manual validation checklist

1. Generate a package with **Written assignment** and/or **Writing response**
2. Grade a submission — rubric criteria appear with editable scores on confirm
3. Confirm grade — print per-student rubric score card
4. Confirm all students — assignment shows **Download class rubric report (Word)**

---

## Subject gradebook grid (2026-06-02)

### What was implemented

- Spreadsheet-style **subject gradebook** for each **9-week grading period** tied to the teacher's active pacing plan.
- Grid layout: **students in rows**, **TEKS groups with assignment columns** plus **TEKS mastery summary** (yellow-style) columns.
- Assignment cells show **percentage + M/D/B/Missing**; TEKS mastery auto-calculates from assignment average unless any assignment is Missing.
- **`Missing`** added as fourth mastery level (derived in grid; stored on confirmed grades only when scored).
- **Grade-entry assignments** (`TEACHER_GRADE_ENTRY`) require TEKS mapping and appear automatically in the grid; inline grade save from the grid.
- **CSV export** matching the grid for school-system import (`85|D`, `Missing`, etc.).

### Backend files changed

- `gradebook_grid.py`, `gradebook_manual_entry.py`, `mastery_constants.py`, `assignment_constants.py`, `grade_review_constants.py`, `grade_reviews.py`
- API: `teacher_assist_v2.py`, schemas `teacher_assist_v2.py`
- Tests: `test_teacher_assist_v2_gradebook_grid.py`

### Frontend files changed

- `teacher-assist-v2-gradebook-screen.tsx`, `teacher-assist-v2-mastery.ts`, `mastery-level-badge.tsx`
- `teacher-assist-v2-types.ts`, `teacher-assist-v2-api.ts`

### Tests run

- `pytest tests/test_teacher_assist_v2_gradebook_grid.py`
- `pytest tests/test_teacher_assist_v2_mastery_constants.py`

### Manual validation checklist

1. Open **Gradebook** → pick subject + 9-week period
2. Grid shows students × TEKS/assignment columns
3. Click cell → enter 0–100 → badge updates
4. TEKS summary shows Missing if any assignment missing; average otherwise
5. **Download CSV** exports spreadsheet-style file

### Remaining gaps

- No student name column (uses Student #001 roster numbers only)
- Multi-TEKS assignments group under first linked objective only
- Package-generated assignments appear once confirmed through normal grading flow

---

## Teacher-created assignments and QR cover sheets (2026-06-02)

### What was implemented

- Teachers can create assignments outside generated packages via **Create assignment** (`/teacher-assist-v2/assignments/create`): week, subject, TEKS objectives, title, type.
- Assignments stored with `creation_origin=TEACHER_MANUAL` and anchored to teacher pacing context.
- **Cover sheet generation** after create (and from assignment detail): one Word (`.docx`) page per student based on onboarding class size, each with QR code + student number for batch upload matching. Stored as `v2-assignment-{id}-cover-sheets.docx` for direct print from Word.
- Print packets now use `packet_kind` (`STUDENT_PACKET` vs `COVER_SHEET`) so cover sheets do not overwrite full QR student packets.

### Backend files changed

- `manual_assignments.py`, `assignment_print_packets.py`, `student_packet_docx.py`, `assignments.py`, `assignment_constants.py`
- Models: `teacher_assist_v2_assignment.py`, `teacher_assist_v2_assignment_print_packet.py`
- Migration: `085_teacher_assist_v2_manual_assignments.py`
- API: `teacher_assist_v2.py`, schemas `teacher_assist_v2.py`
- Tests: `test_teacher_assist_v2_manual_assignments.py`

### Frontend files changed

- `teacher-assist-v2-create-assignment-screen.tsx`, `assignments/create/page.tsx`
- `teacher-assist-v2-assignments-screen.tsx`, `teacher-assist-v2-assignment-viewer-screen.tsx`
- `teacher-assist-v2-types.ts`, `teacher-assist-v2-api.ts`

### Tests run

- `pytest tests/test_teacher_assist_v2_manual_assignments.py`
- `alembic upgrade head` (local)

### Manual validation checklist

1. Assignments → **Create assignment**
2. Pick week, subject, TEKS, title → submit
3. Word cover sheet file downloads with N pages (class size)
4. Assignment detail shows **Download cover sheets (Word)** / **Generate cover sheets (Word)**
5. Staple workflow: upload batch scan still matches students via QR on cover sheet

### Remaining gaps

- Upload assignment PDF + answer key on teacher-created assignments not yet in this pass (cover sheets + record only).
- Cover sheet regeneration if class size changes requires re-download (uses current onboarding `student_count`).
- Assignments created before the DOCX switch may still have old HTML cover sheet files in storage until regenerated.

### Next recommended phase

Upload external assignment + answer key on manual assignments, then full auto-grade on that content.


### What was implemented

- Stored `mastery_level` on `teacher_assist_v2_assignment_grades` and `teacher_assist_v2_gradebook_records` (migration `084_teacher_assist_v2_grade_mastery_level`), backfilled from percentage thresholds: **Mastery** 80–100, **Developing** 60–79, **Beginning** 0–59.
- Set on grade persist (confirm/modify/reject flows), Google Form import drafts, and gradebook sync.
- API responses now include `mastery_level` and `mastery_level_label` on assignment grades, grading drafts, gradebook records, grade review queue rows, and mastery evidence.
- Frontend gradebook, assignment review, assignment viewer grade table, student submission viewer, and mastery screen show mastery badges alongside numeric scores.

### Explicit non-goals preserved

- Mastery thresholds remain fixed at 80/60; no per-assignment or per-rubric customization in this pass.
- Grading drafts do not persist a separate mastery column; level is derived from draft percentage at serialize time.

### Backend files changed

- `mastery_constants.py`, `grade_reviews.py`, `gradebook_sync.py`, `grading_drafts.py`, `google_form_quizzes.py`, `submission_intake.py`
- Models: `teacher_assist_v2_assignment_grade.py`, `teacher_assist_v2_gradebook_record.py`
- Migration: `084_teacher_assist_v2_grade_mastery_level.py`
- Tests: `test_teacher_assist_v2_mastery_constants.py`, `test_teacher_assist_v2_gradebook_mastery.py`

### Frontend files changed

- `teacher-assist-v2-types.ts`, `teacher-assist-v2-mastery.ts`, `mastery-level-badge.tsx`
- `teacher-assist-v2-gradebook-screen.tsx`, `teacher-assist-v2-assignment-review-screen.tsx`, `teacher-assist-v2-assignment-viewer-screen.tsx`, `teacher-assist-v2-student-submission-viewer-screen.tsx`, `teacher-assist-v2-mastery-screen.tsx`

### Tests added / run

- `pytest tests/test_teacher_assist_v2_mastery_constants.py tests/test_teacher_assist_v2_gradebook_mastery.py`

### Manual validation checklist

1. Upload and auto-grade student work → open review → confirm suggested grade shows **Mastery / Developing / Beginning** badge next to score.
2. Adjust score in review → badge updates for adjusted percentage before confirm.
3. Confirm grade → gradebook row shows score and mastery badge.
4. Assignment viewer grade table shows draft and confirmed mastery columns.

### Remaining gaps

- Gradebook record revision history does not yet store previous/new mastery level separately (score/percentage only).
- Legacy v1 gradebook UI unchanged.

### Next recommended phase

Phase 42 — Mastery v2 / gradebook v2 UI completion, parent communication send, or LMS/SIS import.

## Package detail UI cleanup — Daily Teaching Plans vs Teaching Mode (2026-06-02)

### What was implemented

- Removed duplicate **Teaching Mode** quick-launch block and separate **Daily plans** list from the instructional package detail page.
- Consolidated teaching artifacts into one **Teaching Materials** section with:
  - **Daily Teaching Plans** — day, title, subjects included, status, and **Review / Present / Download** actions
  - **Subject Slide Decks** — subject, title, week range, status, and **Review / Present / Download** actions
- Reorganized remaining package sections: **Assessment Materials**, **Communication**, **Student Materials** (when generated), **Supporting Materials** (district + teacher supplemental), close-out unchanged.
- Inline empty states for missing daily teaching plans and subject slide decks (no duplicate fallback lists).
- Teaching Mode launcher copy updated to **Choose what to present** with helper text distinguishing **Daily Teaching Plan** vs **Subject Slide Deck**; primary action labeled **Present**.

### Explicit non-goals preserved

- No backend/API schema changes; `teaching_presentations` API field retained for compatibility.
- No full package page redesign; existing panel styling and close-out flow preserved.
- PPTX export and non-HTML download formats still deferred where unavailable.

### Frontend files changed

- `frontend/apps/web/components/teacher-assist-v2/teacher-assist-v2-package-viewer-screen.tsx`
- `frontend/apps/web/components/teacher-assist-v2/teacher-assist-v2-teach-screen.tsx`
- `frontend/apps/web/lib/teacher-assist-v2-types.ts` (optional `status` on artifacts)

### Tests added / run

- `read_lints` on updated package viewer and teach screen — no issues.
- Dev servers confirmed reachable (`/health` 200, frontend 200); full teacher login UI walkthrough not automated in this pass.

### Manual validation checklist

1. Login as teacher → Packages → open generated package
2. Confirm daily lesson artifacts appear only under **Daily Teaching Plans**
3. Confirm each daily item shows Review, Present, Download
4. **Present** opens `/teacher-assist-v2/teach?...&mode=daily&day=...&start=1`
5. **Subject Slide Decks** section separate with Present → `mode=subject&artifactId=...`
6. Confirm **Daily plans** label no longer appears
7. Teaching Mode launcher uses **Choose what to present** and consistent product labels

### Remaining gaps

- Download remains HTML-only where export exists; PPTX noted as coming soon in backend metadata.
- Student Materials section shown when optional outputs exist but not listed in the product brief’s top-level outline.

### Next recommended phase

Phase 42 — Mastery v2 / gradebook v2 UI completion, parent communication send, or LMS/SIS import.

## ELA Week 1 demo artifact backfill — Real teacher-facing outputs (2026-06-02)

### What was implemented

- Deterministic golden-path content for **ELA Week 1: Main Idea** with daily plans, slide deck (layouts/visuals), quiz + answer key + Google Forms-ready JSON, exit ticket, written assignment, rubric, newsletter, and supporting student materials.
- In-place package backfill for `test2@teacher.com` (`package_demo_backfill.py`, `backfill_ela_week1_demo_package.py`).
- Rich HTML exports, v2 QR student packet generation (3 students from onboarding class size), API artifact enrichment (`description`, `objective_mapping`, `additional_downloads`, `qr_student_packet`).
- Teaching Mode visual slide layouts; package detail regrouped into Instruction / Assessment / Student Materials / Rubric / Communication with Review, Present, Download, Print.

### Backend files changed

- `demo_content/ela_week1_main_idea.py`, `package_demo_backfill.py`, `assignment_print_packets.py`, `slide_visuals.py`, `package_export.py`, `planning_workflow.py`, `package_dashboard.py`, `scripts/backfill_ela_week1_demo_package.py`, `models/__init__.py`

### Frontend files changed

- `teacher-assist-v2-package-viewer-screen.tsx`, `teaching-mode-presentation.tsx`, `teaching-slide-visual.tsx`, `teaching-mode-slides.ts`, `teacher-assist-v2-types.ts`

### Tests run

- Backfill script succeeded for package `61e86258-fa86-498d-8a23-802740f8a068`; 0 mock/placeholder strings in preview HTML.

### Remaining gaps

- Superseded by **Golden-path artifact limitations** section below for v2 package generation/export behavior.

## Golden-path artifact limitations — production-real artifacts (2026-06-02)

### What was implemented

- **Deterministic classroom-ready generation** for all new v2 packages when real AI is off (`deterministic_package_content.py`, refactored `instructional_package_generation.py` with `artifact_persistence.py`). No `[MOCK OUTPUT]` / placeholder copy in teacher-facing artifacts.
- **Quiz exports:** Word DOCX quiz + answer key, read-only Google Forms JSON, teacher answer-key HTML preview. Labels: Download Quiz DOCX, Download Answer Key DOCX, Download Google Forms JSON, Preview Answer Key.
- **Fix (2026-06-10):** Quiz/JSON/DOCX downloads no longer 500 when artifact titles contain Unicode punctuation (e.g. em dash). `build_content_disposition()` now uses an ASCII `filename=` fallback plus RFC 5987 `filename*=`; quiz export metadata stores safe `original_filename` values for download tokens.
- **Per-student assessment DOCX (2026-06-10):** Quiz, written assignment, and new **Writing Response** artifacts export a class-set DOCX via `assessment_student_exports.py` + `student_packet_docx.py`. Each student receives every content page; each page repeats that student's QR code (top-left) and `Student #NNN` label with Word page breaks between pages. Class size comes from onboarding `student_count`. Google Forms JSON now includes a required student-number dropdown (`Student #001`…). Planning adds optional **Writing Response Pages** output type.
- **Print-friendly HTML:** shared print CSS, page breaks, `Print / Save as PDF` control on exports (`package_export.py`).
- **Slide deck UX:** PPTX note only (no dead PPTX button); actions Review / Present / Print / Save as PDF.
- **Package viewer:** Preview Quiz/Assignment/Rubric/Newsletter/Packet; quiz Google Forms helper text; QR packet on assignment card.
- **QR upload UX:** filename-token MVP instructions + highlighted manual match fallback on assignment viewer.
- **Admin regeneration:** `POST /v1/teacher-assist-v2/admin/packages/{package_id}/regenerate-artifacts` (idempotent; ELA Week 1 uses demo backfill, others use deterministic refresh via `package_artifact_refresh.py`).

### Backend files changed

- `deterministic_package_content.py`, `artifact_persistence.py`, `package_artifact_refresh.py`, `instructional_package_generation.py`, `package_export.py`, `demo_content/ela_week1_main_idea.py`, `api/v1/teacher_assist_v2.py`

### Frontend files changed

- `teacher-assist-v2-package-viewer-screen.tsx`, `teacher-assist-v2-assignment-viewer-screen.tsx`

### Tests run

- `python3 -m compileall` on modified v2 service modules (pass).

### Manual validation checklist

1. Login as `test2@teacher.com` → Packages → **ELA Week 1: Main Idea**.
2. Confirm quiz actions: Preview Quiz, Download Quiz DOCX, Download Answer Key DOCX, Download Google Forms JSON, Print / Save as PDF, Preview Answer Key; helper text for Forms JSON (not live integration).
3. Confirm slide deck shows PPTX note, no PPTX download button.
4. Generate a **new** package; confirm artifacts are classroom-ready without manual backfill.
5. Optional: `POST /v1/teacher-assist-v2/admin/packages/{id}/regenerate-artifacts` to refresh stale mock artifacts.

### Remaining gaps

- No Google Forms API or Google Classroom publish.
- No native PDF/PPTX server export (browser print HTML only).
- QR scan/OCR extraction not implemented; filename token + manual match only.
- Answer key preview opens exported HTML in a new tab (not inline iframe).

### Next recommended phase

Wire real AI generation for v2 packages when provider enabled; optional inline answer-key preview.

## Multi-student QR scan upload — PDF/image intake (2026-06-02)

### What was implemented

- **QR decode from uploaded files:** `qr_decoding.py` renders PDF pages (PyMuPDF) and image uploads, decodes QR payloads (OpenCV), extracts `qr_token` values, and matches each to assignment print pages. One scanned PDF with three student QR codes now creates three matched submissions in a single batch.
- **Student dedupe:** Multiple pages for the same student in one scan collapse to one submission with merged `page_range`.
- **Per-student file extraction (2026-06-10):** Multi-student PDF scans are split in `submission_pdf_split.py` — each matched submission stores its own extracted PDF (only that student&apos;s pages) so Review shows individual work, not the full batch file.
- **Always extract on upload:** Batch uploads now always persist `{batch}-student-{NNN}.pdf` extracts (no longer reuse the master batch file key).
- **Auto-repair on review:** Opening a submission re-splits from the batch master PDF when the row still points at the combined upload and QR pages match the assignment.
- **Upload 500 / false CORS fix (2026-06-10):** QR JSON payloads can reference stale `packet_id` values after packet regeneration. `resolve_qr_match_from_content()` now validates against `teacher_assist_v2_assignment_print_packets` and falls back to the assignment&apos;s current packet instead of inserting a nonexistent FK.
- **Filename fallback preserved:** QR tokens embedded in filenames still match when file-content decode finds nothing.
- **Upload UX:** Assignment viewer clarifies that optional **Student #** forces every selected file to that student; leave blank for QR auto-match from scan content.

### Explicit non-goals preserved

- No handwritten OCR or answer extraction from scanned work.

### Backend files changed

- `qr_decoding.py`, `qr_matching.py`, `submission_intake.py`, `submission_pdf_split.py`, `grade_reviews.py`, `pyproject.toml` (`pymupdf`, `opencv-python-headless`, `numpy`)

### Frontend files changed

- `teacher-assist-v2-assignment-viewer-screen.tsx`

### Tests added / run

- `tests/test_teacher_assist_v2_qr_decoding.py` — JSON payload parse, PNG multi-QR extract, PDF 3-student extract (3 passed).

### Manual validation checklist

1. Download **Student Assignment DOCX** (or print QR packet) for an assignment with 3+ students.
2. Scan/print completed work so each student's pages include their QR code (top-left).
3. Combine into one PDF scan (or upload a multi-page image).
4. On assignment viewer → **Upload student work**, leave **Student #** blank, upload the scan.
5. Confirm submissions list shows one row per detected student (e.g. Students #1, #2, #3) with match method **QR**.

### Remaining gaps

- Low-quality scans, skewed pages, or damaged QR codes may still fall back to **NEEDS_REVIEW** (manual match).
- Handwritten OCR and auto-grading remain deferred.

### Fix (2026-06-10): submission review viewer 500 / false CORS

- **Root cause:** Opening the submission viewer fires duplicate GET requests (React Strict Mode). `record_submission_review_view()` used check-then-insert and the second concurrent request hit unique constraint `(teacher_user_id, student_submission_id)` → HTTP 500. Browsers report this as a CORS error because error responses omit CORS headers.
- **Fix:** Upsert with PostgreSQL `ON CONFLICT DO NOTHING` in `grade_reviews.py`.
- **Test:** `test_v2_submission_review_view_is_idempotent` — two consecutive GETs both return 200.

### Next recommended phase

Teacher review UX polish (bulk confirm, keyboard prev/next), Google Form import aligned to same review queue, and stronger scan-quality diagnostics.

## Assignment upload → auto-grade → review workflow redesign (2026-06-10)

### What was implemented

- **New submission statuses:** `PROCESSING` → `READY_FOR_REVIEW` → `CONFIRMED` (per student); roster placeholders use `NOT_UPLOADED`; teacher may mark `INCOMPLETE`. Assignment moves to `COMPLETED` when every roster student is `CONFIRMED` or `INCOMPLETE`.
- **Upload pipeline:** QR batch uploads reject wrong-assignment QR payloads, split into per-student files, create `NOT_UPLOADED` placeholders for missing roster students, and **auto-run AI grading** before teacher review.
- **Gradebook timing:** Gradebook sync still occurs only on teacher **confirm** (accept/modify official grade) — not at auto-grade time.
- **Review APIs:** `GET .../review-queue`, `POST .../incomplete`, `POST .../supplement-upload` for missing-student uploads during review.
- **Review UI:** New `/teacher-assist-v2/assignments/review` screen with embedded PDF/image viewer, previous/next navigation, grade adjust + confirm, mark incomplete, and supplemental upload for missing work.
- **Inline preview (2026-06-10):** Preview URLs now use `Content-Disposition: inline` (`get_teacher_assist_preview_url`, `?inline=1` on local-download). Review screens load PDFs via in-browser blob iframe (`embedded-student-work-viewer.tsx`) so files no longer launch externally.
- **Assignment viewer:** Simplified upload copy; primary CTA **Open review** instead of manual match / ready-for-grading / generate-grade steps.

### Explicit non-goals preserved

- Handwritten OCR answer extraction still deferred.
- Google Form API import still uses student-number matching (no QR); status aligned to `READY_FOR_REVIEW`.

### Backend files changed

- `submission_intake_constants.py`, `submission_intake.py`, `submission_workflow.py`, `qr_decoding.py`, `grading_drafts.py`, `grade_reviews.py`, `google_form_quizzes.py`, `api/v1/teacher_assist_v2.py`

### Frontend files changed

- `teacher-assist-v2-assignment-review-screen.tsx`, `teacher-assist-v2-assignment-viewer-screen.tsx`, `teacher-assist-v2-api.ts`, `teacher-assist-v2-types.ts`, `app/teacher-assist-v2/assignments/review/page.tsx`

### Tests added / run

- Updated `tests/test_teacher_assist_v2_submission_intake.py` for reject-on-unmatched batch and new statuses (full suite blocked by unrelated pacing seed `period_day_id` fixture issue in local env).

### Manual validation checklist

1. Upload multi-student QR packet on the **matching** assignment (leave Student # blank).
2. Confirm one row per uploaded student plus `NOT_UPLOADED` placeholders for missing roster students.
3. Confirm uploaded rows reach **READY_FOR_REVIEW** with AI draft after processing.
4. Open **Open review** → embedded viewer, prev/next, confirm grade.
5. Upload wrong-assignment QR scan → upload rejected with clear error.
6. For a missing student, upload supplemental file or mark incomplete during review.
7. After all students confirmed/incomplete → assignment **COMPLETED** and gradebook records present only for confirmed grades.

### Remaining gaps

- Legacy rows with `NEEDS_REVIEW` / `READY_FOR_GRADING` remain readable but are not part of the new upload path.
- Auto-grade failures leave submission in `PROCESSING` with error details only in batch `grading_result` (no dedicated teacher alert yet).
- Google Form import does not yet create roster placeholders or auto-enter the same review screen flow.

### Next recommended phase

Unify Google Form import into the review queue; add batch-level processing progress UI; migrate/archive legacy submission statuses.

## Pacing guide day linkage, clone materials, daily UI (2026-06-02)

### What was implemented

- **Day records:** New `pacing_guide_period_days` table with stable `day_id` per week day; daily topic, objective focus, materials, assessment, and notes stored on the row (not title/description hacks).
- **Day-level resources:** `pacing_guide_supporting_materials.period_day_id` FK; list/upload/link/note APIs accept `period_day_id`; validation requires topic on save and title + file/URL/note for day resources.
- **Clone copies materials:** `copy_pacing_guide(..., copy_materials=True)` used by v2 clone; copies guide/period/day metadata, reuses `storage_key` for files, sets `source_resource_id` and `source_pacing_guide_id`.
- **Planning context:** `get_pacing_guide_planning_context` and teacher `build_planning_review_context` return guide/week/day/objective resource groupings plus per-day topic, notes, materials, assessment, and attachments.
- **Builder UI:** Collapsible day cards expose all daily plan fields; compact day-level attachment panel after structure save; week panels use `week_level_only`.

### Backend files changed

- `083_pacing_guide_period_days.py`, `teacher_assist_pacing_guide_period_day.py`, `pacing_guide_period_days.py`, `pacing_guide_builder.py`, `supporting_materials.py`, `pacing_guides.py`, `pacing_guide_foundation.py`, `planning_workflow.py`, `teacher_assist_v2.py`, schemas, models

### Frontend files changed

- `teacher-assist-v2-pacing-guide-builder-screen.tsx`, `pacing-guide-supporting-materials-panel.tsx`, `teacher-assist-v2-pacing-guide-viewer-screen.tsx`, `teacher-assist-v2-api.ts`, `teacher-assist-v2-types.ts`

### Tests run

- `pytest tests/test_teacher_assist_v2_pacing_guide_builder.py` (4 passed, includes daily topic validation)
- Migration `083` applied locally

### Remaining gaps

- Manual browser walkthrough (Tuesday day resources + clone + teacher planning) not automated in this pass.
- District→teacher clone scope rules rely on existing guide assignment flows; clone API still targets `DISTRICT` type (teacher copy path unchanged from prior foundation).
- `archived_at` set on archive; `active=false` retained for backward compatibility.

### Next recommended phase

Teacher planning smoke test using Week 1 Tuesday day context; optional integration test for clone-with-materials.

## Root admin pacing guide builder (2026-06-09)

### What was implemented

- **Guided builder UI** at `/teacher-assist-v2/admin/pacing-guides/create` and `/edit` with steps: Scope → Objectives → Weekly/Daily Plan → Resources & Links → Review & Save.
- **Create/update API:** `POST /instructional/pacing-guides/builder`, `PUT /instructional/pacing-guides/{id}/builder` with validation for district/school year/grade/subject, catalog objectives only, week/day plans, school scope requires school.
- **Daily plans** stored in `pacing_guide_periods.metadata_json`; guide unit metadata in `pacing_guides.metadata_json` (migration `082`).
- **List enhancements:** scope, grade/subject names, objective count, resource count, status; Create / Edit / Duplicate / Archive actions.
- **Planning context:** `get_pacing_guide_planning_context` returns daily plans, guide metadata, guide-level materials.
- **Guide-level attachments:** supporting materials can attach at guide scope (no orphan linkage validation failure).
- **Resource types:** added `website`, `video`, `curriculum_reference` for reference links.

### Backend files changed

- `082_pacing_guide_builder_metadata.py`, `pacing_guide_builder.py`, `pacing_guides.py`, `supporting_materials.py`, `supporting_materials_constants.py`, `teacher_assist_v2.py`, `schemas/teacher_assist_v2.py`, `schemas/pacing_guide.py`, models for pacing guide + period metadata

### Frontend files changed

- `teacher-assist-v2-pacing-guide-builder-screen.tsx`, `teacher-assist-v2-pacing-guides-screen.tsx`, `teacher-assist-v2-pacing-guide-viewer-screen.tsx`, `pacing-guide-supporting-materials-panel.tsx`, create/edit pages, `teacher-assist-v2-api.ts`, `teacher-assist-v2-types.ts`

### Tests run

- `pytest tests/test_teacher_assist_v2_pacing_guide_builder.py` (week validation)

### Remaining gaps

- Day-level resource attachment uses week/objective/guide scope only (no separate day FK); day-specific links can use week materials with day noted in title/description.
- Clone does not copy supporting materials (existing foundation behavior).
- Full browser manual walkthrough not automated in this pass.

### Follow-up polish (teacher pacing guide visibility, 2026-06-09)

- Teachers filter available guides by district, grade, school year, and school scope (district-wide or matching school).
- **Pacing Guides** added to teacher nav; teachers can re-open setup to adopt newly published guides.
- Planning and Packages surfaces show active guide plus all available guides for the teacher's scope.
- **Fix (2026-06-09):** Re-saving pacing guide choices no longer 500s. `save_pacing_guide_setup()` upserts the existing `(user, school_year, subject)` assignment row instead of deactivating all rows and inserting duplicates (which violated `uq_teacher_assist_v2_pacing_assignment_user_year_subject`). Browser CORS errors on this POST were a symptom of the 500, not a missing CORS config.
- **Fix (2026-06-09):** Saving after a prior **Copy to my guide** assignment no longer fails with “Selected pacing guide is not available for this subject.” The UI was sending the teacher-copy guide id as `source_guide_id`; save now resolves district catalog ids (metadata, title match, or available-list lookup) and the setup form exposes `source_district_guide_id` for correct dropdown initialization.

### Backend files changed (re-save fix)

- `pacing_guide_setup.py` — upsert assignment on save; deactivate only prior school-year rows
- `tests/test_teacher_assist_v2_teacher_flow.py` — assert second save succeeds

### Tests run (re-save fix)

- Manual DB verification: `test2@teacher.com` can adopt **My ELA 2026-2027** without unique-constraint failure
- `pytest tests/test_teacher_assist_v2_teacher_flow.py` — re-save assertion added (full seed path blocked by unrelated `period_day_id` seed regression)

### Follow-up polish (same pass)

- Guide detail API now returns `platform_school_year_id`, `ownership_scope`, and unit/week metadata from `metadata_json` for reliable edit-mode load.
- Supporting materials list uses explicit `guide_level_only` query param so planning context and objective-level listings are not filtered incorrectly.
- Builder resources step shows guide-level and week-level attachment panels after structure save.

### Next recommended phase

Teacher pacing guide adoption smoke test with newly created district guides; optional day-level material metadata field.

## Offline quiz exports — replace Google Cloud integration (2026-06-02)

### What was implemented

- **Deferred Google Cloud UI:** Google OAuth, Connect Google, Create Google Form, Open Form, Open Responses, and admin/teacher Google settings nav links are hidden from TeacherAssist v2 MVP surfaces. Backend Google integration code and migration `081` remain in repo but are not exposed.
- **Quiz DOCX export:** Word quiz document with title, subject, grade, school year, learning objective, student number line, instructions, questions, choices, point values, and written-response space (`quiz_exports.py` — zip/XML DOCX, no new dependency).
- **Answer Key DOCX export:** Separate teacher document with question number, correct answer, explanation, objective mapping, and points.
- **Google Forms read-only JSON:** Downloadable future-ready structure (`formTitle`, `formDescription`, `studentNumberQuestion`, `questions`, `choices`, `correctAnswer`, `pointValue`, `objectiveMappings`, `teacherAssistAssignmentId`, `packageId`, catalog IDs). Not a published form; helper text in UI and JSON payload.
- **Quiz card actions:** Preview Quiz (student-facing, no answers), Download Quiz DOCX, Download Answer Key DOCX, Download Google Forms JSON, Print / Save as PDF, Preview Answer Key (teacher-only iframe).
- **Export context linkage:** `quiz_export_context.py` attaches assignment_id, package_id, teacher_id, school_year_id, district_id, school_id, grade_id, subject_id, objective_ids to all quiz exports.
- **Print-friendly HTML:** Student quiz preview retains print CSS, page breaks, hidden nav on print.

### Explicit non-goals preserved

- No Google OAuth, Google Cloud setup, Google Forms API, or Google Classroom API in MVP UI.
- No server-side PDF generation (browser print only).
- Google backend routes/models kept but not surfaced.

### Backend files changed

- `quiz_exports.py`, `quiz_export_context.py`, `package_export.py`, `artifact_persistence.py`, `package_artifact_refresh.py`, `planning_workflow.py`, `package_dashboard.py`, `instructional_package_generation.py`, `package_demo_backfill.py`

### Frontend files changed

- `teacher-assist-v2-quiz-artifact-card.tsx` — offline-only quiz actions + helper copy
- `teacher-assist-v2-package-viewer-screen.tsx` — removed Google props from quiz card
- `teacher-assist-v2-assignment-viewer-screen.tsx` — removed Google Form quiz section
- `teacher-assist-v2-nav.ts` — removed admin Google Settings nav link

### Tests run

- `python3 -m compileall` on modified v2 quiz export modules (pass).
- ELA Week 1 demo backfill script for package `61e86258-fa86-498d-8a23-802740f8a068`.

### Manual validation checklist

1. Login as `test2@teacher.com` → Packages → **ELA Week 1: Main Idea** → Assessment → Quiz.
2. Confirm no Google OAuth / Connect Google / Create Google Form buttons.
3. Preview Quiz — answers hidden; student number line and point values visible.
4. Download Quiz DOCX and Answer Key DOCX — open in Word; confirm layout.
5. Download Google Forms JSON — confirm title, questions, choices, answers, points, objective mappings, package/assignment IDs.
6. Print / Save as PDF — clean printable layout without app chrome.
7. Preview Answer Key — teacher-only; separate from student preview.

### Remaining gaps

- Google Forms API / Classroom integration deferred; JSON is manual/future import only.
- Google settings pages exist at direct URLs but are not linked in nav.
- Native PDF/PPTX server export still not implemented.
- QR scan/OCR extraction not implemented.

### Next recommended phase

Optional guarded Google Forms API integration after teachers validate offline export quality; or wire real AI generation for v2 packages.

## Google Forms quiz integration (deferred — backend only, 2026-06-02)

Backend scaffolding exists but is **not exposed in MVP UI**:

- Migration `081`, `google_oauth.py`, `google_forms_client.py`, `google_form_quizzes.py`, admin/teacher Google settings pages at direct URLs only.
- See `docs/teacher-assist/GOOGLE_FORMS_SETUP.md` for future enablement.

### Remaining limitations

- Google Classroom assignment publishing not implemented.
- MVP uses offline DOCX + read-only JSON exports instead.

## Phase 36 — Teacher Time Savings Engine

### Schema changes / migration

- Migration `063_teacher_assist_time_savings_foundation`:
  - `pacing_guides`: `ownership_type`, `visibility_scope`, `planning_group_id`
  - `teacher_assist_planning_groups`, `teacher_assist_planning_group_members`
  - `teacher_assist_week_templates`
  - `teacher_assist_reuse_events`

### Services

- `InstructionalAssetReuseService` + `ReuseScore` (subject/grade/objective/resource/week similarity → 0–100)
- `week_duplication.duplicate_week`, `generate_next_week.generate_next_week_draft`
- `week_templates` (save/list/apply), `rollover_v2.rollover_school_year_v2`
- `recommendation_service.build_week_recommendations`
- `teacher_efficiency` dashboard + home time-savings summary
- `planning_groups` CRUD/join, `reuse_events.record_reuse_event`

### APIs (`/v1/teacher-assist/*`)

- `GET /reuse/search`, `POST /pacing-guide-periods/{id}/duplicate`, `POST /pacing-guide-periods/{id}/generate-next-week`
- `GET /pacing-guide-periods/{id}/recommendations`
- `GET|POST /week-templates`, `POST /pacing-guide-periods/{id}/templates`, `POST /week-templates/{id}/apply`
- `POST /rollover/v2`, `GET|POST /planning-groups`, `POST /planning-groups/{id}/join`
- `GET /efficiency-dashboard`
- Home workspace extended with `continue_planning`, `recommended_reuse`, `time_savings`, `efficiency_summary`

### UI

- Home: Continue Planning, Generate Next Week, Recommended Reuse, Recent Templates, Time Saved This Year
- Week workspace: duplicate week, generate next week, save template actions; Recommendations tab
- Template library: `/teacher-assist/planning/templates` with filters, preview, apply
- Nav: Template Library under Planning

### Seed data

- `python -m oziebot_api.scripts.seed_time_savings` — 2025–2026 / 2026–2027 school years, 5th Grade Math Team planning group, shared team pacing guide, sample templates, sample reuse events

### Tests

- `backend/services/api/tests/test_time_savings_foundation.py`

### Limitations

- No real AI generation for next-week drafts (rules-based suggestions only)
- Rollover v2 API only; no dedicated teacher UI yet
- Team pacing guide ownership fields exist but guide create/update UI not wired
- Save-as-template currently week-scoped; assignment/quiz/rubric/newsletter template save deferred
- No collaborative editing, marketplace, district analytics, or automatic publishing

### Estimated teacher time-saving impact

Initial assumptions (configurable constants): lesson reused 30 min, assignment 20 min, rubric 10 min, quiz 15 min, week duplicate 45 min, template apply 25 min. Reuse events accumulate into home/efficiency dashboards as estimated hours saved and reuse rate.

### Recommended next phase

Phase 37 — week-centric instructional workspace (completed).

## Phase 37 — Week-Centric Instructional Workspace

### Schema changes / migration

- Migration `064_teacher_assist_instructional_week_foundation`:
  - `instructional_weeks`, `instructional_week_objectives`, `instructional_week_snapshots`
  - Nullable `instructional_week_id` on `weekly_plans`, `assignments`, `teacher_assist_newsletters`, `teacher_assist_generated_artifacts`

### Services

- `instructional_weeks` — create from pacing period, auto-create DRAFT on active guide selection, objectives CRUD, preview, artifact linking
- `instructional_week_workspace` — tabbed workspace, health indicators, action center, timeline, structured mastery tab data
- `instructional_week_snapshots` — freeze week workspace JSON
- `instructional_week_reuse` — generate next instructional week, prior-year reuse
- `generated_artifacts.register_generated_artifact` — auto-sets `instructional_week_id` when week exists
- `pacing_guide_workspace` — exposes current/upcoming instructional week navigation
- `home_workspace` — upcoming instructional week links, recently used resources from current week context

### APIs

- `GET/POST /instructional-weeks`, `GET /instructional-weeks/by-period/{period_id}`
- `GET /instructional-weeks/{id}/workspace`
- `GET /pacing-guide-periods/{id}/instructional-week-preview`
- `POST /pacing-guide-periods/{id}/instructional-weeks`
- `POST /instructional-weeks/{id}/generate-next-week`, `/reuse`, `/snapshots`

### UI

- Primary workspace: `/teacher-assist/week/[id]` with 9 tabs (Mastery tab shows coverage summary, objectives, assessments)
- Home: instructional week routing, upcoming week link, recently used resources card
- Pacing guide workspace: open/create instructional week for current and upcoming periods
- Legacy pacing week tools remain at `/teacher-assist/planning/weeks`

### Compatibility strategy

- Existing APIs and workspaces unchanged; artifacts linked via nullable FK + pacing period fallback queries
- Active pacing guide selection auto-creates a DRAFT instructional week for the resolved current period

### Seed

- `python3 -m oziebot_api.scripts.seed_instructional_weeks` — weeks 1–3 with linked generated artifacts

### Tests

- `backend/services/api/tests/test_instructional_week_foundation.py` — create/workspace/snapshot/next week, auto-create on active selection, artifact linking

### Recommended next phase

Phase 38 — assignment → gradebook → mastery → reteach loop (completed).

## Phase 38 — Instructional Feedback Loop

### Schema changes / migration

- Migration `065_teacher_assist_instructional_loop_foundation`:
  - `teacher_assist_instructional_evidence`
  - `teacher_assist_student_support_groups`, `teacher_assist_student_support_group_members`
  - `teacher_assist_instructional_reflections`
  - `teacher_assist_instructional_week_closures`, `teacher_assist_instructional_week_summaries`
  - `teacher_assist_reteach_effectiveness_records`
  - Reteach plan v2 fields: `instructional_week_id`, `objective_id`, `reason`, `expected_outcome`

### Services

- `ObjectivePerformanceService` — transparent objective performance (students assessed, mastery %, trend)
- `instructional_evidence` — teacher-confirmed evidence CRUD
- `assignment_coverage` — assignment objective coverage view
- `mastery_dashboard_v2` — objective health, support signals, strongest/weakest objectives
- `student_support_groups` — teacher-reviewed reteach groupings
- `reteach_workspace` — objectives, groups, plans, effectiveness history
- `instructional_reflections`, `instructional_week_closure`, week summary generation
- `recommendation_v2`, `instructional_health_report`, `gradebook_v2`, `reteach_effectiveness`

### APIs (`/v1/teacher-assist/*`)

- `GET /mastery-dashboard/v2`, `/objective-performance`, `/assignment-coverage`, `/gradebook/v2`
- `GET|POST /instructional-evidence`, `POST /instructional-evidence/{id}/confirm`
- `GET /reteach-workspace`, `GET|POST /support-groups`, `PATCH /support-groups/{id}/status`
- `GET|PUT /instructional-reflections`
- `GET|PATCH /instructional-weeks/{id}/closure`, `POST /instructional-weeks/{id}/summary`
- `GET /instructional-loop/recommendations`, `/instructional-health-report`
- `GET|POST /reteach-plans/{id}/effectiveness`

### UI

- `/teacher-assist/reteach` — reteach workspace
- Home — instructional health, loop recommendations
- Instructional week workspace — mastery results, reteach needs, closure checklist in tabs

### Seed & tests

- `python3 -m oziebot_api.scripts.seed_instructional_loop`
- `backend/services/api/tests/test_instructional_loop_foundation.py`

### Recommended next phase

Phase 40 — LMS/SIS import adapters, parent communication from week summaries, district instructional analytics.

## Phase 39 — Teacher Copilot

### Schema changes / migration

- Migration `066_teacher_assist_copilot_foundation`:
  - `teacher_copilot_sessions` — tenant, teacher, title, timestamps
  - `teacher_copilot_messages` — session, role (teacher/assistant/system), content, `context_snapshot` JSON audit payload, optional `ai_usage_event_id`

### Services

- `teacher_context_engine.py` — `TeacherContextEngine` builds context packets: current week, pacing guide, objectives, mastery, reteach, assessments, resources, reflections, recommendations, school year
- `teacher_copilot_intents.py` — intent routing and explainable handlers (objective analysis, student support, small group builder, week/grading period summarizer, resource recommender, lesson gap analysis, reteach assistant, reflection assistant, admin copilot)
- `teacher_copilot_service.py` — session/message CRUD, mock provider orchestration, daily cost limit check, audit in `context_snapshot`

### APIs (`/v1/teacher-assist/copilot/*`)

- `GET /suggested-questions`, `GET /context`
- `GET|POST /sessions`, `GET|POST /sessions/{id}/messages`
- `POST /admin/query` (root admin only)
- Home workspace payload includes `copilot` card data (suggested questions, objectives/students needing attention, suggested actions)

### Provider architecture

- Default: mock/rule-based analysis from context packets (no LLM)
- `provider_mode=real` is guarded and rejected unless real provider is explicitly enabled; circuit breaker and model allowlist respected when enabled in future phases
- Usage tracked via `TeacherAssistAIUsageEvent` with `feature=teacher_copilot`

### UI

- `/teacher-assist/copilot` — conversation, suggested questions, context indicators, evidence/recommendations panel
- Home — Ask Teacher Copilot card, suggested actions, weekly summary deep link
- Primary nav — Copilot link

### Audit design

- Teacher message stores context packet keys used
- Assistant message stores full `analysis`, context packets, and audit block (prompt, intent, provider, timestamp)
- Linked AI usage events for cost/debug analytics

### Seed & tests

- `python3 -m oziebot_api.scripts.seed_teacher_copilot`
- `backend/services/api/tests/test_teacher_copilot_foundation.py`

### Limitations

- Mock provider only for conversational responses; no rich LLM prose
- Small group drafts are suggestions — teacher must confirm before creating support groups
- Admin copilot uses catalog context snapshot; full district-wide scans require catalog admin APIs
- No autonomous actions (grades, mastery commits, publishing, communications)

### Recommended next phase

Phase 42 — Mastery v2 / gradebook v2 UI, parent communication send, or LMS/SIS import.

## Phase 41 — Pilot Readiness & Production Hardening

### Schema / migration

- Migration `067_teacher_assist_pilot_readiness_foundation`:
  - `teacher_assist_pilot_feedback` — category, severity, feature_area, description, requested_improvement, status
  - `teacher_assist_usage_metrics` — daily metric_key aggregates per tenant/user

### Services

- `product_completion_review.py` — structured feature inventory (implemented/partial/deferred)
- `pilot_feedback.py` — teacher feedback CRUD
- `usage_metrics.py` — daily counters + snapshot from DB counts
- `system_health_dashboard.py` — root admin ops summary
- `pilot_seed_validation.py` — Texas/LISD/Mason seed checks

### APIs (`/v1/teacher-assist/pilot/*`)

- `GET /completion-review`, `GET /seed-validation`, `GET /usage-metrics`, `POST /usage-metrics/login`
- `GET|POST /feedback`, `PATCH /feedback/{id}`
- `GET /system-health` (root admin)

### UI

- `/teacher-assist/feedback` — pilot feedback workspace
- `/teacher-assist/administration/system-health` — root admin health dashboard
- Navigation audit — teacher workflow primary links
- `TeacherAssistDashboardHeader` — shared dashboard chrome

### Documentation

- `FEATURE_INVENTORY.md`, `DEPLOYMENT_GUIDE.md`, `PRODUCTION_CHECKLIST.md`, `PILOT_READINESS.md`

### Tests

- `backend/services/api/tests/test_teacher_assist_pilot_foundation.py`

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
  - creates Ozie Ighedosa (`dvaten.1992@gmail.com`) with the same TeacherAssist-only bootstrap flow when missing
  - sets Ozie's default product to `teacher_assist`
- Existing trading access is preserved:
  - no trading product access is removed
  - no trading entitlements are removed
- If Awele or Ozie is created without an explicit password environment variable, the script stores a generated temporary password hash without printing a password value.

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
  - `TEACHER_ASSIST_OZIE_EMAIL`
  - `TEACHER_ASSIST_OZIE_FULL_NAME`
  - `TEACHER_ASSIST_OZIE_TENANT_NAME`
  - `TEACHER_ASSIST_OZIE_PASSWORD`

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
