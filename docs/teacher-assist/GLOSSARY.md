# TeacherAssist AI Glossary

## TeacherAssist AI

A planned Oziebot product module focused on teacher planning, instructional material generation, grading support, and classroom insights. It is separate from crypto trading functionality.

## Product module

A distinct product area within the same platform that has its own routes, UI shell, backend namespace, workflows, and domain data while reusing shared platform foundations like auth and tenancy.

## Product access

The platform-level authorization model that determines whether a tenant or user can use a product module such as trading or TeacherAssist.

## Default app

The product module a user lands in by default after login when the user has access to more than one product.

## Teacher profile

TeacherAssist-specific setup and preference data associated with a logged-in user, such as grade level, instructional preferences, supported subjects, and classroom configuration.

## School year

The top-level academic time container for TeacherAssist setup and planning, such as the 2026-2027 school year.

## Grading period

A configured academic segment inside a school year used for planning, assessment tracking, and mastery reporting.

## 9 weeks

A common grading period format used by many districts. TeacherAssist must treat this as one possible grading period type, not the only one.

## Subject

An instructional area taught by the teacher, such as math, reading, science, or social studies.

## Class

A teacher-defined instructional group for a subject and student roster count, such as 5th Grade Math Homeroom A.

## STUDENT #

The anonymous student identifier used inside TeacherAssist. It is a numeric classroom identifier like `1` through `23`, with the real-world identity mapping kept outside the platform.

## TEKS

Texas Essential Knowledge and Skills standards. In TeacherAssist, TEKS are standards that planning items, questions, assignments, and mastery tracking can map to.

## Pacing guide

A structured curriculum planning source that describes what should be taught over time, often organized by week and day, including standards, objectives, and focus areas.

## Pacing item

A specific entry within a pacing guide that represents a teachable unit of work, such as a weekly topic, daily lesson focus, or standards/objectives grouping.

## Resource library

A storage and reference layer for curriculum files, links, slide decks, worksheets, and supporting materials that can be mapped to pacing items and planning workflows.

## Weekly plan

A generated or edited instructional plan for a full teaching week, typically organized by subject and grounded in pacing data, standards, and teacher inputs.

## Daily teaching deck

A classroom-ready slide deck for a specific day that combines generated lesson content into a PPTX-first format suitable for later Google Slides upload.

## Lesson artifact

A generated teaching output such as a lesson plan, slide deck, quiz, assignment, guided notes set, or differentiation material.

## Assessment

A standards-aligned evaluation item, such as a quiz, checkpoint, or rubric-backed activity, used to measure student understanding.

## Assignment

A classroom work item given to students, including printable work, quizzes, or written responses, often mapped to standards and later reviewed for grading support.

## Mastery matrix

A structured standards-tracking view where rows represent `STUDENT #` values and columns represent TEKS standards and related assessment checkpoints.

## Mastery / Developing / Beginning

The allowed TeacherAssist mastery values:

- **Mastery**: student demonstrates strong understanding
- **Developing**: student shows partial or emerging understanding
- **Beginning**: student shows limited understanding and likely needs more support

## Workflow run

A persisted background job execution for an expensive TeacherAssist process such as plan generation, deck export, extraction, or grading assistance. Expected statuses include queued, running, completed, failed, and cancelled.

## Lesson effectiveness

A derived signal showing how well a lesson appears to have worked based on downstream mastery and assessment outcomes.

## Reteach recommendation

A suggested follow-up action indicating that a concept, standard, or lesson likely needs additional instruction based on low or mixed mastery outcomes.

## Newsletter

A teacher-editable family communication draft generated from weekly class activity, plans, and teacher notes without storing parent or student names.

## No PII

A TeacherAssist privacy rule meaning the system must not store personally identifiable student or parent information, including names and district student IDs.
