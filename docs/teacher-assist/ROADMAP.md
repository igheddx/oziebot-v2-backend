# TeacherAssist Roadmap

## Completed / Current Foundation

- Product access and app switcher
- TeacherAssist isolated route subtree and shell
- Teacher profile and academic setup
- School years
- Grading periods
- Subjects
- Classes
- Class-subject assignments
- Standards / TEKS
- Resource library
- Pacing guides
- Pacing items
- Planning input drafts
- Context preview
- Workflow execution
- Mock weekly-plan generation
- Weekly-plan artifact persistence
- Weekly-plan editing and versioning
- Mock provider seam
- AI usage scaffolding
- Instructional planning scope support
- Duration-aware planning context preview
- Flexible mock instructional-plan output
- Reusable-plan lineage foundation
- Software-only plan copy flow
- Guarded provider validation/config
- Shared instructional plan library foundation
- Owner-managed sharing/template controls
- Prior-year plan discovery
- Annual curriculum rollover copy foundation
- Plan-library and rollover UI routes
- Dedicated TeacherAssist worker foundation
- Workflow lease / retry / heartbeat metadata
- Guarded provider activation seams and workflow cost metadata
- Controlled real-provider instructional-plan execution
- Teacher-facing plan quality review metadata and review UI
- Section-level instructional-plan regeneration
- Plan-scoped regeneration usage metadata
- Teacher-driven targeted rewrite controls in the plan viewer
- Assignment model foundation
- Assignment workspace and lifecycle controls
- Software-only weekly-plan-to-assignment starter
- Printable packet foundation
- Student-work intake foundation
- Manual grading-review foundation
- Unified TeacherAssist workspace
- Append-only activity-event foundation
- Class-centric operational grouping and needs-attention surfacing
- Recent activity timeline and review-required queue
- Storage-provider abstraction with local and private S3 backends
- Backend-generated temporary download URLs for stored TeacherAssist files
- AWS bucket bootstrap and IAM guidance for TeacherAssist object storage
- Async extraction jobs and extracted-text preview foundation on top of private TeacherAssist storage
- Mock-first OCR provider seam and worker-managed artifact processing
- Extraction remediation workflows with retry lineage and teacher review drill-down
- Extraction review statuses, confidence metadata, issue flagging, and operational `/teacher-assist/extractions` workspace

## Current Direction

TeacherAssist now supports a flexible instructional-planning foundation plus reusable-plan discovery, annual rollover foundations, dedicated worker-managed workflow execution, controlled real-provider instructional-plan execution, teacher-owned assignment/review workflows, a unified operational workspace, private object-storage foundations, and extraction remediation/review tooling while preserving weekly compatibility and mock-first safety defaults.

TeacherAssist is evolving toward:
- reusable instructional planning
- collaborative/shared planning
- teacher personalization branching
- annual curriculum rollover
- reusable instructional asset libraries

## Near-Term Phases

1. Teacher personalization branching/versioning
2. Team/shared curriculum workspaces
3. Richer shared instructional plan template workflows
4. Reusable instructional asset library beyond plans
5. Cross-school-year artifact reuse beyond plan copies
6. Target grading-period remapping workflows
7. Workspace action drill-down and remediation flows
8. Real OCR provider evaluation and guarded rollout
9. Direct-browser upload and larger-file transfer workflows
10. Retention cleanup jobs for temporary artifacts and exports
11. Teacher review workspace beyond plan-level review metadata
12. Team/shared curriculum governance and approval flows
13. Grading assistance workflows
14. TEKS mastery matrix
15. Lesson effectiveness insights
16. Parent/newsletter communication assistant
17. Export features such as PPTX/Google Slides-compatible outputs

## Future Planning Model Direction

### Planning Model Evolution

TeacherAssist is evolving from:
- isolated weekly-plan generation

toward:
- instructional planning engine
- reusable curriculum assets
- multi-year instructional continuity
- teacher/team collaboration
- modular instructional sequencing
- incremental AI-assisted refinement

## Deferred / Not Yet

- real OCR providers
- handwriting extraction
- embeddings
- PPTX export
- Google Slides integration
- quiz generation
- grading automation
- mastery matrix
- newsletters
- PDF/export packaging for printable packets
- QR assignment ingestion
- Google Classroom integration
- SIS/gradebook integration
- public CDN delivery for TeacherAssist files
