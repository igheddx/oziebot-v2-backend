# TeacherAssist AI Decisions and Assumptions

## Confirmed from current repo

1. **Frontend is not served from Lightsail today.**
   - Backend repo deploys to Lightsail lean Docker Compose.
   - Frontend repo deploys static assets to S3/CloudFront.
   - TeacherAssist UI should therefore plan for the existing frontend deployment path unless intentionally changed later.

2. **There is no separate profile table today.**
   - User profile data currently lives in `users` plus `tenant_memberships`.
   - “Reuse existing profile” should therefore mean reuse `users` and extend with TeacherAssist-specific profile/setup tables rather than inventing a second identity source.

3. **There is no current multi-product access model.**
   - No `user_products`, `default_app`, or product switcher exists.
   - Current access model is trading-centric and strategy-centric.

4. **TeacherAssist must not reuse trading entitlement semantics.**
   - `tenant_entitlements` are about trading strategies and Stripe strategy access.
   - Product/module access should be modeled separately.

5. **TeacherAssist should use namespaced tables and routes.**
   - Existing repo pattern already supports isolated modules such as strategic allocation and volatility harvest.
   - TeacherAssist should follow that isolation pattern at a larger product boundary.

6. **TeacherAssist async work should use worker/outbox patterns already present in the repo.**
   - Current platform has dedicated worker services and Postgres queue leasing.
   - This is safer than inventing a one-off async mechanism inside trading services.

## Architecture decisions

1. **TeacherAssist will be a separate product module inside the existing platform.**
   - It will share platform foundations but remain separate from trading product behavior and trading domain logic.

2. **Product access will use a platform-level product access model instead of trading entitlements.**
   - TeacherAssist access should be modeled separately from strategy access.

3. **TeacherAssist will reuse shared auth, user, and tenant infrastructure but not trading-specific services.**
   - Shared platform identity and tenancy stay reusable.
   - Trading-specific services remain outside TeacherAssist dependencies.

4. **TeacherAssist will use a separate frontend route subtree and light-mode shell.**
   - Route-scoped UI isolation is the preferred way to avoid contaminating the trading dark theme.

5. **Expensive generation work will use background workflow jobs.**
   - Long-running generation, extraction, export, and grading-assist flows should use persisted workflow jobs rather than synchronous request processing.

6. **OpenAI access will be backend-only through a TeacherAssist LLM service abstraction later.**
   - Frontend access to model providers is explicitly out of bounds.

7. **No student PII will be stored; `STUDENT #` is the only student identifier.**
   - Student and parent names, as well as district student IDs, must remain outside the product.

## Phase 1 implementation decisions

1. **Product access is tenant-level and user-visible access is aggregated through tenant memberships.**
   - Phase 1 models product access at the tenant boundary with user-specific default selection layered on top.

2. **`/v1/me` remains the authoritative frontend bootstrap contract for product routing.**
   - Product list and default product are exposed through the existing bootstrap response instead of adding a second independent bootstrap endpoint.

3. **Default app changes require explicit user action.**
   - App switching navigates between products.
   - The default product changes only when the user explicitly chooses `Set default`.

4. **TeacherAssist light mode uses scoped theme overrides rather than changing the root dark theme.**
   - The current root layout still hard-codes dark mode.
   - Phase 1 isolates TeacherAssist styling through a route-scoped wrapper and CSS variable overrides.

5. **Migration backfill favors preserving existing trading behavior over strict billing interpretation.**
   - Existing tenants are backfilled with trading product access so current trading login and dashboard behavior stay intact during the platform-product transition.

6. **TeacherAssist placeholder routes are allowed in Phase 1 because they establish shell and routing boundaries without introducing business workflows.**
   - Planning, uploads, LLM generation, grading, TEKS mastery, PPTX, QR, OCR, and newsletters remain out of scope.

## Phase 2 implementation decisions

1. **TeacherAssist setup data is split between user-scoped and tenant-scoped ownership.**
   - `teacher_profiles` is user-scoped.
   - school years, grading periods, subjects, classes, class-subject assignments, and standards are tenant-scoped.

2. **TeacherAssist tenant context resolves from product access rather than trading primary-tenant logic.**
   - Setup APIs use the first tenant membership with selectable TeacherAssist access instead of assuming the trading primary tenant is the right context.

3. **Anonymous classroom identity remains derived, not persisted.**
   - No student identity table was added.
   - `STUDENT #` ranges are derived dynamically from `student_count`.

4. **Phase 2 uses the user-requested educational table names even though the model files remain TeacherAssist-prefixed.**
   - The table layer uses `teacher_profiles`, `school_years`, `grading_periods`, `subjects`, `classes`, `class_subjects`, and `standards`.
   - The Python model files stay clearly TeacherAssist-scoped to preserve repo isolation.

5. **TeacherAssist setup UI is centralized in the settings workspace and guided from the dashboard checklist.**
   - The dashboard is now setup-first.
   - The settings page owns the CRUD surface for profile, school years, grading periods, classes, subjects, and standards.

6. **Validation is intentionally strict for dates, ownership, and anonymous roster counts before any planning workflows exist.**
   - This phase blocks overlapping grading periods, out-of-window grading periods, unsupported grade levels, invalid timezones, and non-positive student counts to keep later planning phases grounded on reliable setup data.

## Phase 3 implementation decisions

1. **Resources are independent reusable assets, not embedded blobs on pacing items or planning drafts.**
   - `resource_library_items` stores reusable metadata rows.
   - `pacing_item_resources` and `planning_input_draft_resources` handle linking.

2. **TeacherAssist uploads are metadata-first and storage-abstracted in this phase.**
   - Upload routes persist file metadata plus a storage key.
   - File writes go through `services/teacher_assist/storage.py` with local storage as the initial backend.

3. **No extraction, OCR, embeddings, or AI generation were added in Phase 3.**
   - Uploaded files are saved as references only.
   - Resource-library and planning flows stop at organization and draft preparation.

4. **Pacing guides are tenant-scoped structured timelines with creator tracking and future sharing support.**
   - `pacing_guides` keeps `created_by_user_id` plus `is_shared`.
   - This preserves future district/campus sharing options without implementing them yet.

5. **Planning drafts remain explicit pre-generation teacher artifacts.**
   - Teachers must save context deliberately before any generation phase exists.
   - `planning_input_drafts` are user-scoped within the TeacherAssist tenant context.

6. **Phase 3 keeps manual structure entry ahead of import automation.**
    - The foundation supports files, links, notes, standards, subjects, grading periods, and school years now.
    - Spreadsheet parsing/import automation can be layered later without replacing the normalized storage model.

## Phase 4 implementation decisions

1. **Planning drafts now use mapping tables for multi-subject and richer saved scope.**
   - `planning_input_draft_subjects`, `planning_input_draft_pacing_items`, and `planning_input_draft_standards` extend the Phase 3 draft model without embedding planning context directly into a single row.
   - The legacy `subject_id` column remains as a primary/backward-compatible convenience field while readiness and preview logic rely on the mapping-table set.

2. **Draft readiness validation is server-owned and blocks `ready` transitions.**
   - The backend, not the frontend, decides whether a draft is ready.
   - This keeps future workflow triggers safe even if multiple clients or batch tools update drafts later.

3. **Context preview is the future generation input contract.**
   - `GET /planning-drafts/{id}/context-preview` returns the saved instructional context, related entities, and readiness state in one structured payload.
   - Later mock-generation and real-generation phases should build from this preview contract instead of reassembling context ad hoc in multiple places.

4. **Generation remains visible in the UX but intentionally disabled in Phase 4.**
   - The weekly-planning workspace now shows direction toward “Generate Weekly Plan” without triggering any AI call or workflow job.
   - This preserves user understanding of the product path while honoring the no-generation boundary for this phase.

5. **Phase 4 remains metadata-and-structure only; no OCR, extraction, embeddings, or workflow jobs were introduced.**
   - Resource uploads stay metadata-first.
   - Planning refinement stops at explicit saved context, readiness review, and placeholder generation messaging.

## Phase 5 implementation decisions

1. **TeacherAssist workflows are persisted before real LLM integration exists.**
   - Phase 5 introduces stored workflow rows and step rows ahead of any provider calls.
   - This proves orchestration, status tracking, and artifact persistence independently of token spend.

2. **Generation always snapshots the saved context-preview contract before processing.**
   - `input_snapshot_json` captures the Phase 4 planning context at workflow start.
   - Later generation phases should use that saved snapshot rather than reading mutable draft state mid-run.

3. **Mock generation proves the end-to-end generation path before OpenAI usage.**
   - The current generator is deterministic and clearly labeled as mock output.
   - This keeps cost, drift, and prompt-quality questions out of the workflow-foundation phase.

4. **Weekly plans are stored as structured JSON artifacts.**
   - `weekly_plans.content_json` and `weekly_plans.source_context_json` persist machine-readable outputs and the source context used to build them.
   - This keeps later export/render/edit paths flexible without committing to PPTX, Slides, grading, or newsletter output yet.

5. **Generation remains teacher-triggered, not automatic.**
   - Ready drafts do not auto-start workflows.
   - Teachers explicitly initiate weekly-plan generation from the weekly-planning workspace.

6. **Phase 5 uses an isolated in-API background runner instead of borrowing trading workers.**
   - The repo already has worker/outbox precedent, but no dedicated TeacherAssist worker exists yet.
   - For mock generation, an isolated background-task runner bound to the current API DB engine keeps Phase 5 async-oriented without coupling TeacherAssist to trading worker infrastructure.

## Phase 6 implementation decisions

1. **Real provider integration remains disabled by default.**
   - Phase 6 adds provider-selection and usage-tracking seams without enabling any production model call.
   - The default provider remains `mock` until a future phase explicitly changes that boundary.

2. **The provider abstraction is backend-only.**
   - Provider choice, fixture behavior, and usage logging live in backend services only.
   - The frontend only consumes persisted artifacts and summary metadata.

3. **Weekly-plan edits always create versions.**
   - Saving teacher review changes creates a new `weekly_plan_versions` snapshot instead of overwriting prior output.
   - Future regeneration should follow the same preservation rule.

4. **Usage tracking begins before real OpenAI integration.**
   - Mock generation now records provider/model/cost scaffolding through `teacher_assist_ai_usage_events`.
   - This establishes cost/usage observability before billable providers exist.

5. **Weekly plans are teacher-review artifacts first.**
   - Generated plans now begin in `in_progress`.
   - Teachers explicitly mark them `completed` after review rather than generation silently finalizing them.

6. **Fixture support is optional and dev-only.**
   - Structured payload record/replay exists to help local validation and future provider rollout.
   - Production behavior does not depend on fixture files.

## Proposed decisions

## 1. Product-access model

Recommended:

- `platform_products`
- `tenant_product_access`
- `user_product_preferences`

Not recommended:

- overloading `tenant_entitlements`
- adding TeacherAssist rows to `platform_strategies`

## 2. TeacherAssist theme strategy

Recommended:

- route-scoped TeacherAssist shell
- local light-theme CSS variable overrides within TeacherAssist subtree

Avoid:

- rewriting the global trading dark theme
- introducing a heavy UI framework

## 3. TeacherAssist data naming

Recommended:

- prefix TeacherAssist tables and backend modules with `teacher_assist_...`

Reason:

- reduces accidental coupling
- simplifies migrations and future archival
- keeps grep/search clear

## 4. File handling

Recommended:

- original and generated files in S3-compatible object storage
- metadata and extracted text in Postgres
- backend-issued upload flow

Avoid:

- base64 blobs in Postgres
- storing generated files forever without retention rules

## 5. LLM integration

Recommended:

- backend-only provider abstraction
- mock provider first
- structured JSON outputs
- fixture capture for validated real runs

Avoid:

- frontend API key usage
- freeform unvalidated text writes directly into persistent mastery data

## 6. Workflow jobs

Recommended:

- dedicated TeacherAssist worker service
- persisted workflow rows with status history
- input snapshot at job start

Avoid:

- running large planning/export jobs synchronously in FastAPI request handlers
- piggybacking on trading workers

## Open questions before implementation

1. **Access/billing**
   - Is TeacherAssist billed per tenant, per teacher user, or bundled with trading for some customers?

2. **Data ownership**
   - Should school year, pacing guide, and resources be user-owned, tenant-owned, or mixed?
   - Current repo is tenant-aware but operationally close to one-user-per-tenant for trading.

3. **Deployment**
   - Should TeacherAssist stay in the existing frontend static export repo, or does the product need a different hosting model later because of upload-heavy workflows?

4. **Storage**
   - Which bucket/path should hold TeacherAssist uploads and expiring exports?
   - Reuse existing AWS account and add a new dedicated bucket/prefix, or use an S3-compatible alternative?

5. **Document extraction**
   - Is OCR part of MVP or phase 2?
   - Which formats must be extractable on day one versus just storable?

6. **Teacher model**
   - Is one teacher per tenant still a valid assumption for TeacherAssist MVP?
   - If not, setup and access design should become more organization-aware before build starts.

7. **Google ecosystem**
   - Is manual PPTX upload the only MVP requirement, or is Google auth/integration needed shortly after launch?

8. **Frontend runtime limits**
   - Because the frontend is static-exported, TeacherAssist should plan on backend APIs for uploads, workflow polling, and export download metadata rather than relying on Next server actions or custom frontend server runtime features.

## Non-negotiable protections

- do not modify trading strategy logic
- do not modify Coinbase execution paths for TeacherAssist
- do not commingle TeacherAssist workflow jobs with trading worker queues
- do not store student or parent names
- do not allow AI to commit final grades without teacher confirmation
