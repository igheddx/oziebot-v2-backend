# TeacherAssist AI Implementation Rules

## Purpose

These rules define how TeacherAssist should be built once implementation starts. They exist to keep delivery disciplined, phase-scoped, and safe for the existing Oziebot platform.

## Core build discipline

1. **Inventory before implementation.**
   - Read the current repo structure, affected modules, existing docs, and deployment constraints before changing code.

2. **Implement one phase at a time.**
   - Work should follow the phase order in `BUILD_PHASES.md` unless an explicit decision changes that order.

3. **Do not exceed phase scope.**
   - A phase should land only the functionality, schema, routes, UI, and docs needed for that phase.

4. **No hidden refactors.**
   - Do not slip in unrelated cleanup or broad architectural rewrites under a TeacherAssist task.

5. **No trading logic changes unless explicitly approved.**
   - TeacherAssist work must not alter trading behavior, Coinbase behavior, or trading worker responsibilities unless the user explicitly asks for that change.

## Documentation discipline

1. **Every phase must update `IMPLEMENTATION_SUMMARY.md`.**
   - The summary should stay current with what changed, what remains, and what the next recommended phase is.

2. **Every DB change must include migration notes.**
   - Notes should describe new tables, modified columns, backfill assumptions, and rollback concerns.

3. **Every backend route must include ownership and access-control notes.**
   - Document whether the route is tenant-scoped, user-scoped, admin-only, or product-access-gated.

4. **Every async workflow must include status and error behavior.**
   - Define lifecycle states, failure behavior, retry expectations, and what the UI should display.

## AI feature discipline

1. **Every AI feature must support mock mode before real OpenAI.**
   - The mock path should be usable for development and integration testing before real model traffic is enabled.

2. **Every AI output that affects grading must require teacher confirmation.**
   - AI may recommend scores, mastery, feedback, or evidence, but it must not commit final grading outcomes automatically.

3. **AI outputs must stay structured and reviewable.**
   - Favor validated JSON outputs and explicit review states over freeform persistence.

## UX and theme discipline

1. **Preserve TeacherAssist light-mode UI without contaminating Oziebot dark UI.**
   - TeacherAssist should use route-scoped theming and its own shell.
   - Trading pages should remain on the existing dark theme unless separately approved.

2. **Prefer workflow-first UI.**
   - TeacherAssist should optimize for planning, generation review, grading review, and insight workflows rather than mirroring trading navigation patterns.

## Boundary enforcement

1. **TeacherAssist must follow `BOUNDARIES.md`.**
   - If an implementation step conflicts with those boundaries, the step is out of scope until the architecture decision is changed explicitly.

2. **Shared code must be platform-generic.**
   - If reuse is needed, extract neutral utilities rather than coupling TeacherAssist to trading-shaped code.
