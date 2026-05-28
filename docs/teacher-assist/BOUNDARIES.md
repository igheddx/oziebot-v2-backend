# TeacherAssist AI Boundaries

## Purpose

This document defines the non-negotiable boundaries for adding **TeacherAssist AI** to the Oziebot platform. These boundaries exist to keep TeacherAssist modular, protect existing trading behavior, and prevent long-term architectural coupling.

## Product boundary

1. **TeacherAssist must remain a separate product module.**
   - It is not a trading strategy.
   - It is not a trading feature flag.
   - It is not an extension of Coinbase workflows.

2. **TeacherAssist must reuse only platform-level foundations.**
   - Shared auth, user identity, tenant membership, Postgres access, deployment, and generic utility layers may be reused.
   - Reuse must happen through clean, platform-level abstractions rather than direct dependence on trading modules.

## Trading isolation boundary

TeacherAssist must **not** import, depend on, or modify trading-specific services or engines.

This includes:

- `backend/services/strategy-engine`
- `backend/services/risk-engine`
- `backend/services/execution-engine`
- `backend/services/alerts-worker`
- `backend/services/market-data-ingestor`
- Coinbase execution or Coinbase account integration services
- trading strategy evaluation logic
- trading allocation logic
- trading performance logic

TeacherAssist work must not be implemented by piggybacking on any of the services above.

## Entitlement and access boundary

1. **TeacherAssist must not reuse trading strategy entitlements.**
   - `tenant_entitlements` are trading-strategy-centric.
   - TeacherAssist access must use a platform-level product access model.

2. **TeacherAssist must not be represented as a trading strategy.**
   - Do not add TeacherAssist as a `platform_strategy`.
   - Do not encode TeacherAssist access through strategy subscription semantics.

## Frontend boundary

1. **TeacherAssist must have its own frontend route subtree.**
   - Recommended home: `frontend/apps/web/app/teacher-assist/`

2. **TeacherAssist must have its own UI shell and theme.**
   - TeacherAssist should use a dedicated light-mode shell.
   - Trading pages must keep the current dark, trading-oriented shell.

3. **TeacherAssist must not contaminate the trading UI.**
   - Do not globally replace the trading theme.
   - Do not restructure the entire frontend to accommodate TeacherAssist if route-scoped isolation is sufficient.

## Backend boundary

1. **TeacherAssist must have its own backend namespace.**
   - Recommended API namespace: `api/v1/teacher_assist.py`
   - Recommended supporting packages:
     - `schemas/teacher_assist/`
     - `services/teacher_assist/`
     - `models/teacher_assist_*.py`

2. **TeacherAssist background work must be isolated from trading workers.**
   - Use the existing platform outbox/worker pattern.
   - Do not place TeacherAssist jobs onto trading worker queues.

## Shared utility boundary

1. **Any shared utilities must be generic platform utilities.**
   - Shared code may cover auth, storage, workflow orchestration, validation helpers, or generic UI primitives.
   - Shared code must not assume trading concepts, strategies, positions, Coinbase accounts, or market data.

2. **Do not promote trading-specific helpers into pseudo-shared dependencies.**
   - If a helper is trading-shaped, TeacherAssist should not depend on it.
   - If reuse is needed, extract a genuinely generic utility first.

## Data and privacy boundary

1. **TeacherAssist must keep its own schema areas and workflow records.**
   - Do not blend TeacherAssist state into trading domain tables.

2. **TeacherAssist must not store student PII.**
   - No student names
   - No parent names
   - No district student IDs
   - `STUDENT #` is the only allowed classroom student identifier inside the product

## Working rule

If a proposed implementation path requires changing trading engines, reusing trading entitlements, coupling to Coinbase services, or globally rewriting the trading UI, that path violates TeacherAssist boundaries and must be rejected in favor of a more isolated design.
