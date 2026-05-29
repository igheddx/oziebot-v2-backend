# TeacherAssist Feature Inventory

Generated as part of **Phase 41 — Pilot Readiness**. API mirror: `GET /v1/teacher-assist/pilot/completion-review`.

## Summary

| Status | Count | Meaning |
|--------|------:|---------|
| Implemented | 18 | Production-usable for pilot |
| Partial | 7 | Usable with documented gaps |
| Deferred | 3 | Intentionally out of scope |
| Deprecated | 1 | Legacy compatibility only |
| Blocked | 0 | None |

**Pilot-ready feature areas:** 22 of 25 tracked domains.

## Feature Areas

| Feature | Status | Pilot Ready | Routes / API | Notes |
|---------|--------|:-----------:|--------------|-------|
| Education Catalog | Implemented | Yes | `/teacher-assist/catalog` | Read-only teacher browse |
| State / District / School | Implemented | Yes | `/v1/teacher-assist/catalog/*` | Hierarchy via catalog access |
| Curriculum Resources | Implemented | Yes | `/teacher-assist/resources` | Upload + library |
| Objectives | Implemented | Yes | Catalog, Mastery | TEKS linkage via pacing |
| Pacing Guides | Implemented | Yes | `/teacher-assist/pacing-guides` | District + teacher guides |
| Instructional Weeks | Implemented | Yes | `/teacher-assist/week/[id]` | Primary execution layer |
| Assignments | Implemented | Yes | `/teacher-assist/assignments` | Full lifecycle |
| Assessments | Partial | Yes | Assignments, Student Work | No dedicated assessments hub |
| Gradebook | Implemented | Yes | `/teacher-assist/gradebook` | Teacher-confirmed commits |
| Mastery | Partial | Yes | `/teacher-assist/mastery` | v2 API richer than UI |
| Reteach | Implemented | Yes | `/teacher-assist/reteach` | Workspace + plans |
| Teacher Copilot | Implemented | Yes | `/teacher-assist/copilot` | Mock-first, no auto-actions |
| Templates | Implemented | Yes | `/teacher-assist/planning/templates` | Week template library |
| Reuse Engine | Implemented | Yes | Home, Week workspace | ReuseScore + events |
| Communication Hub | Partial | Yes | `/teacher-assist/newsletters` | Draft-only, no send |
| Newsletters | Implemented | Yes | `/teacher-assist/newsletters` | HTML/PDF/DOCX export |
| Administration | Partial | Yes | Settings, Catalog Admin | Root-admin catalog mgmt |
| Authentication | Implemented | Yes | `/v1/auth/*` | JWT + refresh |
| Authorization | Implemented | Yes | Tenant-scoped | Root admin for catalog admin |
| Object Storage | Implemented | Yes | Storage APIs | Local + private S3 |
| Exports | Partial | Yes | `/teacher-assist/exports` | Newsletter + print HTML |
| Background Jobs | Partial | Yes | Worker service | Env-dependent deployment |
| LMS / SIS Integration | Deferred | No | — | Post-pilot |
| Parent Portal | Deferred | No | — | Post-pilot |
| District Analytics | Deferred | No | — | Post-pilot |
| Legacy Pacing Items | Deprecated | No | `/legacy/pacing-guides` | Backward compatibility |

## Workflow Gaps

1. Parent communication is draft-only — no outbound email/SMS.
2. Mastery v2 and gradebook v2 UIs lag behind API capabilities.
3. Dedicated assessments hub not implemented; assignments cover most teacher flows.
4. LMS/SIS roster import deferred.
5. Real-provider AI disabled by default (mock-first policy).

## Navigation (Phase 41)

Primary: **Home → Instructional Weeks → Pacing Guides → Assignments → Mastery → Resources → Communication → Copilot**

Secondary (Operations / Insights groups): Work Queue, Current Week, Catalog, Reteach, Reviews, Admin.

Legacy routes preserved: `/today`, `/workspace`, `/actions`, `/planning/weeks`.

## Pilot Feedback

Teachers submit feedback at `/teacher-assist/feedback` (category, severity, feature area, description, status workflow).

## Related Docs

- [PILOT_READINESS.md](./PILOT_READINESS.md)
- [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
- [PRODUCTION_CHECKLIST.md](./PRODUCTION_CHECKLIST.md)
- [KNOWN_LIMITATIONS.md](./KNOWN_LIMITATIONS.md)
