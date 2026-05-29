# TeacherAssist Pilot Readiness

Phase 41 checklist for real-world teacher pilots.

## Pilot Goal

Move TeacherAssist from **feature complete** to **teacher ready**: reliable, fast, simple, professional, and cohesive.

## Teacher Onboarding

- [ ] Teachers receive TeacherAssist product access (`seed_teacher_assist_access.py` or admin grant)
- [ ] Default landing: `/teacher-assist/home`
- [ ] Guided onboarding at `/teacher-assist/get-started` (10-step progress)
- [ ] Active pacing guide selected before week work begins

## Training (30-minute overview)

1. **Home** — priorities, copilot, instructional health
2. **Instructional Weeks** — primary workspace tabs
3. **Assignments & Student Work** — create, print, upload, review
4. **Mastery & Reteach** — teacher-confirmed evidence only
5. **Copilot** — ask grounded questions; review evidence; no auto-actions
6. **Feedback** — `/teacher-assist/feedback` for pilot issues

## Sample Data

Texas / LISD / Mason Elementary / Grade 5 seed stack:

```bash
python3 -m oziebot_api.scripts.seed_education_catalog
python3 -m oziebot_api.scripts.seed_pacing_guides
python3 -m oziebot_api.scripts.seed_instructional_weeks
python3 -m oziebot_api.scripts.seed_instructional_loop
python3 -m oziebot_api.scripts.seed_teacher_copilot
```

Validate via root admin **System Health → Seed validation** or API.

## Feedback Collection

- Teachers: `/teacher-assist/feedback`
- Categories: bug, usability, feature_request, performance, data, documentation
- Root admin: review open items in system health dashboard

## Support Process

1. Teacher submits feedback with severity
2. Pilot coordinator triages within 1 business day
3. Critical bugs → hotfix path; usability → Phase 42 backlog
4. Status updates: open → reviewing → planned → resolved

## Known Limitations (Pilot Scope)

See [KNOWN_LIMITATIONS.md](./KNOWN_LIMITATIONS.md). Key pilot constraints:

- No outbound parent email/SMS
- No LMS/SIS roster sync
- Mock-first AI (copilot, planning, newsletters)
- Mastery/gradebook v2 UI partially deferred
- Single-tenant TeacherAssist membership resolution per user

## Success Metrics (Pilot)

| Metric | Target | Source |
|--------|--------|--------|
| Weekly active teachers | Track | Usage metrics / login activity |
| Instructional weeks created | ≥1 per teacher | Usage metrics |
| Assignments created | ≥2 per teacher | DB counts |
| Copilot questions | ≥3 per teacher | Copilot messages |
| Feedback response time | <24h critical | Pilot feedback status |
| Critical workflow failures | 0 unresolved | System health |

## Pilot Readiness Assessment

| Area | Status |
|------|--------|
| Core instructional workflow | Ready |
| Assessment → mastery loop | Ready (manual commits) |
| Copilot assistance | Ready (mock analysis) |
| Catalog & pacing | Ready |
| Communication | Draft-only |
| Integrations | Not ready (deferred) |
| Production ops | Ready with checklist |

## Recommended Next Steps After Pilot

1. **Phase 42** — Mastery v2 / gradebook v2 UI completion
2. **Phase 43** — Parent communication send integration (with approval)
3. **Phase 44** — LMS/SIS import adapters
4. **Phase 45** — Real-provider AI rollout with cost controls

## Related

- [FEATURE_INVENTORY.md](./FEATURE_INVENTORY.md)
- [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
- [PRODUCTION_CHECKLIST.md](./PRODUCTION_CHECKLIST.md)
