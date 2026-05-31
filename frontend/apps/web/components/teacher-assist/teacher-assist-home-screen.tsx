"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { useTeacherAssistOnboarding } from "@/components/teacher-assist/teacher-assist-onboarding-context";
import { useAuth } from "@/components/providers/auth-provider";
import { fetchTeacherAssistHomeWorkspace } from "@/lib/teacher-assist-api";
import { recordPilotLoginMetric } from "@/lib/pilot-api";
import type {
  TeacherAssistHomePriorityItem,
  TeacherAssistHomeWorkspace,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function greetingName(fullName: string | null | undefined) {
  if (!fullName?.trim()) return "there";
  return fullName.trim().split(/\s+/)[0];
}

function PriorityCard({ item, disabled }: { item: TeacherAssistHomePriorityItem; disabled?: boolean }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white px-3 py-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="text-sm font-semibold text-slate-900">{item.title}</p>
          <p className="mt-0.5 text-sm text-slate-600">{item.description}</p>
        </div>
        {disabled ? (
          <span className="ta-button-secondary shrink-0 cursor-not-allowed text-xs opacity-45">Complete setup first</span>
        ) : (
          <Link href={item.navigation.href} className="ta-button-secondary shrink-0 text-xs">
            {item.navigation.label}
          </Link>
        )}
      </div>
    </article>
  );
}

export function TeacherAssistHomeScreen() {
  const { user } = useAuth();
  const { isComplete: onboardingComplete } = useTeacherAssistOnboarding();
  const [payload, setPayload] = useState<TeacherAssistHomeWorkspace | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    void recordPilotLoginMetric().catch(() => undefined);
    fetchTeacherAssistHomeWorkspace()
      .then((data) => {
        if (active) setPayload(data);
      })
      .catch((err: Error) => {
        if (active) setError(err.message);
      });
    return () => {
      active = false;
    };
  }, []);

  const priorities = payload?.priorities.items ?? [];
  const hasActiveGuide = Boolean(payload?.current_week?.has_active_guide);
  const priorityCounts = useMemo(() => {
    const grouped = payload?.priorities.grouped ?? {};
    return {
      critical: grouped.critical?.length ?? 0,
      high: grouped.high?.length ?? 0,
      medium: grouped.medium?.length ?? 0,
    };
  }, [payload]);

  if (error) {
    return (
      <TeacherAssistAlert
        variant="error"
        title="Unable to load home workspace"
        description={error}
        actionLabel="Retry"
        onAction={() => window.location.reload()}
      />
    );
  }
  if (!payload) {
    return <p className="text-sm text-slate-600">Loading home workspace...</p>;
  }

  return (
    <div className="space-y-5">
      <header className="space-y-2">
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">
          Good {new Date().getHours() < 12 ? "morning" : "afternoon"}, {greetingName(user?.full_name)}
        </h1>
        <p className="text-sm text-slate-600">
          {!payload.onboarding.is_complete
            ? "Complete setup first, then browse pacing guides to begin weekly planning."
            : payload.current_week?.has_active_guide
              ? "Upload curriculum for this week, then generate your lesson plan, slides, and assignments."
              : "Browse district pacing guides for your grade, copy one, then start weekly planning by subject."}
        </p>
        {!payload.onboarding.is_complete ? (
          <TeacherAssistAlert
            variant="warning"
            title={`Setup ${payload.onboarding.progress_percent}% complete`}
            description="Finish school placement, school year, and homeroom on the Setup page."
            actionLabel="Continue setup"
            actionHref="/teacher-assist/get-started"
            className="py-2"
          />
        ) : null}
      </header>

      {onboardingComplete && payload.current_week?.has_active_guide ? (
        <section className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
          <article className="ta-panel p-4">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-700">Continue planning</p>
                <h2 className="mt-1 text-lg font-semibold text-slate-900">
                  {(payload.current_week.current_week as { title?: string } | null)?.title ?? "Week not resolved"}
                </h2>
                <p className="mt-1 text-sm text-slate-600">
                  {(payload.current_week.pacing_guide as { title?: string } | null)?.title ?? "Pacing guide"} ·{" "}
                  {(payload.current_week.school_year as { title?: string } | null)?.title ?? "School year"}
                </p>
              </div>
              <Link
                href={payload.continue_planning?.current_week_href ?? "/teacher-assist/planning/weeks"}
                className="ta-button-primary text-xs"
              >
                Open weekly planning
              </Link>
            </div>
            <Link
              href="/teacher-assist/resources"
              className="mt-3 inline-flex ta-button-secondary text-xs"
            >
              Upload curriculum files
            </Link>
            <p className="mt-3 text-sm text-slate-600">
              {(payload.current_week.current_week as { description?: string } | null)?.description ??
                "Attach pacing-week resources, then use Generate Plan to produce lesson artifacts."}
            </p>
            <div className="mt-3 flex flex-wrap gap-1.5">
              {(
                (payload.current_week.current_week as { objectives?: Array<{ objective_code?: string }> } | null)
                  ?.objectives ?? []
              ).map((row, index) => (
                <span
                  key={`${row.objective_code}-${index}`}
                  className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700"
                >
                  {row.objective_code}
                </span>
              ))}
            </div>
          </article>

          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Time saved & next steps</h2>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="rounded-xl border border-emerald-100 bg-emerald-50 px-3 py-2.5">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-emerald-700">Time saved this year</p>
                <p className="mt-1 text-2xl font-semibold text-emerald-900">
                  {payload.time_savings?.time_saved_this_year_hours ?? 0}h
                </p>
                <p className="text-xs text-emerald-800">
                  Reuse rate {payload.efficiency_summary?.reuse_rate_percent ?? 0}%
                </p>
              </div>
              <div className="rounded-xl border border-slate-200 px-3 py-2.5">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Upcoming week</p>
                <p className="mt-1 text-sm font-medium text-slate-900">
                  {(payload.current_week.upcoming_week as { title?: string } | null)?.title ?? "No upcoming week yet"}
                </p>
                {(payload.continue_planning?.upcoming_instructional_week_href ??
                  payload.continue_planning?.generate_next_week_href) ? (
                  <Link
                    href={
                      payload.continue_planning.upcoming_instructional_week_href ??
                      payload.continue_planning.generate_next_week_href ??
                      "/teacher-assist/planning/weeks"
                    }
                    className="mt-2 inline-flex ta-button-secondary text-xs"
                  >
                    Plan upcoming week
                  </Link>
                ) : null}
              </div>
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Teaching progress</p>
              <p className="mt-1 text-sm text-slate-600">
                {(payload.current_week.teaching_progress as { weeks_completed?: number; weeks_total?: number } | undefined)
                  ?.weeks_completed ?? 0}
                /
                {(payload.current_week.teaching_progress as { weeks_total?: number } | undefined)?.weeks_total ?? 0} weeks
                ·{" "}
                {(payload.current_week.teaching_progress as { objectives_remaining?: number } | undefined)
                  ?.objectives_remaining ?? 0}{" "}
                objectives remaining
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Link href={payload.continue_planning?.current_week_href ?? "/teacher-assist/planning/weeks"} className="ta-button-primary text-xs">
                Plan this week
              </Link>
            <Link
              href={
                payload.continue_planning?.current_week_href
                  ? `${payload.continue_planning.current_week_href}${payload.continue_planning.current_week_href.includes("?") ? "&" : "?"}focus=resources`
                  : "/teacher-assist/resources"
              }
              className="ta-button-secondary text-xs"
            >
              Upload materials
            </Link>
            </div>
          </article>
        </section>
      ) : onboardingComplete ? (
        <section className="ta-panel p-5">
          <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-700">Start planning</p>
          <h2 className="mt-1 text-lg font-semibold text-slate-900">Pacing guide workflow</h2>
          <ol className="mt-4 space-y-3 text-sm text-slate-600">
            <li className="rounded-xl border border-slate-200 px-4 py-3">
              <span className="font-semibold text-slate-900">1. Browse pacing guides</span>
              <p className="mt-1">Find the district guide for your grade level and subject.</p>
              <Link href="/teacher-assist/pacing-guides" className="mt-2 inline-flex ta-button-secondary text-xs">
                Browse pacing guides
              </Link>
            </li>
            <li className="rounded-xl border border-slate-200 px-4 py-3">
              <span className="font-semibold text-slate-900">2. Copy to your library</span>
              <p className="mt-1">Copy the district guide, then set it active in pacing workspace.</p>
              <Link href="/teacher-assist/planning/pacing-guides/workspace" className="mt-2 inline-flex ta-button-secondary text-xs">
                Open pacing workspace
              </Link>
            </li>
            <li className="rounded-xl border border-slate-200 px-4 py-3">
              <span className="font-semibold text-slate-900">3. Weekly planning by subject</span>
              <p className="mt-1">Upload curriculum and reference links, then click Generate Plan in weekly planning.</p>
              <Link href="/teacher-assist/planning/weeks" className="mt-2 inline-flex ta-button-primary text-xs">
                Start weekly planning
              </Link>
            </li>
          </ol>
        </section>
      ) : (
        <section className="ta-panel p-4">
          <p className="text-sm text-slate-600">
            Complete setup on the Setup page before browsing pacing guides or planning weekly instruction.
          </p>
          <Link href="/teacher-assist/get-started" className="ta-button-primary mt-3 inline-flex text-xs">
            Continue setup
          </Link>
        </section>
      )}

      {onboardingComplete && hasActiveGuide ? (
        <section className="ta-panel p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-base font-semibold text-slate-900">Recently used resources</h2>
            <Link
              href={
                payload.continue_planning?.current_week_href
                  ? `${payload.continue_planning.current_week_href}${payload.continue_planning.current_week_href.includes("?") ? "&" : "?"}focus=resources`
                  : "/teacher-assist/resources"
              }
              className="text-xs font-semibold text-sky-700"
            >
              View all resources
            </Link>
          </div>
          <div className="mt-3 space-y-2">
            {(payload.recently_used_resources ?? []).length === 0 ? (
              <p className="text-sm text-slate-500">Resources from your current pacing week will appear here.</p>
            ) : (
              (payload.recently_used_resources ?? []).map((row, index) => (
                <div key={`${row.title}-${index}`} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">{row.title}</p>
                    {row.resource_type ? <p className="text-xs text-slate-500">{row.resource_type}</p> : null}
                  </div>
                  <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                    Open
                  </Link>
                </div>
              ))
            )}
          </div>
        </section>
      ) : null}

      {onboardingComplete && payload.current_week?.has_active_guide && payload.copilot ? (
        <section className="grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
          <article className="ta-panel p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-sky-700">Teacher Copilot</p>
                <h2 className="text-base font-semibold text-slate-900">Ask Teacher Copilot</h2>
              </div>
              <Link href={payload.copilot.href} className="ta-button-primary text-xs">
                Open copilot
              </Link>
            </div>
            <p className="mt-2 text-sm text-slate-600">
              Get grounded answers about objectives, students, and this week — recommendations only, you stay in control.
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              {(payload.copilot.suggested_questions ?? []).slice(0, 4).map((question) => (
                <Link
                  key={question}
                  href={`${payload.copilot?.href}?prompt=${encodeURIComponent(question)}`}
                  className="ta-button-secondary text-xs"
                >
                  {question}
                </Link>
              ))}
            </div>
            {payload.copilot.weekly_summary_href ? (
              <Link href={payload.copilot.weekly_summary_href} className="mt-3 inline-flex text-xs font-semibold text-sky-700">
                Summarize this week
              </Link>
            ) : null}
          </article>

          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Suggested actions</h2>
            <div className="mt-3 space-y-2">
              {(payload.copilot.suggested_actions ?? []).length === 0 ? (
                <p className="text-sm text-slate-500">Copilot actions appear once assignments and mastery evidence exist.</p>
              ) : (
                (payload.copilot.suggested_actions ?? []).slice(0, 4).map((row, index) => (
                  <div key={`${row.title}-${index}`} className="rounded-xl border border-slate-200 px-3 py-2.5">
                    <p className="text-sm font-semibold text-slate-900">{row.title}</p>
                    {row.description ? <p className="text-xs text-slate-500">{row.description}</p> : null}
                    {row.navigation_href ? (
                      <Link href={row.navigation_href} className="mt-1 inline-flex text-xs font-semibold text-sky-700">
                        Review
                      </Link>
                    ) : null}
                  </div>
                ))
              )}
            </div>
          </article>
        </section>
      ) : null}

      {onboardingComplete && payload.current_week?.has_active_guide && payload.instructional_loop ? (
        <section className="grid gap-4 lg:grid-cols-2">
          <article className="ta-panel p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-base font-semibold text-slate-900">Instructional health</h2>
              <Link href="/teacher-assist/reteach" className="text-xs font-semibold text-sky-700">
                Open reteach workspace
              </Link>
            </div>
            <div className="mt-3 grid gap-3 sm:grid-cols-3">
              <div className="rounded-xl border border-slate-200 px-3 py-2.5">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Need support</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {payload.instructional_loop.instructional_health?.students_needing_support_count ?? 0}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200 px-3 py-2.5">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Open reteach</p>
                <p className="mt-1 text-xl font-semibold text-slate-900">
                  {payload.instructional_loop.instructional_health?.open_reteach_plan_count ?? 0}
                </p>
              </div>
              <div className="rounded-xl border border-slate-200 px-3 py-2.5">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Week closure</p>
                <p className="mt-1 text-sm font-semibold text-slate-900">
                  {payload.instructional_loop.week_closure_status?.status?.replaceAll("_", " ") ?? "Not started"}
                </p>
              </div>
            </div>
            <div className="mt-3 space-y-2">
              {(payload.instructional_loop.objectives_requiring_attention ?? []).length === 0 ? (
                <p className="text-sm text-slate-500">No objectives flagged for attention.</p>
              ) : (
                (payload.instructional_loop.objectives_requiring_attention ?? []).slice(0, 4).map((row, index) => (
                  <div key={`${row.objective_code}-${index}`} className="rounded-xl border border-amber-100 bg-amber-50 px-3 py-2 text-sm">
                    <p className="font-semibold text-amber-900">{row.objective_code ?? "Objective"}</p>
                    <p className="text-xs text-amber-800">Mastery {row.mastery_pct ?? 0}%</p>
                  </div>
                ))
              )}
            </div>
          </article>

          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Loop recommendations</h2>
            <div className="mt-3 space-y-2">
              {(payload.instructional_loop.loop_recommendations ?? []).length === 0 ? (
                <p className="text-sm text-slate-500">Recommendations appear once assignments and mastery evidence exist.</p>
              ) : (
                (payload.instructional_loop.loop_recommendations ?? []).slice(0, 5).map((row, index) => (
                  <div key={`${row.title}-${index}`} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{row.title}</p>
                      {row.description ? <p className="text-xs text-slate-500">{row.description}</p> : null}
                    </div>
                    {row.navigation_href ? (
                      <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                        Review
                      </Link>
                    ) : null}
                  </div>
                ))
              )}
            </div>
          </article>
        </section>
      ) : null}

      {onboardingComplete && payload.current_week?.has_active_guide ? (
        <section className="grid gap-4 lg:grid-cols-2">
          <article className="ta-panel p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-base font-semibold text-slate-900">Recommended reuse</h2>
              <Link
                href={payload.continue_planning?.current_week_href ?? "/teacher-assist/planning/weeks"}
                className="text-xs font-semibold text-sky-700"
              >
                Open week workspace
              </Link>
            </div>
            <div className="mt-3 space-y-2">
              {(payload.recommended_reuse ?? []).length === 0 ? (
                <p className="text-sm text-slate-500">Reuse suggestions appear once you have prior weeks, templates, or artifacts.</p>
              ) : (
                (payload.recommended_reuse ?? []).slice(0, 5).map((row) => (
                  <div key={`${row.entity_type}-${row.entity_id}`} className="rounded-xl border border-slate-200 px-3 py-2.5">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div>
                        <p className="text-sm font-semibold text-slate-900">{row.title}</p>
                        <p className="text-xs text-slate-500">
                          {row.source ?? "Reuse candidate"} · Score {row.reuse_score?.score ?? "—"}
                        </p>
                      </div>
                      {row.navigation_href ? (
                        <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                          Open
                        </Link>
                      ) : null}
                    </div>
                  </div>
                ))
              )}
            </div>
          </article>

          <article className="ta-panel p-4">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-base font-semibold text-slate-900">Recent templates</h2>
              <Link
                href={payload.continue_planning?.template_library_href ?? "/teacher-assist/planning/templates"}
                className="text-xs font-semibold text-sky-700"
              >
                View library
              </Link>
            </div>
            <div className="mt-3 space-y-2">
              {(payload.efficiency_summary?.recent_templates ?? []).length === 0 ? (
                <p className="text-sm text-slate-500">Save a week as a template from the week workspace to build your library.</p>
              ) : (
                (payload.efficiency_summary?.recent_templates ?? []).map((row) => (
                  <div key={row.id} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{row.name}</p>
                      <p className="text-xs text-slate-500">{row.artifact_type}</p>
                    </div>
                    <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                      Open
                    </Link>
                  </div>
                ))
              )}
            </div>
          </article>
        </section>
      ) : null}

      {onboardingComplete && hasActiveGuide && priorities.length > 0 ? (
        <section className="ta-panel p-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-base font-semibold text-slate-900">Today&apos;s priorities</h2>
            <Link href="/teacher-assist/work-queue" className="text-xs font-semibold text-sky-700">
              Open work queue
            </Link>
          </div>
          <div className="mt-2 flex flex-wrap gap-2 text-xs text-slate-500">
            {priorityCounts.critical > 0 ? (
              <span className="rounded-full bg-rose-50 px-2 py-0.5 font-semibold text-rose-700">
                {priorityCounts.critical} critical
              </span>
            ) : null}
            {priorityCounts.high > 0 ? (
              <span className="rounded-full bg-amber-50 px-2 py-0.5 font-semibold text-amber-800">
                {priorityCounts.high} high
              </span>
            ) : null}
            {priorityCounts.medium > 0 ? (
              <span className="rounded-full bg-slate-100 px-2 py-0.5 font-semibold text-slate-700">
                {priorityCounts.medium} medium
              </span>
            ) : null}
          </div>
          <div className="mt-3 space-y-2">
            {priorities.slice(0, 6).map((item) => (
              <PriorityCard key={item.action_key} item={item} disabled={!onboardingComplete} />
            ))}
          </div>
        </section>
      ) : null}

      {onboardingComplete && hasActiveGuide ? (
        <>
          <section className="grid gap-4 lg:grid-cols-2">
            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">My classes</h2>
              <div className="mt-3 space-y-2">
                {payload.classes.length === 0 ? (
                  <TeacherAssistEmptyState
                    title="No classes yet"
                    description="Add classes in settings to start tracking work by class."
                    actionLabel="Open settings"
                    actionHref="/teacher-assist/settings"
                  />
                ) : (
                  payload.classes.map((row) => (
                    <div key={row.class_id} className="rounded-xl border border-slate-200 px-3 py-2.5">
                      <p className="font-semibold text-slate-900">{row.class_name}</p>
                      <p className="mt-0.5 text-xs text-slate-500">
                        {row.student_count ?? 0} students · {row.open_action_count ?? 0} open actions
                      </p>
                      <Link href={row.navigation_href} className="mt-1 inline-block text-xs font-semibold text-sky-700">
                        Open class
                      </Link>
                    </div>
                  ))
                )}
              </div>
            </article>

            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">Operational timeline</h2>
              <p className="mt-1 text-sm text-slate-600">
                {payload.this_week.assignments_due_count ?? 0} assignments due ·{" "}
                {payload.this_week.completed_plans_count ?? 0} completed plans
              </p>
              <div className="mt-3 space-y-1.5">
                {payload.timeline.slice(0, 6).map((event, index) => (
                  <div key={`${event.event_type}-${index}`} className="flex items-center justify-between gap-2 text-sm">
                    <span className="shrink-0 text-xs text-slate-500">{event.event_date}</span>
                    <Link href={event.navigation_href} className="truncate font-medium text-slate-900 hover:text-sky-700">
                      {labelize(event.event_type)} · {event.title}
                    </Link>
                  </div>
                ))}
                {payload.timeline.length === 0 ? (
                  <p className="text-sm text-slate-500">No upcoming items this week.</p>
                ) : null}
              </div>
            </article>
          </section>

          <section className="grid gap-4 lg:grid-cols-2">
            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">Mastery alerts</h2>
              <div className="mt-3 space-y-2">
                {payload.mastery_alerts.length === 0 ? (
                  <p className="text-sm text-slate-500">No mastery alerts right now.</p>
                ) : (
                  payload.mastery_alerts.slice(0, 5).map((alert, index) => (
                    <div key={`${alert.alert_type}-${index}`} className="rounded-xl border border-slate-200 px-3 py-2.5">
                      <p className="text-sm font-semibold text-slate-900">{alert.title}</p>
                      {alert.description ? <p className="mt-0.5 text-xs text-slate-600">{alert.description}</p> : null}
                      <Link href={alert.navigation_href} className="mt-1 inline-block text-xs font-semibold text-sky-700">
                        Review
                      </Link>
                    </div>
                  ))
                )}
              </div>
            </article>

            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">Recent activity</h2>
              <ul className="mt-3 space-y-1.5 text-sm text-slate-700">
                {payload.recent_activity.length === 0 ? (
                  <li className="text-slate-500">No recent activity.</li>
                ) : (
                  payload.recent_activity.slice(0, 8).map((event) => (
                    <li key={event.id} className="rounded-lg border border-slate-200 px-2.5 py-2 text-xs">
                      {event.summary_text}
                    </li>
                  ))
                )}
              </ul>
            </article>
          </section>
        </>
      ) : null}
    </div>
  );
}
