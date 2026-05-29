"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { fetchTeacherAssistHomeWorkspace } from "@/lib/teacher-assist-api";
import type { TeacherAssistHomeWorkspace } from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

export function TeacherAssistHomeScreen() {
  const [payload, setPayload] = useState<TeacherAssistHomeWorkspace | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
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

  if (error) {
    return <p className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">{error}</p>;
  }
  if (!payload) {
    return <p className="text-sm text-slate-600">Loading home workspace...</p>;
  }

  const priorities = payload.priorities.items ?? [];

  return (
    <div className="space-y-6">
      <header className="ta-panel p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Home</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">What needs your attention</h1>
        <p className="mt-2 text-sm text-slate-600">
          Strategic overview across classes, priorities, and this week&apos;s workload.
        </p>
        {!payload.onboarding.is_complete ? (
          <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
            Setup {payload.onboarding.progress_percent}% complete.{" "}
            <Link href="/teacher-assist/get-started" className="font-semibold underline">
              Continue onboarding
            </Link>
          </div>
        ) : null}
      </header>

      <section className="grid gap-4 md:grid-cols-3">
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Open priorities</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">{priorities.length}</p>
        </article>
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Classes</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">{payload.classes.length}</p>
        </article>
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Due this week</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">
            {payload.this_week.assignments_due_count ?? 0}
          </p>
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <article className="ta-panel p-5">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-lg font-semibold text-slate-900">Today&apos;s priorities</h2>
            <Link href="/teacher-assist/work-queue" className="text-sm font-semibold text-sky-700">
              Open work queue
            </Link>
          </div>
          <div className="mt-4 space-y-3">
            {priorities.length === 0 ? (
              <TeacherAssistEmptyState
                title="No urgent priorities"
                description="You're caught up on actionable items."
                whyItMatters="Priorities surface grading, reviews, mastery alerts, and workflow failures."
                actionLabel="View work queue"
                actionHref="/teacher-assist/work-queue"
              />
            ) : (
              priorities.slice(0, 8).map((item) => (
                <div key={item.action_key} className="rounded-2xl border border-slate-200 px-4 py-3">
                  <p className="text-sm font-semibold text-slate-900">{item.title}</p>
                  <p className="mt-1 text-sm text-slate-600">{item.description}</p>
                  <Link href={item.navigation.href} className="mt-2 inline-block text-sm font-semibold text-sky-700">
                    {item.navigation.label}
                  </Link>
                </div>
              ))
            )}
          </div>
        </article>

        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">My classes</h2>
          <div className="mt-4 space-y-3">
            {payload.classes.length === 0 ? (
              <TeacherAssistEmptyState
                title="No classes yet"
                description="Add classes during onboarding or in settings."
                actionLabel="Get started"
                actionHref="/teacher-assist/get-started"
              />
            ) : (
              payload.classes.map((row) => (
                <div key={row.class_id} className="rounded-2xl border border-slate-200 px-4 py-3">
                  <p className="font-semibold text-slate-900">{row.class_name}</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {row.student_count ?? 0} students · {row.open_action_count ?? 0} open actions
                  </p>
                  <Link href={row.navigation_href} className="mt-2 inline-block text-sm font-semibold text-sky-700">
                    Open class workspace
                  </Link>
                </div>
              ))
            )}
          </div>
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Quick actions</h2>
          <div className="mt-4 flex flex-wrap gap-2">
            {payload.quick_actions.map((action) => (
              <Link
                key={action.action_key}
                href={action.navigation_href}
                className="ta-button-secondary text-sm"
              >
                {action.label}
              </Link>
            ))}
          </div>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Smart shortcuts</h2>
          <ul className="mt-4 space-y-2 text-sm text-slate-700">
            {payload.shortcuts.most_used_class ? (
              <li>
                Most-used class:{" "}
                <Link href={payload.shortcuts.most_used_class.navigation_href} className="font-semibold text-sky-700">
                  {payload.shortcuts.most_used_class.class_name}
                </Link>
              </li>
            ) : null}
            {payload.shortcuts.recent_plans?.slice(0, 3).map((plan) => (
              <li key={plan.weekly_plan_id}>
                Recent plan:{" "}
                <Link href={plan.navigation_href} className="font-semibold text-sky-700">
                  {plan.title}
                </Link>
              </li>
            ))}
          </ul>
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Mastery alerts</h2>
          <div className="mt-4 space-y-3">
            {payload.mastery_alerts.length === 0 ? (
              <p className="text-sm text-slate-500">No mastery alerts right now.</p>
            ) : (
              payload.mastery_alerts.slice(0, 5).map((alert, index) => (
                <div key={`${alert.alert_type}-${index}`} className="rounded-2xl border border-slate-200 px-4 py-3">
                  <p className="text-sm font-semibold text-slate-900">{alert.title}</p>
                  {alert.description ? <p className="mt-1 text-sm text-slate-600">{alert.description}</p> : null}
                  <Link href={alert.navigation_href} className="mt-2 inline-block text-sm font-semibold text-sky-700">
                    View mastery
                  </Link>
                </div>
              ))
            )}
          </div>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Recent activity</h2>
          <ul className="mt-4 space-y-2 text-sm text-slate-700">
            {payload.recent_activity.length === 0 ? (
              <li className="text-slate-500">No recent activity.</li>
            ) : (
              payload.recent_activity.slice(0, 8).map((event) => (
                <li key={event.id} className="rounded-xl border border-slate-200 px-3 py-2">
                  {event.summary_text}
                </li>
              ))
            )}
          </ul>
        </article>
      </section>

      <article className="ta-panel p-5">
        <h2 className="text-lg font-semibold text-slate-900">This week</h2>
        <p className="mt-2 text-sm text-slate-600">
          {payload.this_week.assignments_due_count ?? 0} assignments due ·{" "}
          {payload.this_week.completed_plans_count ?? 0} completed plans
        </p>
        <div className="mt-4 space-y-2">
          {payload.timeline.slice(0, 8).map((event, index) => (
            <div key={`${event.event_type}-${index}`} className="flex items-center justify-between gap-3 text-sm">
              <span className="text-slate-600">{event.event_date}</span>
              <Link href={event.navigation_href} className="font-medium text-slate-900 hover:text-sky-700">
                {labelize(event.event_type)} · {event.title}
              </Link>
            </div>
          ))}
        </div>
      </article>
    </div>
  );
}
