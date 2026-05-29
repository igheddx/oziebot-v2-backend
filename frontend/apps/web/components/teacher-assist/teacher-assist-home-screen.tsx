"use client";

import Link from "next/link";
import { useEffect, useMemo, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { useAuth } from "@/components/providers/auth-provider";
import { fetchTeacherAssistHomeWorkspace } from "@/lib/teacher-assist-api";
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

function PriorityCard({ item }: { item: TeacherAssistHomePriorityItem }) {
  return (
    <article className="rounded-xl border border-slate-200 bg-white px-3 py-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="text-sm font-semibold text-slate-900">{item.title}</p>
          <p className="mt-0.5 text-sm text-slate-600">{item.description}</p>
        </div>
        <Link href={item.navigation.href} className="ta-button-secondary shrink-0 text-xs">
          {item.navigation.label}
        </Link>
      </div>
    </article>
  );
}

export function TeacherAssistHomeScreen() {
  const { user } = useAuth();
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

  const priorities = payload?.priorities.items ?? [];
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
          {priorities.length > 0
            ? `${priorities.length} item${priorities.length === 1 ? "" : "s"} need attention today.`
            : "You're caught up — review classes and this week's plan below."}
        </p>
        {!payload.onboarding.is_complete ? (
          <TeacherAssistAlert
            variant="warning"
            title={`Setup ${payload.onboarding.progress_percent}% complete`}
            description="Finish school-year setup to unlock the full workflow."
            actionLabel="Continue onboarding"
            actionHref="/teacher-assist/get-started"
            className="py-2"
          />
        ) : null}
      </header>

      {priorities.length > 0 ? (
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
              <PriorityCard key={item.action_key} item={item} />
            ))}
          </div>
        </section>
      ) : null}

      <section className="flex flex-wrap gap-2">
        {payload.quick_actions.map((action) => (
          <Link key={action.action_key} href={action.navigation_href} className="ta-button-secondary text-xs">
            {action.label}
          </Link>
        ))}
      </section>

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
          <h2 className="text-base font-semibold text-slate-900">This week</h2>
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
    </div>
  );
}
