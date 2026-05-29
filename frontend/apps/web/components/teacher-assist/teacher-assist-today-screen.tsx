"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { TeacherAssistOnboardingChecklistPanel } from "@/components/teacher-assist/teacher-assist-onboarding-checklist";
import { TeacherAssistWorkflowProgressCardView } from "@/components/teacher-assist/teacher-assist-workflow-progress-card";
import { fetchTeacherAssistTodayWorkspace } from "@/lib/teacher-assist-api";
import type {
  TeacherAssistActionWorkspaceItem,
  TeacherAssistActionWorkspaceSeverity,
  TeacherAssistTodayWorkspace,
} from "@/lib/teacher-assist-types";

const POLL_INTERVAL_MS = 60_000;

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function severityClasses(severity: TeacherAssistActionWorkspaceSeverity) {
  switch (severity) {
    case "critical":
      return "border-rose-200 bg-rose-50 text-rose-900";
    case "warning":
      return "border-amber-200 bg-amber-50 text-amber-900";
    case "review":
      return "border-violet-200 bg-violet-50 text-violet-900";
    case "ready":
      return "border-emerald-200 bg-emerald-50 text-emerald-900";
    default:
      return "border-sky-200 bg-sky-50 text-sky-900";
  }
}

function SummaryCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: number | string;
  detail: string;
}) {
  return (
    <article className="ta-panel p-4 sm:p-5">
      <p className="text-sm font-semibold text-slate-500">{label}</p>
      <p className="mt-2 text-2xl font-semibold text-slate-900 sm:text-3xl">{value}</p>
      <p className="mt-2 text-sm text-slate-600">{detail}</p>
    </article>
  );
}

function PriorityItemCard({ item }: { item: TeacherAssistActionWorkspaceItem & { today_category?: string | null } }) {
  return (
    <article className={`rounded-2xl border px-4 py-4 ${severityClasses(item.severity)}`}>
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="rounded-full bg-white/70 px-2.5 py-0.5 text-xs font-semibold uppercase tracking-wide">
              {labelize(item.severity)}
            </span>
            {item.today_category ? (
              <span className="text-xs font-medium uppercase tracking-wide opacity-75">
                {labelize(item.today_category)}
              </span>
            ) : null}
          </div>
          <h3 className="mt-2 text-base font-semibold">{item.title}</h3>
          <p className="mt-1 text-sm leading-6 opacity-90">{item.description}</p>
          <p className="mt-2 text-xs opacity-75">
            Updated {formatDateTime(item.updated_at ?? item.created_at)}
          </p>
        </div>
        <Link href={item.navigation.href} className="ta-button-secondary shrink-0 self-start">
          {item.navigation.label}
        </Link>
      </div>
    </article>
  );
}

export function TeacherAssistTodayScreen() {
  const [today, setToday] = useState<TeacherAssistTodayWorkspace | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const payload = await fetchTeacherAssistTodayWorkspace();
      setToday(payload);
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load today workspace.");
      setToday(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => {
      void load();
    }, POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [load]);

  const summary = today?.summary;
  const hasOpenWork = useMemo(
    () => (summary?.today_open_count ?? 0) > 0,
    [summary?.today_open_count],
  );

  return (
    <div className="space-y-6">
      <section className="ta-panel p-5 sm:p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.2em] text-sky-700">Today</p>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight text-slate-900 sm:text-3xl">
              What do I need to do today?
            </h1>
            <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-600 sm:text-[15px]">
              Your prioritized teacher queue across review, grading, extraction, gradebook, and mastery —
              without jumping across multiple screens.
            </p>
            <div className="mt-3 flex flex-wrap gap-2 text-sm text-slate-600">
              <span className="rounded-full bg-slate-100 px-3 py-1">
                School Year: {today?.current_school_year?.title ?? "Loading..."}
              </span>
              <span className="rounded-full bg-sky-50 px-3 py-1 text-sky-800">
                Grading Period: {today?.active_grading_period?.title ?? "Not set"}
              </span>
            </div>
          </div>
          <div className="flex flex-wrap gap-2">
            <Link href="/teacher-assist/actions" className="ta-button-secondary">
              All actions
            </Link>
            <Link href="/teacher-assist/workspace" className="ta-button-secondary">
              Full workspace
            </Link>
          </div>
        </div>
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}

      <TeacherAssistOnboardingChecklistPanel checklist={today?.onboarding_checklist ?? null} />

      <section className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
        <SummaryCard
          label="Open today"
          value={loading ? "..." : summary?.today_open_count ?? 0}
          detail="Prioritized items across all workflow areas."
        />
        <SummaryCard
          label="Needs review"
          value={loading ? "..." : summary?.items_needing_review_count ?? 0}
          detail="Plans, assignments, and review-required items."
        />
        <SummaryCard
          label="Grading pending"
          value={loading ? "..." : summary?.grading_pending_count ?? 0}
          detail="Draft reviews and grading prep follow-ups."
        />
        <SummaryCard
          label="Mastery alerts"
          value={loading ? "..." : summary?.mastery_alert_count ?? 0}
          detail="Reteach insights and low mastery attention."
        />
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <article className="ta-panel p-5 sm:p-6">
          <h2 className="text-xl font-semibold text-slate-900">Priority queue</h2>
          <p className="mt-1 text-sm text-slate-600">Sorted by urgency and recency.</p>
          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading today&apos;s queue...</p>
          ) : !hasOpenWork ? (
            <div className="mt-4">
              <TeacherAssistEmptyState
                title="You're caught up for now"
                description="No urgent teacher actions are waiting in your queue."
                whyItMatters="Use this time to review mastery trends or prepare the next assignment."
                actionLabel="Open mastery workspace"
                actionHref="/teacher-assist/mastery"
              />
            </div>
          ) : (
            <div className="mt-5 space-y-3">
              {today?.priority_items.map((item) => (
                <PriorityItemCard key={item.action_key} item={item} />
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-5 sm:p-6">
          <h2 className="text-xl font-semibold text-slate-900">Category snapshot</h2>
          <div className="mt-4 grid gap-3">
            {[
              ["items_needing_review", "Items needing review", summary?.items_needing_review_count],
              ["grading_pending", "Grading pending", summary?.grading_pending_count],
              ["extraction_pending", "Extraction pending", summary?.extraction_pending_count],
              ["gradebook_pending", "Gradebook pending", summary?.gradebook_pending_count],
              ["mastery_alerts", "Mastery alerts", summary?.mastery_alert_count],
              ["reteach_plans_pending", "Reteach insights pending", summary?.reteach_plans_pending_count],
            ].map(([key, label, count]) => (
              <div key={key} className="rounded-xl border border-slate-200 bg-white px-4 py-3">
                <div className="flex items-center justify-between gap-3">
                  <p className="text-sm font-medium text-slate-900">{label}</p>
                  <span className="rounded-full bg-slate-100 px-2.5 py-0.5 text-xs font-semibold text-slate-700">
                    {loading ? "..." : count ?? 0}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="space-y-4">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">Workflow progress</h2>
          <p className="mt-1 text-sm text-slate-600">
            Lesson Plan → Assignment → Student Work → Grading Review → Gradebook → Mastery
          </p>
        </div>
        {loading ? (
          <p className="text-sm text-slate-500">Loading workflow cards...</p>
        ) : today && today.workflow_progress_cards.length > 0 ? (
          <div className="grid gap-4 lg:grid-cols-2">
            {today.workflow_progress_cards.map((card) => (
              <TeacherAssistWorkflowProgressCardView key={card.assignment_id} card={card} />
            ))}
          </div>
        ) : (
          <TeacherAssistEmptyState
            title="No assignment pipelines yet"
            description="Create an assignment from a plan to see end-to-end workflow progress here."
            whyItMatters="Progress cards help you spot where student work stalls before gradebook or mastery."
            actionLabel="Create assignment"
            actionHref="/teacher-assist/assignments"
          />
        )}
      </section>

      <section className="ta-panel p-5 sm:p-6">
        <h2 className="text-xl font-semibold text-slate-900">Recent activity</h2>
        {loading ? (
          <p className="mt-4 text-sm text-slate-500">Loading activity...</p>
        ) : today && today.recent_activity.length > 0 ? (
          <ul className="mt-4 space-y-2">
            {today.recent_activity.map((event) => (
              <li key={event.id} className="rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm">
                <p className="font-medium text-slate-900">{event.summary_text}</p>
                <p className="mt-1 text-xs text-slate-500">
                  {labelize(event.event_type)} · {formatDateTime(event.created_at)}
                </p>
              </li>
            ))}
          </ul>
        ) : (
          <div className="mt-4">
            <TeacherAssistEmptyState
              title="No recent activity yet"
              description="TeacherAssist activity will appear here as you review, grade, and commit records."
              actionLabel="Open workspace"
              actionHref="/teacher-assist/workspace"
            />
          </div>
        )}
      </section>
    </div>
  );
}
