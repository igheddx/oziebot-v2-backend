"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import { fetchTeacherAssistActionWorkspace } from "@/lib/teacher-assist-api";
import type {
  TeacherAssistActionWorkspace,
  TeacherAssistActionWorkspaceItem,
  TeacherAssistActionWorkspaceSection,
  TeacherAssistActionWorkspaceSeverity,
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

function severityBadgeClasses(severity: TeacherAssistActionWorkspaceSeverity) {
  switch (severity) {
    case "critical":
      return "bg-rose-100 text-rose-800";
    case "warning":
      return "bg-amber-100 text-amber-800";
    case "review":
      return "bg-violet-100 text-violet-800";
    case "ready":
      return "bg-emerald-100 text-emerald-800";
    default:
      return "bg-sky-100 text-sky-800";
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
    <article className="ta-panel p-5">
      <p className="text-sm font-semibold text-slate-500">{label}</p>
      <p className="mt-3 text-3xl font-semibold text-slate-900">{value}</p>
      <p className="mt-2 text-sm text-slate-600">{detail}</p>
    </article>
  );
}

function ActionItemCard({ item }: { item: TeacherAssistActionWorkspaceItem }) {
  return (
    <article className={`rounded-2xl border px-4 py-4 ${severityClasses(item.severity)}`}>
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span
              className={`rounded-full px-2.5 py-0.5 text-xs font-semibold uppercase tracking-wide ${severityBadgeClasses(item.severity)}`}
            >
              {labelize(item.severity)}
            </span>
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {labelize(item.action_type)}
            </span>
          </div>
          <h3 className="mt-2 text-base font-semibold">{item.title}</h3>
          <p className="mt-1 text-sm leading-6 opacity-90">{item.description}</p>
          <p className="mt-2 text-xs opacity-75">
            Updated {formatDateTime(item.updated_at ?? item.created_at)}
          </p>
        </div>
        <Link href={item.navigation.href} className="ta-button-secondary shrink-0">
          {item.navigation.label}
        </Link>
      </div>
    </article>
  );
}

function SectionPanel({ section }: { section: TeacherAssistActionWorkspaceSection }) {
  return (
    <article className="ta-panel p-6">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">{section.title}</h2>
          <p className="mt-1 text-sm text-slate-600">
            {section.count === 0
              ? "Nothing open in this area right now."
              : `${section.count} open item${section.count === 1 ? "" : "s"} in this section.`}
          </p>
        </div>
        <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
          {section.count}
        </span>
      </div>

      {section.items.length === 0 ? (
        <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
          No actions to show.
        </div>
      ) : (
        <div className="mt-5 space-y-3">
          {section.items.map((item) => (
            <ActionItemCard key={item.action_key} item={item} />
          ))}
        </div>
      )}
    </article>
  );
}

export function TeacherAssistActionWorkspaceScreen() {
  const [workspace, setWorkspace] = useState<TeacherAssistActionWorkspace | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const payload = await fetchTeacherAssistActionWorkspace();
      setWorkspace(payload);
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load action workspace.");
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

  const summary = workspace?.summary;

  return (
    <div className="space-y-8">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Actions
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            What needs your attention right now
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            One operational command center for extractions, grading reviews, gradebook commits, workflows,
            exports, and planning work. This page is read-only and routes you to the right workspace to act.
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            <Link href="/teacher-assist/workspace" className="ta-button-secondary">
              Back to Workspace Summary
            </Link>
          </div>
        </div>
      </section>

      <section className="ta-alert ta-alert-info">
        The action workspace aggregates existing TeacherAssist records only. It does not auto-commit grades,
        update mastery, send parent communication, or call external LMS/SIS systems.
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
        <SummaryCard
          label="Open actions"
          value={loading ? "..." : summary?.total_open_actions ?? 0}
          detail="Critical, warning, review, and ready items."
        />
        <SummaryCard
          label="Critical"
          value={loading ? "..." : summary?.critical_count ?? 0}
          detail="Failed jobs and blockers that need immediate attention."
        />
        <SummaryCard
          label="Needs review"
          value={loading ? "..." : summary?.review_count ?? 0}
          detail="Teacher confirmation or extraction review work."
        />
        <SummaryCard
          label="Ready to commit / download"
          value={loading ? "..." : summary?.ready_count ?? 0}
          detail="Gradebook-ready reviews and completed exports."
        />
        <SummaryCard
          label="Warnings"
          value={loading ? "..." : summary?.warning_count ?? 0}
          detail="Draft reviews, retries, and planning follow-ups."
        />
        <SummaryCard
          label="Mastery alerts"
          value={loading ? "..." : summary?.mastery_alert_count ?? 0}
          detail="Reteach and low-mastery read-model insights."
        />
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Priority actions</h2>
              <p className="mt-1 text-sm text-slate-600">
                Top items sorted by severity and recency.
              </p>
            </div>
            <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
              {workspace?.priority_items.length ?? 0}
            </span>
          </div>

          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading priority actions...</p>
          ) : workspace && workspace.priority_items.length === 0 ? (
            <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Nothing urgent is waiting right now.
            </div>
          ) : (
            <div className="mt-5 space-y-3">
              {workspace?.priority_items.map((item) => (
                <ActionItemCard key={item.action_key} item={item} />
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Class rollups</h2>
              <p className="mt-1 text-sm text-slate-600">
                Open actions grouped by class when class context is available.
              </p>
            </div>
          </div>

          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading class rollups...</p>
          ) : workspace && workspace.class_rollups.length === 0 ? (
            <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              No class-scoped actions are open right now.
            </div>
          ) : (
            <div className="mt-5 space-y-3">
              {workspace?.class_rollups.map((rollup) => (
                <article
                  key={rollup.class_id}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-4"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h3 className="text-base font-semibold text-slate-900">{rollup.class_name}</h3>
                      <p className="mt-1 text-sm text-slate-600">
                        {rollup.open_action_count} open action{rollup.open_action_count === 1 ? "" : "s"}
                      </p>
                    </div>
                    <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                      {rollup.open_action_count}
                    </span>
                  </div>
                  <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
                    <span className="rounded-full bg-slate-50 px-2.5 py-1">
                      Extractions {rollup.extraction_count}
                    </span>
                    <span className="rounded-full bg-slate-50 px-2.5 py-1">
                      Grading {rollup.grading_count}
                    </span>
                    <span className="rounded-full bg-slate-50 px-2.5 py-1">
                      Gradebook {rollup.gradebook_count}
                    </span>
                    <span className="rounded-full bg-slate-50 px-2.5 py-1">
                      Workflows/Exports {rollup.workflow_export_count}
                    </span>
                    <span className="rounded-full bg-slate-50 px-2.5 py-1">
                      Planning {rollup.planning_assignment_count}
                    </span>
                  </div>
                </article>
              ))}
            </div>
          )}
        </article>
      </section>

      <section className="space-y-6">
        {(workspace?.sections ?? []).map((section) => (
          <SectionPanel key={section.section_key} section={section} />
        ))}
      </section>

      <section className="ta-panel p-6">
        <div className="flex items-start justify-between gap-3">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Recent activity</h2>
            <p className="mt-1 text-sm text-slate-600">
              Latest TeacherAssist activity events for context while triaging actions.
            </p>
          </div>
        </div>

        {loading ? (
          <p className="mt-4 text-sm text-slate-500">Loading recent activity...</p>
        ) : workspace && workspace.recent_activity.length === 0 ? (
          <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
            No recent activity yet.
          </div>
        ) : (
          <div className="mt-5 space-y-3">
            {workspace?.recent_activity.map((event) => (
              <article
                key={event.id}
                className="rounded-2xl border border-slate-200 bg-white px-4 py-4"
              >
                <div className="flex flex-wrap items-center gap-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                  <span>{labelize(event.event_category)}</span>
                  <span>{labelize(event.event_type)}</span>
                </div>
                <p className="mt-2 text-sm text-slate-800">{event.summary_text}</p>
                <p className="mt-2 text-xs text-slate-500">{formatDateTime(event.created_at)}</p>
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
