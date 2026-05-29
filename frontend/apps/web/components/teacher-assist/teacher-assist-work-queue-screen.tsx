"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { fetchTeacherAssistWorkQueue } from "@/lib/teacher-assist-api";
import type { TeacherAssistActionWorkspaceItem, TeacherAssistWorkQueue } from "@/lib/teacher-assist-types";

const PRIORITY_ORDER = ["critical", "high", "medium", "informational"] as const;

const PRIORITY_LABELS: Record<string, string> = {
  critical: "Critical",
  high: "High",
  medium: "Medium",
  informational: "Informational",
};

const PRIORITY_STYLES: Record<string, string> = {
  critical: "border-rose-200 bg-rose-50/80",
  high: "border-amber-200 bg-amber-50/80",
  medium: "border-sky-200 bg-sky-50/50",
  informational: "border-slate-200 bg-slate-50/80",
};

function labelize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatTimestamp(value: string | null | undefined) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toLocaleString([], { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function QueueItemCard({ item }: { item: TeacherAssistActionWorkspaceItem & { priority_level?: string } }) {
  const updated = formatTimestamp(item.updated_at ?? item.created_at);

  return (
    <article className="rounded-xl border border-slate-200 bg-white px-3 py-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0 flex-1">
          <p className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
            {labelize(item.action_type)}
          </p>
          <h3 className="mt-0.5 text-sm font-semibold text-slate-900">{item.title}</h3>
          <p className="mt-0.5 text-sm text-slate-600">{item.description}</p>
          {updated ? <p className="mt-1 text-xs text-slate-400">Updated {updated}</p> : null}
        </div>
        <Link href={item.navigation.href} className="ta-button-secondary shrink-0 text-xs">
          {item.navigation.label}
        </Link>
      </div>
    </article>
  );
}

export function TeacherAssistWorkQueueScreen() {
  const [payload, setPayload] = useState<TeacherAssistWorkQueue | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setPayload(await fetchTeacherAssistWorkQueue());
  }, []);

  useEffect(() => {
    load().catch((err: Error) => setError(err.message));
  }, [load]);

  const priorityGroups = useMemo(() => {
    if (!payload) return [];
    const grouped = new Map<string, TeacherAssistActionWorkspaceItem[]>();
    for (const item of payload.items) {
      const level = (item as TeacherAssistActionWorkspaceItem & { priority_level?: string }).priority_level ?? "medium";
      const bucket = grouped.get(level) ?? [];
      bucket.push(item);
      grouped.set(level, bucket);
    }
    return PRIORITY_ORDER.map((level) => ({
      level,
      label: PRIORITY_LABELS[level],
      items: grouped.get(level) ?? [],
    })).filter((group) => group.items.length > 0);
  }, [payload]);

  if (error) {
    return (
      <TeacherAssistAlert
        variant="error"
        title="Unable to load work queue"
        description={error}
        actionLabel="Retry"
        onAction={() => {
          void load().catch((err: Error) => setError(err.message));
        }}
      />
    );
  }
  if (!payload) {
    return <p className="text-sm text-slate-600">Loading work queue...</p>;
  }

  return (
    <div className="space-y-5">
      <header>
        <h1 className="text-2xl font-semibold tracking-tight text-slate-900">Work queue</h1>
        <p className="mt-1 text-sm text-slate-600">
          {payload.summary.total_actionable} actionable item
          {payload.summary.total_actionable === 1 ? "" : "s"} — reviews, grades, commits, and failures.
        </p>
      </header>

      {payload.summary.total_actionable === 0 ? (
        <TeacherAssistEmptyState
          title="Work queue is clear"
          description="No reviews, grades, commits, or workflow failures need attention right now."
          actionLabel="Return to Home"
          actionHref="/teacher-assist/home"
        />
      ) : (
        <div className="space-y-4">
          {priorityGroups.map((group) => (
            <section key={group.level} className={`ta-panel p-4 ${PRIORITY_STYLES[group.level] ?? ""}`}>
              <div className="flex items-center justify-between gap-2">
                <h2 className="text-base font-semibold text-slate-900">{group.label}</h2>
                <span className="rounded-full bg-white/80 px-2 py-0.5 text-xs font-semibold text-slate-700">
                  {group.items.length}
                </span>
              </div>
              <div className="mt-3 space-y-2">
                {group.items.map((item) => (
                  <QueueItemCard key={item.action_key} item={item} />
                ))}
              </div>
            </section>
          ))}
        </div>
      )}
    </div>
  );
}
