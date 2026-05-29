"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { fetchTeacherAssistWorkQueue } from "@/lib/teacher-assist-api";
import type {
  TeacherAssistActionWorkspaceItem,
  TeacherAssistWorkQueue,
  TeacherAssistWorkQueueSection,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (match) => match.toUpperCase());
}

function QueueItemCard({ item }: { item: TeacherAssistActionWorkspaceItem }) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white px-4 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">
            {labelize(item.action_type)}
          </p>
          <h3 className="mt-1 text-base font-semibold text-slate-900">{item.title}</h3>
          <p className="mt-1 text-sm text-slate-600">{item.description}</p>
        </div>
        <Link href={item.navigation.href} className="ta-button-secondary shrink-0 text-sm">
          {item.navigation.label}
        </Link>
      </div>
    </article>
  );
}

function QueueSection({ section }: { section: TeacherAssistWorkQueueSection }) {
  return (
    <article className="ta-panel p-5">
      <div className="flex items-center justify-between gap-3">
        <h2 className="text-lg font-semibold text-slate-900">{section.title}</h2>
        <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
          {section.count}
        </span>
      </div>
      <div className="mt-4 space-y-3">
        {section.items.length === 0 ? (
          <p className="text-sm text-slate-500">No actionable items in this section.</p>
        ) : (
          section.items.map((item) => <QueueItemCard key={item.action_key} item={item} />)
        )}
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

  if (error) {
    return <p className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">{error}</p>;
  }
  if (!payload) {
    return <p className="text-sm text-slate-600">Loading work queue...</p>;
  }

  const actionableSections = payload.sections.filter((section) => section.count > 0);

  return (
    <div className="space-y-6">
      <header className="ta-panel p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Work Queue</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">Operational action center</h1>
        <p className="mt-2 text-sm text-slate-600">
          Every item here is actionable — reviews, grades, commits, mastery, reteach, newsletters, and failures.
        </p>
      </header>

      <section className="grid gap-4 md:grid-cols-4">
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Total actionable</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">{payload.summary.total_actionable}</p>
        </article>
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Critical</p>
          <p className="mt-2 text-3xl font-semibold text-rose-700">{payload.summary.critical_count}</p>
        </article>
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">High</p>
          <p className="mt-2 text-3xl font-semibold text-amber-700">{payload.summary.high_count}</p>
        </article>
        <article className="ta-panel p-4">
          <p className="text-sm text-slate-500">Medium</p>
          <p className="mt-2 text-3xl font-semibold text-slate-900">{payload.summary.medium_count}</p>
        </article>
      </section>

      {payload.summary.total_actionable === 0 ? (
        <TeacherAssistEmptyState
          title="Work queue is clear"
          description="No reviews, grades, commits, or workflow failures need attention right now."
          whyItMatters="The work queue keeps operational tasks in one place so nothing slips through."
          actionLabel="Return to Home"
          actionHref="/teacher-assist/home"
        />
      ) : (
        <div className="space-y-6">
          {actionableSections.map((section) => (
            <QueueSection key={section.section_key} section={section} />
          ))}
        </div>
      )}
    </div>
  );
}
