"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { fetchTeacherAssistClassOperationalWorkspace } from "@/lib/teacher-assist-api";
import type { TeacherAssistClassOperationalWorkspace } from "@/lib/teacher-assist-types";

const TAB_KEYS = [
  "overview",
  "assignments",
  "student_work",
  "reviews",
  "gradebook",
  "mastery",
  "reteach",
  "reflections",
] as const;

type TabKey = (typeof TAB_KEYS)[number];

function labelize(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (match) => match.toUpperCase());
}

function TabPanel({
  workspace,
  activeTab,
}: {
  workspace: TeacherAssistClassOperationalWorkspace;
  activeTab: TabKey;
}) {
  const tabs = workspace.tabs as Record<string, Record<string, unknown>>;

  if (activeTab === "overview") {
    const overview = tabs.overview ?? {};
    const recentAssignments = (overview.recent_assignments as Array<Record<string, unknown>>) ?? [];
    return (
      <div className="space-y-4">
        <div className="grid gap-4 md:grid-cols-3">
          <article className="rounded-2xl border border-slate-200 p-4">
            <p className="text-sm text-slate-500">Pending actions</p>
            <p className="mt-2 text-2xl font-semibold">{workspace.summary.pending_actions_count}</p>
          </article>
          <article className="rounded-2xl border border-slate-200 p-4">
            <p className="text-sm text-slate-500">Assignments</p>
            <p className="mt-2 text-2xl font-semibold">{workspace.summary.assignment_count}</p>
          </article>
          <article className="rounded-2xl border border-slate-200 p-4">
            <p className="text-sm text-slate-500">Mastery matrices</p>
            <p className="mt-2 text-2xl font-semibold">{workspace.summary.mastery_matrix_count}</p>
          </article>
        </div>
        <div>
          <h3 className="text-base font-semibold text-slate-900">Recent assignments</h3>
          <ul className="mt-3 space-y-2">
            {recentAssignments.length === 0 ? (
              <li className="text-sm text-slate-500">No assignments yet.</li>
            ) : (
              recentAssignments.map((item) => (
                <li key={String(item.assignment_id)}>
                  <Link href={String(item.navigation_href)} className="text-sm font-medium text-sky-700">
                    {String(item.title)}
                  </Link>
                </li>
              ))
            )}
          </ul>
        </div>
      </div>
    );
  }

  const tab = tabs[activeTab];
  if (!tab) {
    return <p className="text-sm text-slate-500">No data for this tab.</p>;
  }

  const navigationHref = tab.navigation_href as string | undefined;
  const items = (tab.items as Array<Record<string, unknown>>) ?? [];
  const openItems = (tab.open_items as Array<Record<string, unknown>>) ?? [];

  return (
    <div className="space-y-4">
      {navigationHref ? (
        <Link href={navigationHref} className="ta-button-secondary inline-flex text-sm">
          Open full {labelize(activeTab)} view
        </Link>
      ) : null}
      {(items.length > 0 ? items : openItems).map((item, index) => {
        const href = item.navigation_href as string | undefined;
        const title = (item.title as string) ?? `Item ${index + 1}`;
        return href ? (
          <div key={href} className="rounded-2xl border border-slate-200 px-4 py-3">
            <Link href={href} className="text-sm font-semibold text-sky-700">
              {title}
            </Link>
          </div>
        ) : (
          <div key={`${activeTab}-${index}`} className="rounded-2xl border border-slate-200 px-4 py-3 text-sm">
            {title}
          </div>
        );
      })}
      {items.length === 0 && openItems.length === 0 ? (
        <TeacherAssistEmptyState
          title={`No ${labelize(activeTab).toLowerCase()} items`}
          description={`Manage ${labelize(activeTab).toLowerCase()} for this class from the dedicated module.`}
          actionLabel={navigationHref ? `Open ${labelize(activeTab)}` : undefined}
          actionHref={navigationHref}
        />
      ) : null}
    </div>
  );
}

export function TeacherAssistClassWorkspaceScreen({ classId: classIdProp }: { classId?: string }) {
  const searchParams = useSearchParams();
  const classId = classIdProp ?? searchParams.get("id") ?? "";
  const [workspace, setWorkspace] = useState<TeacherAssistClassOperationalWorkspace | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>("overview");
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (!classId) return;
    setWorkspace(await fetchTeacherAssistClassOperationalWorkspace(classId));
  }, [classId]);

  useEffect(() => {
    if (!classId) return;
    load().catch((err: Error) => setError(err.message));
  }, [classId, load]);

  const tabLabels = useMemo(
    () => Object.fromEntries(TAB_KEYS.map((key) => [key, labelize(key)])) as Record<TabKey, string>,
    [],
  );

  if (!classId) {
    return (
      <div className="ta-panel p-6 text-sm text-slate-600">
        Missing class id. Open a class from Home or the class list.
      </div>
    );
  }

  if (error) {
    return <p className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-800">{error}</p>;
  }
  if (!workspace) {
    return <p className="text-sm text-slate-600">Loading class workspace...</p>;
  }

  return (
    <div className="space-y-6">
      <header className="ta-panel p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Class workspace</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">{workspace.class_name}</h1>
        <p className="mt-2 text-sm text-slate-600">
          Grade {workspace.grade_level ?? "—"} · {workspace.student_count ?? 0} students ·{" "}
          {workspace.summary.pending_actions_count} pending actions
        </p>
        <div className="mt-4 flex flex-wrap gap-2">
          <Link href={`/teacher-assist/work-queue?class_id=${classId}`} className="ta-button-secondary text-sm">
            Review work
          </Link>
          <Link href={`/teacher-assist/assignments?class_id=${classId}`} className="ta-button-secondary text-sm">
            Open assignments
          </Link>
          <Link href={`/teacher-assist/mastery?class_id=${classId}`} className="ta-button-secondary text-sm">
            View mastery
          </Link>
          <Link href={`/teacher-assist/reteach-plans?class_id=${classId}`} className="ta-button-secondary text-sm">
            Create reteach plan
          </Link>
        </div>
      </header>

      <nav className="flex flex-wrap gap-2">
        {TAB_KEYS.map((key) => (
          <button
            key={key}
            type="button"
            onClick={() => setActiveTab(key)}
            className={`rounded-2xl border px-3 py-2 text-sm font-semibold transition ${
              activeTab === key
                ? "border-sky-300 bg-sky-50 text-sky-900"
                : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50"
            }`}
          >
            {tabLabels[key]}
          </button>
        ))}
      </nav>

      <article className="ta-panel p-5">
        <TabPanel workspace={workspace} activeTab={activeTab} />
      </article>
    </div>
  );
}
