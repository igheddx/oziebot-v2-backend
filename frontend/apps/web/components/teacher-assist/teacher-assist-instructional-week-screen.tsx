"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  createInstructionalWeekSnapshot,
  fetchInstructionalWeekWorkspace,
  generateNextInstructionalWeek,
} from "@/lib/instructional-week-api";

type TabKey =
  | "overview"
  | "lessons"
  | "assignments"
  | "assessments"
  | "resources"
  | "newsletter"
  | "mastery"
  | "timeline"
  | "actions";

type ItemRow = {
  id?: string;
  title?: string;
  status?: string;
  artifact_type?: string;
  navigation_href?: string;
};

const TAB_LABELS: Record<TabKey, string> = {
  overview: "Overview",
  lessons: "Lessons",
  assignments: "Assignments",
  assessments: "Assessments",
  resources: "Resources",
  newsletter: "Newsletter",
  mastery: "Mastery",
  timeline: "Timeline",
  actions: "Action Center",
};

function readString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function readRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function ItemList({ items, emptyLabel }: { items: ItemRow[]; emptyLabel: string }) {
  if (items.length === 0) {
    return <p className="text-sm text-slate-500">{emptyLabel}</p>;
  }
  return (
    <div className="space-y-2">
      {items.map((row, index) => (
        <div key={row.id ?? `${row.title}-${index}`} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5">
          <div>
            <p className="text-sm font-semibold text-slate-900">{row.title ?? "Untitled"}</p>
            <p className="text-xs text-slate-500">
              {row.artifact_type ?? "Item"} {row.status ? `· ${row.status}` : ""}
            </p>
          </div>
          {row.navigation_href ? (
            <Link href={row.navigation_href} className="ta-button-secondary text-xs">
              Open
            </Link>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function HealthBadge({ label, ready }: { label: string; ready: boolean }) {
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-xs font-semibold ${
        ready ? "bg-emerald-50 text-emerald-700" : "bg-slate-100 text-slate-600"
      }`}
    >
      {label}
    </span>
  );
}

type MasteryTabPayload = {
  objective_coverage?: {
    objectives?: Array<{
      objective_code?: string | null;
      objective_description?: string | null;
      coverage_status?: string;
      period_title?: string;
    }>;
    summary?: {
      total?: number;
      covered?: number;
      planned?: number;
      not_yet_scheduled?: number;
    };
  };
  expected_mastery?: Array<{ objective_code?: string | null; source_type?: string; is_required?: boolean }>;
  assessment_coverage?: ItemRow[];
};

function MasteryTab({ mastery }: { mastery: MasteryTabPayload }) {
  const coverage = mastery.objective_coverage ?? {};
  const summary = coverage.summary ?? {};
  const statusClass = (status?: string) => {
    if (status === "covered") return "bg-emerald-50 text-emerald-700";
    if (status === "planned") return "bg-sky-50 text-sky-700";
    return "bg-slate-100 text-slate-600";
  };

  return (
    <article className="ta-panel space-y-4 p-4">
      <div>
        <h2 className="text-base font-semibold text-slate-900">Mastery</h2>
        <p className="mt-1 text-sm text-slate-600">
          Objective coverage and assessment alignment for this instructional week.
        </p>
      </div>

      <div className="grid gap-3 sm:grid-cols-4">
        {[
          { label: "Total objectives", value: summary.total ?? 0 },
          { label: "Covered", value: summary.covered ?? 0 },
          { label: "Planned", value: summary.planned ?? 0 },
          { label: "Not scheduled", value: summary.not_yet_scheduled ?? 0 },
        ].map((row) => (
          <div key={row.label} className="rounded-xl border border-slate-200 px-3 py-2.5">
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{row.label}</p>
            <p className="mt-1 text-xl font-semibold text-slate-900">{row.value}</p>
          </div>
        ))}
      </div>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold text-slate-900">Week objectives</h3>
        {(mastery.expected_mastery ?? []).length === 0 ? (
          <p className="text-sm text-slate-500">No objectives linked to this week yet.</p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {(mastery.expected_mastery ?? []).map((row, index) => (
              <span
                key={`${row.objective_code}-${index}`}
                className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700"
              >
                {row.objective_code ?? "Objective"}
              </span>
            ))}
          </div>
        )}
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold text-slate-900">Coverage by objective</h3>
        {(coverage.objectives ?? []).length === 0 ? (
          <p className="text-sm text-slate-500">Coverage data appears once objectives are scheduled in the pacing guide.</p>
        ) : (
          <div className="max-h-72 space-y-2 overflow-y-auto">
            {(coverage.objectives ?? []).map((row, index) => (
              <div key={`${row.objective_code}-${index}`} className="rounded-lg border border-slate-200 px-3 py-2 text-sm">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <span className="font-semibold text-slate-900">{row.objective_code ?? "Objective"}</span>
                  <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${statusClass(row.coverage_status)}`}>
                    {(row.coverage_status ?? "unknown").replaceAll("_", " ")}
                  </span>
                </div>
                {row.objective_description ? <p className="mt-1 text-xs text-slate-600">{row.objective_description}</p> : null}
                {row.period_title ? <p className="mt-1 text-xs text-slate-500">{row.period_title}</p> : null}
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="space-y-2">
        <h3 className="text-sm font-semibold text-slate-900">Linked assessments</h3>
        <ItemList items={mastery.assessment_coverage ?? []} emptyLabel="No assessments linked to this week yet." />
      </section>
    </article>
  );
}

export function TeacherAssistInstructionalWeekScreen({ weekId: weekIdProp }: { weekId?: string }) {
  const router = useRouter();
  const searchParams = useSearchParams();
  const weekId = weekIdProp ?? searchParams.get("id") ?? "";
  const requestedTab = (searchParams.get("tab") as TabKey | null) ?? "overview";
  const requestedAction = searchParams.get("action");
  const actionRef = useRef<string | null>(null);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>(requestedTab);
  const [workspace, setWorkspace] = useState<Awaited<ReturnType<typeof fetchInstructionalWeekWorkspace>> | null>(null);

  const refresh = useCallback(async () => {
    if (!weekId) return;
    setLoading(true);
    clearSectionAlert("instructionalWeek");
    try {
      const payload = await fetchInstructionalWeekWorkspace(weekId);
      setWorkspace(payload);
    } catch (nextError) {
      setSectionAlert(
        "instructionalWeek",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load instructional week.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, setSectionAlert, weekId]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (requestedTab) setTab(requestedTab);
  }, [requestedTab]);

  const week = readRecord(workspace?.instructional_week);
  const tabs = readRecord(workspace?.tabs);
  const overview = readRecord(tabs.overview);
  const health = readRecord(workspace?.health_indicators);
  const actionCenter = (workspace?.action_center ?? []) as Array<{ action_key: string; label: string; navigation_href: string }>;
  const legacyHref = workspace?.legacy_pacing_workspace_href as string | undefined;

  const runGenerateNextWeek = useCallback(async () => {
    setBusyAction("generate_next_week");
    clearSectionAlert("instructionalWeek");
    try {
      const payload = await generateNextInstructionalWeek(weekId);
      setSectionAlert(
        "instructionalWeek",
        sectionSuccess(`Next week draft prepared for ${payload.next_period_title}.`, "Next week ready"),
      );
      if (payload.navigation_href) router.push(String(payload.navigation_href));
    } catch (nextError) {
      setSectionAlert(
        "instructionalWeek",
        sectionError(nextError instanceof Error ? nextError.message : "Could not generate next week.", "Action failed"),
      );
    } finally {
      setBusyAction(null);
    }
  }, [clearSectionAlert, router, setSectionAlert, weekId]);

  useEffect(() => {
    if (!requestedAction || loading || !workspace) return;
    if (actionRef.current === requestedAction) return;
    actionRef.current = requestedAction;
    if (requestedAction === "generate_next_week") {
      void runGenerateNextWeek();
    }
  }, [loading, requestedAction, runGenerateNextWeek, workspace]);

  const tabContent = useMemo(() => {
    if (!workspace) return null;
    switch (tab) {
      case "overview":
        return (
          <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
            <article className="ta-panel space-y-3 p-4">
              <h2 className="text-base font-semibold text-slate-900">Week overview</h2>
              <p className="text-sm text-slate-600">
                {readString(overview.description, readString(week.description, "No description yet."))}
              </p>
              <div className="flex flex-wrap gap-1.5">
                {(Array.isArray(overview.objectives) ? overview.objectives : []).map((row, index: number) => {
                  const objective = readRecord(row);
                  return (
                  <span key={`${readString(objective.objective_code, readString(objective.id, String(index)))}-${index}`} className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700">
                    {readString(objective.objective_code, "Objective")}
                  </span>
                  );
                })}
              </div>
              {readString(overview.notes) ? (
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Notes</p>
                  <p className="mt-1 text-sm text-slate-600">{readString(overview.notes)}</p>
                </div>
              ) : null}
            </article>
            <article className="ta-panel space-y-3 p-4">
              <h2 className="text-base font-semibold text-slate-900">Week health</h2>
              <div className="flex flex-wrap gap-2">
                <HealthBadge label="Objectives" ready={Boolean(health.objectives_covered)} />
                <HealthBadge label="Lessons" ready={Boolean(health.lessons_created)} />
                <HealthBadge label="Assignments" ready={Boolean(health.assignments_created)} />
                <HealthBadge label="Assessments" ready={Boolean(health.assessments_created)} />
                <HealthBadge label="Resources" ready={Boolean(health.resources_attached)} />
                <HealthBadge label="Newsletter" ready={Boolean(health.newsletter_ready)} />
                <HealthBadge label="Mastery" ready={Boolean(health.mastery_coverage)} />
              </div>
            </article>
          </section>
        );
      case "lessons":
        return (
          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Lessons</h2>
            <ItemList items={(readRecord(tabs.lessons).items as ItemRow[] | undefined) ?? []} emptyLabel="No lessons linked to this week yet." />
          </article>
        );
      case "assignments":
        return (
          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Assignments</h2>
            <ItemList items={(readRecord(tabs.assignments).items as ItemRow[] | undefined) ?? []} emptyLabel="No assignments for this week yet." />
          </article>
        );
      case "assessments":
        return (
          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Assessments</h2>
            <ItemList items={(readRecord(tabs.assessments).items as ItemRow[] | undefined) ?? []} emptyLabel="No assessments for this week yet." />
          </article>
        );
      case "resources":
        return (
          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Resources</h2>
            <ItemList items={(readRecord(tabs.resources).items as ItemRow[] | undefined) ?? []} emptyLabel="No resources attached yet." />
          </article>
        );
      case "newsletter":
        return (
          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Newsletter</h2>
            <ItemList items={(readRecord(tabs.newsletter).items as ItemRow[] | undefined) ?? []} emptyLabel="No newsletter drafts for this week yet." />
          </article>
        );
      case "mastery":
        return <MasteryTab mastery={readRecord(tabs.mastery) as MasteryTabPayload} />;
      case "timeline":
        return (
          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Timeline</h2>
            <ItemList items={(readRecord(tabs.timeline).items as ItemRow[] | undefined) ?? []} emptyLabel="No week activity yet." />
          </article>
        );
      case "actions":
        return (
          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Recommended actions</h2>
            <div className="flex flex-wrap gap-2">
              {actionCenter.map((action) => (
                <Link key={action.action_key} href={action.navigation_href} className="ta-button-secondary text-xs">
                  {action.label}
                </Link>
              ))}
              <button
                type="button"
                className="ta-button-primary text-xs"
                disabled={busyAction === "snapshot"}
                onClick={() => {
                  setBusyAction("snapshot");
                  void createInstructionalWeekSnapshot(weekId, `${readString(week.title, "Week")} Snapshot`)
                    .then(() => {
                      setSectionAlert("instructionalWeek", sectionSuccess("Week snapshot saved.", "Snapshot saved"));
                    })
                    .catch((nextError: Error) => {
                      setSectionAlert("instructionalWeek", sectionError(nextError.message, "Snapshot failed"));
                    })
                    .finally(() => setBusyAction(null));
                }}
              >
                {busyAction === "snapshot" ? "Saving..." : "Save week snapshot"}
              </button>
              <button
                type="button"
                className="ta-button-primary text-xs"
                disabled={busyAction === "generate_next_week"}
                onClick={() => void runGenerateNextWeek()}
              >
                {busyAction === "generate_next_week" ? "Generating..." : "Generate next week"}
              </button>
            </div>
          </article>
        );
      default:
        return null;
    }
  }, [actionCenter, busyAction, health, overview, runGenerateNextWeek, setSectionAlert, tab, tabs, week.description, week.title, weekId, workspace]);

  return (
    <div className="space-y-4">
      {!weekId ? (
        <div className="ta-panel p-6 text-sm text-slate-600">
          Missing instructional week id. Open a week from Home or the pacing workspace.
        </div>
      ) : (
        <>
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Instructional Week</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">{readString(week.title, "Week workspace")}</h1>
        <p className="mt-1 text-sm text-slate-600">
          {readString(overview.subject, "Subject")} · Grade {readString(overview.grade, "—")} ·{" "}
          {readString(readRecord(overview.dates).start_date, "—")} to {readString(readRecord(overview.dates).end_date, "—")} ·{" "}
          {readString(week.status, "DRAFT")}
        </p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Link href="/teacher-assist/home" className="ta-button-secondary text-xs">
            Home
          </Link>
          {legacyHref ? (
            <Link href={legacyHref} className="ta-button-secondary text-xs">
              Legacy week tools
            </Link>
          ) : null}
        </div>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("instructionalWeek")} onDismiss={() => clearSectionAlert("instructionalWeek")} />

      <section className="ta-panel p-3">
        <div className="flex flex-wrap gap-2">
          {(Object.keys(TAB_LABELS) as TabKey[]).map((key) => (
            <button
              key={key}
              type="button"
              className={`rounded-full px-3 py-1 text-xs font-semibold ${
                tab === key ? "bg-sky-600 text-white" : "bg-slate-100 text-slate-700"
              }`}
              onClick={() => setTab(key)}
            >
              {TAB_LABELS[key]}
            </button>
          ))}
        </div>
      </section>

      {loading || !workspace ? <p className="text-sm text-slate-600">Loading instructional week workspace...</p> : tabContent}
        </>
      )}
    </div>
  );
}
