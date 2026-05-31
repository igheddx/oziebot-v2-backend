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
  createInstructionalWeekFromPeriod,
  fetchInstructionalWeekByPeriod,
} from "@/lib/instructional-week-api";
import {
  duplicatePacingWeek,
  duplicateWeekArtifact,
  fetchWeekWorkspace,
  generateNextWeek,
  generateWeekArtifact,
  saveWeekTemplate,
} from "@/lib/pacing-guide-api";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

type ArtifactRow = {
  id: string;
  artifact_type: string;
  title: string;
  status: string;
  created_at: string;
  created_by_name?: string | null;
  version_count?: number;
  navigation_href: string;
};

type TabKey = "overview" | "artifacts" | "history" | "recommendations";

const ARTIFACT_LABELS: Record<string, string> = {
  LESSON_PLAN: "Lesson Plan",
  ASSIGNMENT: "Assignment",
  QUIZ: "Quiz",
  RUBRIC: "Rubric",
  NEWSLETTER: "Newsletter",
  PARENT_COMMUNICATION: "Parent Communication",
};

function formatDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function readString(value: unknown, fallback = ""): string {
  return typeof value === "string" ? value : fallback;
}

function readRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

export function TeacherAssistWeekWorkspaceScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const periodId = searchParams.get("period_id") ?? "";
  const requestedAction = searchParams.get("action");
  const actionPrefillRef = useRef<string | null>(null);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [workspace, setWorkspace] = useState<Awaited<ReturnType<typeof fetchWeekWorkspace>> | null>(null);
  const [instructionalWeekHref, setInstructionalWeekHref] = useState<string | null>(null);
  const [tab, setTab] = useState<TabKey>("overview");

  const refresh = useCallback(async () => {
    if (!periodId) return;
    setLoading(true);
    clearSectionAlert("weekWorkspace");
    try {
      const payload = await fetchWeekWorkspace(periodId);
      setWorkspace(payload);
      try {
        const instructionalWeek = await fetchInstructionalWeekByPeriod(periodId);
        setInstructionalWeekHref(`/teacher-assist/week/?id=${encodeURIComponent(String(instructionalWeek.id))}`);
      } catch {
        setInstructionalWeekHref(null);
      }
    } catch (nextError) {
      setSectionAlert(
        "weekWorkspace",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load week workspace.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, periodId, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const weekContext = (workspace?.week_context ?? {}) as Record<string, unknown>;
  const period = (workspace?.period ?? {}) as Record<string, unknown>;
  const artifacts = (workspace?.generated_artifacts ?? []) as ArtifactRow[];
  const history = (workspace?.generation_history ?? []) as ArtifactRow[];
  const weekActions = (workspace?.week_actions ?? []) as Array<{ action_key: string; label: string; artifact_type: string }>;
  const recommendations = (workspace?.recommendations ?? {}) as Record<string, unknown>;
  const topReuse = (recommendations.recommended_for_this_week as { top_reusable?: Array<Record<string, unknown>> } | undefined)?.top_reusable ?? [];

  const artifactLibrary = (workspace?.artifact_library ?? {}) as Record<string, ArtifactRow[]>;

  const runSpecialAction = useCallback(
    async (actionKey: string) => {
      if (!periodId) return;
      setBusyAction(actionKey);
      clearSectionAlert("weekWorkspace");
      try {
        if (actionKey === "generate_next_week") {
          const payload = await generateNextWeek(periodId);
          setSectionAlert(
            "weekWorkspace",
            sectionSuccess(`Draft prepared for ${payload.next_period_title}. Review before saving.`, "Next week draft ready"),
          );
          if (payload.navigation_href) router.push(String(payload.navigation_href));
        } else if (actionKey === "duplicate_week") {
          const payload = await duplicatePacingWeek(periodId, { copy_objectives: true, copy_resources: true, copy_notes: true });
          setSectionAlert("weekWorkspace", sectionSuccess("Week duplicated.", "Week duplicated"));
          if (payload.navigation_href) router.push(String(payload.navigation_href));
        } else if (actionKey === "save_template") {
          await saveWeekTemplate(periodId, {
            name: `${readString(weekContext.period_title, "Week")} Template`,
            artifact_type: "WEEK",
            template_type: "TEACHER",
            visibility: "PRIVATE",
          });
          setSectionAlert("weekWorkspace", sectionSuccess("Week saved as template.", "Template saved"));
        } else if (actionKey === "create_instructional_week") {
          const created = await createInstructionalWeekFromPeriod(periodId, { status: "ACTIVE" });
          const href = `/teacher-assist/week/?id=${encodeURIComponent(String(created.id))}`;
          setInstructionalWeekHref(href);
          setSectionAlert("weekWorkspace", sectionSuccess("Instructional week created.", "Week ready"));
          router.push(href);
        }
        await refresh();
      } catch (nextError) {
        setSectionAlert(
          "weekWorkspace",
          sectionError(nextError instanceof Error ? nextError.message : "Action failed.", "Action failed"),
        );
      } finally {
        setBusyAction(null);
      }
    },
    [clearSectionAlert, periodId, refresh, router, setSectionAlert, weekContext.period_title],
  );

  const runGenerate = useCallback(
    async (artifactType: string, actionKey?: string) => {
      if (!periodId) return;
      setBusyAction(actionKey ?? artifactType);
      clearSectionAlert("weekWorkspace");
      try {
        const created = await withPreservedScroll("week-workspace-panel", async () =>
          generateWeekArtifact(periodId, { artifact_type: artifactType }),
        );
        setSectionAlert(
          "weekWorkspace",
          sectionSuccess(`${ARTIFACT_LABELS[artifactType] ?? artifactType} created from week context.`, "Generated"),
        );
        await refresh();
        if (created.navigation_href) {
          router.push(String(created.navigation_href));
        }
      } catch (nextError) {
        setSectionAlert(
          "weekWorkspace",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not generate artifact from week context.",
            "Generation failed",
          ),
        );
      } finally {
        setBusyAction(null);
      }
    },
    [clearSectionAlert, periodId, refresh, router, setSectionAlert],
  );

  useEffect(() => {
    if (!requestedAction || !periodId || loading || !workspace) return;
    if (actionPrefillRef.current === requestedAction) return;
    actionPrefillRef.current = requestedAction;
    if (["generate_next_week", "duplicate_week", "save_template", "create_instructional_week"].includes(requestedAction)) {
      void runSpecialAction(requestedAction);
      return;
    }
    const action = weekActions.find((row) => row.action_key === requestedAction);
    if (!action) return;
    void runGenerate(action.artifact_type, action.action_key);
  }, [loading, periodId, requestedAction, runGenerate, runSpecialAction, weekActions, workspace]);

  const librarySections = useMemo(
    () => [
      { key: "lesson_plans", label: "Lesson Plans" },
      { key: "assignments", label: "Assignments" },
      { key: "quizzes", label: "Quizzes" },
      { key: "rubrics", label: "Rubrics" },
      { key: "newsletters", label: "Newsletters" },
      { key: "parent_communications", label: "Parent Communication" },
    ],
    [],
  );

  if (!periodId) {
    return (
      <div className="ta-panel p-4">
        <p className="text-sm text-slate-600">Select a pacing week to open the week workspace.</p>
        <Link href="/teacher-assist/planning/pacing-guides/workspace" className="ta-button-primary mt-3 inline-flex text-xs">
          Open pacing workspace
        </Link>
      </div>
    );
  }

  return (
    <div id="week-workspace-panel" className="space-y-4">
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Week Workspace</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">
          {readString(weekContext.period_title, readString(period.title, "Selected week"))}
        </h1>
        <p className="mt-1 text-sm text-slate-600">
          {readString(weekContext.subject_name, "Subject")} · Grade{" "}
          {readString(weekContext.grade_display_name, readString(weekContext.grade_level, "—"))} ·{" "}
          {readString(weekContext.pacing_guide_title, "Pacing guide")}
        </p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Link href="/teacher-assist/home" className="ta-button-secondary text-xs">
            Home
          </Link>
          <Link href="/teacher-assist/planning/pacing-guides/workspace" className="ta-button-secondary text-xs">
            Pacing workspace
          </Link>
          {instructionalWeekHref ? (
            <Link href={instructionalWeekHref} className="ta-button-primary text-xs">
              Open instructional week
            </Link>
          ) : (
            <button
              type="button"
              className="ta-button-primary text-xs"
              onClick={() => void runSpecialAction("create_instructional_week")}
            >
              Create instructional week
            </button>
          )}
        </div>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("weekWorkspace")} onDismiss={() => clearSectionAlert("weekWorkspace")} />

      <section className="ta-panel p-3">
        <div className="flex flex-wrap gap-2">
          {(["overview", "recommendations", "artifacts", "history"] as TabKey[]).map((key) => (
            <button
              key={key}
              type="button"
              className={`rounded-full px-3 py-1 text-xs font-semibold ${
                tab === key ? "bg-sky-600 text-white" : "bg-slate-100 text-slate-700"
              }`}
              onClick={() => setTab(key)}
            >
              {key === "overview"
                ? "Overview"
                : key === "recommendations"
                  ? "Recommendations"
                  : key === "artifacts"
                    ? "Week Artifacts"
                    : "Generation History"}
            </button>
          ))}
        </div>
      </section>

      {loading || !workspace ? (
        <p className="text-sm text-slate-600">Loading week workspace...</p>
      ) : tab === "overview" ? (
        <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Week context</h2>
            <p className="text-sm text-slate-600">
              {readString(period.description, readString(weekContext.notes, "No week description yet."))}
            </p>
            <div className="flex flex-wrap gap-1.5">
              {(Array.isArray(period.objectives) ? period.objectives : Array.isArray(weekContext.objectives) ? weekContext.objectives : []).map(
                (row, index: number) => {
                  const objective = readRecord(row);
                  return (
                <span key={`${readString(objective.objective_code, String(index))}-${index}`} className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700">
                  {readString(objective.objective_code)}
                </span>
                  );
                },
              )}
            </div>
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Resources</p>
              <ul className="mt-2 space-y-1 text-sm text-slate-700">
                {(Array.isArray(period.resources) ? period.resources : Array.isArray(weekContext.resources) ? weekContext.resources : [])
                  .slice(0, 8)
                  .map((row, index: number) => {
                    const resource = readRecord(row);
                    return (
                  <li key={`${readString(resource.title, readString(resource.resource_title, String(index)))}-${index}`}>
                    {readString(resource.resource_title, readString(resource.title, "Resource"))}
                  </li>
                    );
                  })}
              </ul>
            </div>
            {readString(period.teacher_notes) || readString(weekContext.notes) ? (
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Teacher notes</p>
                <p className="mt-1 text-sm text-slate-600">{readString(period.teacher_notes, readString(weekContext.notes))}</p>
              </div>
            ) : null}
          </article>

          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Generate from this week</h2>
            <div className="flex flex-wrap gap-2">
              {weekActions.map((action) => {
                const isSpecial = ["generate_next_week", "duplicate_week", "save_template", "create_instructional_week"].includes(action.action_key);
                return (
                  <button
                    key={action.action_key}
                    type="button"
                    className={isSpecial ? "ta-button-primary text-xs" : "ta-button-secondary text-xs"}
                    disabled={busyAction === action.action_key}
                    onClick={() =>
                      void (isSpecial
                        ? runSpecialAction(action.action_key)
                        : runGenerate(action.artifact_type, action.action_key))
                    }
                  >
                    {busyAction === action.action_key ? "Working..." : action.label}
                  </button>
                );
              })}
            </div>
            {topReuse.length > 0 ? (
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Recommended for this week</p>
                <div className="mt-2 space-y-1.5">
                  {topReuse.slice(0, 3).map((row, index) => {
                    const reuse = readRecord(row);
                    const reuseScore = readRecord(reuse.reuse_score);
                    return (
                    <Link
                      key={`${readString(reuse.entity_type, String(index))}-${readString(reuse.entity_id, String(index))}`}
                      href={readString(reuse.navigation_href, "#")}
                      className="block rounded-lg border border-slate-200 px-2.5 py-2 text-xs hover:border-sky-300"
                    >
                      <span className="font-semibold text-slate-900">{readString(reuse.title, "Reuse candidate")}</span>
                      <span className="ml-2 text-slate-500">Score {readString(reuseScore.score, "—")}</span>
                    </Link>
                    );
                  })}
                </div>
                <button type="button" className="mt-2 text-xs font-semibold text-sky-700" onClick={() => setTab("recommendations")}>
                  View all recommendations
                </button>
              </div>
            ) : null}
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Generated so far</p>
              <p className="mt-1 text-sm text-slate-600">{artifacts.length} artifact{artifacts.length === 1 ? "" : "s"} linked to this week.</p>
            </div>
          </article>
        </section>
      ) : tab === "recommendations" ? (
        <section className="ta-panel space-y-4 p-4">
          <h2 className="text-base font-semibold text-slate-900">Recommended for this week</h2>
          {topReuse.length === 0 ? (
            <p className="text-sm text-slate-500">No reusable matches yet. Generate artifacts or save templates to build reuse history.</p>
          ) : (
            <div className="space-y-2">
              {topReuse.map((row, index) => {
                const reuse = readRecord(row);
                const reuseScore = readRecord(reuse.reuse_score);
                return (
                <div
                  key={`${readString(reuse.entity_type, String(index))}-${readString(reuse.entity_id, String(index))}`}
                  className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5"
                >
                  <div>
                    <p className="text-sm font-semibold text-slate-900">{readString(reuse.title, "Reuse candidate")}</p>
                    <p className="text-xs text-slate-500">
                      {readString(reuse.source, "Reuse candidate")} · Score {readString(reuseScore.score, "—")} ·{" "}
                      {readString(reuse.artifact_type, readString(reuse.entity_type, "Item"))}
                    </p>
                  </div>
                  <Link href={readString(reuse.navigation_href, "#")} className="ta-button-secondary text-xs">
                    Open
                  </Link>
                </div>
                );
              })}
            </div>
          )}
        </section>
      ) : tab === "artifacts" ? (
        <section className="space-y-4">
          {librarySections.map((section) => {
            const rows = artifactLibrary[section.key] ?? [];
            return (
              <article key={section.key} className="ta-panel p-4">
                <h2 className="text-base font-semibold text-slate-900">{section.label}</h2>
                {rows.length === 0 ? (
                  <p className="mt-2 text-sm text-slate-500">Nothing generated yet.</p>
                ) : (
                  <div className="mt-3 space-y-2">
                    {rows.map((row) => (
                      <div key={row.id} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5">
                        <div>
                          <p className="text-sm font-semibold text-slate-900">{row.title}</p>
                          <p className="text-xs text-slate-500">
                            {ARTIFACT_LABELS[row.artifact_type] ?? row.artifact_type} · {row.status} · v{row.version_count ?? 1}
                          </p>
                        </div>
                        <div className="flex flex-wrap gap-2">
                          <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                            Open
                          </Link>
                          <button
                            type="button"
                            className="ta-button-secondary text-xs"
                            onClick={() => {
                              void duplicateWeekArtifact(periodId, row.id)
                                .then(async () => {
                                  setSectionAlert("weekWorkspace", sectionSuccess("Artifact duplicated.", "Duplicated"));
                                  await refresh();
                                })
                                .catch((nextError: Error) => {
                                  setSectionAlert("weekWorkspace", sectionError(nextError.message, "Duplicate failed"));
                                });
                            }}
                          >
                            Duplicate
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </article>
            );
          })}
        </section>
      ) : (
        <section className="ta-panel p-4">
          <h2 className="text-base font-semibold text-slate-900">Generation history</h2>
          {history.length === 0 ? (
            <p className="mt-2 text-sm text-slate-500">No generation history for this week yet.</p>
          ) : (
            <div className="mt-3 space-y-2">
              {history.map((row) => (
                <div key={row.id} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <div>
                    <p className="font-semibold text-slate-900">{row.title}</p>
                    <p className="text-xs text-slate-500">
                      {ARTIFACT_LABELS[row.artifact_type] ?? row.artifact_type} · {formatDate(row.created_at)} ·{" "}
                      {row.created_by_name ?? "Teacher"} · v{row.version_count ?? 1}
                    </p>
                  </div>
                  <Link href={row.navigation_href} className="ta-button-secondary text-xs">
                    Open
                  </Link>
                </div>
              ))}
            </div>
          )}
        </section>
      )}
    </div>
  );
}
