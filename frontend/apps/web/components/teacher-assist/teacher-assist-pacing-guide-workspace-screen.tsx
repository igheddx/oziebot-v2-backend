"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { TeacherAssistOnboardingGatePanel } from "@/components/teacher-assist/teacher-assist-onboarding-gate-panel";
import { useTeacherAssistOnboarding } from "@/components/teacher-assist/teacher-assist-onboarding-context";
import {
  coercePacingGuideId,
  fetchCatalogPacingGuides,
  fetchPacingGuideWorkspace,
  updateActivePacingGuideSelection,
  updatePacingPeriodNotes,
} from "@/lib/pacing-guide-api";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

type PeriodPayload = {
  id?: string;
  title?: string;
  description?: string | null;
  objectives?: Array<{ objective_code?: string | null; objective_description?: string | null }>;
  resources?: Array<{ resource_title?: string | null; resource_type?: string | null; notes?: string | null }>;
  teacher_notes?: string | null;
  linked_plans?: Array<{ id: string; title: string; navigation_href: string }>;
};

function PeriodCard({ period, label }: { period: PeriodPayload | null | undefined; label: string }) {
  if (!period) {
    return (
      <div className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-3 text-sm text-slate-600">
        No {label.toLowerCase()} resolved yet.
      </div>
    );
  }
  return (
    <div className="rounded-xl border border-slate-200 bg-white px-3 py-3">
      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</p>
      <p className="mt-1 text-sm font-semibold text-slate-900">{period.title}</p>
      {period.description ? <p className="mt-1 text-sm text-slate-600">{period.description}</p> : null}
      <div className="mt-2 flex flex-wrap gap-1.5">
        {(period.objectives ?? []).map((row, index) => (
          <span key={`${row.objective_code}-${index}`} className="rounded-full bg-emerald-50 px-2 py-0.5 text-xs font-semibold text-emerald-700">
            {row.objective_code}
          </span>
        ))}
      </div>
    </div>
  );
}

export function TeacherAssistPacingGuideWorkspaceScreen() {
  const { isComplete: onboardingComplete, loading: onboardingLoading } = useTeacherAssistOnboarding();
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [workspace, setWorkspace] = useState<Awaited<ReturnType<typeof fetchPacingGuideWorkspace>> | null>(null);
  const [guides, setGuides] = useState<Awaited<ReturnType<typeof fetchCatalogPacingGuides>>>([]);
  const [selectedGuideId, setSelectedGuideId] = useState("");
  const [selectedPeriodId, setSelectedPeriodId] = useState("");
  const [notesDraft, setNotesDraft] = useState("");

  const refresh = useCallback(async (guideId?: string, periodId?: string) => {
    setLoading(true);
    clearSectionAlert("workspace");
    try {
      const guideRows = await fetchCatalogPacingGuides({ active_only: true });
      const resolvedGuideId =
        coercePacingGuideId(guideId) ??
        coercePacingGuideId(selectedGuideId) ??
        undefined;
      const resolvedPeriodId =
        coercePacingGuideId(periodId) ??
        coercePacingGuideId(selectedPeriodId) ??
        undefined;
      const payload = await fetchPacingGuideWorkspace({
        guide_id: resolvedGuideId,
        period_id: resolvedPeriodId,
      });
      setGuides(guideRows);
      setWorkspace(payload);
      const context = payload.current_week_context as Record<string, unknown>;
      if (!guideId) {
        setSelectedGuideId(
          coercePacingGuideId(context.active_pacing_guide_id) ??
            coercePacingGuideId(context.pacing_guide) ??
            coercePacingGuideId(guideRows[0]?.id) ??
            "",
        );
      }
      const period = (payload.selected_period ?? context.current_week) as PeriodPayload | null;
      if (!periodId) {
        setSelectedPeriodId(coercePacingGuideId(period?.id) ?? "");
      }
      setNotesDraft(period?.teacher_notes ?? "");
    } catch (nextError) {
      setSectionAlert(
        "workspace",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load workspace.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, selectedGuideId, selectedPeriodId, setSectionAlert]);

  useEffect(() => {
    if (!onboardingComplete) return;
    void refresh();
  }, [onboardingComplete, refresh]);

  const context = (workspace?.current_week_context ?? {}) as Record<string, unknown>;
  const selectedPeriod = (workspace?.selected_period ?? context.current_week) as PeriodPayload | null;
  const coverage = workspace?.objective_coverage as
    | { objectives?: Array<{ objective_code?: string; coverage_status?: string; period_title?: string }>; summary?: Record<string, number> }
    | undefined;
  const progress = context.teaching_progress as Record<string, number> | undefined;

  const weeklyPlanningHref = selectedPeriod?.id
    ? `/teacher-assist/planning/weeks?period_id=${selectedPeriod.id}`
    : "/teacher-assist/planning/weeks";
  const uploadResourcesHref = "/teacher-assist/resources";

  if (!onboardingLoading && !onboardingComplete) {
    return (
      <div id="pacing-workspace-panel" className="space-y-4">
        <TeacherAssistOnboardingGatePanel title="Finish setup before using Pacing Workspace" />
      </div>
    );
  }

  return (
    <div id="pacing-workspace-panel" className="space-y-4">
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Planning</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">Pacing Guide Workspace</h1>
        <p className="mt-1 text-sm text-slate-600">
          Review your active guide and current week, then open weekly planning to upload materials and generate your plan.
        </p>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("workspace")} />

      <section className="ta-panel grid gap-3 p-4 md:grid-cols-[1fr_auto] md:items-end">
        <label className="space-y-1 text-sm">
          <span className="font-medium text-slate-700">Active pacing guide</span>
          <select
            className="ta-input"
            value={selectedGuideId}
            onChange={(event) => setSelectedGuideId(event.target.value)}
          >
            <option value="">Select guide</option>
            {guides.map((guide) => (
              <option key={guide.id} value={coercePacingGuideId(guide.id) ?? ""}>
                {guide.title} ({guide.guide_type})
              </option>
            ))}
          </select>
        </label>
        <button
          type="button"
          className="ta-button-primary"
          onClick={() => {
            void withPreservedScroll("pacing-workspace-panel", async () => {
              await updateActivePacingGuideSelection({
                active_pacing_guide_id: coercePacingGuideId(selectedGuideId) ?? null,
              });
              setSectionAlert("workspace", sectionSuccess("Active pacing guide updated."));
              await refresh();
            }).catch((nextError) => {
              setSectionAlert(
                "workspace",
                sectionError(nextError instanceof Error ? nextError.message : "Could not update guide.", "Update failed"),
              );
            });
          }}
        >
          Set active guide
        </button>
      </section>

      {loading || !workspace ? (
        <p className="text-sm text-slate-600">Loading pacing workspace...</p>
      ) : (
        <>
          <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
            <article className="ta-panel space-y-3 p-4">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <h2 className="text-base font-semibold text-slate-900">Current week</h2>
                <Link href="/teacher-assist/home" className="text-xs font-semibold text-sky-700">
                  Back to home
                </Link>
              </div>
              <div className="grid gap-2 text-sm text-slate-600 md:grid-cols-2">
                <p>School year: {(context.school_year as { title?: string } | null)?.title ?? "Not set"}</p>
                <p>Guide: {(context.pacing_guide as { title?: string } | null)?.title ?? "Not selected"}</p>
                <p>Grading period: {(context.grading_period as { title?: string } | null)?.title ?? "Not resolved"}</p>
                <p>Unit: {(context.guide_unit as { title?: string } | null)?.title ?? "Not resolved"}</p>
              </div>
              <PeriodCard period={context.current_week as PeriodPayload} label="Current week" />
              <PeriodCard period={context.upcoming_week as PeriodPayload} label="Upcoming week" />
              <div className="flex flex-wrap gap-2">
                <Link href={weeklyPlanningHref} className="ta-button-primary text-xs">
                  Open weekly planning
                </Link>
                <Link href={uploadResourcesHref} className="ta-button-secondary text-xs">
                  Upload curriculum files
                </Link>
              </div>
            </article>

            <article className="ta-panel space-y-3 p-4">
              <h2 className="text-base font-semibold text-slate-900">Teaching progress</h2>
              {progress ? (
                <div className="grid gap-2 text-sm">
                  <p>{progress.weeks_completed}/{progress.weeks_total} weeks completed</p>
                  <p>{progress.objectives_covered}/{progress.objectives_total} objectives covered</p>
                  <p>{progress.objectives_remaining} objectives remaining</p>
                </div>
              ) : (
                <p className="text-sm text-slate-600">Progress will appear once a pacing guide is active.</p>
              )}
              <h3 className="text-sm font-semibold text-slate-900">Timeline</h3>
              <div className="max-h-72 space-y-1 overflow-y-auto">
                {(workspace.timeline as Array<{ id: string; period_type: string; title: string; is_current?: boolean }>).map((row) => (
                  <button
                    key={row.id}
                    type="button"
                    onClick={() => {
                      setSelectedPeriodId(row.id);
                      void updateActivePacingGuideSelection({ manual_pacing_period_id: row.id }).then(() => refresh());
                    }}
                    className={`flex w-full items-center justify-between rounded-lg border px-3 py-2 text-left text-sm ${
                      row.is_current ? "border-sky-300 bg-sky-50" : "border-slate-200 bg-white"
                    }`}
                  >
                    <span>{row.title}</span>
                    <span className="text-xs uppercase text-slate-500">{row.period_type}</span>
                  </button>
                ))}
              </div>
            </article>
          </section>

          <section className="grid gap-4 xl:grid-cols-[1fr_1fr]">
            <article className="ta-panel space-y-3 p-4">
              <h2 className="text-base font-semibold text-slate-900">Selected week detail</h2>
              {selectedPeriod ? (
                <>
                  <p className="text-sm font-medium text-slate-900">{selectedPeriod.title}</p>
                  {selectedPeriod.description ? <p className="text-sm text-slate-600">{selectedPeriod.description}</p> : null}
                  <div className="space-y-2">
                    {(selectedPeriod.resources ?? []).map((resource, index) => (
                      <div key={`${resource.resource_title}-${index}`} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-sm">
                        <p className="font-medium text-slate-900">{resource.resource_title}</p>
                        <p className="text-xs text-slate-500">{resource.resource_type}</p>
                      </div>
                    ))}
                  </div>
                  {(selectedPeriod.linked_plans ?? []).length > 0 ? (
                    <div className="space-y-1">
                      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Linked plans</p>
                      {selectedPeriod.linked_plans?.map((plan) => (
                        <Link key={plan.id} href={plan.navigation_href} className="block text-sm font-semibold text-sky-700">
                          {plan.title}
                        </Link>
                      ))}
                    </div>
                  ) : null}
                  <label className="block space-y-1 text-sm">
                    <span className="font-medium text-slate-700">Teacher notes</span>
                    <textarea
                      className="ta-input min-h-24"
                      value={notesDraft}
                      onChange={(event) => setNotesDraft(event.target.value)}
                    />
                  </label>
                  <button
                    type="button"
                    className="ta-button-secondary"
                    onClick={() => {
                      if (!selectedPeriod.id) return;
                      const guideId = coercePacingGuideId(selectedGuideId);
                      if (!guideId) return;
                      void withPreservedScroll("pacing-workspace-panel", async () => {
                        await updatePacingPeriodNotes(guideId, selectedPeriod.id!, notesDraft || null);
                        setSectionAlert("workspace", sectionSuccess("Week notes saved."));
                        await refresh();
                      }).catch((nextError) => {
                        setSectionAlert(
                          "workspace",
                          sectionError(nextError instanceof Error ? nextError.message : "Could not save notes.", "Save failed"),
                        );
                      });
                    }}
                  >
                    Save notes
                  </button>
                </>
              ) : (
                <p className="text-sm text-slate-600">Select a timeline week to inspect resources and notes.</p>
              )}
            </article>

            <article className="ta-panel space-y-3 p-4">
              <h2 className="text-base font-semibold text-slate-900">Objective coverage</h2>
              {coverage?.objectives?.length ? (
                <div className="max-h-80 space-y-2 overflow-y-auto">
                  {coverage.objectives.map((row, index) => (
                    <div key={`${row.objective_code}-${index}`} className="rounded-lg border border-slate-200 px-3 py-2 text-sm">
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-semibold text-slate-900">{row.objective_code}</span>
                        <span className="text-xs uppercase text-slate-500">{row.coverage_status?.replaceAll("_", " ")}</span>
                      </div>
                      <p className="text-xs text-slate-600">{row.period_title}</p>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-sm text-slate-600">No objective coverage data yet.</p>
              )}
            </article>
          </section>
        </>
      )}
    </div>
  );
}
