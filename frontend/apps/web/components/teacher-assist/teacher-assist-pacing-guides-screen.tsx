"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";

import { useAuth } from "@/components/providers/auth-provider";
import { TeacherAssistOnboardingGatePanel } from "@/components/teacher-assist/teacher-assist-onboarding-gate-panel";
import { useTeacherAssistOnboarding } from "@/components/teacher-assist/teacher-assist-onboarding-context";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  copyCatalogPacingGuide,
  fetchCatalogPacingGuideDetail,
  fetchCatalogPacingGuides,
} from "@/lib/pacing-guide-api";
import {
  PACING_GUIDE_TYPE_OPTIONS,
  type CatalogPacingGuideDetail,
  type CatalogPacingGuideSummary,
  type PacingGuideType,
} from "@/lib/pacing-guide-types";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

function formatDateRange(start: string | null, end: string | null) {
  if (!start && !end) return "Dates not set";
  if (start && end) return `${start} to ${end}`;
  return start ?? end ?? "Dates not set";
}

export function TeacherAssistPacingGuidesScreen() {
  const { user } = useAuth();
  const { isComplete: onboardingComplete, loading: onboardingLoading } = useTeacherAssistOnboarding();
  const isRootAdmin = Boolean(user?.is_root_admin);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [guideType, setGuideType] = useState<PacingGuideType | "">("");
  const [guides, setGuides] = useState<CatalogPacingGuideSummary[]>([]);
  const [selectedGuideId, setSelectedGuideId] = useState("");
  const [detail, setDetail] = useState<CatalogPacingGuideDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [copying, setCopying] = useState(false);

  const loadGuides = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("pacing-guides");
    try {
      const rows = await fetchCatalogPacingGuides({
        guide_type: guideType || undefined,
        active_only: true,
      });
      setGuides(rows);
      setSelectedGuideId((current) => {
        if (current && rows.some((row) => row.id === current)) return current;
        return rows[0]?.id ?? "";
      });
    } catch (nextError) {
      setSectionAlert(
        "pacing-guides",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load pacing guides.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, guideType, setSectionAlert]);

  useEffect(() => {
    void loadGuides();
  }, [loadGuides]);

  useEffect(() => {
    if (!selectedGuideId) {
      setDetail(null);
      return;
    }
    setDetailLoading(true);
    void fetchCatalogPacingGuideDetail(selectedGuideId)
      .then(setDetail)
      .catch((nextError) => {
        setSectionAlert(
          "pacing-guides",
          sectionError(nextError instanceof Error ? nextError.message : "Could not load guide detail.", "Detail failed"),
        );
      })
      .finally(() => setDetailLoading(false));
  }, [selectedGuideId, setSectionAlert]);

  const selectedGuide = useMemo(
    () => guides.find((guide) => guide.id === selectedGuideId) ?? null,
    [guides, selectedGuideId],
  );

  const copyDistrictGuide = async () => {
    if (!detail) return;
    setCopying(true);
    clearSectionAlert("pacing-guides");
    try {
      await withPreservedScroll("pacing-guides-panel", async () => {
        const copied = await copyCatalogPacingGuide(detail.id, {
          target_guide_type: "TEACHER",
          title: `${detail.title} (My Copy)`,
        });
        setSectionAlert("pacing-guides", sectionSuccess("District guide copied to your teacher pacing guide."));
        await loadGuides();
        setSelectedGuideId(copied.id);
      });
    } catch (nextError) {
      setSectionAlert(
        "pacing-guides",
        sectionError(nextError instanceof Error ? nextError.message : "Copy failed.", "Unable to copy"),
      );
    } finally {
      setCopying(false);
    }
  };

  if (!onboardingLoading && !onboardingComplete) {
    return (
      <div id="pacing-guides-panel" className="space-y-5">
        <TeacherAssistOnboardingGatePanel
          title="Finish setup before browsing pacing guides"
          description="Pacing guides depend on your district, school, grade, and classroom setup. Complete onboarding first, then return here to browse and copy district guides."
        />
      </div>
    );
  }

  return (
    <div id="pacing-guides-panel" className="space-y-5">
      <section className="ta-panel p-5">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Planning</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">Pacing Guides</h1>
        <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
          Browse district, grade-level, and personal pacing guides aligned to the education catalog. Review weekly
          objectives and resources, then copy a district guide into your own teacher guide when you are ready. Open{" "}
          <Link href="/teacher-assist/planning/pacing-guides/workspace" className="font-semibold text-sky-700 underline">
            Pacing Workspace
          </Link>{" "}
          to work from your active guide week by week.
        </p>
        {isRootAdmin ? (
          <div className="mt-4 rounded-xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
            <p className="font-semibold">Root admin: create district pacing guides</p>
            <p className="mt-1 leading-6">
              There is no pacing-guide file upload yet. Create guides in{" "}
              <Link
                href="/teacher-assist/administration/education-catalog?tab=pacing_guides"
                className="font-semibold text-amber-900 underline"
              >
                Catalog Admin → Pacing Guides
              </Link>
              , or import objectives first under{" "}
              <Link
                href="/teacher-assist/administration/education-catalog?tab=objectives"
                className="font-semibold text-amber-900 underline"
              >
                Objectives (CSV)
              </Link>
              .
            </p>
          </div>
        ) : null}
      </section>

      <TeacherAssistInlineAlert alert={getSectionAlert("pacing-guides")} />

      <section className="grid gap-5 xl:grid-cols-[0.85fr_1.15fr]">
        <article className="ta-panel p-4">
          <div className="flex items-center justify-between gap-3">
            <h2 className="text-base font-semibold text-slate-900">Available guides</h2>
            <select
              className="ta-input max-w-[180px]"
              value={guideType}
              onChange={(event) => setGuideType(event.target.value as PacingGuideType | "")}
            >
              {PACING_GUIDE_TYPE_OPTIONS.map((option) => (
                <option key={option.label} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>

          {loading ? (
            <p className="mt-4 text-sm text-slate-600">Loading pacing guides...</p>
          ) : guides.length === 0 ? (
            <div className="mt-4 space-y-3 rounded-xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
              <p>No pacing guides match this filter yet.</p>
              {isRootAdmin ? (
                <p>
                  Create a district guide in{" "}
                  <Link
                    href="/teacher-assist/administration/education-catalog?tab=pacing_guides"
                    className="font-semibold underline"
                  >
                    Catalog Admin
                  </Link>
                  .
                </p>
              ) : (
                <p>Ask a root admin to publish district pacing guides, then copy one here for your classroom.</p>
              )}
            </div>
          ) : (
            <div className="mt-4 space-y-2">
              {guides.map((guide) => (
                <button
                  key={guide.id}
                  type="button"
                  onClick={() => setSelectedGuideId(guide.id)}
                  className={`w-full rounded-xl border px-4 py-3 text-left ${
                    selectedGuideId === guide.id ? "border-sky-300 bg-sky-50" : "border-slate-200 bg-white"
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{guide.title}</p>
                      <p className="mt-1 text-xs text-slate-600">
                        {guide.guide_type.replace("_", " ")} · {guide.school_year_label ?? "School year"} ·{" "}
                        {guide.period_count} period{guide.period_count === 1 ? "" : "s"}
                      </p>
                    </div>
                    <span className="rounded-full bg-slate-100 px-2 py-1 text-[11px] font-semibold uppercase tracking-wide text-slate-600">
                      {guide.guide_type === "DISTRICT" ? "District" : guide.guide_type === "GRADE_LEVEL" ? "Grade" : "Mine"}
                    </span>
                  </div>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-4">
          {!selectedGuide ? (
            <p className="text-sm text-slate-600">Select a pacing guide to review periods, objectives, and resources.</p>
          ) : detailLoading || !detail ? (
            <p className="text-sm text-slate-600">Loading guide detail...</p>
          ) : (
            <div className="space-y-4">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-slate-900">{detail.title}</h2>
                  <p className="mt-1 text-sm text-slate-600">{detail.description || "No description saved."}</p>
                  <p className="mt-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                    {detail.guide_type.replace("_", " ")} · {detail.school_year_label ?? "School year"}
                  </p>
                </div>
                {detail.guide_type === "DISTRICT" ? (
                  <button type="button" className="ta-button-primary" disabled={copying} onClick={() => void copyDistrictGuide()}>
                    {copying ? "Copying..." : "Copy to my guide"}
                  </button>
                ) : null}
              </div>

              {detail.periods.length === 0 ? (
                <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                  This guide does not have periods yet.
                </div>
              ) : (
                <div className="space-y-3">
                  {detail.periods.map((period) => (
                    <div key={period.id} className="rounded-xl border border-slate-200 bg-white px-4 py-3">
                      <div className="flex flex-wrap items-center justify-between gap-2">
                        <p className="text-sm font-semibold text-slate-900">
                          {period.sequence_number}. {period.title}
                        </p>
                        <span className="text-xs text-slate-500">{formatDateRange(period.start_date, period.end_date)}</span>
                      </div>
                      {period.description ? <p className="mt-2 text-sm text-slate-600">{period.description}</p> : null}
                      <div className="mt-3">
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Objectives</p>
                        <div className="mt-2 flex flex-wrap gap-2">
                          {period.objectives.length > 0 ? (
                            period.objectives.map((row) => (
                              <span
                                key={row.id}
                                className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                                title={row.objective_description ?? undefined}
                              >
                                {row.objective_code ?? "Objective"}
                              </span>
                            ))
                          ) : (
                            <span className="text-sm text-slate-500">No objectives mapped.</span>
                          )}
                        </div>
                      </div>
                      <div className="mt-3">
                        <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Resources</p>
                        <div className="mt-2 space-y-2">
                          {period.resources.length > 0 ? (
                            period.resources.map((row) => (
                              <div key={row.id} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-sm">
                                <p className="font-medium text-slate-900">
                                  {row.resource_title ?? "Resource"}
                                  {row.is_primary ? " · Primary" : ""}
                                </p>
                                {row.notes ? <p className="mt-1 text-slate-600">{row.notes}</p> : null}
                              </div>
                            ))
                          ) : (
                            <span className="text-sm text-slate-500">No resources mapped.</span>
                          )}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </article>
      </section>
    </div>
  );
}
