"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  fetchClasses,
  fetchGradingPeriods,
  fetchPacingGuides,
  fetchPlanningDrafts,
  fetchResources,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherProfile,
} from "@/lib/teacher-assist-api";
import type {
  GradingPeriod,
  PacingGuide,
  PlanningDraft,
  ResourceLibraryItem,
  SchoolYear,
  Standard,
  Subject,
  TeacherClass,
  TeacherProfile,
} from "@/lib/teacher-assist-types";

type DashboardSnapshot = {
  profile: TeacherProfile;
  schoolYears: SchoolYear[];
  gradingPeriods: GradingPeriod[];
  classes: TeacherClass[];
  subjects: Subject[];
  standards: Standard[];
  resources: ResourceLibraryItem[];
  pacingGuides: PacingGuide[];
  planningDrafts: PlanningDraft[];
};

type ChecklistItem = {
  key: string;
  label: string;
  done: boolean;
  href: string;
  detail: string;
};

export function TeacherAssistDashboardScreen() {
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [
        profile,
        schoolYears,
        gradingPeriods,
        classes,
        subjects,
        standards,
        resources,
        pacingGuides,
        planningDrafts,
      ] = await Promise.all([
        fetchTeacherProfile(),
        fetchSchoolYears(),
        fetchGradingPeriods(),
        fetchClasses(),
        fetchSubjects(),
        fetchStandards(),
        fetchResources(),
        fetchPacingGuides(),
        fetchPlanningDrafts(),
      ]);
      setSnapshot({
        profile,
        schoolYears,
        gradingPeriods,
        classes,
        subjects,
        standards,
        resources,
        pacingGuides,
        planningDrafts,
      });
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load TeacherAssist dashboard.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const checklist = useMemo<ChecklistItem[]>(() => {
    const activeSchoolYear = snapshot?.schoolYears.find((row) => row.is_active) ?? null;
    const setupReady = Boolean(
      snapshot?.profile.preferred_grade_level &&
        snapshot?.profile.preferred_grading_period_type &&
        snapshot?.profile.timezone &&
        activeSchoolYear &&
        (snapshot?.gradingPeriods.length ?? 0) > 0 &&
        (snapshot?.classes.length ?? 0) > 0 &&
        (snapshot?.subjects.length ?? 0) > 0 &&
        (snapshot?.standards.length ?? 0) > 0,
    );

    return [
      {
        key: "setup",
        label: "Teacher foundation",
        done: setupReady,
        href: "/teacher-assist/settings",
        detail: setupReady
          ? "Profile, school year, grading periods, classes, subjects, and standards are ready."
          : "Complete the TeacherAssist setup foundation before organizing planning context.",
      },
      {
        key: "resources",
        label: "Resource library",
        done: (snapshot?.resources.length ?? 0) > 0,
        href: "/teacher-assist/resources",
        detail:
          (snapshot?.resources.length ?? 0) > 0
            ? `${snapshot?.resources.length ?? 0} curriculum resources saved for reuse.`
            : "Upload files or save links so pacing items and planning drafts have grounded source material.",
      },
      {
        key: "pacing-guides",
        label: "Pacing guides",
        done: (snapshot?.pacingGuides.length ?? 0) > 0,
        href: "/teacher-assist/pacing-guides",
        detail:
          (snapshot?.pacingGuides.length ?? 0) > 0
            ? `${snapshot?.pacingGuides.length ?? 0} pacing guides organized into structured instructional timelines.`
            : "Create a pacing guide and add pacing items by grading period, week, or day.",
      },
      {
        key: "planning-drafts",
        label: "Planning drafts",
        done: (snapshot?.planningDrafts.length ?? 0) > 0,
        href: "/teacher-assist/weekly-planning",
        detail:
          (snapshot?.planningDrafts.length ?? 0) > 0
            ? `${snapshot?.planningDrafts.length ?? 0} planning drafts saved without triggering generation.`
            : "Prepare and save planning context explicitly before any future generation phase exists.",
      },
    ];
  }, [snapshot]);

  const incompleteItems = checklist.filter((item) => !item.done);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Dashboard
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Structured planning context foundation
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            This phase is about organizing instructional context only: pacing guides, reusable
            resources, standards grounding, and explicit planning drafts. Generation, grading, OCR,
            exports, and workflow jobs are intentionally out of scope.
          </p>
        </div>
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}

      <section className="grid gap-4 lg:grid-cols-4">
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Curriculum resources</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading ? "..." : (snapshot?.resources.length ?? 0)}
          </p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Pacing guides</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading ? "..." : (snapshot?.pacingGuides.length ?? 0)}
          </p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Pacing items</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading
              ? "..."
              : (snapshot?.pacingGuides.reduce((sum, row) => sum + row.item_count, 0) ?? 0)}
          </p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Planning drafts</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading ? "..." : (snapshot?.planningDrafts.length ?? 0)}
          </p>
        </article>
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.4fr_1fr]">
        <article className="ta-panel p-6">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Context readiness checklist</h2>
              <p className="mt-1 text-sm text-slate-600">
                Teachers should prepare and save context before any generation workflow exists.
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Link href="/teacher-assist/resources" className="ta-button-secondary">
                Open resources
              </Link>
              <Link href="/teacher-assist/pacing-guides" className="ta-button-primary">
                Open pacing guides
              </Link>
            </div>
          </div>

          <div className="mt-5 grid gap-3">
            {checklist.map((item) => (
              <Link
                key={item.key}
                href={item.href}
                className="rounded-2xl border border-slate-200 bg-white p-4 transition hover:border-sky-300 hover:bg-sky-50/40"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-base font-semibold text-slate-900">{item.label}</p>
                    <p className="mt-2 text-sm leading-6 text-slate-600">{item.detail}</p>
                  </div>
                  <span
                    className={`rounded-full px-3 py-1 text-xs font-semibold ${
                      item.done ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"
                    }`}
                  >
                    {item.done ? "Ready" : "Needs setup"}
                  </span>
                </div>
              </Link>
            ))}
          </div>
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Current guidance</h2>
          {loading ? (
            <p className="mt-3 text-sm text-slate-600">Loading dashboard state...</p>
          ) : incompleteItems.length > 0 ? (
            <div className="mt-3 space-y-3">
              <p className="text-sm leading-6 text-slate-600">
                TeacherAssist is still prep-first. Save context here now so later phases can consume
                structured inputs instead of freeform uploads or ad hoc prompts.
              </p>
              <ul className="space-y-2 text-sm text-slate-700">
                {incompleteItems.map((item) => (
                  <li key={item.key} className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <span className="font-semibold text-slate-900">{item.label}:</span> {item.detail}
                  </li>
                ))}
              </ul>
            </div>
          ) : (
            <div className="mt-3 rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-800">
              Your planning context foundation is ready. The next phase can build richer draft-save
              and preparation workflows on top of these resources and pacing structures.
            </div>
          )}

          <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
            No <span className="font-semibold">Generate</span> button exists yet by design.
          </div>
        </article>
      </section>
    </div>
  );
}
