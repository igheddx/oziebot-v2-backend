"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  createCurriculumRolloverCopy,
  fetchCurriculumRolloverCandidates,
  fetchSchoolYears,
  fetchSubjects,
} from "@/lib/teacher-assist-api";
import type {
  CurriculumRolloverCandidate,
  CurriculumRolloverCandidates,
  SchoolYear,
  Subject,
} from "@/lib/teacher-assist-types";

type Filters = {
  source_school_year_id: string;
  target_school_year_id: string;
  subject_id: string;
  planning_scope: string;
  reuse_status: string;
  preserve_titles: boolean;
  title_suffix: string;
};

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function scopeLabel(value: CurriculumRolloverCandidate["planning_scope"]) {
  switch (value) {
    case "multi_week":
      return "Multi-Week";
    case "grading_period":
      return "Grading Period";
    default:
      return value.charAt(0).toUpperCase() + value.slice(1);
  }
}

export function TeacherAssistCurriculumRolloverScreen() {
  const searchParams = useSearchParams();
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [results, setResults] = useState<CurriculumRolloverCandidates | null>(null);
  const [selectedPlanIds, setSelectedPlanIds] = useState<string[]>([]);
  const [loadingMeta, setLoadingMeta] = useState(true);
  const [loadingCandidates, setLoadingCandidates] = useState(false);
  const [copying, setCopying] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [filters, setFilters] = useState<Filters>({
    source_school_year_id: searchParams.get("source_school_year_id") ?? "",
    target_school_year_id: "",
    subject_id: "",
    planning_scope: "",
    reuse_status: "reusable",
    preserve_titles: true,
    title_suffix: "",
  });

  useEffect(() => {
    let cancelled = false;
    setLoadingMeta(true);
    setError(null);
    void Promise.all([fetchSchoolYears(), fetchSubjects()])
      .then(([nextSchoolYears, nextSubjects]) => {
        if (cancelled) return;
        setSchoolYears(nextSchoolYears);
        setSubjects(nextSubjects);
        setFilters((current) => ({
          ...current,
          source_school_year_id:
            current.source_school_year_id || nextSchoolYears.find((row) => row.is_active)?.id || "",
          target_school_year_id:
            current.target_school_year_id ||
            nextSchoolYears.find((row) => !row.is_active && row.id !== current.source_school_year_id)?.id ||
            nextSchoolYears[0]?.id ||
            "",
        }));
      })
      .catch((nextError) => {
        if (cancelled) return;
        setError(nextError instanceof Error ? nextError.message : "Could not load rollover workspace.");
      })
      .finally(() => {
        if (!cancelled) {
          setLoadingMeta(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [searchParams]);

  const loadCandidates = useCallback(async () => {
    if (!filters.source_school_year_id || !filters.target_school_year_id) {
      setError("Choose both a source and target school year before loading rollover candidates.");
      return;
    }
    setLoadingCandidates(true);
    setError(null);
    setMessage(null);
    setWarnings([]);
    try {
      const nextResults = await fetchCurriculumRolloverCandidates({
        source_school_year_id: filters.source_school_year_id,
        target_school_year_id: filters.target_school_year_id,
        subject_id: filters.subject_id || undefined,
        planning_scope: filters.planning_scope || undefined,
        reuse_status: filters.reuse_status || undefined,
      });
      setResults(nextResults);
      const requestedPlanId = searchParams.get("plan_id");
      if (requestedPlanId && nextResults.items.some((item) => item.id === requestedPlanId)) {
        setSelectedPlanIds([requestedPlanId]);
      } else {
        setSelectedPlanIds(nextResults.items.filter((item) => !item.already_copied_to_target).map((item) => item.id));
      }
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load rollover candidates.");
    } finally {
      setLoadingCandidates(false);
    }
  }, [filters, searchParams]);

  const selectedCount = selectedPlanIds.length;
  const selectablePlans = useMemo(
    () => results?.items.filter((item) => !item.already_copied_to_target) ?? [],
    [results],
  );

  const togglePlan = (planId: string) => {
    setSelectedPlanIds((current) =>
      current.includes(planId) ? current.filter((item) => item !== planId) : [...current, planId],
    );
  };

  const handleCopy = useCallback(async () => {
    if (!filters.source_school_year_id || !filters.target_school_year_id || selectedPlanIds.length === 0) {
      setError("Select at least one plan before running rollover copy.");
      return;
    }
    setCopying(true);
    setError(null);
    setMessage(null);
    setWarnings([]);
    try {
      const result = await createCurriculumRolloverCopy({
        source_school_year_id: filters.source_school_year_id,
        target_school_year_id: filters.target_school_year_id,
        plan_ids: selectedPlanIds,
        copy_mode: "rollover_copy",
        preserve_titles: filters.preserve_titles,
        title_suffix: filters.title_suffix.trim() || undefined,
      });
      setWarnings(result.warnings);
      setMessage(
        result.copied_plans.length > 0
          ? `${result.copied_plans.length} plan${result.copied_plans.length === 1 ? "" : "s"} copied into the target school year without calling AI.`
          : "No new plans were copied.",
      );
      await loadCandidates();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not copy rollover plans.");
    } finally {
      setCopying(false);
    }
  }, [filters, loadCandidates, selectedPlanIds]);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Curriculum Rollover
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Annual curriculum rollover foundation
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Find prior-year reusable instructional plans, review duplicate detection, and copy selected
            artifacts into a new school year through deterministic software-only workflows.
          </p>
        </div>
      </section>

      <section className="ta-alert ta-alert-info">
        Rollover copy is software-only. It preserves plan lineage and does not trigger provider usage.
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {message ? <section className="ta-alert ta-alert-success">{message}</section> : null}
      {warnings.length > 0 ? (
        <section className="rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-900">
          <p className="font-semibold">Warnings</p>
          <ul className="mt-2 space-y-1">
            {warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        </section>
      ) : null}

      <section className="ta-panel p-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Rollover selection</h2>
            <p className="mt-1 text-sm text-slate-600">
              Choose source and target school years, then load reusable candidate plans.
            </p>
          </div>
          <Link href="/teacher-assist/plans" className="ta-button-secondary">
            Open Plan Library
          </Link>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <label className="space-y-2">
            <span className="ta-label">Source school year</span>
            <select
              value={filters.source_school_year_id}
              onChange={(event) =>
                setFilters((current) => ({ ...current, source_school_year_id: event.target.value }))
              }
              className="ta-input"
              disabled={loadingMeta}
            >
              <option value="">Select source year</option>
              {schoolYears.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.title}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Target school year</span>
            <select
              value={filters.target_school_year_id}
              onChange={(event) =>
                setFilters((current) => ({ ...current, target_school_year_id: event.target.value }))
              }
              className="ta-input"
              disabled={loadingMeta}
            >
              <option value="">Select target year</option>
              {schoolYears.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.title}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Subject</span>
            <select
              value={filters.subject_id}
              onChange={(event) => setFilters((current) => ({ ...current, subject_id: event.target.value }))}
              className="ta-input"
            >
              <option value="">All subjects</option>
              {subjects.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.name}
                </option>
              ))}
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Planning scope</span>
            <select
              value={filters.planning_scope}
              onChange={(event) =>
                setFilters((current) => ({ ...current, planning_scope: event.target.value }))
              }
              className="ta-input"
            >
              <option value="">All scopes</option>
              <option value="weekly">Weekly</option>
              <option value="multi_week">Multi-Week</option>
              <option value="module">Module</option>
              <option value="unit">Unit</option>
              <option value="grading_period">Grading Period</option>
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Reuse status</span>
            <select
              value={filters.reuse_status}
              onChange={(event) =>
                setFilters((current) => ({ ...current, reuse_status: event.target.value }))
              }
              className="ta-input"
            >
              <option value="">Any reusable state</option>
              <option value="reusable">Reusable</option>
              <option value="active">Active</option>
              <option value="archived">Archived</option>
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Title suffix</span>
            <input
              value={filters.title_suffix}
              onChange={(event) =>
                setFilters((current) => ({ ...current, title_suffix: event.target.value }))
              }
              className="ta-input"
              placeholder="2027-2028"
            />
          </label>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-4">
          <label className="inline-flex items-center gap-2 text-sm text-slate-700">
            <input
              type="checkbox"
              checked={filters.preserve_titles}
              onChange={(event) =>
                setFilters((current) => ({ ...current, preserve_titles: event.target.checked }))
              }
              className="h-4 w-4 rounded border-slate-300 text-sky-600 focus:ring-sky-500"
            />
            Preserve original titles
          </label>
          <button
            type="button"
            onClick={() => void loadCandidates()}
            disabled={loadingMeta || loadingCandidates}
            className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loadingCandidates ? "Loading candidates..." : "Load reusable candidates"}
          </button>
        </div>
      </section>

      {results ? (
        <>
          <section className="grid gap-4 md:grid-cols-3">
            <article className="ta-panel p-5">
              <p className="text-sm font-semibold text-slate-500">Candidates</p>
              <p className="mt-3 text-3xl font-semibold text-slate-900">{results.items.length}</p>
            </article>
            <article className="ta-panel p-5">
              <p className="text-sm font-semibold text-slate-500">Selected</p>
              <p className="mt-3 text-3xl font-semibold text-slate-900">{selectedCount}</p>
            </article>
            <article className="ta-panel p-5">
              <p className="text-sm font-semibold text-slate-500">Scopes represented</p>
              <p className="mt-3 text-lg font-semibold text-slate-900">
                {Object.keys(results.summary_counts_by_planning_scope).join(", ") || "None"}
              </p>
            </article>
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
            <article className="ta-panel p-6">
              <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">Candidate plans</h2>
                  <p className="mt-1 text-sm text-slate-600">
                    Duplicate lineage detection marks plans that already exist in the target school year.
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => setSelectedPlanIds(selectablePlans.map((item) => item.id))}
                    className="ta-button-secondary"
                  >
                    Select all available
                  </button>
                  <button
                    type="button"
                    onClick={() => setSelectedPlanIds([])}
                    className="ta-button-secondary"
                  >
                    Clear
                  </button>
                </div>
              </div>

              <div className="mt-5 space-y-3">
                {results.items.length === 0 ? (
                  <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                    No rollover candidates matched the current filters.
                  </div>
                ) : (
                  results.items.map((item) => (
                    <label
                      key={item.id}
                      className="flex gap-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm shadow-slate-950/5"
                    >
                      <input
                        type="checkbox"
                        checked={selectedPlanIds.includes(item.id)}
                        onChange={() => togglePlan(item.id)}
                        disabled={item.already_copied_to_target}
                        className="mt-1 h-4 w-4 rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                      />
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-base font-semibold text-slate-900">{item.plan_title}</span>
                          <span className="rounded-full bg-sky-50 px-2.5 py-1 text-xs font-semibold text-sky-800">
                            {scopeLabel(item.planning_scope)}
                          </span>
                          {item.already_copied_to_target ? (
                            <span className="rounded-full bg-amber-50 px-2.5 py-1 text-xs font-semibold text-amber-800">
                              Already copied
                            </span>
                          ) : null}
                        </div>
                        <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-600">
                          <span>Source year: {item.source_school_year_title ?? "Unspecified"}</span>
                          <span>Visibility: {item.visibility_scope}</span>
                          <span>Reuse: {item.reuse_status}</span>
                          <span>Updated: {formatDateTime(item.updated_at)}</span>
                        </div>
                        <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-500">
                          {item.subject_names.length > 0 ? (
                            <span>Subjects: {item.subject_names.join(", ")}</span>
                          ) : null}
                          {item.grading_period_title ? (
                            <span>Grading period: {item.grading_period_title}</span>
                          ) : null}
                          {item.class_name ? <span>Class: {item.class_name}</span> : null}
                        </div>
                      </div>
                    </label>
                  ))
                )}
              </div>
            </article>

            <article className="ta-panel p-6">
              <h2 className="text-xl font-semibold text-slate-900">Rollover summary</h2>
              <div className="mt-4 space-y-4 text-sm text-slate-600">
                <div>
                  <p className="font-semibold text-slate-900">Subjects represented</p>
                  <p className="mt-1">
                    {results.subjects_represented.length > 0
                      ? results.subjects_represented.join(", ")
                      : "No subject metadata available."}
                  </p>
                </div>
                <div>
                  <p className="font-semibold text-slate-900">Grading periods represented</p>
                  <p className="mt-1">
                    {results.grading_periods_represented.length > 0
                      ? results.grading_periods_represented.join(", ")
                      : "No grading-period metadata available."}
                  </p>
                </div>
                <div>
                  <p className="font-semibold text-slate-900">Planning scope counts</p>
                  <div className="mt-2 space-y-1">
                    {Object.entries(results.summary_counts_by_planning_scope).map(([scope, count]) => (
                      <p key={scope}>
                        {scopeLabel(scope as CurriculumRolloverCandidate["planning_scope"])}: {count}
                      </p>
                    ))}
                  </div>
                </div>
              </div>

              <button
                type="button"
                onClick={() => void handleCopy()}
                disabled={copying || selectedPlanIds.length === 0}
                className="ta-button-primary mt-6 w-full disabled:cursor-not-allowed disabled:opacity-60"
              >
                {copying ? "Copying selected plans..." : "Copy selected plans into target year"}
              </button>
            </article>
          </section>
        </>
      ) : (
        <section className="ta-panel p-6">
          <p className="text-sm text-slate-600">
            Load reusable candidates to review prior-year plans and start rollover copy.
          </p>
        </section>
      )}
    </div>
  );
}
