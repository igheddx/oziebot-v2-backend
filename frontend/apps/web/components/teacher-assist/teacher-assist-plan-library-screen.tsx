"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  copyWeeklyPlan,
  fetchInstructionalPlanLibrary,
  fetchSchoolYears,
  fetchSubjects,
  updateWeeklyPlanSharing,
} from "@/lib/teacher-assist-api";
import type {
  InstructionalPlanLibraryItem,
  SchoolYear,
  Subject,
  WeeklyPlanSharingUpdateInput,
} from "@/lib/teacher-assist-types";

type Filters = {
  school_year_id: string;
  subject_id: string;
  planning_scope: string;
  visibility_scope: string;
  reuse_status: string;
  template_mode: "all" | "templates" | "non_templates";
  q: string;
};

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function filterTemplateValue(value: Filters["template_mode"]) {
  if (value === "templates") return true;
  if (value === "non_templates") return false;
  return undefined;
}

function scopeLabel(value: InstructionalPlanLibraryItem["planning_scope"]) {
  switch (value) {
    case "multi_week":
      return "Multi-Week";
    case "grading_period":
      return "Grading Period";
    default:
      return value.charAt(0).toUpperCase() + value.slice(1);
  }
}

function sectionedPlans(plans: InstructionalPlanLibraryItem[]) {
  return {
    myPlans: plans.filter((plan) => plan.is_owner),
    sharedPlans: plans.filter((plan) => !plan.is_owner && plan.visibility_scope !== "private"),
    templates: plans.filter((plan) => plan.is_template),
    priorYearPlans: plans.filter((plan) => Boolean(plan.source_school_year_title)),
  };
}

function ownerActionPayload(plan: InstructionalPlanLibraryItem): WeeklyPlanSharingUpdateInput {
  if (plan.is_template || plan.reuse_status === "reusable" || plan.visibility_scope !== "private") {
    return {
      is_template: false,
      visibility_scope: "private",
      reuse_status: "active",
    };
  }
  return {
    is_template: true,
    visibility_scope: "shared",
    reuse_status: "reusable",
  };
}

function ownerActionLabel(plan: InstructionalPlanLibraryItem) {
  if (plan.is_template || plan.reuse_status === "reusable" || plan.visibility_scope !== "private") {
    return "Set Private";
  }
  return "Mark Reusable";
}

function PlanSection({
  title,
  description,
  plans,
  busyPlanId,
  onCopy,
  onUpdateSharing,
}: {
  title: string;
  description: string;
  plans: InstructionalPlanLibraryItem[];
  busyPlanId: string | null;
  onCopy: (planId: string) => void;
  onUpdateSharing: (plan: InstructionalPlanLibraryItem) => void;
}) {
  return (
    <article className="ta-panel p-6">
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h2 className="text-xl font-semibold text-slate-900">{title}</h2>
          <p className="mt-1 text-sm text-slate-600">{description}</p>
        </div>
        <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
          {plans.length} plan{plans.length === 1 ? "" : "s"}
        </span>
      </div>

      {plans.length === 0 ? (
        <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
          No plans matched this section yet.
        </div>
      ) : (
        <div className="mt-5 space-y-3">
          {plans.map((plan) => (
            <div
              key={plan.id}
              className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm shadow-slate-950/5"
            >
              <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <h3 className="text-base font-semibold text-slate-900">{plan.plan_title}</h3>
                    <span className="rounded-full bg-sky-50 px-2.5 py-1 text-xs font-semibold text-sky-800">
                      {scopeLabel(plan.planning_scope)}
                    </span>
                    {plan.is_template ? (
                      <span className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-semibold text-violet-800">
                        Template
                      </span>
                    ) : null}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-600">
                    <span>School year: {plan.source_school_year_title ?? "Unspecified"}</span>
                    <span>Visibility: {plan.visibility_scope}</span>
                    <span>Reuse: {plan.reuse_status}</span>
                    <span>Owner: {plan.owner_name ?? "Teacher"}</span>
                    <span>Updated: {formatDateTime(plan.updated_at)}</span>
                  </div>
                  <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-500">
                    {plan.module_title ? <span>Module: {plan.module_title}</span> : null}
                    {plan.class_name ? <span>Class: {plan.class_name}</span> : null}
                    {plan.grading_period_title ? <span>Grading period: {plan.grading_period_title}</span> : null}
                    {plan.subject_names.length > 0 ? (
                      <span>Subjects: {plan.subject_names.join(", ")}</span>
                    ) : null}
                  </div>
                </div>

                <div className="flex flex-wrap gap-2 xl:justify-end">
                  <Link
                    href={`/teacher-assist/weekly-planning/plans?id=${plan.id}`}
                    className="ta-button-secondary"
                  >
                    Open
                  </Link>
                  <button
                    type="button"
                    onClick={() => onCopy(plan.id)}
                    disabled={busyPlanId === plan.id}
                    className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {busyPlanId === plan.id ? "Copying..." : "Copy to My Plans"}
                  </button>
                  {plan.is_owner ? (
                    <button
                      type="button"
                      onClick={() => onUpdateSharing(plan)}
                      disabled={busyPlanId === plan.id}
                      className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {ownerActionLabel(plan)}
                    </button>
                  ) : null}
                  <Link
                    href={`/teacher-assist/curriculum-rollover?source_school_year_id=${plan.source_school_year_id ?? ""}&plan_id=${plan.id}`}
                    className="ta-button-secondary"
                  >
                    Copy to New School Year
                  </Link>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </article>
  );
}

export function TeacherAssistPlanLibraryScreen() {
  const [plans, setPlans] = useState<InstructionalPlanLibraryItem[]>([]);
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [busyPlanId, setBusyPlanId] = useState<string | null>(null);
  const [filters, setFilters] = useState<Filters>({
    school_year_id: "",
    subject_id: "",
    planning_scope: "",
    visibility_scope: "",
    reuse_status: "",
    template_mode: "all",
    q: "",
  });

  const load = useCallback(async (currentFilters: Filters) => {
    setLoading(true);
    setError(null);
    try {
      const [nextSchoolYears, nextSubjects, nextPlans] = await Promise.all([
        fetchSchoolYears(),
        fetchSubjects(),
        fetchInstructionalPlanLibrary({
          school_year_id: currentFilters.school_year_id || undefined,
          subject_id: currentFilters.subject_id || undefined,
          planning_scope: currentFilters.planning_scope || undefined,
          visibility_scope: currentFilters.visibility_scope || undefined,
          reuse_status: currentFilters.reuse_status || undefined,
          is_template: filterTemplateValue(currentFilters.template_mode),
          q: currentFilters.q.trim() || undefined,
        }),
      ]);
      setSchoolYears(nextSchoolYears);
      setSubjects(nextSubjects);
      setPlans(nextPlans);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load plan library.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load(filters);
  }, [filters, load]);

  const sections = useMemo(() => sectionedPlans(plans), [plans]);

  const handleCopy = useCallback(
    async (planId: string) => {
      setBusyPlanId(planId);
      setError(null);
      setMessage(null);
      try {
        await copyWeeklyPlan(planId, { copy_mode: "personal_copy" });
        setMessage("Plan copied into My Plans without calling AI.");
        await load(filters);
      } catch (nextError) {
        setError(nextError instanceof Error ? nextError.message : "Could not copy plan.");
      } finally {
        setBusyPlanId(null);
      }
    },
    [filters, load],
  );

  const handleUpdateSharing = useCallback(
    async (plan: InstructionalPlanLibraryItem) => {
      setBusyPlanId(plan.id);
      setError(null);
      setMessage(null);
      try {
        await updateWeeklyPlanSharing(plan.id, ownerActionPayload(plan));
        setMessage("Plan sharing settings updated.");
        await load(filters);
      } catch (nextError) {
        setError(
          nextError instanceof Error ? nextError.message : "Could not update plan sharing settings.",
        );
      } finally {
        setBusyPlanId(null);
      }
    },
    [filters, load],
  );

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Plan Library
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Reusable instructional plans and teacher-owned copies
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Browse plans visible to your TeacherAssist tenant, open reusable artifacts, and create
            teacher-owned copies without triggering AI usage.
          </p>
        </div>
      </section>

      <section className="ta-alert ta-alert-info">
        Copying, sharing, and template updates are software-only flows. No provider call runs here.
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {message ? <section className="ta-alert ta-alert-success">{message}</section> : null}

      <section className="ta-panel p-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Library filters</h2>
            <p className="mt-1 text-sm text-slate-600">
              Narrow visible plans by school year, subject, scope, sharing state, template status, or
              a text search.
            </p>
          </div>
          <Link href="/teacher-assist/weekly-planning" className="ta-button-secondary">
            Open Instructional Planning
          </Link>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <label className="space-y-2">
            <span className="ta-label">School year</span>
            <select
              value={filters.school_year_id}
              onChange={(event) =>
                setFilters((current) => ({ ...current, school_year_id: event.target.value }))
              }
              className="ta-input"
            >
              <option value="">All school years</option>
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
              onChange={(event) =>
                setFilters((current) => ({ ...current, subject_id: event.target.value }))
              }
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
            <span className="ta-label">Visibility</span>
            <select
              value={filters.visibility_scope}
              onChange={(event) =>
                setFilters((current) => ({ ...current, visibility_scope: event.target.value }))
              }
              className="ta-input"
            >
              <option value="">All visibility</option>
              <option value="private">Private</option>
              <option value="shared">Shared</option>
              <option value="grade_team">Grade Team</option>
              <option value="school">School</option>
              <option value="district">District</option>
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
              <option value="">All statuses</option>
              <option value="active">Active</option>
              <option value="reusable">Reusable</option>
              <option value="archived">Archived</option>
            </select>
          </label>
          <label className="space-y-2">
            <span className="ta-label">Template filter</span>
            <select
              value={filters.template_mode}
              onChange={(event) =>
                setFilters((current) => ({
                  ...current,
                  template_mode: event.target.value as Filters["template_mode"],
                }))
              }
              className="ta-input"
            >
              <option value="all">All plans</option>
              <option value="templates">Templates only</option>
              <option value="non_templates">Non-templates only</option>
            </select>
          </label>
          <label className="space-y-2 xl:col-span-2">
            <span className="ta-label">Search</span>
            <input
              value={filters.q}
              onChange={(event) => setFilters((current) => ({ ...current, q: event.target.value }))}
              className="ta-input"
              placeholder="Search titles, modules, subjects, or notes"
            />
          </label>
        </div>
      </section>

      {loading ? (
        <section className="ta-panel p-6">
          <p className="text-sm text-slate-600">Loading plan library...</p>
        </section>
      ) : (
        <div className="space-y-6">
          <PlanSection
            title="My Plans"
            description="Teacher-owned plans, including copies and personalized versions."
            plans={sections.myPlans}
            busyPlanId={busyPlanId}
            onCopy={handleCopy}
            onUpdateSharing={handleUpdateSharing}
          />
          <PlanSection
            title="Shared / Reusable Plans"
            description="Plans visible through sharing scopes in your current TeacherAssist tenant."
            plans={sections.sharedPlans}
            busyPlanId={busyPlanId}
            onCopy={handleCopy}
            onUpdateSharing={handleUpdateSharing}
          />
          <PlanSection
            title="Templates"
            description="Instructional plans marked as reusable templates."
            plans={sections.templates}
            busyPlanId={busyPlanId}
            onCopy={handleCopy}
            onUpdateSharing={handleUpdateSharing}
          />
          <PlanSection
            title="Prior-Year Plans"
            description="Plans with prior-year school context that can feed annual rollover workflows."
            plans={sections.priorYearPlans}
            busyPlanId={busyPlanId}
            onCopy={handleCopy}
            onUpdateSharing={handleUpdateSharing}
          />
        </div>
      )}
    </div>
  );
}
