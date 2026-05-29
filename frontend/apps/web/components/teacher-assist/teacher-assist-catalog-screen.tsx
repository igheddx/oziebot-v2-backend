"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { useAuth } from "@/components/providers/auth-provider";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  fetchCatalogDistricts,
  fetchCatalogSchools,
  fetchCatalogStates,
} from "@/lib/education-catalog-api";
import {
  fetchCatalogBrowseContext,
  fetchCatalogBrowseGrades,
  fetchCatalogBrowseObjectives,
  fetchCatalogBrowseResources,
  fetchCatalogBrowseSubjects,
} from "@/lib/teacher-catalog-api";
import type { CatalogBrowseSection, CatalogListFilters } from "@/lib/teacher-catalog-types";
import {
  CATALOG_COVERAGE_TYPE_OPTIONS,
  CATALOG_GRADE_OPTIONS,
  CATALOG_OBJECTIVE_TYPE_OPTIONS,
  CATALOG_RESOURCE_TYPE_OPTIONS,
  CATALOG_SUBJECT_OPTIONS,
} from "@/lib/teacher-catalog-types";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

const SECTIONS: Array<{ key: CatalogBrowseSection; label: string }> = [
  { key: "grades", label: "Grades" },
  { key: "subjects", label: "Subjects" },
  { key: "objectives", label: "Objectives" },
  { key: "resources", label: "Resources" },
];

const PAGE_SIZE = 25;

type ScopeFilters = {
  state_id: string;
  district_id: string;
  school_id: string;
  grade_level: string;
  subject_code: string;
  objective_type: string;
  coverage_type: string;
  resource_type: string;
};

const EMPTY_SCOPE: ScopeFilters = {
  state_id: "",
  district_id: "",
  school_id: "",
  grade_level: "",
  subject_code: "",
  objective_type: "",
  coverage_type: "",
  resource_type: "",
};

function resourceTypeLabel(value: string) {
  if (value === "curriculum") return "Curriculum Guide";
  if (value === "textbook") return "Textbook";
  if (value === "reference") return "Reference";
  return value;
}

export function TeacherAssistCatalogScreen() {
  const { user } = useAuth();
  const isRootAdmin = Boolean(user?.is_root_admin);
  const { clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [section, setSection] = useState<CatalogBrowseSection>("grades");
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [context, setContext] = useState<Awaited<ReturnType<typeof fetchCatalogBrowseContext>> | null>(null);
  const [grades, setGrades] = useState<Awaited<ReturnType<typeof fetchCatalogBrowseGrades>> | null>(null);
  const [subjects, setSubjects] = useState<Awaited<ReturnType<typeof fetchCatalogBrowseSubjects>> | null>(null);
  const [objectives, setObjectives] = useState<Awaited<ReturnType<typeof fetchCatalogBrowseObjectives>> | null>(null);
  const [resources, setResources] = useState<Awaited<ReturnType<typeof fetchCatalogBrowseResources>> | null>(null);
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [scopeFilters, setScopeFilters] = useState<ScopeFilters>(EMPTY_SCOPE);
  const [states, setStates] = useState<Awaited<ReturnType<typeof fetchCatalogStates>>>([]);
  const [districts, setDistricts] = useState<Awaited<ReturnType<typeof fetchCatalogDistricts>>>([]);
  const [schools, setSchools] = useState<Awaited<ReturnType<typeof fetchCatalogSchools>>>([]);

  const adminScopeQuery = useMemo(
    () => ({
      state_id: scopeFilters.state_id || undefined,
      district_id: scopeFilters.district_id || undefined,
      school_id: scopeFilters.school_id || undefined,
    }),
    [scopeFilters.district_id, scopeFilters.school_id, scopeFilters.state_id],
  );

  const listFilters = useMemo<CatalogListFilters>(
    () => ({
      page,
      page_size: PAGE_SIZE,
      q: search || undefined,
      grade_level: scopeFilters.grade_level || undefined,
      subject_code: scopeFilters.subject_code || undefined,
      objective_type: scopeFilters.objective_type || undefined,
      coverage_type: scopeFilters.coverage_type || undefined,
      resource_type: scopeFilters.resource_type || undefined,
      ...(isRootAdmin ? adminScopeQuery : {}),
    }),
    [adminScopeQuery, isRootAdmin, page, scopeFilters, search],
  );

  useEffect(() => {
    if (!isRootAdmin) return;
    void fetchCatalogStates().then(setStates).catch(() => setStates([]));
  }, [isRootAdmin]);

  useEffect(() => {
    if (!isRootAdmin || !scopeFilters.state_id) {
      setDistricts([]);
      return;
    }
    void fetchCatalogDistricts(scopeFilters.state_id)
      .then(setDistricts)
      .catch(() => setDistricts([]));
  }, [isRootAdmin, scopeFilters.state_id]);

  useEffect(() => {
    if (!isRootAdmin || !scopeFilters.district_id) {
      setSchools([]);
      return;
    }
    void fetchCatalogSchools(scopeFilters.district_id)
      .then(setSchools)
      .catch(() => setSchools([]));
  }, [isRootAdmin, scopeFilters.district_id]);

  const loadSection = useCallback(
    async (silent = false) => {
      if (!silent) setLoading(true);
      setPageError(null);
      try {
        const nextContext = await fetchCatalogBrowseContext(isRootAdmin ? adminScopeQuery : undefined);
        setContext(nextContext);

        if (!nextContext.can_browse) {
          setGrades(null);
          setSubjects(null);
          setObjectives(null);
          setResources(null);
          return;
        }

        await withPreservedScroll("teacher-catalog-panel", async () => {
          if (section === "grades") {
            setGrades(await fetchCatalogBrowseGrades(listFilters));
          } else if (section === "subjects") {
            setSubjects(await fetchCatalogBrowseSubjects(listFilters));
          } else if (section === "objectives") {
            setObjectives(await fetchCatalogBrowseObjectives(listFilters));
          } else {
            setResources(await fetchCatalogBrowseResources(listFilters));
          }
        });
      } catch (nextError) {
        if (!silent) {
          setPageError(nextError instanceof Error ? nextError.message : "Could not load catalog.");
        }
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [adminScopeQuery, isRootAdmin, listFilters, section],
  );

  useEffect(() => {
    void loadSection();
  }, [loadSection]);

  useEffect(() => {
    setPage(1);
  }, [section, search, scopeFilters]);

  const activeMeta =
    section === "grades"
      ? grades?.meta
      : section === "subjects"
        ? subjects?.meta
        : section === "objectives"
          ? objectives?.meta
          : resources?.meta;

  const showContentFilters =
    section === "objectives" || section === "resources" || section === "subjects" || section === "grades";

  return (
    <div className="space-y-4">
      <section className="ta-panel px-5 py-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-700">Catalog</p>
            <h1 className="mt-1 text-xl font-semibold text-slate-900">Instructional catalog</h1>
          </div>
          {user?.is_root_admin ? (
            <Link href="/teacher-assist/administration/education-catalog" className="text-sm font-medium text-sky-700">
              Manage catalog
            </Link>
          ) : null}
        </div>
        {context?.scope_banner ? (
          <p className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
            {context.scope_banner}
          </p>
        ) : null}
        {context?.multiple_assignments_detected ? (
          <p className="mt-2 text-xs text-amber-700">
            Multiple active school assignments were detected. Showing your most recent assignment.
          </p>
        ) : null}
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load catalog" message={pageError} />

      {!loading && context && !context.can_browse ? (
        <section className="ta-panel p-5">
          <TeacherAssistInlineAlert
            alert={sectionError(
              "No school assignment found. Please contact an administrator.",
              "Catalog unavailable",
            )}
          />
        </section>
      ) : (
        <div className="grid gap-4 lg:grid-cols-[220px_1fr]">
          <nav className="ta-panel p-3">
            <p className="px-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Browse</p>
            <div className="mt-2 space-y-1">
              {SECTIONS.map((item) => (
                <button
                  key={item.key}
                  type="button"
                  className={`block w-full rounded-xl px-3 py-2 text-left text-sm font-medium ${
                    section === item.key ? "bg-sky-50 text-sky-900" : "text-slate-700 hover:bg-slate-50"
                  }`}
                  onClick={() => setSection(item.key)}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </nav>

          <section id="teacher-catalog-panel" className="ta-panel p-5">
            {isRootAdmin ? (
              <div className="grid gap-3 md:grid-cols-3">
                <select
                  className="ta-input"
                  value={scopeFilters.state_id}
                  onChange={(event) =>
                    setScopeFilters((current) => ({
                      ...current,
                      state_id: event.target.value,
                      district_id: "",
                      school_id: "",
                    }))
                  }
                >
                  <option value="">All states</option>
                  {states.map((row) => (
                    <option key={row.id} value={row.id}>
                      {row.name}
                    </option>
                  ))}
                </select>
                <select
                  className="ta-input"
                  value={scopeFilters.district_id}
                  disabled={!scopeFilters.state_id}
                  onChange={(event) =>
                    setScopeFilters((current) => ({
                      ...current,
                      district_id: event.target.value,
                      school_id: "",
                    }))
                  }
                >
                  <option value="">All districts</option>
                  {districts.map((row) => (
                    <option key={row.id} value={row.id}>
                      {row.name}
                    </option>
                  ))}
                </select>
                <select
                  className="ta-input"
                  value={scopeFilters.school_id}
                  disabled={!scopeFilters.district_id}
                  onChange={(event) =>
                    setScopeFilters((current) => ({
                      ...current,
                      school_id: event.target.value,
                    }))
                  }
                >
                  <option value="">All schools</option>
                  {schools.map((row) => (
                    <option key={row.id} value={row.id}>
                      {row.name}
                    </option>
                  ))}
                </select>
              </div>
            ) : null}

            {showContentFilters ? (
              <div className={`flex flex-wrap items-center gap-3 ${isRootAdmin ? "mt-3" : ""}`}>
                <select
                  className="ta-input max-w-[120px]"
                  value={scopeFilters.grade_level}
                  onChange={(event) =>
                    setScopeFilters((current) => ({ ...current, grade_level: event.target.value }))
                  }
                >
                  <option value="">All grades</option>
                  {CATALOG_GRADE_OPTIONS.map((grade) => (
                    <option key={grade} value={grade}>
                      {grade}
                    </option>
                  ))}
                </select>
                <select
                  className="ta-input max-w-[160px]"
                  value={scopeFilters.subject_code}
                  onChange={(event) =>
                    setScopeFilters((current) => ({ ...current, subject_code: event.target.value }))
                  }
                >
                  <option value="">All subjects</option>
                  {CATALOG_SUBJECT_OPTIONS.map((subject) => (
                    <option key={subject} value={subject}>
                      {subject}
                    </option>
                  ))}
                </select>
                {section === "objectives" ? (
                  <>
                    <select
                      className="ta-input max-w-[160px]"
                      value={scopeFilters.objective_type}
                      onChange={(event) =>
                        setScopeFilters((current) => ({ ...current, objective_type: event.target.value }))
                      }
                    >
                      <option value="">All objective types</option>
                      {CATALOG_OBJECTIVE_TYPE_OPTIONS.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                    <select
                      className="ta-input max-w-[160px]"
                      value={scopeFilters.coverage_type}
                      onChange={(event) =>
                        setScopeFilters((current) => ({ ...current, coverage_type: event.target.value }))
                      }
                    >
                      <option value="">All coverage</option>
                      {CATALOG_COVERAGE_TYPE_OPTIONS.map((value) => (
                        <option key={value} value={value}>
                          {value}
                        </option>
                      ))}
                    </select>
                    <input
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                      className="ta-input max-w-xs"
                      placeholder="Search objectives..."
                    />
                  </>
                ) : null}
                {section === "resources" ? (
                  <>
                    <select
                      className="ta-input max-w-[160px]"
                      value={scopeFilters.resource_type}
                      onChange={(event) =>
                        setScopeFilters((current) => ({ ...current, resource_type: event.target.value }))
                      }
                    >
                      <option value="">All resource types</option>
                      {CATALOG_RESOURCE_TYPE_OPTIONS.map((value) => (
                        <option key={value} value={value}>
                          {resourceTypeLabel(value)}
                        </option>
                      ))}
                    </select>
                    <input
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                      className="ta-input max-w-xs"
                      placeholder="Search resources..."
                    />
                  </>
                ) : null}
                <button
                  type="button"
                  className="ta-button-secondary"
                  onClick={() => {
                    clearSectionAlert("catalog-browse");
                    void loadSection();
                  }}
                >
                  Apply
                </button>
              </div>
            ) : null}

            <TeacherAssistInlineAlert
              alert={getSectionAlert("catalog-browse")}
              onDismiss={() => clearSectionAlert("catalog-browse")}
              className="mt-4"
            />

            {loading ? (
              <p className="mt-5 text-sm text-slate-600">Loading catalog...</p>
            ) : section === "grades" ? (
              <div className="mt-5 space-y-2">
                {(grades?.items ?? []).map((row) => (
                  <div key={row.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                    <p className="font-semibold text-slate-900">
                      {row.display_name} ({row.grade_code})
                    </p>
                    <p className="mt-1 text-slate-600">
                      {row.subject_count} subject{row.subject_count === 1 ? "" : "s"} · {row.active ? "Active" : "Inactive"}
                    </p>
                  </div>
                ))}
              </div>
            ) : section === "subjects" ? (
              <div className="mt-5 space-y-2">
                {(subjects?.items ?? []).map((row) => (
                  <div key={row.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                    <p className="font-semibold text-slate-900">
                      {row.display_name} ({row.subject_code})
                    </p>
                    <p className="mt-1 text-slate-600">
                      Grade {row.grade_code ?? "—"} · {row.objective_count} objectives · {row.resource_count} resources
                    </p>
                  </div>
                ))}
              </div>
            ) : section === "objectives" ? (
              <div className="mt-5 space-y-2">
                {(objectives?.items ?? []).map((row) => (
                  <div key={row.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                    <p className="font-semibold text-slate-900">{row.objective_id}</p>
                    <p className="mt-1 text-slate-600">
                      {row.objective_type} · Grade {row.grade_level} · {row.subject_code} · {row.coverage_type}
                    </p>
                    <p className="mt-2 text-slate-700">{row.description}</p>
                    {row.linked_resources.length > 0 ? (
                      <div className="mt-3 space-y-1 text-xs text-slate-600">
                        {row.linked_resources.map((resource) => (
                          <div key={resource.id}>
                            <span className="font-medium text-slate-700">
                              {resourceTypeLabel(resource.resource_type)}:
                            </span>{" "}
                            {resource.title}
                            {resource.reference_links.length > 0 ? (
                              <span className="text-sky-700">
                                {" "}
                                ·{" "}
                                {resource.reference_links.map((link, index) => (
                                  <span key={link.id}>
                                    {index > 0 ? ", " : null}
                                    <a href={link.url} target="_blank" rel="noreferrer" className="hover:underline">
                                      {link.link_title}
                                    </a>
                                  </span>
                                ))}
                              </span>
                            ) : null}
                          </div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            ) : (
              <div className="mt-5 space-y-3">
                {(resources?.items ?? []).map((row) => (
                  <div key={row.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                    <p className="font-semibold text-slate-900">{row.title}</p>
                    <p className="mt-1 text-slate-600">
                      {resourceTypeLabel(row.resource_type)} · Grade {row.grade_level} · {row.subject_code}
                    </p>
                    {row.description ? <p className="mt-2 text-slate-700">{row.description}</p> : null}
                    {row.reference_links.length > 0 ? (
                      <ul className="mt-2 space-y-1 text-xs text-sky-700">
                        {row.reference_links.map((link) => (
                          <li key={link.id}>
                            Reference Link:{" "}
                            <a href={link.url} target="_blank" rel="noreferrer" className="hover:underline">
                              {link.link_title}
                            </a>
                          </li>
                        ))}
                      </ul>
                    ) : null}
                    {row.associated_objectives.length > 0 ? (
                      <div className="mt-2 space-y-1 text-xs text-slate-600">
                        {row.associated_objectives.map((objective) => (
                          <p key={objective.id}>
                            {objective.objective_id} · {objective.objective_type} · {objective.coverage_type}
                          </p>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ))}
              </div>
            )}

            {activeMeta && activeMeta.total_pages > 1 ? (
              <div className="mt-5 flex items-center justify-between text-sm text-slate-600">
                <span>
                  Page {activeMeta.page} of {activeMeta.total_pages} · {activeMeta.total} total
                </span>
                <div className="flex gap-2">
                  <button
                    type="button"
                    className="ta-button-secondary"
                    disabled={activeMeta.page <= 1}
                    onClick={() => setPage((current) => Math.max(1, current - 1))}
                  >
                    Previous
                  </button>
                  <button
                    type="button"
                    className="ta-button-secondary"
                    disabled={activeMeta.page >= activeMeta.total_pages}
                    onClick={() => setPage((current) => current + 1)}
                  >
                    Next
                  </button>
                </div>
              </div>
            ) : null}
          </section>
        </div>
      )}

      <p className="text-xs text-slate-500">
        Campus administrator role is intentionally deferred; campus admin users currently receive teacher-equivalent
        catalog browse access.
      </p>
    </div>
  );
}
