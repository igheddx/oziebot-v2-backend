"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  fetchCatalogDistricts,
  fetchCatalogGrades,
  fetchCatalogObjectives,
  fetchCatalogSchools,
  fetchCatalogStates,
  fetchCatalogSubjects,
  fetchCatalogCurriculumResources,
} from "@/lib/education-catalog-api";
import {
  addCatalogPacingGuideObjective,
  addCatalogPacingGuideResource,
  createCatalogPacingGuide,
  createCatalogPacingGuidePeriod,
  deactivateCatalogPacingGuide,
  fetchCatalogPacingGuideDetail,
  fetchCatalogPacingGuides,
  fetchPacingGuideSchoolYearOptions,
} from "@/lib/pacing-guide-api";
import { PACING_GUIDE_PERIOD_TYPE_OPTIONS } from "@/lib/pacing-guide-types";
import type { CatalogPacingGuideDetail, CatalogPacingGuideSummary, PacingSchoolYearOption } from "@/lib/pacing-guide-types";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

export function TeacherAssistPacingGuideAdminPanel() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [guides, setGuides] = useState<CatalogPacingGuideSummary[]>([]);
  const [selectedGuideId, setSelectedGuideId] = useState("");
  const [detail, setDetail] = useState<CatalogPacingGuideDetail | null>(null);
  const [schoolYears, setSchoolYears] = useState<PacingSchoolYearOption[]>([]);
  const [states, setStates] = useState<Awaited<ReturnType<typeof fetchCatalogStates>>>([]);
  const [districts, setDistricts] = useState<Awaited<ReturnType<typeof fetchCatalogDistricts>>>([]);
  const [schools, setSchools] = useState<Awaited<ReturnType<typeof fetchCatalogSchools>>>([]);
  const [grades, setGrades] = useState<Awaited<ReturnType<typeof fetchCatalogGrades>>>([]);
  const [subjects, setSubjects] = useState<Awaited<ReturnType<typeof fetchCatalogSubjects>>>([]);
  const [schoolYearsLoading, setSchoolYearsLoading] = useState(true);
  const [objectives, setObjectives] = useState<Awaited<ReturnType<typeof fetchCatalogObjectives>>>([]);
  const [resources, setResources] = useState<Awaited<ReturnType<typeof fetchCatalogCurriculumResources>>>([]);
  const [guideForm, setGuideForm] = useState({
    school_year_id: "",
    title: "",
    description: "",
    catalog_state_id: "",
    catalog_district_id: "",
    catalog_school_id: "",
    catalog_grade_id: "",
    catalog_subject_id: "",
  });
  const [periodForm, setPeriodForm] = useState({
    period_type: "WEEK",
    title: "",
    description: "",
    sequence_number: "",
    start_date: "",
    end_date: "",
  });
  const [mappingForm, setMappingForm] = useState({
    period_id: "",
    objective_id: "",
    resource_id: "",
    notes: "",
  });

  const loadSchoolYearOptions = useCallback(async () => {
    setSchoolYearsLoading(true);
    try {
      const nextSchoolYears = await fetchPacingGuideSchoolYearOptions();
      setSchoolYears(nextSchoolYears.options);
      setGuideForm((current) => ({
        ...current,
        school_year_id:
          current.school_year_id && nextSchoolYears.options.some((row) => row.id === current.school_year_id)
            ? current.school_year_id
            : nextSchoolYears.default_school_year_id ||
              nextSchoolYears.options.find((row) => row.is_default)?.id ||
              nextSchoolYears.options[0]?.id ||
              "",
      }));
    } catch (nextError) {
      setSchoolYears([]);
      setSectionAlert(
        "pacing-admin",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load school year options.",
          "School years unavailable",
        ),
      );
    } finally {
      setSchoolYearsLoading(false);
    }
  }, [setSectionAlert]);

  const refresh = useCallback(async (preserveGuideId?: string) => {
    setLoading(true);
    clearSectionAlert("pacing-admin");
    try {
      const [
        nextGuides,
        nextStates,
        nextDistricts,
        nextSchools,
        nextGrades,
        nextObjectives,
        nextResources,
      ] = await Promise.all([
        fetchCatalogPacingGuides({ guide_type: "DISTRICT", active_only: true }),
        fetchCatalogStates(),
        fetchCatalogDistricts(),
        fetchCatalogSchools(),
        fetchCatalogGrades(),
        fetchCatalogObjectives(),
        fetchCatalogCurriculumResources(),
      ]);
      setGuides(nextGuides);
      setStates(nextStates);
      setDistricts(nextDistricts);
      setSchools(nextSchools);
      setGrades(nextGrades);
      setObjectives(nextObjectives);
      setResources(nextResources);
      await loadSchoolYearOptions();
      const nextSelected = preserveGuideId || selectedGuideId || nextGuides[0]?.id || "";
      setSelectedGuideId(nextSelected);
      if (nextSelected) {
        setDetail(await fetchCatalogPacingGuideDetail(nextSelected));
      } else {
        setDetail(null);
      }
    } catch (nextError) {
      setSectionAlert(
        "pacing-admin",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load pacing guides.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, loadSchoolYearOptions, selectedGuideId, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  useEffect(() => {
    if (!guideForm.catalog_grade_id) {
      setSubjects([]);
      setGuideForm((current) => ({ ...current, catalog_subject_id: "" }));
      return;
    }
    void fetchCatalogSubjects(guideForm.catalog_grade_id)
      .then((rows) => {
        const seen = new Set<string>();
        const distinct = rows.filter((row) => {
          const key = `${row.grade_id ?? ""}:${row.subject_code}`;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
        setSubjects(distinct);
        setGuideForm((current) => ({
          ...current,
          catalog_subject_id: distinct.some((row) => row.id === current.catalog_subject_id)
            ? current.catalog_subject_id
            : "",
        }));
      })
      .catch(() => setSubjects([]));
  }, [guideForm.catalog_grade_id]);

  const gradeLabelById = useMemo(
    () => new Map(grades.map((row) => [row.id, `${row.display_name} (${row.grade_code})`])),
    [grades],
  );
  const subjectLabelById = useMemo(
    () => new Map(subjects.map((row) => [row.id, `${row.display_name} (${row.subject_code})`])),
    [subjects],
  );
  const selectedSchoolYearId =
    guideForm.school_year_id ||
    schoolYears.find((row) => row.is_default)?.id ||
    schoolYears[0]?.id ||
    "";
  const canCreateGuide = Boolean(selectedSchoolYearId && guideForm.title.trim());

  const runSave = async (action: () => Promise<void>, successMessage: string) => {
    clearSectionAlert("pacing-admin");
    try {
      await withPreservedScroll("pacing-admin-panel", action);
      setSectionAlert("pacing-admin", sectionSuccess(successMessage));
      await refresh(selectedGuideId);
    } catch (nextError) {
      setSectionAlert(
        "pacing-admin",
        sectionError(nextError instanceof Error ? nextError.message : "Save failed.", "Unable to save"),
      );
    }
  };

  return (
    <div id="pacing-admin-panel" className="space-y-5">
      <TeacherAssistInlineAlert alert={getSectionAlert("pacing-admin")} />

      <div className="grid gap-5 xl:grid-cols-[0.9fr_1.1fr]">
        <article className="rounded-2xl border border-slate-200 bg-white p-4">
          <h3 className="text-base font-semibold text-slate-900">Create district pacing guide</h3>
          <form
            className="mt-4 grid gap-3"
            onSubmit={(event) => {
              event.preventDefault();
              if (!selectedSchoolYearId) {
                setSectionAlert(
                  "pacing-admin",
                  sectionError("Select a school year before creating a district pacing guide.", "School year required"),
                );
                return;
              }
              void runSave(async () => {
                const created = await createCatalogPacingGuide({
                  school_year_id: selectedSchoolYearId,
                  guide_type: "DISTRICT",
                  title: guideForm.title.trim(),
                  description: guideForm.description || null,
                  catalog_state_id: guideForm.catalog_state_id || null,
                  catalog_district_id: guideForm.catalog_district_id || null,
                  catalog_school_id: guideForm.catalog_school_id || null,
                  catalog_grade_id: guideForm.catalog_grade_id || null,
                  catalog_subject_id: guideForm.catalog_subject_id || null,
                  is_shared: true,
                });
                setSelectedGuideId(created.id);
                setGuideForm((current) => ({ ...current, title: "", description: "" }));
              }, "District pacing guide created.");
            }}
          >
            <select
              className="ta-input"
              value={selectedSchoolYearId}
              onChange={(e) => setGuideForm((c) => ({ ...c, school_year_id: e.target.value }))}
              required
              disabled={schoolYearsLoading || schoolYears.length === 0}
            >
              {schoolYearsLoading ? (
                <option value="">Loading school years...</option>
              ) : schoolYears.length === 0 ? (
                <option value="">No school years available</option>
              ) : (
                schoolYears.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.title}
                    {row.is_default ? " (default)" : ""}
                  </option>
                ))
              )}
            </select>
            <input className="ta-input" placeholder="Guide title" value={guideForm.title} onChange={(e) => setGuideForm((c) => ({ ...c, title: e.target.value }))} />
            <textarea className="ta-input min-h-20" placeholder="Description" value={guideForm.description} onChange={(e) => setGuideForm((c) => ({ ...c, description: e.target.value }))} />
            <select className="ta-input" value={guideForm.catalog_state_id} onChange={(e) => setGuideForm((c) => ({ ...c, catalog_state_id: e.target.value }))}>
              <option value="">State</option>
              {states.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
            </select>
            <select className="ta-input" value={guideForm.catalog_district_id} onChange={(e) => setGuideForm((c) => ({ ...c, catalog_district_id: e.target.value }))}>
              <option value="">District</option>
              {districts.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
            </select>
            <select className="ta-input" value={guideForm.catalog_school_id} onChange={(e) => setGuideForm((c) => ({ ...c, catalog_school_id: e.target.value }))}>
              <option value="">School</option>
              {schools.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
            </select>
            <select
              className="ta-input"
              value={guideForm.catalog_grade_id}
              onChange={(e) =>
                setGuideForm((c) => ({
                  ...c,
                  catalog_grade_id: e.target.value,
                  catalog_subject_id: "",
                }))
              }
            >
              <option value="">Grade</option>
              {grades.map((row) => (
                <option key={row.id} value={row.id}>
                  {gradeLabelById.get(row.id)}
                </option>
              ))}
            </select>
            <select
              className="ta-input"
              value={guideForm.catalog_subject_id}
              onChange={(e) => setGuideForm((c) => ({ ...c, catalog_subject_id: e.target.value }))}
              disabled={!guideForm.catalog_grade_id}
            >
              <option value="">{guideForm.catalog_grade_id ? "Subject" : "Select grade first"}</option>
              {subjects.map((row) => (
                <option key={row.id} value={row.id}>
                  {subjectLabelById.get(row.id)}
                </option>
              ))}
            </select>
            <button type="submit" className="ta-button-primary" disabled={!canCreateGuide}>
              Create district guide
            </button>
          </form>
        </article>

        <article className="rounded-2xl border border-slate-200 bg-white p-4">
          <h3 className="text-base font-semibold text-slate-900">District guides</h3>
          {loading ? (
            <p className="mt-4 text-sm text-slate-600">Loading pacing guides...</p>
          ) : guides.length === 0 ? (
            <p className="mt-4 text-sm text-slate-600">No district pacing guides yet.</p>
          ) : (
            <div className="mt-4 space-y-2">
              {guides.map((guide) => (
                <button
                  key={guide.id}
                  type="button"
                  onClick={() => {
                    setSelectedGuideId(guide.id);
                    void fetchCatalogPacingGuideDetail(guide.id).then(setDetail);
                  }}
                  className={`w-full rounded-xl border px-4 py-3 text-left text-sm ${
                    selectedGuideId === guide.id ? "border-sky-300 bg-sky-50" : "border-slate-200 bg-slate-50"
                  }`}
                >
                  <p className="font-semibold text-slate-900">{guide.title}</p>
                  <p className="mt-1 text-xs text-slate-600">
                    {guide.school_year_label ?? "School year"} · {guide.period_count} period{guide.period_count === 1 ? "" : "s"}
                  </p>
                </button>
              ))}
            </div>
          )}
        </article>
      </div>

      {detail ? (
        <div className="grid gap-5 xl:grid-cols-[0.95fr_1.05fr]">
          <article className="rounded-2xl border border-slate-200 bg-white p-4">
            <div className="flex items-start justify-between gap-3">
              <div>
                <h3 className="text-base font-semibold text-slate-900">{detail.title}</h3>
                <p className="mt-1 text-sm text-slate-600">{detail.description || "No description."}</p>
              </div>
              <button
                type="button"
                className="ta-button-secondary"
                onClick={() => {
                  void runSave(async () => {
                    await deactivateCatalogPacingGuide(detail.id);
                  }, "Pacing guide deactivated.");
                }}
              >
                Deactivate
              </button>
            </div>

            <form
              className="mt-4 grid gap-3 border-t border-slate-100 pt-4"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(async () => {
                  await createCatalogPacingGuidePeriod(detail.id, {
                    period_type: periodForm.period_type,
                    title: periodForm.title,
                    description: periodForm.description || null,
                    sequence_number: periodForm.sequence_number ? Number(periodForm.sequence_number) : null,
                    start_date: periodForm.start_date || null,
                    end_date: periodForm.end_date || null,
                  });
                  setPeriodForm((current) => ({ ...current, title: "", description: "", sequence_number: "" }));
                }, "Period added.");
              }}
            >
              <p className="text-sm font-semibold text-slate-900">Add period</p>
              <select className="ta-input" value={periodForm.period_type} onChange={(e) => setPeriodForm((c) => ({ ...c, period_type: e.target.value }))}>
                {PACING_GUIDE_PERIOD_TYPE_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>{option.label}</option>
                ))}
              </select>
              <input className="ta-input" placeholder="Period title" value={periodForm.title} onChange={(e) => setPeriodForm((c) => ({ ...c, title: e.target.value }))} />
              <textarea className="ta-input min-h-20" placeholder="Description" value={periodForm.description} onChange={(e) => setPeriodForm((c) => ({ ...c, description: e.target.value }))} />
              <div className="grid gap-3 md:grid-cols-3">
                <input className="ta-input" placeholder="Sequence" value={periodForm.sequence_number} onChange={(e) => setPeriodForm((c) => ({ ...c, sequence_number: e.target.value }))} />
                <input className="ta-input" type="date" value={periodForm.start_date} onChange={(e) => setPeriodForm((c) => ({ ...c, start_date: e.target.value }))} />
                <input className="ta-input" type="date" value={periodForm.end_date} onChange={(e) => setPeriodForm((c) => ({ ...c, end_date: e.target.value }))} />
              </div>
              <button type="submit" className="ta-button-primary">Add period</button>
            </form>
          </article>

          <article className="rounded-2xl border border-slate-200 bg-white p-4">
            <h3 className="text-base font-semibold text-slate-900">Periods, objectives, and resources</h3>
            <form
              className="mt-4 grid gap-3 rounded-xl border border-slate-100 bg-slate-50 p-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (!mappingForm.period_id) return;
                void runSave(async () => {
                  if (mappingForm.objective_id) {
                    await addCatalogPacingGuideObjective(detail.id, mappingForm.period_id, {
                      objective_id: mappingForm.objective_id,
                      is_required: true,
                      notes: mappingForm.notes || null,
                    });
                  }
                  if (mappingForm.resource_id) {
                    await addCatalogPacingGuideResource(detail.id, mappingForm.period_id, {
                      catalog_resource_id: mappingForm.resource_id,
                      is_primary: true,
                      notes: mappingForm.notes || null,
                    });
                  }
                  setMappingForm((current) => ({ ...current, notes: "" }));
                }, "Mappings added.");
              }}
            >
              <select className="ta-input" value={mappingForm.period_id} onChange={(e) => setMappingForm((c) => ({ ...c, period_id: e.target.value }))}>
                <option value="">Select period</option>
                {detail.periods.map((period) => (
                  <option key={period.id} value={period.id}>{period.sequence_number}. {period.title}</option>
                ))}
              </select>
              <select className="ta-input" value={mappingForm.objective_id} onChange={(e) => setMappingForm((c) => ({ ...c, objective_id: e.target.value }))}>
                <option value="">Catalog objective</option>
                {objectives.map((row) => (
                  <option key={row.id} value={row.id}>{row.objective_id} · {row.description}</option>
                ))}
              </select>
              <select className="ta-input" value={mappingForm.resource_id} onChange={(e) => setMappingForm((c) => ({ ...c, resource_id: e.target.value }))}>
                <option value="">Catalog resource</option>
                {resources.map((row) => (
                  <option key={row.id} value={row.id}>{row.title}</option>
                ))}
              </select>
              <input className="ta-input" placeholder="Notes" value={mappingForm.notes} onChange={(e) => setMappingForm((c) => ({ ...c, notes: e.target.value }))} />
              <button type="submit" className="ta-button-secondary">Add mappings</button>
            </form>

            <div className="mt-4 space-y-3">
              {detail.periods.map((period) => (
                <div key={period.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <p className="font-semibold text-slate-900">{period.sequence_number}. {period.title}</p>
                  <p className="mt-1 text-slate-600">{period.description || "No description."}</p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {period.objectives.map((row) => (
                      <span key={row.id} className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700">
                        {row.objective_code ?? "Objective"}
                      </span>
                    ))}
                    {period.resources.map((row) => (
                      <span key={row.id} className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-700">
                        {row.resource_title ?? "Resource"}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          </article>
        </div>
      ) : null}
    </div>
  );
}
