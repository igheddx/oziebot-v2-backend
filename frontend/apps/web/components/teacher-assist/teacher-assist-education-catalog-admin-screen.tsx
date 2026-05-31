"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "next/navigation";

import { useAuth } from "@/components/providers/auth-provider";
import { TeacherAssistPacingGuideAdminPanel } from "@/components/teacher-assist/teacher-assist-pacing-guide-admin-panel";
import { TeacherAssistTeacherAssignmentsPanel } from "@/components/teacher-assist/teacher-assist-teacher-assignments-panel";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  commitCatalogObjectivesImport,
  createCatalogCurriculumResource,
  createCatalogDistrict,
  createCatalogGrade,
  createCatalogObjective,
  createCatalogSchool,
  createCatalogState,
  createCatalogSubject,
  fetchCatalogCurriculumResources,
  fetchCatalogDistricts,
  fetchCatalogGrades,
  fetchCatalogObjectives,
  fetchCatalogSchools,
  fetchCatalogStates,
  fetchCatalogSubjects,
  previewCatalogObjectivesImport,
  updateCatalogCurriculumResource,
  updateCatalogDistrict,
  updateCatalogGrade,
  updateCatalogObjective,
  updateCatalogSchool,
  updateCatalogState,
  updateCatalogSubject,
} from "@/lib/education-catalog-api";
import type { CatalogSection } from "@/lib/education-catalog-types";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";

const SECTIONS: Array<{ key: CatalogSection; label: string }> = [
  { key: "states", label: "States" },
  { key: "districts", label: "Districts" },
  { key: "schools", label: "Schools" },
  { key: "grades", label: "Grades" },
  { key: "subjects", label: "Subjects" },
  { key: "objectives", label: "Objectives" },
  { key: "curriculum", label: "Curriculum" },
  { key: "assignments", label: "Teacher Assignments" },
  { key: "pacing_guides", label: "Pacing Guides" },
];

export function TeacherAssistEducationCatalogAdminScreen() {
  const { user } = useAuth();
  const searchParams = useSearchParams();
  const initialSection = useMemo(() => {
    const tab = searchParams.get("tab");
    return SECTIONS.some((row) => row.key === tab) ? (tab as CatalogSection) : "states";
  }, [searchParams]);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [section, setSection] = useState<CatalogSection>(initialSection);

  useEffect(() => {
    setSection(initialSection);
  }, [initialSection]);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [search, setSearch] = useState("");
  const [states, setStates] = useState<Awaited<ReturnType<typeof fetchCatalogStates>>>([]);
  const [districts, setDistricts] = useState<Awaited<ReturnType<typeof fetchCatalogDistricts>>>([]);
  const [schools, setSchools] = useState<Awaited<ReturnType<typeof fetchCatalogSchools>>>([]);
  const [objectives, setObjectives] = useState<Awaited<ReturnType<typeof fetchCatalogObjectives>>>([]);
  const [grades, setGrades] = useState<Awaited<ReturnType<typeof fetchCatalogGrades>>>([]);
  const [subjects, setSubjects] = useState<Awaited<ReturnType<typeof fetchCatalogSubjects>>>([]);
  const [curriculumResources, setCurriculumResources] = useState<
    Awaited<ReturnType<typeof fetchCatalogCurriculumResources>>
  >([]);
  const [stateForm, setStateForm] = useState({ name: "", abbreviation: "", active: true });
  const [districtForm, setDistrictForm] = useState({ state_id: "", name: "", active: true });
  const [schoolForm, setSchoolForm] = useState({ district_id: "", name: "", school_type: "elementary", active: true });
  const [gradeForm, setGradeForm] = useState({ school_id: "", grade_code: "", display_name: "", active: true });
  const [subjectForm, setSubjectForm] = useState({ grade_id: "", subject_code: "", display_name: "", active: true });
  const [curriculumForm, setCurriculumForm] = useState({
    school_id: "",
    grade_level: "5",
    subject_code: "",
    resource_type: "curriculum",
    title: "",
    description: "",
    storage_key: "",
    active: true,
  });
  const [objectiveForm, setObjectiveForm] = useState({
    state_id: "",
    grade_level: "5",
    subject_code: "Math",
    objective_type: "TEKS",
    objective_id: "",
    description: "",
    coverage_type: "required",
    active: true,
  });
  const [editingStateId, setEditingStateId] = useState<string | null>(null);
  const [editingDistrictId, setEditingDistrictId] = useState<string | null>(null);
  const [editingSchoolId, setEditingSchoolId] = useState<string | null>(null);
  const [editingGradeId, setEditingGradeId] = useState<string | null>(null);
  const [editingSubjectId, setEditingSubjectId] = useState<string | null>(null);
  const [editingCurriculumId, setEditingCurriculumId] = useState<string | null>(null);
  const [editingObjectiveId, setEditingObjectiveId] = useState<string | null>(null);
  const [importPreview, setImportPreview] = useState<Awaited<ReturnType<typeof previewCatalogObjectivesImport>> | null>(null);
  const [importRows, setImportRows] = useState<Array<Record<string, string>>>([]);

  const refresh = useCallback(
    async (silent = false) => {
      if (!silent) setLoading(true);
      setPageError(null);
      try {
        const [nextStates, nextDistricts, nextSchools, nextGrades, nextSubjects, nextObjectives, nextCurriculum] =
          await Promise.all([
          fetchCatalogStates(search || undefined),
          fetchCatalogDistricts(undefined, search || undefined),
          fetchCatalogSchools(undefined, search || undefined),
          fetchCatalogGrades(),
          fetchCatalogSubjects(),
          fetchCatalogObjectives({ q: search || undefined, active_only: false }),
          fetchCatalogCurriculumResources({ active_only: false }),
        ]);
        setStates(nextStates);
        setDistricts(nextDistricts);
        setSchools(nextSchools);
        setGrades(nextGrades);
        setSubjects(nextSubjects);
        setObjectives(nextObjectives);
        setCurriculumResources(nextCurriculum);
      } catch (nextError) {
        if (!silent) {
          setPageError(nextError instanceof Error ? nextError.message : "Could not load education catalog.");
        }
      } finally {
        if (!silent) setLoading(false);
      }
    },
    [search],
  );

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const stateNameById = useMemo(() => new Map(states.map((row) => [row.id, row.name])), [states]);
  const districtNameById = useMemo(() => new Map(districts.map((row) => [row.id, row.name])), [districts]);
  const schoolNameById = useMemo(() => new Map(schools.map((row) => [row.id, row.name])), [schools]);
  const schoolById = useMemo(() => new Map(schools.map((row) => [row.id, row])), [schools]);
  const districtById = useMemo(() => new Map(districts.map((row) => [row.id, row])), [districts]);
  const gradeLabelById = useMemo(
    () => new Map(grades.map((row) => [row.id, `${row.display_name} (${schoolNameById.get(row.school_id ?? "") ?? "global"})`])),
    [grades, schoolNameById],
  );

  const runSave = async (action: () => Promise<void>, success: string, successOverride?: string) => {
    clearSectionAlert("catalog");
    try {
      await withPreservedScroll("education-catalog-panel", action);
      await refresh(true);
      setSectionAlert("catalog", sectionSuccess(successOverride ?? success));
    } catch (nextError) {
      setSectionAlert(
        "catalog",
        sectionError(nextError instanceof Error ? nextError.message : "Request failed.", "Unable to save"),
      );
    }
  };

  if (!user?.is_root_admin) {
    return (
      <TeacherAssistFormErrorSummary
        title="Root admin access required"
        message="Education Catalog administration is limited to TeacherAssist root admins."
      />
    );
  }

  return (
    <div className="space-y-6">
      <section className="ta-panel p-5 sm:p-6">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.18em] text-sky-700">Administration</p>
          <h1 className="mt-2 text-2xl font-semibold text-slate-900">Education Catalog</h1>
          <p className="mt-1 text-sm text-slate-600">
            Platform-managed instructional hierarchy: state, district, school, grade, subject, objectives, and curriculum.
          </p>
        </div>
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load catalog" message={pageError} />

      <div className="grid gap-6 lg:grid-cols-[220px_1fr]">
        <nav className="ta-panel p-3">
          <p className="px-2 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Catalog</p>
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
          <Link href="/teacher-assist/catalog" className="mt-4 block px-3 text-xs font-medium text-sky-700">
            Teacher catalog view
          </Link>
        </nav>

        <section id="education-catalog-panel" className="ta-panel p-6">
          <div className="flex flex-wrap items-center gap-3">
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              className="ta-input max-w-xs"
              placeholder="Search..."
            />
            <button type="button" className="ta-button-secondary" onClick={() => void refresh()}>
              Refresh
            </button>
          </div>

          <TeacherAssistInlineAlert
            alert={getSectionAlert("catalog")}
            onDismiss={() => clearSectionAlert("catalog")}
            className="mt-4"
          />

          {loading ? (
            <p className="mt-5 text-sm text-slate-600">Loading catalog...</p>
          ) : section === "states" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const payload = { ...stateForm };
                    if (editingStateId) {
                      await updateCatalogState(editingStateId, payload);
                    } else {
                      await createCatalogState(payload);
                    }
                    setStateForm({ name: "", abbreviation: "", active: true });
                    setEditingStateId(null);
                  }, editingStateId ? "State updated." : "State created.");
                }}
              >
                <input className="ta-input" placeholder="Name" value={stateForm.name} onChange={(e) => setStateForm((c) => ({ ...c, name: e.target.value }))} />
                <input className="ta-input" placeholder="Abbreviation" value={stateForm.abbreviation} onChange={(e) => setStateForm((c) => ({ ...c, abbreviation: e.target.value }))} />
                <button type="submit" className="ta-button-primary">{editingStateId ? "Save state" : "Add state"}</button>
              </form>
              <div className="space-y-2">
                {states.map((row) => (
                  <div key={row.id} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 text-sm">
                    <span>{row.name} ({row.abbreviation}) {row.active ? "" : "· inactive"}</span>
                    <button type="button" className="ta-button-secondary" onClick={() => { setEditingStateId(row.id); setStateForm({ name: row.name, abbreviation: row.abbreviation, active: row.active }); }}>Edit</button>
                  </div>
                ))}
              </div>
            </div>
          ) : section === "districts" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const payload = { ...districtForm };
                    if (editingDistrictId) {
                      await updateCatalogDistrict(editingDistrictId, payload);
                    } else {
                      await createCatalogDistrict(payload);
                    }
                    setDistrictForm({ state_id: states[0]?.id ?? "", name: "", active: true });
                    setEditingDistrictId(null);
                  }, editingDistrictId ? "District updated." : "District created.");
                }}
              >
                <select className="ta-input" value={districtForm.state_id} onChange={(e) => setDistrictForm((c) => ({ ...c, state_id: e.target.value }))}>
                  <option value="">Select state</option>
                  {states.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
                </select>
                <input className="ta-input" placeholder="District name" value={districtForm.name} onChange={(e) => setDistrictForm((c) => ({ ...c, name: e.target.value }))} />
                <button type="submit" className="ta-button-primary">{editingDistrictId ? "Save district" : "Add district"}</button>
              </form>
              {districts.map((row) => (
                <div key={row.id} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <span>{row.name} · {stateNameById.get(row.state_id) ?? row.state_id} {row.active ? "" : "· inactive"}</span>
                  <button type="button" className="ta-button-secondary" onClick={() => { setEditingDistrictId(row.id); setDistrictForm({ state_id: row.state_id, name: row.name, active: row.active }); }}>Edit</button>
                </div>
              ))}
            </div>
          ) : section === "schools" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-4"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const payload = { ...schoolForm };
                    if (editingSchoolId) {
                      await updateCatalogSchool(editingSchoolId, payload);
                    } else {
                      await createCatalogSchool(payload);
                    }
                    setSchoolForm({ district_id: districts[0]?.id ?? "", name: "", school_type: "elementary", active: true });
                    setEditingSchoolId(null);
                  }, editingSchoolId ? "School updated." : "School created.");
                }}
              >
                <select className="ta-input" value={schoolForm.district_id} onChange={(e) => setSchoolForm((c) => ({ ...c, district_id: e.target.value }))}>
                  <option value="">Select district</option>
                  {districts.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
                </select>
                <input className="ta-input" placeholder="School name" value={schoolForm.name} onChange={(e) => setSchoolForm((c) => ({ ...c, name: e.target.value }))} />
                <input className="ta-input" placeholder="School type" value={schoolForm.school_type} onChange={(e) => setSchoolForm((c) => ({ ...c, school_type: e.target.value }))} />
                <button type="submit" className="ta-button-primary">{editingSchoolId ? "Save school" : "Add school"}</button>
              </form>
              {schools.map((row) => (
                <div key={row.id} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <span>{row.name} · {districtNameById.get(row.district_id) ?? row.district_id} {row.active ? "" : "· inactive"}</span>
                  <button type="button" className="ta-button-secondary" onClick={() => { setEditingSchoolId(row.id); setSchoolForm({ district_id: row.district_id, name: row.name, school_type: row.school_type ?? "elementary", active: row.active }); }}>Edit</button>
                </div>
              ))}
            </div>
          ) : section === "grades" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-4"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const payload = { ...gradeForm, school_id: gradeForm.school_id || null };
                    if (editingGradeId) {
                      await updateCatalogGrade(editingGradeId, payload);
                    } else {
                      await createCatalogGrade(payload);
                    }
                    setGradeForm({ school_id: schools[0]?.id ?? "", grade_code: "", display_name: "", active: true });
                    setEditingGradeId(null);
                  }, editingGradeId ? "Grade updated." : "Grade created.");
                }}
              >
                <select className="ta-input" value={gradeForm.school_id} onChange={(e) => setGradeForm((c) => ({ ...c, school_id: e.target.value }))}>
                  <option value="">Global (no school)</option>
                  {schools.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
                </select>
                <input className="ta-input" placeholder="Grade code (K, 1, 2…)" value={gradeForm.grade_code} onChange={(e) => setGradeForm((c) => ({ ...c, grade_code: e.target.value }))} />
                <input className="ta-input" placeholder="Display name" value={gradeForm.display_name} onChange={(e) => setGradeForm((c) => ({ ...c, display_name: e.target.value }))} />
                <button type="submit" className="ta-button-primary">{editingGradeId ? "Save grade" : "Add grade"}</button>
              </form>
              {grades.map((row) => (
                <div key={row.id} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <span>{row.display_name} ({row.grade_code}) · {schoolNameById.get(row.school_id ?? "") ?? "global"} {row.active ? "" : "· inactive"}</span>
                  <button type="button" className="ta-button-secondary" onClick={() => { setEditingGradeId(row.id); setGradeForm({ school_id: row.school_id ?? "", grade_code: row.grade_code, display_name: row.display_name, active: row.active }); }}>Edit</button>
                </div>
              ))}
            </div>
          ) : section === "subjects" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-4"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const payload = { ...subjectForm, grade_id: subjectForm.grade_id || null };
                    if (editingSubjectId) {
                      await updateCatalogSubject(editingSubjectId, payload);
                    } else {
                      await createCatalogSubject(payload);
                    }
                    setSubjectForm({ grade_id: grades[0]?.id ?? "", subject_code: "", display_name: "", active: true });
                    setEditingSubjectId(null);
                  }, editingSubjectId ? "Subject updated." : "Subject created.");
                }}
              >
                <select className="ta-input" value={subjectForm.grade_id} onChange={(e) => setSubjectForm((c) => ({ ...c, grade_id: e.target.value }))}>
                  <option value="">Global (no grade)</option>
                  {grades.map((row) => <option key={row.id} value={row.id}>{gradeLabelById.get(row.id)}</option>)}
                </select>
                <input className="ta-input" placeholder="Subject code" value={subjectForm.subject_code} onChange={(e) => setSubjectForm((c) => ({ ...c, subject_code: e.target.value }))} />
                <input className="ta-input" placeholder="Display name" value={subjectForm.display_name} onChange={(e) => setSubjectForm((c) => ({ ...c, display_name: e.target.value }))} />
                <button type="submit" className="ta-button-primary">{editingSubjectId ? "Save subject" : "Add subject"}</button>
              </form>
              {subjects.map((row) => (
                <div key={row.id} className="flex items-center justify-between rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <span>{row.display_name} ({row.subject_code}) · {gradeLabelById.get(row.grade_id ?? "") ?? "global"} {row.active ? "" : "· inactive"}</span>
                  <button type="button" className="ta-button-secondary" onClick={() => { setEditingSubjectId(row.id); setSubjectForm({ grade_id: row.grade_id ?? "", subject_code: row.subject_code, display_name: row.display_name, active: row.active }); }}>Edit</button>
                </div>
              ))}
            </div>
          ) : section === "objectives" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    if (editingObjectiveId) {
                      await updateCatalogObjective(editingObjectiveId, objectiveForm);
                    } else {
                      await createCatalogObjective(objectiveForm);
                    }
                    setObjectiveForm((c) => ({ ...c, objective_id: "", description: "" }));
                    setEditingObjectiveId(null);
                  }, editingObjectiveId ? "Objective updated." : "Objective created.");
                }}
              >
                <select className="ta-input" value={objectiveForm.state_id} onChange={(e) => setObjectiveForm((c) => ({ ...c, state_id: e.target.value }))}>
                  <option value="">Select state</option>
                  {states.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
                </select>
                <input className="ta-input" placeholder="Grade level" value={objectiveForm.grade_level} onChange={(e) => setObjectiveForm((c) => ({ ...c, grade_level: e.target.value }))} />
                <input className="ta-input" placeholder="Subject code" value={objectiveForm.subject_code} onChange={(e) => setObjectiveForm((c) => ({ ...c, subject_code: e.target.value }))} />
                <input className="ta-input" placeholder="Objective ID" value={objectiveForm.objective_id} onChange={(e) => setObjectiveForm((c) => ({ ...c, objective_id: e.target.value }))} />
                <textarea className="ta-input min-h-24 md:col-span-2" placeholder="Description" value={objectiveForm.description} onChange={(e) => setObjectiveForm((c) => ({ ...c, description: e.target.value }))} />
                <button type="submit" className="ta-button-primary md:col-span-2">{editingObjectiveId ? "Save objective" : "Add objective"}</button>
              </form>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p className="text-sm font-semibold text-slate-900">CSV import</p>
                <p className="mt-1 text-xs text-slate-600">Headers: state_abbreviation,grade_level,subject_code,objective_type,objective_id,description,coverage_type</p>
                <input
                  type="file"
                  accept=".csv,text/csv"
                  className="mt-3 block text-sm"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    event.target.value = "";
                    if (!file) return;
                    void file.text().then(async (csv_content) => {
                      const lines = csv_content.trim().split("\n");
                      const headers = (lines[0] ?? "").split(",").map((value) => value.trim());
                      const rows = lines.slice(1).map((line) => {
                        const values = line.split(",");
                        return Object.fromEntries(headers.map((header, index) => [header, (values[index] ?? "").trim()]));
                      });
                      setImportRows(rows);
                      const preview = await previewCatalogObjectivesImport({ csv_content });
                      setImportPreview(preview);
                    });
                  }}
                />
                {importPreview ? (
                  <div className="mt-3 space-y-2 text-sm">
                    <p>Valid: {importPreview.valid_count} · Invalid: {importPreview.invalid_count} · Duplicates: {importPreview.duplicate_count}</p>
                    {importPreview.valid_count > 0 ? (
                      <button
                        type="button"
                        className="ta-button-primary"
                        onClick={() => {
                          void (async () => {
                            clearSectionAlert("catalog");
                            try {
                              await withPreservedScroll("education-catalog-panel", async () => {
                                const result = await commitCatalogObjectivesImport({ rows: importRows });
                                setImportPreview(null);
                                setImportRows([]);
                                setSectionAlert(
                                  "catalog",
                                  sectionSuccess(
                                    `${result.created_count} objective${result.created_count === 1 ? "" : "s"} imported.` +
                                      (result.skipped_duplicate_count
                                        ? ` ${result.skipped_duplicate_count} duplicate${result.skipped_duplicate_count === 1 ? "" : "s"} skipped.`
                                        : ""),
                                  ),
                                );
                              });
                              await refresh(true);
                            } catch (nextError) {
                              setSectionAlert(
                                "catalog",
                                sectionError(nextError instanceof Error ? nextError.message : "Import failed.", "Unable to import"),
                              );
                            }
                          })();
                        }}
                      >
                        Import valid rows
                      </button>
                    ) : null}
                  </div>
                ) : null}
              </div>
              {objectives.map((row) => (
                <div key={row.id} className="flex items-start justify-between gap-3 rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <div>
                    <p className="font-semibold text-slate-900">{row.objective_id}</p>
                    <p className="mt-1 text-slate-600">{row.description}</p>
                  </div>
                  <button type="button" className="ta-button-secondary" onClick={() => { setEditingObjectiveId(row.id); setObjectiveForm({ state_id: row.state_id, grade_level: row.grade_level, subject_code: row.subject_code, objective_type: row.objective_type, objective_id: row.objective_id, description: row.description, coverage_type: row.coverage_type, active: row.active }); }}>Edit</button>
                </div>
              ))}
            </div>
          ) : section === "curriculum" ? (
            <div className="mt-5 space-y-5">
              <form
                className="grid gap-3 md:grid-cols-2"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(async () => {
                    const school = schoolById.get(curriculumForm.school_id);
                    const district = school ? districtById.get(school.district_id) : undefined;
                    const payload = {
                      state_id: district?.state_id ?? null,
                      district_id: school?.district_id ?? null,
                      school_id: curriculumForm.school_id || null,
                      grade_level: curriculumForm.grade_level,
                      subject_code: curriculumForm.subject_code,
                      resource_type: curriculumForm.resource_type,
                      title: curriculumForm.title,
                      description: curriculumForm.description || null,
                      storage_key: curriculumForm.storage_key || null,
                      active: curriculumForm.active,
                    };
                    if (editingCurriculumId) {
                      await updateCatalogCurriculumResource(editingCurriculumId, payload);
                    } else {
                      await createCatalogCurriculumResource(payload);
                    }
                    setCurriculumForm({
                      school_id: schools[0]?.id ?? "",
                      grade_level: "5",
                      subject_code: "",
                      resource_type: "curriculum",
                      title: "",
                      description: "",
                      storage_key: "",
                      active: true,
                    });
                    setEditingCurriculumId(null);
                  }, editingCurriculumId ? "Curriculum resource updated." : "Curriculum resource created.");
                }}
              >
                <select className="ta-input" value={curriculumForm.school_id} onChange={(e) => setCurriculumForm((c) => ({ ...c, school_id: e.target.value }))}>
                  <option value="">No school scope</option>
                  {schools.map((row) => <option key={row.id} value={row.id}>{row.name}</option>)}
                </select>
                <select className="ta-input" value={curriculumForm.resource_type} onChange={(e) => setCurriculumForm((c) => ({ ...c, resource_type: e.target.value }))}>
                  <option value="curriculum">Curriculum</option>
                  <option value="textbook">Textbook</option>
                  <option value="reference">Reference</option>
                </select>
                <input className="ta-input" placeholder="Grade level" value={curriculumForm.grade_level} onChange={(e) => setCurriculumForm((c) => ({ ...c, grade_level: e.target.value }))} />
                <input className="ta-input" placeholder="Subject code" value={curriculumForm.subject_code} onChange={(e) => setCurriculumForm((c) => ({ ...c, subject_code: e.target.value }))} />
                <input className="ta-input md:col-span-2" placeholder="Title" value={curriculumForm.title} onChange={(e) => setCurriculumForm((c) => ({ ...c, title: e.target.value }))} />
                <textarea className="ta-input min-h-20 md:col-span-2" placeholder="Description" value={curriculumForm.description} onChange={(e) => setCurriculumForm((c) => ({ ...c, description: e.target.value }))} />
                <input className="ta-input md:col-span-2" placeholder="Storage key (optional placeholder PDF path)" value={curriculumForm.storage_key} onChange={(e) => setCurriculumForm((c) => ({ ...c, storage_key: e.target.value }))} />
                <button type="submit" className="ta-button-primary md:col-span-2">{editingCurriculumId ? "Save resource" : "Add resource"}</button>
              </form>
              <p className="text-xs text-slate-600">Reference links are seeded with curriculum resources. Manage links via API in this phase.</p>
              {curriculumResources.map((row) => (
                <div key={row.id} className="flex items-start justify-between gap-3 rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <div>
                    <p className="font-semibold text-slate-900">{row.title}</p>
                    <p className="mt-1 text-slate-600">{row.resource_type} · Grade {row.grade_level} · {row.subject_code} {row.active ? "" : "· inactive"}</p>
                  </div>
                  <button
                    type="button"
                    className="ta-button-secondary"
                    onClick={() => {
                      setEditingCurriculumId(row.id);
                      setCurriculumForm({
                        school_id: row.school_id ?? "",
                        grade_level: row.grade_level,
                        subject_code: row.subject_code,
                        resource_type: row.resource_type,
                        title: row.title,
                        description: row.description ?? "",
                        storage_key: row.storage_key ?? "",
                        active: row.active,
                      });
                    }}
                  >
                    Edit
                  </button>
                </div>
              ))}
            </div>
          ) : section === "assignments" ? (
            <TeacherAssistTeacherAssignmentsPanel
              states={states}
              districts={districts}
              schools={schools}
              grades={grades}
              onRefreshCatalog={() => refresh(true)}
            />
          ) : (
            <div className="mt-5">
              <TeacherAssistPacingGuideAdminPanel />
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
