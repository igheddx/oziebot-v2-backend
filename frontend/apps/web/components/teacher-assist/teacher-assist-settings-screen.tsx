"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  attachClassSubject,
  commitStandardsImport,
  createClass,
  createGradingPeriod,
  createSchoolYear,
  createStandard,
  createSubject,
  fetchClasses,
  fetchGradingPeriods,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherAssistOptions,
  fetchTeacherProfile,
  previewStandardsImport,
  saveTeacherProfile,
  updateClass,
  updateGradingPeriod,
  updateSchoolYear,
  updateStandard,
} from "@/lib/teacher-assist-api";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";
import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import {
  TeacherAssistFieldError,
  fieldErrorInputClass,
} from "@/components/teacher-assist/teacher-assist-field-error";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  sectionWarning,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import type {
  GradingPeriod,
  SchoolYear,
  Standard,
  StandardImportPreview,
  Subject,
  TeacherAssistOptions,
  TeacherClass,
  TeacherProfile,
} from "@/lib/teacher-assist-types";

const SECTION_ELEMENT_IDS: Record<string, string> = {
  profile: "teacher-profile",
  schoolYear: "school-years",
  gradingPeriods: "grading-periods",
  classes: "classes",
  subjects: "subjects",
  classSubjects: "subjects",
  standards: "standards",
};

const STANDARD_CSV_FORMAT = `code,type,subject,description
5.ELA.1,TEKS,ELA,"Students will identify the main idea and supporting details in informational and literary texts."
5.MATH.1,TEKS,Math,"Students will add, subtract, multiply, and divide decimals to solve real-world problems."`;

type SetupSnapshot = {
  options: TeacherAssistOptions;
  profile: TeacherProfile;
  schoolYears: SchoolYear[];
  gradingPeriods: GradingPeriod[];
  classes: TeacherClass[];
  subjects: Subject[];
  standards: Standard[];
};

type SchoolYearForm = {
  title: string;
  start_date: string;
  end_date: string;
  is_active: boolean;
};

type GradingPeriodForm = {
  school_year_id: string;
  title: string;
  grading_period_type: string;
  start_date: string;
  end_date: string;
  sort_order: string;
};

type ClassForm = {
  school_year_id: string;
  name: string;
  grade_level: string;
  student_count: string;
};

type SubjectForm = {
  code: string;
  name: string;
};

type StandardForm = {
  subject_id: string;
  standard_type: string;
  code: string;
  description: string;
  grade_level: string;
  school_year_id: string;
};

function formatDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function emptySchoolYearForm(): SchoolYearForm {
  return { title: "", start_date: "", end_date: "", is_active: false };
}

function emptyGradingPeriodForm(): GradingPeriodForm {
  return {
    school_year_id: "",
    title: "",
    grading_period_type: "",
    start_date: "",
    end_date: "",
    sort_order: "0",
  };
}

function emptyClassForm(): ClassForm {
  return { school_year_id: "", name: "", grade_level: "", student_count: "" };
}

function emptySubjectForm(): SubjectForm {
  return { code: "", name: "" };
}

function emptyStandardForm(): StandardForm {
  return {
    subject_id: "",
    standard_type: "",
    code: "",
    description: "",
    grade_level: "",
    school_year_id: "",
  };
}

function validateGradingPeriodForm(form: GradingPeriodForm): Partial<Record<keyof GradingPeriodForm, string>> {
  const errors: Partial<Record<keyof GradingPeriodForm, string>> = {};
  if (!form.school_year_id) errors.school_year_id = "Select a school year.";
  if (!form.title.trim()) errors.title = "Enter a title (for example, 9 Weeks 1).";
  if (!form.grading_period_type) errors.grading_period_type = "Select a grading period type.";
  if (!form.start_date) errors.start_date = "Enter a start date.";
  if (!form.end_date) errors.end_date = "Enter an end date.";
  if (form.start_date && form.end_date && form.end_date < form.start_date) {
    errors.end_date = "End date must be on or after the start date.";
  }
  return errors;
}

function validateStandardForm(form: StandardForm): Partial<Record<keyof StandardForm, string>> {
  const errors: Partial<Record<keyof StandardForm, string>> = {};
  if (!form.subject_id) errors.subject_id = "Select a subject before saving this standard.";
  if (!form.standard_type) errors.standard_type = "Select a standard type.";
  if (!form.code.trim()) errors.code = "Enter a standard code.";
  if (!form.description.trim()) errors.description = "Enter a description.";
  return errors;
}

export function TeacherAssistSettingsScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [snapshot, setSnapshot] = useState<SetupSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [savingKey, setSavingKey] = useState<string | null>(null);

  const [profileForm, setProfileForm] = useState<TeacherProfile>({
    id: null,
    preferred_grade_level: null,
    default_student_count: null,
    preferred_grading_period_type: null,
    timezone: null,
    created_at: null,
    updated_at: null,
  });
  const [schoolYearForm, setSchoolYearForm] = useState<SchoolYearForm>(emptySchoolYearForm());
  const [editingSchoolYearId, setEditingSchoolYearId] = useState<string | null>(null);
  const [gradingPeriodForm, setGradingPeriodForm] = useState<GradingPeriodForm>(emptyGradingPeriodForm());
  const [gradingPeriodFieldErrors, setGradingPeriodFieldErrors] = useState<
    Partial<Record<keyof GradingPeriodForm, string>>
  >({});
  const [editingGradingPeriodId, setEditingGradingPeriodId] = useState<string | null>(null);
  const [classForm, setClassForm] = useState<ClassForm>(emptyClassForm());
  const [editingClassId, setEditingClassId] = useState<string | null>(null);
  const [subjectForm, setSubjectForm] = useState<SubjectForm>(emptySubjectForm());
  const [classSubjectForm, setClassSubjectForm] = useState({ class_id: "", subject_id: "" });
  const [standardForm, setStandardForm] = useState<StandardForm>(emptyStandardForm());
  const [standardFieldErrors, setStandardFieldErrors] = useState<
    Partial<Record<keyof StandardForm, string>>
  >({});
  const [editingStandardId, setEditingStandardId] = useState<string | null>(null);
  const [keepSubjectForNext, setKeepSubjectForNext] = useState(false);
  const [importPreview, setImportPreview] = useState<StandardImportPreview | null>(null);
  const [importFileName, setImportFileName] = useState<string | null>(null);
  const [importBusy, setImportBusy] = useState(false);
  const standardFormRef = useRef<HTMLFormElement | null>(null);
  const importFileInputRef = useRef<HTMLInputElement | null>(null);

  const fetchSnapshotData = useCallback(async (): Promise<SetupSnapshot> => {
    const [options, profile, schoolYears, gradingPeriods, classes, subjects, standards] = await Promise.all([
      fetchTeacherAssistOptions(),
      fetchTeacherProfile(),
      fetchSchoolYears(),
      fetchGradingPeriods(),
      fetchClasses(),
      fetchSubjects(),
      fetchStandards(),
    ]);
    return { options, profile, schoolYears, gradingPeriods, classes, subjects, standards };
  }, []);

  const initializeForms = useCallback((data: SetupSnapshot) => {
    setProfileForm(data.profile);
    setSchoolYearForm(emptySchoolYearForm());
    setGradingPeriodForm({
      ...emptyGradingPeriodForm(),
      school_year_id: data.schoolYears.find((row) => row.is_active)?.id ?? data.schoolYears[0]?.id ?? "",
      grading_period_type: data.options.grading_period_types[0] ?? "",
    });
    setClassForm({
      ...emptyClassForm(),
      school_year_id: data.schoolYears.find((row) => row.is_active)?.id ?? data.schoolYears[0]?.id ?? "",
      grade_level: data.profile.preferred_grade_level ?? data.options.supported_grade_levels[0] ?? "",
      student_count: data.profile.default_student_count?.toString() ?? "",
    });
    setSubjectForm(emptySubjectForm());
    setClassSubjectForm({
      class_id: data.classes[0]?.id ?? "",
      subject_id: data.subjects[0]?.id ?? "",
    });
    setStandardForm({
      ...emptyStandardForm(),
      standard_type: data.options.standard_types[0] ?? "",
      school_year_id: data.schoolYears.find((row) => row.is_active)?.id ?? data.schoolYears[0]?.id ?? "",
      grade_level: data.profile.preferred_grade_level ?? "",
    });
    setStandardFieldErrors({});
    setEditingStandardId(null);
    setEditingSchoolYearId(null);
    setEditingGradingPeriodId(null);
    setEditingClassId(null);
  }, []);

  const refreshSnapshot = useCallback(async () => {
    const data = await fetchSnapshotData();
    setSnapshot(data);
    return data;
  }, [fetchSnapshotData]);

  const loadSnapshot = useCallback(async () => {
    setLoading(true);
    setPageError(null);
    try {
      const data = await fetchSnapshotData();
      setSnapshot(data);
      initializeForms(data);
    } catch (nextError) {
      setPageError(nextError instanceof Error ? nextError.message : "Could not load TeacherAssist setup.");
    } finally {
      setLoading(false);
    }
  }, [fetchSnapshotData, initializeForms]);

  useEffect(() => {
    void loadSnapshot();
  }, [loadSnapshot]);

  const subjectNameById = useMemo(
    () => new Map((snapshot?.subjects ?? []).map((subject) => [subject.id, subject.name])),
    [snapshot?.subjects],
  );
  const schoolYearTitleById = useMemo(
    () => new Map((snapshot?.schoolYears ?? []).map((schoolYear) => [schoolYear.id, schoolYear.title])),
    [snapshot?.schoolYears],
  );
  const activeSchoolYear = snapshot?.schoolYears.find((row) => row.is_active) ?? null;
  const setupIssues = [
    !activeSchoolYear ? "Create an active school year." : null,
    (snapshot?.gradingPeriods.length ?? 0) === 0 ? "Add grading periods inside that school year." : null,
    (snapshot?.classes.length ?? 0) === 0 ? "Create at least one class and set the student count." : null,
    (snapshot?.subjects.length ?? 0) === 0 ? "Add the subjects you teach." : null,
    (snapshot?.standards.length ?? 0) === 0 ? "Enter standards or TEKS for later planning phases." : null,
  ].filter(Boolean) as string[];

  const beginSchoolYearEdit = (row: SchoolYear) => {
    setEditingSchoolYearId(row.id);
    setSchoolYearForm({
      title: row.title,
      start_date: row.start_date,
      end_date: row.end_date,
      is_active: row.is_active,
    });
  };

  const beginGradingPeriodEdit = (row: GradingPeriod) => {
    setEditingGradingPeriodId(row.id);
    setGradingPeriodForm({
      school_year_id: row.school_year_id,
      title: row.title,
      grading_period_type: row.grading_period_type,
      start_date: row.start_date,
      end_date: row.end_date,
      sort_order: row.sort_order.toString(),
    });
  };

  const beginClassEdit = (row: TeacherClass) => {
    setEditingClassId(row.id);
    setClassForm({
      school_year_id: row.school_year_id,
      name: row.name,
      grade_level: row.grade_level,
      student_count: row.student_count.toString(),
    });
  };

  const beginStandardEdit = (row: Standard) => {
    setEditingStandardId(row.id);
    setStandardFieldErrors({});
    setStandardForm({
      subject_id: row.subject_id ?? "",
      standard_type: row.standard_type,
      code: row.code,
      description: row.description,
      grade_level: row.grade_level ?? "",
      school_year_id: row.school_year_id ?? "",
    });
    standardFormRef.current?.scrollIntoView({ block: "nearest" });
  };

  const cancelStandardEdit = () => {
    setEditingStandardId(null);
    setStandardFieldErrors({});
    if (!snapshot) return;
    setStandardForm({
      ...emptyStandardForm(),
      standard_type: snapshot.options.standard_types[0] ?? "",
      subject_id: keepSubjectForNext ? standardForm.subject_id : "",
      school_year_id:
        snapshot.schoolYears.find((row) => row.is_active)?.id ?? snapshot.schoolYears[0]?.id ?? "",
      grade_level: snapshot.profile.preferred_grade_level ?? "",
    });
  };

  const resetStandardFormAfterSave = () => {
    if (!snapshot) return;
    const preservedSubjectId = keepSubjectForNext ? standardForm.subject_id : "";
    setStandardForm({
      ...emptyStandardForm(),
      standard_type: snapshot.options.standard_types[0] ?? "",
      subject_id: preservedSubjectId,
      school_year_id:
        snapshot.schoolYears.find((row) => row.is_active)?.id ?? snapshot.schoolYears[0]?.id ?? "",
      grade_level: snapshot.profile.preferred_grade_level ?? "",
    });
    setStandardFieldErrors({});
    setEditingStandardId(null);
    requestAnimationFrame(() => {
      standardFormRef.current?.querySelector<HTMLElement>("select, input, textarea")?.focus();
    });
  };

  const runSave = async (
    sectionKey: string,
    key: string,
    action: () => Promise<void>,
    successAlert: { title?: string; description: string },
    options?: { onSuccess?: () => void },
  ) => {
    setSavingKey(key);
    clearSectionAlert(sectionKey);
    try {
      await withPreservedScroll(SECTION_ELEMENT_IDS[sectionKey] ?? null, async () => {
        await action();
        await refreshSnapshot();
        options?.onSuccess?.();
      });
      setSectionAlert(sectionKey, sectionSuccess(successAlert.description, successAlert.title));
    } catch (nextError) {
      setSectionAlert(
        sectionKey,
        sectionError(
          nextError instanceof Error ? nextError.message : "Request failed.",
          "Unable to save",
        ),
      );
    } finally {
      setSavingKey(null);
    }
  };

  return (
    <div className="space-y-6">
      <section className="ta-panel p-5 sm:p-6">
        <div className="max-w-3xl">
          <h1 className="text-2xl font-semibold tracking-tight text-slate-900">Settings</h1>
          <p className="mt-1 text-sm text-slate-600">
            School years, grading periods, classes, subjects, and standards.
          </p>
        </div>
      </section>

      <TeacherAssistFormErrorSummary
        title="Unable to load settings"
        message={pageError}
      />
      {setupIssues.length > 0 ? (
        <TeacherAssistAlert
          variant="warning"
          title="Setup incomplete"
          description={
            <ul className="list-disc space-y-1 pl-5">
              {setupIssues.map((issue) => (
                <li key={issue}>{issue}</li>
              ))}
            </ul>
          }
        />
      ) : null}

      {loading || !snapshot ? (
        <section className="ta-panel p-6 text-sm text-slate-600">Loading TeacherAssist setup...</section>
      ) : (
        <>
          <section id="teacher-profile" className="ta-panel p-6">
            <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">Teacher Profile</h2>
                <p className="mt-1 text-sm text-slate-600">
                  Save your default grade level, grading-period preference, timezone, and classroom size.
                </p>
              </div>
            </div>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("profile")}
              onDismiss={() => clearSectionAlert("profile")}
              className="mt-4"
            />
            <form
              className="mt-5 grid gap-4 lg:grid-cols-2"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "profile",
                  "profile",
                  async () => {
                    await saveTeacherProfile({
                      preferred_grade_level: profileForm.preferred_grade_level,
                      default_student_count: profileForm.default_student_count,
                      preferred_grading_period_type: profileForm.preferred_grading_period_type,
                      timezone: profileForm.timezone,
                    });
                  },
                  {
                    title: "Profile saved",
                    description: "Your teacher profile was saved successfully.",
                  },
                );
              }}
            >
              <label className="space-y-2">
                <span className="ta-label">Preferred grade level</span>
                <select
                  value={profileForm.preferred_grade_level ?? ""}
                  onChange={(event) =>
                    setProfileForm((current) => ({
                      ...current,
                      preferred_grade_level: event.target.value || null,
                    }))
                  }
                  className="ta-input"
                >
                  <option value="">Select grade level</option>
                  {snapshot.options.supported_grade_levels.map((gradeLevel) => (
                    <option key={gradeLevel} value={gradeLevel}>
                      {gradeLevel}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Default grading period type</span>
                <select
                  value={profileForm.preferred_grading_period_type ?? ""}
                  onChange={(event) =>
                    setProfileForm((current) => ({
                      ...current,
                      preferred_grading_period_type: event.target.value || null,
                    }))
                  }
                  className="ta-input"
                >
                  <option value="">Select grading period type</option>
                  {snapshot.options.grading_period_types.map((value) => (
                    <option key={value} value={value}>
                      {value.replaceAll("_", " ")}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Timezone</span>
                <input
                  value={profileForm.timezone ?? ""}
                  onChange={(event) =>
                    setProfileForm((current) => ({ ...current, timezone: event.target.value || null }))
                  }
                  className="ta-input"
                  placeholder="America/Chicago"
                />
              </label>
              <label className="space-y-2">
                <span className="ta-label">Default student count</span>
                <input
                  type="number"
                  min={1}
                  value={profileForm.default_student_count ?? ""}
                  onChange={(event) =>
                    setProfileForm((current) => ({
                      ...current,
                      default_student_count: event.target.value ? Number(event.target.value) : null,
                    }))
                  }
                  className="ta-input"
                  placeholder="23"
                />
              </label>
              <div className="lg:col-span-2">
                <button type="submit" className="ta-button-primary" disabled={savingKey === "profile"}>
                  {savingKey === "profile" ? "Saving..." : "Save teacher profile"}
                </button>
              </div>
            </form>
          </section>

          <section id="school-years" className="ta-panel p-6">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="xl:max-w-md">
                <h2 className="text-xl font-semibold text-slate-900">School Year</h2>
                <p className="mt-1 text-sm text-slate-600">
                  Create and edit school years, then mark the active one for TeacherAssist planning.
                </p>
                <TeacherAssistInlineAlert
                  alert={getSectionAlert("schoolYear")}
                  onDismiss={() => clearSectionAlert("schoolYear")}
                  className="mt-4"
                />
              </div>
              <form
                className="grid w-full gap-3 xl:max-w-3xl xl:grid-cols-4"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(
                    "schoolYear",
                    "school-year",
                    async () => {
                      const payload = {
                        title: schoolYearForm.title,
                        start_date: schoolYearForm.start_date,
                        end_date: schoolYearForm.end_date,
                        is_active: schoolYearForm.is_active,
                      };
                      if (editingSchoolYearId) {
                        await updateSchoolYear(editingSchoolYearId, payload);
                      } else {
                        await createSchoolYear(payload);
                      }
                    },
                    editingSchoolYearId
                      ? {
                          title: "School year updated",
                          description: `${schoolYearForm.title || "School year"} was updated successfully.`,
                        }
                      : {
                          title: "School year created",
                          description: `${schoolYearForm.title || "School year"} was added successfully.`,
                        },
                    {
                      onSuccess: () => {
                        setEditingSchoolYearId(null);
                        setSchoolYearForm(emptySchoolYearForm());
                      },
                    },
                  );
                }}
              >
                <input
                  value={schoolYearForm.title}
                  onChange={(event) => setSchoolYearForm((current) => ({ ...current, title: event.target.value }))}
                  className="ta-input xl:col-span-1"
                  placeholder="2026-2027"
                />
                <input
                  type="date"
                  value={schoolYearForm.start_date}
                  onChange={(event) =>
                    setSchoolYearForm((current) => ({ ...current, start_date: event.target.value }))
                  }
                  className="ta-input"
                />
                <input
                  type="date"
                  value={schoolYearForm.end_date}
                  onChange={(event) =>
                    setSchoolYearForm((current) => ({ ...current, end_date: event.target.value }))
                  }
                  className="ta-input"
                />
                <label className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-semibold text-slate-700">
                  <input
                    type="checkbox"
                    checked={schoolYearForm.is_active}
                    onChange={(event) =>
                      setSchoolYearForm((current) => ({ ...current, is_active: event.target.checked }))
                    }
                  />
                  Mark active
                </label>
                <div className="xl:col-span-4 flex flex-wrap gap-2">
                  <button type="submit" className="ta-button-primary" disabled={savingKey === "school-year"}>
                    {savingKey === "school-year"
                      ? "Saving..."
                      : editingSchoolYearId
                        ? "Update school year"
                        : "Create school year"}
                  </button>
                  {editingSchoolYearId ? (
                    <button
                      type="button"
                      className="ta-button-secondary"
                      onClick={() => {
                        setEditingSchoolYearId(null);
                        setSchoolYearForm(emptySchoolYearForm());
                      }}
                    >
                      Cancel edit
                    </button>
                  ) : null}
                </div>
              </form>
            </div>
            <div className="mt-5 grid gap-3 lg:grid-cols-2">
              {snapshot.schoolYears.length > 0 ? (
                snapshot.schoolYears.map((schoolYear) => (
                  <article key={schoolYear.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-base font-semibold text-slate-900">{schoolYear.title}</p>
                        <p className="mt-1 text-sm text-slate-600">
                          {formatDate(schoolYear.start_date)} - {formatDate(schoolYear.end_date)}
                        </p>
                      </div>
                      <div className="flex items-center gap-2">
                        {schoolYear.is_active ? (
                          <span className="rounded-full bg-emerald-100 px-3 py-1 text-xs font-semibold text-emerald-700">
                            Active
                          </span>
                        ) : null}
                        <button type="button" className="ta-button-secondary" onClick={() => beginSchoolYearEdit(schoolYear)}>
                          Edit
                        </button>
                      </div>
                    </div>
                  </article>
                ))
              ) : (
                <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-5 text-sm text-slate-600">
                  No school years yet. Add one above to start the setup flow.
                </div>
              )}
            </div>
          </section>

          <section id="grading-periods" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Grading Periods</h2>
            <p className="mt-1 text-sm text-slate-600">
              Organize nine weeks, six weeks, semester, trimester, or custom periods inside a school year.
            </p>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("gradingPeriods")}
              onDismiss={() => clearSectionAlert("gradingPeriods")}
              className="mt-4"
            />
            <form
              className="mt-5 grid gap-3 xl:grid-cols-6"
              onSubmit={(event) => {
                event.preventDefault();
                const fieldErrors = validateGradingPeriodForm(gradingPeriodForm);
                if (Object.keys(fieldErrors).length > 0) {
                  setGradingPeriodFieldErrors(fieldErrors);
                  setSectionAlert("gradingPeriods", {
                    type: "error",
                    title: "Unable to add grading period",
                    description: "Please correct the highlighted fields below.",
                  });
                  return;
                }
                setGradingPeriodFieldErrors({});
                void runSave(
                  "gradingPeriods",
                  "grading-period",
                  async () => {
                    const payload = {
                      school_year_id: gradingPeriodForm.school_year_id,
                      title: gradingPeriodForm.title.trim(),
                      grading_period_type: gradingPeriodForm.grading_period_type,
                      start_date: gradingPeriodForm.start_date,
                      end_date: gradingPeriodForm.end_date,
                      sort_order: Number(gradingPeriodForm.sort_order || "0"),
                    };
                    if (editingGradingPeriodId) {
                      await updateGradingPeriod(editingGradingPeriodId, payload);
                    } else {
                      await createGradingPeriod(payload);
                    }
                  },
                  editingGradingPeriodId
                    ? {
                        title: "Grading period updated",
                        description: `${gradingPeriodForm.title || "Grading period"} was updated successfully.`,
                      }
                    : {
                        title: "Grading period added",
                        description: `${gradingPeriodForm.title || "Grading period"} was added successfully.`,
                      },
                  {
                    onSuccess: () => {
                      setEditingGradingPeriodId(null);
                      setGradingPeriodFieldErrors({});
                      setGradingPeriodForm({
                        ...emptyGradingPeriodForm(),
                        school_year_id:
                          activeSchoolYear?.id ?? snapshot.schoolYears[0]?.id ?? "",
                        grading_period_type: snapshot.options.grading_period_types[0] ?? "",
                      });
                    },
                  },
                );
              }}
            >
              <label className="space-y-1">
                <span className="ta-label">School year</span>
                <select
                  value={gradingPeriodForm.school_year_id}
                  onChange={(event) => {
                    setGradingPeriodFieldErrors((current) => ({ ...current, school_year_id: undefined }));
                    setGradingPeriodForm((current) => ({ ...current, school_year_id: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(gradingPeriodFieldErrors.school_year_id))}
                  required
                >
                  <option value="">Select school year</option>
                  {snapshot.schoolYears.map((schoolYear) => (
                    <option key={schoolYear.id} value={schoolYear.id}>
                      {schoolYear.title}
                    </option>
                  ))}
                </select>
                <TeacherAssistFieldError message={gradingPeriodFieldErrors.school_year_id} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Title</span>
                <input
                  value={gradingPeriodForm.title}
                  onChange={(event) => {
                    setGradingPeriodFieldErrors((current) => ({ ...current, title: undefined }));
                    setGradingPeriodForm((current) => ({ ...current, title: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(gradingPeriodFieldErrors.title))}
                  placeholder="9 Weeks 1"
                  required
                />
                <TeacherAssistFieldError message={gradingPeriodFieldErrors.title} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Type</span>
                <select
                  value={gradingPeriodForm.grading_period_type}
                  onChange={(event) => {
                    setGradingPeriodFieldErrors((current) => ({ ...current, grading_period_type: undefined }));
                    setGradingPeriodForm((current) => ({ ...current, grading_period_type: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(gradingPeriodFieldErrors.grading_period_type))}
                  required
                >
                  <option value="">Select type</option>
                  {snapshot.options.grading_period_types.map((value) => (
                    <option key={value} value={value}>
                      {value.replaceAll("_", " ")}
                    </option>
                  ))}
                </select>
                <TeacherAssistFieldError message={gradingPeriodFieldErrors.grading_period_type} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Start date</span>
                <input
                  type="date"
                  value={gradingPeriodForm.start_date}
                  onChange={(event) => {
                    setGradingPeriodFieldErrors((current) => ({ ...current, start_date: undefined }));
                    setGradingPeriodForm((current) => ({ ...current, start_date: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(gradingPeriodFieldErrors.start_date))}
                  required
                />
                <TeacherAssistFieldError message={gradingPeriodFieldErrors.start_date} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">End date</span>
                <input
                  type="date"
                  value={gradingPeriodForm.end_date}
                  onChange={(event) => {
                    setGradingPeriodFieldErrors((current) => ({ ...current, end_date: undefined }));
                    setGradingPeriodForm((current) => ({ ...current, end_date: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(gradingPeriodFieldErrors.end_date))}
                  required
                />
                <TeacherAssistFieldError message={gradingPeriodFieldErrors.end_date} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Period order</span>
                <input
                  type="number"
                  min={0}
                  value={gradingPeriodForm.sort_order}
                  onChange={(event) =>
                    setGradingPeriodForm((current) => ({ ...current, sort_order: event.target.value }))
                  }
                  className="ta-input"
                  placeholder="1"
                />
                <p className="text-xs text-slate-500">Display order (1st, 2nd, 3rd…). Does not create multiple periods.</p>
              </label>
              <div className="xl:col-span-6 flex flex-wrap gap-2">
                <button type="submit" className="ta-button-primary" disabled={savingKey === "grading-period"}>
                  {savingKey === "grading-period"
                    ? "Saving..."
                    : editingGradingPeriodId
                      ? "Update grading period"
                      : "Add grading period"}
                </button>
                {editingGradingPeriodId ? (
                  <button
                    type="button"
                    className="ta-button-secondary"
                    onClick={() => {
                      setEditingGradingPeriodId(null);
                      setGradingPeriodForm({
                        ...emptyGradingPeriodForm(),
                        school_year_id: activeSchoolYear?.id ?? snapshot.schoolYears[0]?.id ?? "",
                        grading_period_type: snapshot.options.grading_period_types[0] ?? "",
                      });
                    }}
                  >
                    Cancel edit
                  </button>
                ) : null}
              </div>
            </form>
            <div className="mt-5 grid gap-3 lg:grid-cols-2">
              {snapshot.gradingPeriods.length > 0 ? (
                snapshot.gradingPeriods.map((gradingPeriod) => (
                  <article key={gradingPeriod.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-base font-semibold text-slate-900">{gradingPeriod.title}</p>
                        <p className="mt-1 text-sm text-slate-600">
                          {schoolYearTitleById.get(gradingPeriod.school_year_id) ?? "School year"} ·{" "}
                          {gradingPeriod.grading_period_type.replaceAll("_", " ")}
                        </p>
                        <p className="mt-1 text-sm text-slate-500">
                          {formatDate(gradingPeriod.start_date)} - {formatDate(gradingPeriod.end_date)}
                        </p>
                      </div>
                      <button type="button" className="ta-button-secondary" onClick={() => beginGradingPeriodEdit(gradingPeriod)}>
                        Edit
                      </button>
                    </div>
                  </article>
                ))
              ) : (
                <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-5 text-sm text-slate-600">
                  No grading periods yet. Add them once your school year dates are set.
                </div>
              )}
            </div>
          </section>

          <section id="classes" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Classes</h2>
            <p className="mt-1 text-sm text-slate-600">
              Create classes, assign grade levels, and preview the anonymous STUDENT # range derived from student count.
            </p>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("classes")}
              onDismiss={() => clearSectionAlert("classes")}
              className="mt-4"
            />
            <form
              className="mt-5 grid gap-3 xl:grid-cols-4"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "classes",
                  "class",
                  async () => {
                    const payload = {
                      school_year_id: classForm.school_year_id,
                      name: classForm.name,
                      grade_level: classForm.grade_level,
                      student_count: Number(classForm.student_count),
                    };
                    if (editingClassId) {
                      await updateClass(editingClassId, payload);
                    } else {
                      await createClass(payload);
                    }
                  },
                  editingClassId
                    ? {
                        title: "Class updated",
                        description: `${classForm.name || "Class"} was updated successfully.`,
                      }
                    : {
                        title: "Class added",
                        description: `${classForm.name || "Class"} was added successfully.`,
                      },
                  {
                    onSuccess: () => {
                      setEditingClassId(null);
                      setClassForm({
                        ...emptyClassForm(),
                        school_year_id:
                          activeSchoolYear?.id ?? snapshot.schoolYears[0]?.id ?? "",
                        grade_level:
                          snapshot.profile.preferred_grade_level ??
                          snapshot.options.supported_grade_levels[0] ??
                          "",
                        student_count: snapshot.profile.default_student_count?.toString() ?? "",
                      });
                    },
                  },
                );
              }}
            >
              <select
                value={classForm.school_year_id}
                onChange={(event) => setClassForm((current) => ({ ...current, school_year_id: event.target.value }))}
                className="ta-input"
              >
                <option value="">Select school year</option>
                {snapshot.schoolYears.map((schoolYear) => (
                  <option key={schoolYear.id} value={schoolYear.id}>
                    {schoolYear.title}
                  </option>
                ))}
              </select>
              <input
                value={classForm.name}
                onChange={(event) => setClassForm((current) => ({ ...current, name: event.target.value }))}
                className="ta-input"
                placeholder="5th Grade Homeroom"
              />
              <select
                value={classForm.grade_level}
                onChange={(event) => setClassForm((current) => ({ ...current, grade_level: event.target.value }))}
                className="ta-input"
              >
                <option value="">Select grade level</option>
                {snapshot.options.supported_grade_levels.map((gradeLevel) => (
                  <option key={gradeLevel} value={gradeLevel}>
                    {gradeLevel}
                  </option>
                ))}
              </select>
              <input
                type="number"
                min={1}
                value={classForm.student_count}
                onChange={(event) =>
                  setClassForm((current) => ({ ...current, student_count: event.target.value }))
                }
                className="ta-input"
                placeholder="23"
              />
              <div className="xl:col-span-4 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                Anonymous STUDENT # range preview:{" "}
                <span className="font-semibold">
                  1-{Math.max(1, Number(classForm.student_count || "1"))}
                </span>
              </div>
              <div className="xl:col-span-4 flex flex-wrap gap-2">
                <button type="submit" className="ta-button-primary" disabled={savingKey === "class"}>
                  {savingKey === "class" ? "Saving..." : editingClassId ? "Update class" : "Create class"}
                </button>
                {editingClassId ? (
                  <button
                    type="button"
                    className="ta-button-secondary"
                    onClick={() => {
                      setEditingClassId(null);
                      setClassForm({
                        ...emptyClassForm(),
                        school_year_id: activeSchoolYear?.id ?? snapshot.schoolYears[0]?.id ?? "",
                        grade_level: profileForm.preferred_grade_level ?? snapshot.options.supported_grade_levels[0] ?? "",
                        student_count: profileForm.default_student_count?.toString() ?? "",
                      });
                    }}
                  >
                    Cancel edit
                  </button>
                ) : null}
              </div>
            </form>
            <div className="mt-5 grid gap-3 lg:grid-cols-2">
              {snapshot.classes.length > 0 ? (
                snapshot.classes.map((teacherClass) => (
                  <article key={teacherClass.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <p className="text-base font-semibold text-slate-900">{teacherClass.name}</p>
                        <p className="mt-1 text-sm text-slate-600">
                          Grade {teacherClass.grade_level} · {schoolYearTitleById.get(teacherClass.school_year_id)}
                        </p>
                        <p className="mt-1 text-sm text-slate-500">
                          Anonymous STUDENT # range: {teacherClass.student_number_range_start}-
                          {teacherClass.student_number_range_end}
                        </p>
                        <p className="mt-2 text-sm text-slate-600">
                          Subjects:{" "}
                          {teacherClass.subject_ids.length > 0
                            ? teacherClass.subject_ids.map((subjectId) => subjectNameById.get(subjectId) ?? subjectId).join(", ")
                            : "None assigned yet"}
                        </p>
                      </div>
                      <button type="button" className="ta-button-secondary" onClick={() => beginClassEdit(teacherClass)}>
                        Edit
                      </button>
                    </div>
                  </article>
                ))
              ) : (
                <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-5 text-sm text-slate-600">
                  No classes yet. Add at least one class to define the STUDENT # range for future classroom workflows.
                </div>
              )}
            </div>
          </section>

          <section id="subjects" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Subjects</h2>
            <p className="mt-1 text-sm text-slate-600">
              Create the subjects you teach, then attach them to classes.
            </p>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("subjects")}
              onDismiss={() => clearSectionAlert("subjects")}
              className="mt-4"
            />
            <form
              className="mt-5 grid gap-3 xl:grid-cols-3"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "subjects",
                  "subject",
                  async () => {
                    await createSubject({ code: subjectForm.code || null, name: subjectForm.name });
                  },
                  {
                    title: "Subject added",
                    description: `${subjectForm.name || "Subject"} was added successfully.`,
                  },
                  { onSuccess: () => setSubjectForm(emptySubjectForm()) },
                );
              }}
            >
              <input
                value={subjectForm.code}
                onChange={(event) => setSubjectForm((current) => ({ ...current, code: event.target.value }))}
                className="ta-input"
                placeholder="MATH"
              />
              <input
                value={subjectForm.name}
                onChange={(event) => setSubjectForm((current) => ({ ...current, name: event.target.value }))}
                className="ta-input"
                placeholder="Math"
              />
              <button type="submit" className="ta-button-primary" disabled={savingKey === "subject"}>
                {savingKey === "subject" ? "Saving..." : "Add subject"}
              </button>
            </form>

            <div className="mt-5 grid gap-3 lg:grid-cols-2">
              <article className="rounded-2xl border border-slate-200 bg-white p-4">
                <h3 className="text-base font-semibold text-slate-900">Current subjects</h3>
                <div className="mt-3 space-y-2">
                  {snapshot.subjects.length > 0 ? (
                    snapshot.subjects.map((subject) => (
                      <div key={subject.id} className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                        <p className="font-semibold text-slate-900">{subject.name}</p>
                        <p className="text-sm text-slate-600">{subject.code ?? "No code"}</p>
                      </div>
                    ))
                  ) : (
                    <p className="text-sm text-slate-600">No subjects configured yet.</p>
                  )}
                </div>
              </article>
              <article className="rounded-2xl border border-slate-200 bg-white p-4">
                <h3 className="text-base font-semibold text-slate-900">Assign subjects to classes</h3>
                <TeacherAssistInlineAlert
                  alert={getSectionAlert("classSubjects")}
                  onDismiss={() => clearSectionAlert("classSubjects")}
                  className="mt-3"
                />
                <form
                  className="mt-3 space-y-3"
                  onSubmit={(event) => {
                    event.preventDefault();
                    void runSave(
                      "classSubjects",
                      "class-subject",
                      async () => {
                        await attachClassSubject({
                          class_id: classSubjectForm.class_id,
                          subject_id: classSubjectForm.subject_id,
                        });
                      },
                      {
                        title: "Subject attached",
                        description: "The subject was attached to the class successfully.",
                      },
                    );
                  }}
                >
                  <select
                    value={classSubjectForm.class_id}
                    onChange={(event) =>
                      setClassSubjectForm((current) => ({ ...current, class_id: event.target.value }))
                    }
                    className="ta-input"
                  >
                    <option value="">Select class</option>
                    {snapshot.classes.map((teacherClass) => (
                      <option key={teacherClass.id} value={teacherClass.id}>
                        {teacherClass.name}
                      </option>
                    ))}
                  </select>
                  <select
                    value={classSubjectForm.subject_id}
                    onChange={(event) =>
                      setClassSubjectForm((current) => ({ ...current, subject_id: event.target.value }))
                    }
                    className="ta-input"
                  >
                    <option value="">Select subject</option>
                    {snapshot.subjects.map((subject) => (
                      <option key={subject.id} value={subject.id}>
                        {subject.name}
                      </option>
                    ))}
                  </select>
                  <button type="submit" className="ta-button-primary" disabled={savingKey === "class-subject"}>
                    {savingKey === "class-subject" ? "Saving..." : "Attach subject"}
                  </button>
                </form>
              </article>
            </div>
          </section>

          <section id="standards" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Standards / TEKS</h2>
            <p className="mt-1 text-sm text-slate-600">
              Enter standards manually or import them from CSV. Subject must be selected explicitly for each new
              standard.
            </p>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("standards")}
              onDismiss={() => clearSectionAlert("standards")}
              className="mt-4"
            />
            {!standardForm.subject_id && !editingStandardId ? (
              <TeacherAssistInlineAlert
                alert={sectionWarning(
                  "Subject is required so standards are not accidentally saved under the wrong subject.",
                )}
                className="mt-4"
              />
            ) : null}
            {keepSubjectForNext && standardForm.subject_id ? (
              <div className="mt-4 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                Next standard will use{" "}
                <span className="font-semibold">
                  {subjectNameById.get(standardForm.subject_id) ?? "selected subject"}
                </span>
                .
              </div>
            ) : null}
            <form
              ref={standardFormRef}
              className="mt-5 grid gap-3 xl:grid-cols-3"
              onSubmit={(event) => {
                event.preventDefault();
                const fieldErrors = validateStandardForm(standardForm);
                if (Object.keys(fieldErrors).length > 0) {
                  setStandardFieldErrors(fieldErrors);
                  setSectionAlert("standards", {
                    type: "error",
                    title: "Unable to save standard",
                    description: "Please correct the highlighted fields below.",
                  });
                  return;
                }
                setStandardFieldErrors({});
                const standardCode = standardForm.code.trim();
                void runSave(
                  "standards",
                  "standard",
                  async () => {
                    const payload = {
                      subject_id: standardForm.subject_id,
                      standard_type: standardForm.standard_type,
                      code: standardForm.code.trim(),
                      description: standardForm.description.trim(),
                      grade_level: standardForm.grade_level || null,
                      school_year_id: standardForm.school_year_id || null,
                    };
                    if (editingStandardId) {
                      await updateStandard(editingStandardId, payload);
                    } else {
                      await createStandard(payload);
                    }
                  },
                  editingStandardId
                    ? {
                        title: "Standard updated",
                        description: standardCode
                          ? `${standardCode} was updated successfully.`
                          : "The standard was updated successfully.",
                      }
                    : {
                        title: "Standard added",
                        description: standardCode
                          ? `${standardCode} was added successfully.`
                          : "The standard was added successfully.",
                      },
                  { onSuccess: resetStandardFormAfterSave },
                );
              }}
            >
              <label className="space-y-1">
                <span className="ta-label">Type</span>
                <select
                  value={standardForm.standard_type}
                  onChange={(event) => {
                    setStandardFieldErrors((current) => ({ ...current, standard_type: undefined }));
                    setStandardForm((current) => ({ ...current, standard_type: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(standardFieldErrors.standard_type))}
                >
                  <option value="">Select standard type</option>
                  {snapshot.options.standard_types.map((value) => (
                    <option key={value} value={value}>
                      {value}
                    </option>
                  ))}
                </select>
                <TeacherAssistFieldError message={standardFieldErrors.standard_type} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Subject</span>
                <select
                  value={standardForm.subject_id}
                  onChange={(event) => {
                    setStandardFieldErrors((current) => ({ ...current, subject_id: undefined }));
                    setStandardForm((current) => ({ ...current, subject_id: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(standardFieldErrors.subject_id))}
                >
                  <option value="">Select subject</option>
                  {snapshot.subjects.map((subject) => (
                    <option key={subject.id} value={subject.id}>
                      {subject.name}
                    </option>
                  ))}
                </select>
                <TeacherAssistFieldError message={standardFieldErrors.subject_id} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">School year (optional)</span>
                <select
                  value={standardForm.school_year_id}
                  onChange={(event) =>
                    setStandardForm((current) => ({ ...current, school_year_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional school year</option>
                  {snapshot.schoolYears.map((schoolYear) => (
                    <option key={schoolYear.id} value={schoolYear.id}>
                      {schoolYear.title}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-1">
                <span className="ta-label">Code</span>
                <input
                  value={standardForm.code}
                  onChange={(event) => {
                    setStandardFieldErrors((current) => ({ ...current, code: undefined }));
                    setStandardForm((current) => ({ ...current, code: event.target.value }));
                  }}
                  className={fieldErrorInputClass(Boolean(standardFieldErrors.code))}
                  placeholder="5.3H"
                />
                <TeacherAssistFieldError message={standardFieldErrors.code} />
              </label>
              <label className="space-y-1">
                <span className="ta-label">Grade level (optional)</span>
                <select
                  value={standardForm.grade_level}
                  onChange={(event) =>
                    setStandardForm((current) => ({ ...current, grade_level: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional grade level</option>
                  {snapshot.options.supported_grade_levels.map((gradeLevel) => (
                    <option key={gradeLevel} value={gradeLevel}>
                      {gradeLevel}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-1 xl:col-span-3">
                <span className="ta-label">Description</span>
                <textarea
                  value={standardForm.description}
                  onChange={(event) => {
                    setStandardFieldErrors((current) => ({ ...current, description: undefined }));
                    setStandardForm((current) => ({ ...current, description: event.target.value }));
                  }}
                  className={`${fieldErrorInputClass(Boolean(standardFieldErrors.description))} min-h-28`}
                  placeholder="Describe the standard or TEKS..."
                />
                <TeacherAssistFieldError message={standardFieldErrors.description} />
              </label>
              {!editingStandardId ? (
                <label className="flex items-center gap-2 text-sm text-slate-700 xl:col-span-3">
                  <input
                    type="checkbox"
                    checked={keepSubjectForNext}
                    onChange={(event) => setKeepSubjectForNext(event.target.checked)}
                    className="h-4 w-4 rounded border-slate-300"
                  />
                  Keep selected subject for next standard
                </label>
              ) : null}
              <div className="flex flex-wrap items-center gap-3 xl:col-span-3">
                <button type="submit" className="ta-button-primary" disabled={savingKey === "standard"}>
                  {savingKey === "standard"
                    ? "Saving..."
                    : editingStandardId
                      ? "Save standard"
                      : "Add standard"}
                </button>
                {editingStandardId ? (
                  <button type="button" className="ta-button-secondary" onClick={cancelStandardEdit}>
                    Cancel edit
                  </button>
                ) : null}
              </div>
            </form>

            <article className="mt-6 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <h3 className="text-base font-semibold text-slate-900">Bulk CSV import</h3>
              <p className="mt-1 text-sm text-slate-600">
                Required columns: <code className="text-xs">code,type,subject,description</code>. Subject values must
                match existing subjects by name or code.
              </p>
              <pre className="mt-3 overflow-x-auto rounded-xl border border-slate-200 bg-white p-3 text-xs text-slate-700">
                {STANDARD_CSV_FORMAT}
              </pre>
              <div className="mt-4 flex flex-wrap items-center gap-3">
                <input
                  ref={importFileInputRef}
                  type="file"
                  accept=".csv,text/csv"
                  className="hidden"
                  onChange={(event) => {
                    const file = event.target.files?.[0];
                    event.target.value = "";
                    if (!file) return;
                    void (async () => {
                      clearSectionAlert("standards");
                      setImportBusy(true);
                      setImportFileName(file.name);
                      try {
                        const csvContent = await file.text();
                        const preview = await previewStandardsImport({ csv_content: csvContent });
                        setImportPreview(preview);
                        if (preview.invalid_count > 0) {
                          setSectionAlert(
                            "standards",
                            sectionError(
                              `${preview.invalid_count} row${preview.invalid_count === 1 ? "" : "s"} need attention before this file can be imported.`,
                              "Import has errors",
                            ),
                          );
                        }
                      } catch (nextError) {
                        setImportPreview(null);
                        setSectionAlert(
                          "standards",
                          sectionError(
                            nextError instanceof Error ? nextError.message : "Could not preview CSV import.",
                            "Import failed",
                          ),
                        );
                      } finally {
                        setImportBusy(false);
                      }
                    })();
                  }}
                />
                <button
                  type="button"
                  className="ta-button-secondary"
                  disabled={importBusy}
                  onClick={() => importFileInputRef.current?.click()}
                >
                  {importBusy ? "Validating..." : "Choose CSV file"}
                </button>
                {importFileName ? <span className="text-sm text-slate-600">{importFileName}</span> : null}
              </div>
              {importPreview ? (
                <div className="mt-4 space-y-3 rounded-2xl border border-slate-200 bg-white p-4">
                  <div className="grid gap-2 text-sm text-slate-700 sm:grid-cols-2 xl:grid-cols-4">
                    <p>Total rows: {importPreview.total_rows}</p>
                    <p>Valid rows: {importPreview.valid_count}</p>
                    <p>Invalid rows: {importPreview.invalid_count}</p>
                    <p>Duplicate codes: {importPreview.duplicate_count}</p>
                  </div>
                  {importPreview.errors.length > 0 ? (
                    <ul className="list-disc space-y-1 pl-5 text-sm text-rose-700">
                      {importPreview.errors.map((error) => (
                        <li key={`${error.row_number}-${error.field ?? "general"}-${error.message}`}>
                          Row {error.row_number}: {error.message}
                        </li>
                      ))}
                    </ul>
                  ) : null}
                  {importPreview.valid_count > 0 ? (
                    <button
                      type="button"
                      className="ta-button-primary"
                      disabled={importBusy}
                      onClick={() => {
                        if (!importPreview) return;
                        void withPreservedScroll("standards", async () => {
                          setImportBusy(true);
                          clearSectionAlert("standards");
                          try {
                            const validRows = importPreview.rows.filter(
                              (row) => row.status === "valid" && row.subject_id,
                            );
                            const result = await commitStandardsImport({
                              rows: validRows.map((row) => ({
                                code: row.code,
                                standard_type: row.standard_type,
                                subject_id: row.subject_id as string,
                                description: row.description,
                              })),
                            });
                            await refreshSnapshot();
                            setImportPreview(null);
                            setImportFileName(null);
                            setSectionAlert(
                              "standards",
                              sectionSuccess(
                                `${result.created_count} standard${result.created_count === 1 ? "" : "s"} imported.` +
                                  (result.skipped_duplicate_count > 0
                                    ? ` ${result.skipped_duplicate_count} duplicate${result.skipped_duplicate_count === 1 ? "" : "s"} skipped.`
                                    : ""),
                                "Import complete",
                              ),
                            );
                          } catch (nextError) {
                            setSectionAlert(
                              "standards",
                              sectionError(
                                nextError instanceof Error ? nextError.message : "Could not import standards.",
                                "Import failed",
                              ),
                            );
                          } finally {
                            setImportBusy(false);
                          }
                        });
                      }}
                    >
                      Import {importPreview.valid_count} valid row{importPreview.valid_count === 1 ? "" : "s"}
                    </button>
                  ) : null}
                </div>
              ) : null}
            </article>

            <div className="mt-5 overflow-hidden rounded-2xl border border-slate-200 bg-white">
              <div className="grid grid-cols-[120px_120px_140px_1fr_88px] gap-4 border-b border-slate-200 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                <span>Code</span>
                <span>Type</span>
                <span>Subject</span>
                <span>Description</span>
                <span>Actions</span>
              </div>
              {snapshot.standards.length > 0 ? (
                snapshot.standards.map((standard) => (
                  <div
                    key={standard.id}
                    className="grid grid-cols-[120px_120px_140px_1fr_88px] gap-4 border-b border-slate-100 px-4 py-4 text-sm text-slate-700 last:border-b-0"
                  >
                    <span className="font-semibold text-slate-900">{standard.code}</span>
                    <span>{standard.standard_type}</span>
                    <span>{standard.subject_id ? subjectNameById.get(standard.subject_id) ?? "Subject" : "—"}</span>
                    <span>{standard.description}</span>
                    <button
                      type="button"
                      className="ta-button-secondary justify-self-start px-3 py-1.5 text-xs"
                      onClick={() => beginStandardEdit(standard)}
                    >
                      Edit
                    </button>
                  </div>
                ))
              ) : (
                <div className="px-4 py-5 text-sm text-slate-600">No standards entered yet.</div>
              )}
            </div>
          </section>
        </>
      )}
    </div>
  );
}
