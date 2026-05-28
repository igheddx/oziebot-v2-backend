"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  attachClassSubject,
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
  saveTeacherProfile,
  updateClass,
  updateGradingPeriod,
  updateSchoolYear,
} from "@/lib/teacher-assist-api";
import type {
  GradingPeriod,
  SchoolYear,
  Standard,
  Subject,
  TeacherAssistOptions,
  TeacherClass,
  TeacherProfile,
} from "@/lib/teacher-assist-types";

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

export function TeacherAssistSettingsScreen() {
  const [snapshot, setSnapshot] = useState<SetupSnapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
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
  const [editingGradingPeriodId, setEditingGradingPeriodId] = useState<string | null>(null);
  const [classForm, setClassForm] = useState<ClassForm>(emptyClassForm());
  const [editingClassId, setEditingClassId] = useState<string | null>(null);
  const [subjectForm, setSubjectForm] = useState<SubjectForm>(emptySubjectForm());
  const [classSubjectForm, setClassSubjectForm] = useState({ class_id: "", subject_id: "" });
  const [standardForm, setStandardForm] = useState<StandardForm>(emptyStandardForm());

  const loadSnapshot = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [options, profile, schoolYears, gradingPeriods, classes, subjects, standards] =
        await Promise.all([
          fetchTeacherAssistOptions(),
          fetchTeacherProfile(),
          fetchSchoolYears(),
          fetchGradingPeriods(),
          fetchClasses(),
          fetchSubjects(),
          fetchStandards(),
        ]);
      setSnapshot({ options, profile, schoolYears, gradingPeriods, classes, subjects, standards });
      setProfileForm(profile);
      setSchoolYearForm(emptySchoolYearForm());
      setGradingPeriodForm({
        ...emptyGradingPeriodForm(),
        school_year_id: schoolYears.find((row) => row.is_active)?.id ?? schoolYears[0]?.id ?? "",
        grading_period_type: options.grading_period_types[0] ?? "",
      });
      setClassForm({
        ...emptyClassForm(),
        school_year_id: schoolYears.find((row) => row.is_active)?.id ?? schoolYears[0]?.id ?? "",
        grade_level: profile.preferred_grade_level ?? options.supported_grade_levels[0] ?? "",
        student_count: profile.default_student_count?.toString() ?? "",
      });
      setSubjectForm(emptySubjectForm());
      setClassSubjectForm({
        class_id: classes[0]?.id ?? "",
        subject_id: subjects[0]?.id ?? "",
      });
      setStandardForm({
        ...emptyStandardForm(),
        standard_type: options.standard_types[0] ?? "",
        subject_id: subjects[0]?.id ?? "",
        school_year_id: schoolYears.find((row) => row.is_active)?.id ?? schoolYears[0]?.id ?? "",
        grade_level: profile.preferred_grade_level ?? "",
      });
      setEditingSchoolYearId(null);
      setEditingGradingPeriodId(null);
      setEditingClassId(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load TeacherAssist setup.");
    } finally {
      setLoading(false);
    }
  }, []);

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

  const runSave = async (key: string, action: () => Promise<void>, successMessage: string) => {
    setSavingKey(key);
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(successMessage);
      await loadSnapshot();
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Request failed.");
    } finally {
      setSavingKey(null);
    }
  };

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Settings
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Teacher foundation + school-year setup
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            This phase establishes the academic setup model TeacherAssist will depend on later:
            teacher profile, school years, grading periods, classes, anonymous STUDENT # ranges,
            subjects, and standards.
          </p>
        </div>
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {notice ? <section className="ta-alert ta-alert-success">{notice}</section> : null}
      {setupIssues.length > 0 ? (
        <section className="ta-alert ta-alert-info">
          <p className="font-semibold">Complete your school-year setup before creating lesson plans.</p>
          <ul className="mt-2 list-disc space-y-1 pl-5">
            {setupIssues.map((issue) => (
              <li key={issue}>{issue}</li>
            ))}
          </ul>
        </section>
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
            <form
              className="mt-5 grid gap-4 lg:grid-cols-2"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "profile",
                  async () => {
                    await saveTeacherProfile({
                      preferred_grade_level: profileForm.preferred_grade_level,
                      default_student_count: profileForm.default_student_count,
                      preferred_grading_period_type: profileForm.preferred_grading_period_type,
                      timezone: profileForm.timezone,
                    });
                  },
                  "Teacher profile saved.",
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
              </div>
              <form
                className="grid w-full gap-3 xl:max-w-3xl xl:grid-cols-4"
                onSubmit={(event) => {
                  event.preventDefault();
                  void runSave(
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
                    editingSchoolYearId ? "School year updated." : "School year created.",
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
            <form
              className="mt-5 grid gap-3 xl:grid-cols-6"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "grading-period",
                  async () => {
                    const payload = {
                      school_year_id: gradingPeriodForm.school_year_id,
                      title: gradingPeriodForm.title,
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
                  editingGradingPeriodId ? "Grading period updated." : "Grading period created.",
                );
              }}
            >
              <select
                value={gradingPeriodForm.school_year_id}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, school_year_id: event.target.value }))
                }
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
                value={gradingPeriodForm.title}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, title: event.target.value }))
                }
                className="ta-input"
                placeholder="9 Weeks 1"
              />
              <select
                value={gradingPeriodForm.grading_period_type}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, grading_period_type: event.target.value }))
                }
                className="ta-input"
              >
                <option value="">Select type</option>
                {snapshot.options.grading_period_types.map((value) => (
                  <option key={value} value={value}>
                    {value.replaceAll("_", " ")}
                  </option>
                ))}
              </select>
              <input
                type="date"
                value={gradingPeriodForm.start_date}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, start_date: event.target.value }))
                }
                className="ta-input"
              />
              <input
                type="date"
                value={gradingPeriodForm.end_date}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, end_date: event.target.value }))
                }
                className="ta-input"
              />
              <input
                type="number"
                min={0}
                value={gradingPeriodForm.sort_order}
                onChange={(event) =>
                  setGradingPeriodForm((current) => ({ ...current, sort_order: event.target.value }))
                }
                className="ta-input"
                placeholder="Sort order"
              />
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
            <form
              className="mt-5 grid gap-3 xl:grid-cols-4"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
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
                  editingClassId ? "Class updated." : "Class created.",
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
            <form
              className="mt-5 grid gap-3 xl:grid-cols-3"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "subject",
                  async () => {
                    await createSubject({ code: subjectForm.code || null, name: subjectForm.name });
                  },
                  "Subject created.",
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
                <form
                  className="mt-3 space-y-3"
                  onSubmit={(event) => {
                    event.preventDefault();
                    void runSave(
                      "class-subject",
                      async () => {
                        await attachClassSubject({
                          class_id: classSubjectForm.class_id,
                          subject_id: classSubjectForm.subject_id,
                        });
                      },
                      "Subject attached to class.",
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
              Enter standards manually for now. Pacing-guide import is intentionally deferred to a later phase.
            </p>
            <form
              className="mt-5 grid gap-3 xl:grid-cols-3"
              onSubmit={(event) => {
                event.preventDefault();
                void runSave(
                  "standard",
                  async () => {
                    await createStandard({
                      subject_id: standardForm.subject_id || null,
                      standard_type: standardForm.standard_type,
                      code: standardForm.code,
                      description: standardForm.description,
                      grade_level: standardForm.grade_level || null,
                      school_year_id: standardForm.school_year_id || null,
                    });
                  },
                  "Standard saved.",
                );
              }}
            >
              <select
                value={standardForm.standard_type}
                onChange={(event) =>
                  setStandardForm((current) => ({ ...current, standard_type: event.target.value }))
                }
                className="ta-input"
              >
                <option value="">Select standard type</option>
                {snapshot.options.standard_types.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
              <select
                value={standardForm.subject_id}
                onChange={(event) => setStandardForm((current) => ({ ...current, subject_id: event.target.value }))}
                className="ta-input"
              >
                <option value="">Optional subject</option>
                {snapshot.subjects.map((subject) => (
                  <option key={subject.id} value={subject.id}>
                    {subject.name}
                  </option>
                ))}
              </select>
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
              <input
                value={standardForm.code}
                onChange={(event) => setStandardForm((current) => ({ ...current, code: event.target.value }))}
                className="ta-input"
                placeholder="5.3H"
              />
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
              <textarea
                value={standardForm.description}
                onChange={(event) =>
                  setStandardForm((current) => ({ ...current, description: event.target.value }))
                }
                className="ta-input min-h-28 xl:col-span-3"
                placeholder="Describe the standard or TEKS..."
              />
              <div className="xl:col-span-3">
                <button type="submit" className="ta-button-primary" disabled={savingKey === "standard"}>
                  {savingKey === "standard" ? "Saving..." : "Add standard"}
                </button>
              </div>
            </form>

            <div className="mt-5 overflow-hidden rounded-2xl border border-slate-200 bg-white">
              <div className="grid grid-cols-[120px_140px_140px_1fr] gap-4 border-b border-slate-200 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                <span>Code</span>
                <span>Type</span>
                <span>Subject</span>
                <span>Description</span>
              </div>
              {snapshot.standards.length > 0 ? (
                snapshot.standards.map((standard) => (
                  <div
                    key={standard.id}
                    className="grid grid-cols-[120px_140px_140px_1fr] gap-4 border-b border-slate-100 px-4 py-4 text-sm text-slate-700 last:border-b-0"
                  >
                    <span className="font-semibold text-slate-900">{standard.code}</span>
                    <span>{standard.standard_type}</span>
                    <span>{standard.subject_id ? subjectNameById.get(standard.subject_id) ?? "Subject" : "—"}</span>
                    <span>{standard.description}</span>
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
