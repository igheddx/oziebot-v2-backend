"use client";

import { useCallback, useEffect, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistClassroomPanel } from "@/components/teacher-assist/teacher-assist-classroom-panel";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { TeacherAssistSchoolSetupPanel } from "@/components/teacher-assist/teacher-assist-school-setup-panel";
import { fetchMySchoolSetup } from "@/lib/education-catalog-api";
import {
  createSchoolYear,
  fetchMyClassroom,
  fetchSchoolYears,
  updateSchoolYear,
} from "@/lib/teacher-assist-api";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";
import type { SchoolYear } from "@/lib/teacher-assist-types";

type SchoolYearForm = {
  title: string;
  start_date: string;
  end_date: string;
  is_active: boolean;
};

function formatDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function emptySchoolYearForm(): SchoolYearForm {
  return { title: "", start_date: "", end_date: "", is_active: false };
}

export function TeacherAssistSettingsScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [savingKey, setSavingKey] = useState<string | null>(null);
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [hasSchoolSetup, setHasSchoolSetup] = useState(false);
  const [hasClassroom, setHasClassroom] = useState(false);
  const [classroomSyncKey, setClassroomSyncKey] = useState(0);
  const [schoolYearForm, setSchoolYearForm] = useState<SchoolYearForm>(emptySchoolYearForm());
  const [editingSchoolYearId, setEditingSchoolYearId] = useState<string | null>(null);

  const refreshSnapshot = useCallback(async () => {
    const [setup, years, classroom] = await Promise.all([
      fetchMySchoolSetup(),
      fetchSchoolYears(),
      fetchMyClassroom().catch(() => null),
    ]);
    setSchoolYears(years);
    setHasSchoolSetup(Boolean(setup.assignment && setup.catalog_grade_code && setup.synced_subjects.length > 0));
    setHasClassroom(Boolean(classroom?.class_id && classroom.student_count && classroom.student_count > 0));
    setClassroomSyncKey((current) => current + 1);
    return { setup, years, classroom };
  }, []);

  const loadSnapshot = useCallback(async () => {
    setLoading(true);
    setPageError(null);
    try {
      await refreshSnapshot();
      setSchoolYearForm(emptySchoolYearForm());
      setEditingSchoolYearId(null);
    } catch (nextError) {
      setPageError(nextError instanceof Error ? nextError.message : "Could not load TeacherAssist setup.");
    } finally {
      setLoading(false);
    }
  }, [refreshSnapshot]);

  useEffect(() => {
    void loadSnapshot();
  }, [loadSnapshot]);

  const activeSchoolYear = schoolYears.find((row) => row.is_active) ?? null;
  const setupIssues = [
    !hasSchoolSetup ? "Complete school & district setup (state, district, school, grade)." : null,
    !activeSchoolYear ? "Create an active school year." : null,
    !hasClassroom ? "Set your homeroom name and student count." : null,
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

  const runSchoolYearSave = async () => {
    setSavingKey("school-year");
    clearSectionAlert("schoolYear");
    try {
      await withPreservedScroll("school-years", async () => {
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
        await refreshSnapshot();
      });
      setSectionAlert(
        "schoolYear",
        sectionSuccess(
          editingSchoolYearId ? "School year updated." : "School year created.",
          editingSchoolYearId ? "School year updated" : "School year created",
        ),
      );
      setEditingSchoolYearId(null);
      setSchoolYearForm(emptySchoolYearForm());
    } catch (nextError) {
      setSectionAlert(
        "schoolYear",
        sectionError(nextError instanceof Error ? nextError.message : "Request failed.", "Unable to save"),
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
            Three quick steps: place yourself in your district catalog, set the school year, then configure your homeroom.
            Grading periods, subjects, and standards come from your district pacing guides.
          </p>
        </div>
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load settings" message={pageError} />

      {setupIssues.length > 0 && !loading ? (
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

      {loading ? (
        <section className="ta-panel p-6 text-sm text-slate-600">Loading TeacherAssist setup...</section>
      ) : (
        <>
          <section id="school-setup" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">1. School & District Setup</h2>
            <p className="mt-1 text-sm text-slate-600">
              Select your state, district, school, and grade. Subjects and standards sync automatically from the catalog.
            </p>
            <div className="mt-5">
              <TeacherAssistSchoolSetupPanel onSaved={() => void refreshSnapshot()} />
            </div>
          </section>

          <section id="school-year" className="ta-panel p-6">
            <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
              <div className="xl:max-w-md">
                <h2 className="text-xl font-semibold text-slate-900">2. School Year</h2>
                <p className="mt-1 text-sm text-slate-600">
                  Create the instructional year for your homeroom. Pacing guides define grading periods — you do not need to
                  enter them manually.
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
                  void runSchoolYearSave();
                }}
              >
                <input
                  value={schoolYearForm.title}
                  onChange={(event) => setSchoolYearForm((current) => ({ ...current, title: event.target.value }))}
                  className="ta-input xl:col-span-1"
                  placeholder="2026-2027"
                  required
                />
                <input
                  type="date"
                  value={schoolYearForm.start_date}
                  onChange={(event) => setSchoolYearForm((current) => ({ ...current, start_date: event.target.value }))}
                  className="ta-input"
                  required
                />
                <input
                  type="date"
                  value={schoolYearForm.end_date}
                  onChange={(event) => setSchoolYearForm((current) => ({ ...current, end_date: event.target.value }))}
                  className="ta-input"
                  required
                />
                <label className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-semibold text-slate-700">
                  <input
                    type="checkbox"
                    checked={schoolYearForm.is_active}
                    onChange={(event) => setSchoolYearForm((current) => ({ ...current, is_active: event.target.checked }))}
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
              {schoolYears.length > 0 ? (
                schoolYears.map((schoolYear) => (
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
                  No school years yet. Add one above before configuring your homeroom.
                </div>
              )}
            </div>
          </section>

          <section id="my-classroom" className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">3. My Classroom</h2>
            <p className="mt-1 text-sm text-slate-600">
              Name your homeroom for the active school year and set student count. Grade and teaching subjects are inherited
              from district setup.
            </p>
            <div className="mt-5">
              <TeacherAssistClassroomPanel syncKey={classroomSyncKey} onSaved={() => void refreshSnapshot()} />
            </div>
          </section>
        </>
      )}
    </div>
  );
}
