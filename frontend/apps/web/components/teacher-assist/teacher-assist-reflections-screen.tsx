"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  createLessonReflection,
  createLessonReflectionVersion,
  fetchClasses,
  fetchLessonEffectiveness,
  fetchLessonEffectivenessHistoricalComparison,
  fetchLessonReflection,
  fetchLessonReflectionVersions,
  fetchLessonReflections,
  fetchSchoolYears,
  fetchSubjects,
  generateLessonReflectionAISuggestions,
  updateLessonReflection,
} from "@/lib/teacher-assist-api";
import type {
  LessonEffectiveness,
  LessonEffectivenessHistoricalComparison,
  LessonReflection,
  LessonReflectionContent,
  LessonReflectionVersion,
  SchoolYear,
  Subject,
  TeacherClass,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function linesToList(value: string) {
  return value
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

function listToLines(items: string[] | undefined) {
  return (items ?? []).join("\n");
}

function classificationTone(classification: string) {
  switch (classification) {
    case "highly_effective":
      return "bg-emerald-50 text-emerald-800";
    case "effective":
      return "bg-sky-50 text-sky-800";
    case "needs_adjustment":
      return "bg-amber-50 text-amber-800";
    case "ineffective":
      return "bg-rose-50 text-rose-800";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

export function TeacherAssistReflectionsScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const requestedId = searchParams.get("id");
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();

  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [classes, setClasses] = useState<TeacherClass[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [reflections, setReflections] = useState<LessonReflection[]>([]);
  const [effectivenessRows, setEffectivenessRows] = useState<LessonEffectiveness[]>([]);
  const [historical, setHistorical] = useState<LessonEffectivenessHistoricalComparison | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(requestedId);
  const [reflection, setReflection] = useState<LessonReflection | null>(null);
  const [versions, setVersions] = useState<LessonReflectionVersion[]>([]);
  const [whatWorked, setWhatWorked] = useState("");
  const [whatFailed, setWhatFailed] = useState("");
  const [notesForNextYear, setNotesForNextYear] = useState("");
  const [strengths, setStrengths] = useState("");
  const [weaknesses, setWeaknesses] = useState("");
  const [improvements, setImprovements] = useState("");
  const [teacherInstructions, setTeacherInstructions] = useState("");
  const [changeReason, setChangeReason] = useState("");
  const [createForm, setCreateForm] = useState({
    school_year_id: "",
    class_id: "",
    subject_id: "",
    title: "",
  });
  const [pageError, setPageError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const selectedVersion = useMemo(() => versions.at(-1) ?? null, [versions]);

  const loadLists = useCallback(async () => {
    const [nextSchoolYears, nextClasses, nextSubjects, nextReflections, nextEffectiveness] = await Promise.all([
      fetchSchoolYears(),
      fetchClasses(),
      fetchSubjects(),
      fetchLessonReflections(),
      fetchLessonEffectiveness(),
    ]);
    setSchoolYears(nextSchoolYears);
    setClasses(nextClasses);
    setSubjects(nextSubjects);
    setReflections(nextReflections);
    setEffectivenessRows(nextEffectiveness);
    if (!selectedId && nextReflections.length > 0) {
      setSelectedId(nextReflections[0].id);
    }
  }, [selectedId]);

  const loadDetail = useCallback(async (reflectionId: string) => {
    const [row, versionRows] = await Promise.all([
      fetchLessonReflection(reflectionId),
      fetchLessonReflectionVersions(reflectionId),
    ]);
    setReflection(row);
    setVersions(versionRows);
    const current =
      versionRows.find((item) => item.id === row.current_version_id) ?? versionRows.at(-1) ?? null;
    const content = current?.content_json;
    setWhatWorked(listToLines(content?.what_worked));
    setWhatFailed(listToLines(content?.what_failed));
    setNotesForNextYear(listToLines(content?.notes_for_next_year));
    setStrengths(listToLines(content?.strengths));
    setWeaknesses(listToLines(content?.weaknesses));
    setImprovements(listToLines(content?.improvements));

    if (row.school_year_id && row.class_id && row.subject_id) {
      const comparison = await fetchLessonEffectivenessHistoricalComparison({
        school_year_id: row.school_year_id,
        class_id: row.class_id,
        subject_id: row.subject_id,
        grading_period_id: row.grading_period_id ?? undefined,
      });
      setHistorical(comparison);
    } else {
      setHistorical(null);
    }
  }, []);

  useEffect(() => {
    loadLists().catch((error: Error) => setPageError(error.message));
  }, [loadLists]);

  useEffect(() => {
    if (requestedId) setSelectedId(requestedId);
  }, [requestedId]);

  useEffect(() => {
    if (!selectedId) {
      setReflection(null);
      setVersions([]);
      setHistorical(null);
      return;
    }
    loadDetail(selectedId).catch((error: Error) => setPageError(error.message));
  }, [loadDetail, selectedId]);

  function buildContent(): LessonReflectionContent {
    return {
      what_worked: linesToList(whatWorked),
      what_failed: linesToList(whatFailed),
      notes_for_next_year: linesToList(notesForNextYear),
      strengths: linesToList(strengths),
      weaknesses: linesToList(weaknesses),
      improvements: linesToList(improvements),
      teacher_review_required: true,
      is_ai_draft: false,
    };
  }

  async function handleCreate() {
    if (!createForm.school_year_id || !createForm.class_id || !createForm.subject_id) {
      setSectionAlert("createDraft", {
        type: "error",
        title: "Unable to create reflection",
        description: "School year, class, and subject are required.",
      });
      return;
    }
    setBusy(true);
    clearSectionAlert("createDraft");
    try {
      const created = await createLessonReflection({
        school_year_id: createForm.school_year_id,
        class_id: createForm.class_id,
        subject_id: createForm.subject_id,
        title: createForm.title.trim() || undefined,
      });
      await loadLists();
      setSelectedId(created.id);
      router.replace(`/teacher-assist/reflections?id=${created.id}`);
      setSectionAlert(
        "createDraft",
        sectionSuccess("Reflection workspace created.", "Reflection created"),
      );
    } catch (error) {
      setSectionAlert(
        "createDraft",
        sectionError(error instanceof Error ? error.message : "Could not create reflection.", "Unable to create reflection"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveVersion() {
    if (!reflection) return;
    setBusy(true);
    clearSectionAlert("reflectionEditor");
    try {
      await createLessonReflectionVersion(reflection.id, {
        content_json: buildContent(),
        change_reason: changeReason.trim() || "Teacher saved reflection notes.",
      });
      await loadDetail(reflection.id);
      await loadLists();
      setSectionAlert("reflectionEditor", sectionSuccess("Reflection saved.", "Reflection saved"));
    } catch (error) {
      setSectionAlert(
        "reflectionEditor",
        sectionError(error instanceof Error ? error.message : "Could not save reflection.", "Unable to save reflection"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleAISuggestions() {
    if (!reflection) return;
    setBusy(true);
    clearSectionAlert("reflectionEditor");
    try {
      const payload = await generateLessonReflectionAISuggestions(reflection.id, {
        provider_mode: "mock",
        teacher_instructions: teacherInstructions.trim() || undefined,
      });
      setSectionAlert(
        "reflectionEditor",
        sectionSuccess("AI suggestions generated. Review and edit before saving.", "AI suggestions ready"),
      );
      setReflection(payload.lesson_reflection);
      const content = payload.version.content_json;
      setWhatWorked(listToLines(content.what_worked));
      setWhatFailed(listToLines(content.what_failed));
      setNotesForNextYear(listToLines(content.notes_for_next_year));
      setStrengths(listToLines(content.strengths));
      setWeaknesses(listToLines(content.weaknesses));
      setImprovements(listToLines(content.improvements));
      await loadDetail(reflection.id);
      await loadLists();
    } catch (error) {
      setSectionAlert(
        "reflectionEditor",
        sectionError(error instanceof Error ? error.message : "Could not generate AI suggestions.", "AI suggestions failed"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleArchive() {
    if (!reflection) return;
    setBusy(true);
    clearSectionAlert("reflectionEditor");
    try {
      await updateLessonReflection(reflection.id, { status: "archived" });
      await loadLists();
      setSectionAlert("reflectionEditor", sectionSuccess("Reflection archived.", "Reflection archived"));
    } catch (error) {
      setSectionAlert(
        "reflectionEditor",
        sectionError(error instanceof Error ? error.message : "Could not archive reflection.", "Unable to archive"),
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-6">
      <header className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Phase 31</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">Lesson Reflections</h1>
        <p className="mt-2 max-w-3xl text-sm text-slate-600">
          Review read-only lesson effectiveness, record what worked and what failed, and optionally accept
          mock AI suggestions. Nothing updates grading, mastery, or parent communication automatically.
        </p>
      </header>

      <TeacherAssistFormErrorSummary title="Unable to load reflections" message={pageError} />

      <div className="grid gap-6 xl:grid-cols-[280px_minmax(0,1fr)]">
        <aside className="space-y-4">
          <section className="rounded-3xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-900">Create reflection</h2>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("createDraft")}
              onDismiss={() => clearSectionAlert("createDraft")}
              className="mt-3"
            />
            <div className="mt-3 space-y-3">
              <select
                className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm"
                value={createForm.school_year_id}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, school_year_id: event.target.value }))}
              >
                <option value="">School year</option>
                {schoolYears.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.title}
                  </option>
                ))}
              </select>
              <select
                className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm"
                value={createForm.class_id}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, class_id: event.target.value }))}
              >
                <option value="">Class</option>
                {classes.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <select
                className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm"
                value={createForm.subject_id}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, subject_id: event.target.value }))}
              >
                <option value="">Subject</option>
                {subjects.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <input
                className="w-full rounded-xl border border-slate-200 px-3 py-2 text-sm"
                placeholder="Title (optional)"
                value={createForm.title}
                onChange={(event) => setCreateForm((prev) => ({ ...prev, title: event.target.value }))}
              />
              <button
                type="button"
                disabled={busy}
                onClick={handleCreate}
                className="w-full rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white disabled:opacity-60"
              >
                New reflection
              </button>
            </div>
          </section>

          <section className="rounded-3xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-900">Saved reflections</h2>
            <div className="mt-3 space-y-2">
              {reflections.length === 0 ? (
                <p className="text-sm text-slate-500">No reflections yet.</p>
              ) : (
                reflections.map((row) => (
                  <button
                    key={row.id}
                    type="button"
                    onClick={() => {
                      setSelectedId(row.id);
                      router.replace(`/teacher-assist/reflections?id=${row.id}`);
                    }}
                    className={`w-full rounded-2xl border px-3 py-3 text-left text-sm ${
                      selectedId === row.id
                        ? "border-sky-300 bg-sky-50"
                        : "border-slate-200 bg-white hover:bg-slate-50"
                    }`}
                  >
                    <p className="font-semibold text-slate-900">{row.title}</p>
                    <p className="mt-1 text-xs text-slate-500">{labelize(row.status)}</p>
                  </button>
                ))
              )}
            </div>
          </section>

          <section className="rounded-3xl border border-slate-200 bg-white p-4">
            <h2 className="text-sm font-semibold text-slate-900">Lesson effectiveness</h2>
            <p className="mt-1 text-xs text-slate-500">Read-only scores from assignments, mastery, and reteach activity.</p>
            <div className="mt-3 space-y-2">
              {effectivenessRows.length === 0 ? (
                <p className="text-sm text-slate-500">Complete weekly plans with assignments to see scores.</p>
              ) : (
                effectivenessRows.slice(0, 8).map((row) => (
                  <div key={row.weekly_plan_id} className="rounded-2xl border border-slate-100 px-3 py-2">
                    <p className="text-sm font-semibold text-slate-900">{row.weekly_plan_title}</p>
                    <span
                      className={`mt-2 inline-flex rounded-full px-2 py-0.5 text-xs font-semibold ${classificationTone(row.classification)}`}
                    >
                      {labelize(row.classification)}
                    </span>
                  </div>
                ))
              )}
            </div>
          </section>
        </aside>

        <main className="space-y-4">
          {reflection ? (
            <>
              <section className="rounded-3xl border border-slate-200 bg-white p-5">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-900">{reflection.title}</h2>
                    <p className="mt-1 text-sm text-slate-600">
                      {reflection.class_name ?? "Class"} · {reflection.subject_name ?? "Subject"} ·{" "}
                      {labelize(reflection.status)}
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      disabled={busy}
                      onClick={handleAISuggestions}
                      className="rounded-xl border border-slate-200 px-4 py-2 text-sm font-semibold text-slate-800"
                    >
                      Generate AI suggestions
                    </button>
                    <button
                      type="button"
                      disabled={busy}
                      onClick={handleSaveVersion}
                      className="rounded-xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white"
                    >
                      Save reflection
                    </button>
                    <button
                      type="button"
                      disabled={busy}
                      onClick={handleArchive}
                      className="rounded-xl border border-slate-200 px-4 py-2 text-sm text-slate-600"
                    >
                      Archive
                    </button>
                  </div>
                </div>
                <TeacherAssistInlineAlert
                  alert={getSectionAlert("reflectionEditor")}
                  onDismiss={() => clearSectionAlert("reflectionEditor")}
                  className="mt-4"
                />
                <p className="mt-3 text-xs text-amber-700">
                  Teacher review required. AI suggestions are drafts only and never commit automatically.
                </p>
                {selectedVersion ? (
                  <p className="mt-2 text-xs text-slate-500">
                    Version {selectedVersion.version_number} · {labelize(selectedVersion.version_source)}
                  </p>
                ) : null}
              </section>

              {historical ? (
                <section className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                  <h3 className="text-sm font-semibold text-slate-900">Historical comparison</h3>
                  <div className="mt-3 grid gap-3 md:grid-cols-3">
                    {[
                      { label: "Current period", scope: historical.current_grading_period },
                      { label: "Prior period", scope: historical.prior_grading_period },
                      { label: "Prior school year", scope: historical.prior_school_year },
                    ].map(({ label, scope }) => (
                      <div key={label} className="rounded-2xl border border-slate-200 bg-white p-4">
                        <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</p>
                        {scope ? (
                          <>
                            <p className="mt-2 text-sm font-semibold text-slate-900">
                              {(scope.summary as { lesson_count?: number })?.lesson_count ?? 0} lessons
                            </p>
                            <p className="mt-1 text-xs text-slate-600">
                              Avg mastery{" "}
                              {Math.round(
                                (((scope.summary as { average_mastery_percentage?: number })
                                  ?.average_mastery_percentage ?? 0) as number) * 100,
                              )}
                              %
                            </p>
                          </>
                        ) : (
                          <p className="mt-2 text-sm text-slate-500">No prior data.</p>
                        )}
                      </div>
                    ))}
                  </div>
                </section>
              ) : null}

              <section className="grid gap-4 lg:grid-cols-2">
                {[
                  { label: "What worked", value: whatWorked, setter: setWhatWorked },
                  { label: "What failed", value: whatFailed, setter: setWhatFailed },
                  { label: "Notes for next year", value: notesForNextYear, setter: setNotesForNextYear },
                  { label: "Strengths (AI / teacher)", value: strengths, setter: setStrengths },
                  { label: "Weaknesses (AI / teacher)", value: weaknesses, setter: setWeaknesses },
                  { label: "Improvements (AI / teacher)", value: improvements, setter: setImprovements },
                ].map(({ label, value, setter }) => (
                  <label key={label} className="block rounded-3xl border border-slate-200 bg-white p-4">
                    <span className="text-sm font-semibold text-slate-900">{label}</span>
                    <textarea
                      className="mt-3 min-h-32 w-full rounded-2xl border border-slate-200 px-3 py-2 text-sm text-slate-800"
                      value={value}
                      onChange={(event) => setter(event.target.value)}
                      placeholder="One note per line"
                    />
                  </label>
                ))}
              </section>

              <section className="rounded-3xl border border-slate-200 bg-white p-4">
                <label className="block text-sm font-semibold text-slate-900">AI instructions (optional)</label>
                <textarea
                  className="mt-2 min-h-20 w-full rounded-2xl border border-slate-200 px-3 py-2 text-sm"
                  value={teacherInstructions}
                  onChange={(event) => setTeacherInstructions(event.target.value)}
                />
                <label className="mt-4 block text-sm font-semibold text-slate-900">Change reason</label>
                <input
                  className="mt-2 w-full rounded-2xl border border-slate-200 px-3 py-2 text-sm"
                  value={changeReason}
                  onChange={(event) => setChangeReason(event.target.value)}
                />
              </section>
            </>
          ) : (
            <section className="rounded-3xl border border-dashed border-slate-300 bg-white p-10 text-center">
              <p className="text-sm text-slate-600">Select or create a reflection to begin.</p>
            </section>
          )}
        </main>
      </div>
    </div>
  );
}
