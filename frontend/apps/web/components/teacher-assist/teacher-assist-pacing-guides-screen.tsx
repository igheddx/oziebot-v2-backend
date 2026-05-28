"use client";

import { useCallback, useEffect, useMemo, useState } from "react";

import {
  attachPacingItemResource,
  attachPacingItemStandard,
  createPacingGuide,
  createPacingGuideItem,
  fetchGradingPeriods,
  fetchPacingGuideItems,
  fetchPacingGuides,
  fetchResources,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherAssistOptions,
  updatePacingGuide,
  updatePacingItem,
} from "@/lib/teacher-assist-api";
import type {
  GradingPeriod,
  PacingGuide,
  PacingItem,
  ResourceLibraryItem,
  SchoolYear,
  Standard,
  Subject,
  TeacherAssistOptions,
} from "@/lib/teacher-assist-types";

type Snapshot = {
  options: TeacherAssistOptions;
  schoolYears: SchoolYear[];
  gradingPeriods: GradingPeriod[];
  subjects: Subject[];
  standards: Standard[];
  resources: ResourceLibraryItem[];
  guides: PacingGuide[];
};

type GuideForm = {
  school_year_id: string;
  title: string;
  description: string;
  grade_level: string;
  subject_id: string;
  is_shared: boolean;
};

type ItemForm = {
  grading_period_id: string;
  subject_id: string;
  week_number: string;
  day_number: string;
  instructional_date: string;
  title: string;
  instructional_focus: string;
  objectives: string;
  notes: string;
  sort_order: string;
};

function emptyGuideForm(): GuideForm {
  return {
    school_year_id: "",
    title: "",
    description: "",
    grade_level: "",
    subject_id: "",
    is_shared: false,
  };
}

function emptyItemForm(): ItemForm {
  return {
    grading_period_id: "",
    subject_id: "",
    week_number: "",
    day_number: "",
    instructional_date: "",
    title: "",
    instructional_focus: "",
    objectives: "",
    notes: "",
    sort_order: "",
  };
}

function toOptionalInt(value: string): number | null {
  if (!value.trim()) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isNaN(parsed) ? null : parsed;
}

function formatDate(value: string | null) {
  if (!value) return "Not scheduled";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

export function TeacherAssistPacingGuidesScreen() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [items, setItems] = useState<PacingItem[]>([]);
  const [selectedGuideId, setSelectedGuideId] = useState("");
  const [loading, setLoading] = useState(true);
  const [itemsLoading, setItemsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [savingKey, setSavingKey] = useState<string | null>(null);
  const [guideForm, setGuideForm] = useState<GuideForm>(emptyGuideForm());
  const [editingGuideId, setEditingGuideId] = useState<string | null>(null);
  const [itemForm, setItemForm] = useState<ItemForm>(emptyItemForm());
  const [editingItemId, setEditingItemId] = useState<string | null>(null);
  const [standardSelections, setStandardSelections] = useState<Record<string, string>>({});
  const [resourceSelections, setResourceSelections] = useState<Record<string, string>>({});

  const loadSnapshot = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [options, schoolYears, gradingPeriods, subjects, standards, resources, guides] =
        await Promise.all([
          fetchTeacherAssistOptions(),
          fetchSchoolYears(),
          fetchGradingPeriods(),
          fetchSubjects(),
          fetchStandards(),
          fetchResources(),
          fetchPacingGuides(),
        ]);
      setSnapshot({ options, schoolYears, gradingPeriods, subjects, standards, resources, guides });
      setSelectedGuideId((current) => current || guides[0]?.id || "");
      setGuideForm((current) => ({
        ...current,
        school_year_id:
          current.school_year_id ||
          schoolYears.find((schoolYear) => schoolYear.is_active)?.id ||
          schoolYears[0]?.id ||
          "",
        subject_id: current.subject_id || subjects[0]?.id || "",
      }));
      setItemForm((current) => ({
        ...current,
        grading_period_id: current.grading_period_id || gradingPeriods[0]?.id || "",
        subject_id: current.subject_id || subjects[0]?.id || "",
      }));
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load pacing guides.");
    } finally {
      setLoading(false);
    }
  }, []);

  const loadItems = useCallback(async (guideId: string) => {
    if (!guideId) {
      setItems([]);
      return;
    }
    setItemsLoading(true);
    try {
      setItems(await fetchPacingGuideItems(guideId));
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load pacing items.");
    } finally {
      setItemsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadSnapshot();
  }, [loadSnapshot]);

  useEffect(() => {
    void loadItems(selectedGuideId);
  }, [loadItems, selectedGuideId]);

  const selectedGuide = useMemo(
    () => snapshot?.guides.find((guide) => guide.id === selectedGuideId) ?? null,
    [selectedGuideId, snapshot?.guides],
  );
  const schoolYearTitleById = useMemo(
    () => new Map((snapshot?.schoolYears ?? []).map((row) => [row.id, row.title])),
    [snapshot?.schoolYears],
  );
  const gradingPeriodTitleById = useMemo(
    () => new Map((snapshot?.gradingPeriods ?? []).map((row) => [row.id, row.title])),
    [snapshot?.gradingPeriods],
  );
  const subjectNameById = useMemo(
    () => new Map((snapshot?.subjects ?? []).map((row) => [row.id, row.name])),
    [snapshot?.subjects],
  );
  const standardCodeById = useMemo(
    () => new Map((snapshot?.standards ?? []).map((row) => [row.id, row.code])),
    [snapshot?.standards],
  );
  const resourceTitleById = useMemo(
    () => new Map((snapshot?.resources ?? []).map((row) => [row.id, row.title])),
    [snapshot?.resources],
  );

  const runSave = async (key: string, action: () => Promise<void>, successMessage: string) => {
    setSavingKey(key);
    setError(null);
    setNotice(null);
    try {
      await action();
      setNotice(successMessage);
      await loadSnapshot();
      await loadItems(selectedGuideId);
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
            TeacherAssist Pacing Guides
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Structured instructional timelines
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Create pacing guides, add pacing items by grading period or day, then attach standards
            and reusable resources. This phase stores planning context only—nothing generates yet.
          </p>
        </div>
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {notice ? <section className="ta-alert ta-alert-success">{notice}</section> : null}

      <section className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">
            {editingGuideId ? "Edit pacing guide" : "Create pacing guide"}
          </h2>
          <form
            className="mt-5 space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              void runSave(
                "guide",
                async () => {
                  const body = {
                    school_year_id: guideForm.school_year_id,
                    title: guideForm.title,
                    description: guideForm.description || null,
                    grade_level: guideForm.grade_level || null,
                    subject_id: guideForm.subject_id || null,
                    is_shared: guideForm.is_shared,
                  };
                  const savedGuide = editingGuideId
                    ? await updatePacingGuide(editingGuideId, body)
                    : await createPacingGuide(body);
                  setSelectedGuideId(savedGuide.id);
                  setEditingGuideId(null);
                  setGuideForm({
                    ...emptyGuideForm(),
                    school_year_id: body.school_year_id,
                    subject_id: body.subject_id ?? "",
                    grade_level: body.grade_level ?? "",
                  });
                },
                editingGuideId ? "Pacing guide updated." : "Pacing guide created.",
              );
            }}
          >
            <label className="space-y-2">
              <span className="ta-label">School year</span>
              <select
                value={guideForm.school_year_id}
                onChange={(event) =>
                  setGuideForm((current) => ({ ...current, school_year_id: event.target.value }))
                }
                className="ta-input"
              >
                <option value="">Select school year</option>
                {(snapshot?.schoolYears ?? []).map((schoolYear) => (
                  <option key={schoolYear.id} value={schoolYear.id}>
                    {schoolYear.title}
                  </option>
                ))}
              </select>
            </label>
            <label className="space-y-2">
              <span className="ta-label">Title</span>
              <input
                value={guideForm.title}
                onChange={(event) => setGuideForm((current) => ({ ...current, title: event.target.value }))}
                className="ta-input"
                placeholder="5th Grade Math Pacing"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">Description</span>
              <textarea
                value={guideForm.description}
                onChange={(event) =>
                  setGuideForm((current) => ({ ...current, description: event.target.value }))
                }
                className="ta-input min-h-24"
              />
            </label>
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="ta-label">Grade level</span>
                <select
                  value={guideForm.grade_level}
                  onChange={(event) =>
                    setGuideForm((current) => ({ ...current, grade_level: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional</option>
                  {(snapshot?.options.supported_grade_levels ?? []).map((gradeLevel) => (
                    <option key={gradeLevel} value={gradeLevel}>
                      {gradeLevel}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Subject</span>
                <select
                  value={guideForm.subject_id}
                  onChange={(event) =>
                    setGuideForm((current) => ({ ...current, subject_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional</option>
                  {(snapshot?.subjects ?? []).map((subject) => (
                    <option key={subject.id} value={subject.id}>
                      {subject.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <label className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
              <input
                type="checkbox"
                checked={guideForm.is_shared}
                onChange={(event) =>
                  setGuideForm((current) => ({ ...current, is_shared: event.target.checked }))
                }
              />
              Mark as shareable for later teacher or district collaboration
            </label>
            <div className="flex flex-wrap gap-3">
              <button type="submit" disabled={savingKey === "guide"} className="ta-button-primary">
                {savingKey === "guide"
                  ? "Saving..."
                  : editingGuideId
                    ? "Update pacing guide"
                    : "Create pacing guide"}
              </button>
              {editingGuideId ? (
                <button
                  type="button"
                  className="ta-button-secondary"
                  onClick={() => {
                    setEditingGuideId(null);
                    setGuideForm(emptyGuideForm());
                  }}
                >
                  Cancel edit
                </button>
              ) : null}
            </div>
          </form>
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Saved pacing guides</h2>
          <p className="mt-1 text-sm text-slate-600">
            Choose a guide to manage pacing items below.
          </p>
          {loading ? (
            <p className="mt-5 text-sm text-slate-600">Loading pacing guides...</p>
          ) : snapshot && snapshot.guides.length > 0 ? (
            <div className="mt-5 space-y-3">
              {snapshot.guides.map((guide) => (
                <article
                  key={guide.id}
                  className={`rounded-2xl border p-4 ${
                    selectedGuideId === guide.id
                      ? "border-sky-300 bg-sky-50/70"
                      : "border-slate-200 bg-white"
                  }`}
                >
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                    <div>
                      <button
                        type="button"
                        onClick={() => setSelectedGuideId(guide.id)}
                        className="text-left text-base font-semibold text-slate-900"
                      >
                        {guide.title}
                      </button>
                      <p className="mt-2 text-sm leading-6 text-slate-600">
                        {guide.description || "No description saved yet."}
                      </p>
                      <p className="mt-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                        {schoolYearTitleById.get(guide.school_year_id) ?? "School year"} ·{" "}
                        {guide.item_count} pacing item{guide.item_count === 1 ? "" : "s"}
                      </p>
                    </div>
                    <button
                      type="button"
                      className="ta-button-secondary"
                      onClick={() => {
                        setSelectedGuideId(guide.id);
                        setEditingGuideId(guide.id);
                        setGuideForm({
                          school_year_id: guide.school_year_id,
                          title: guide.title,
                          description: guide.description ?? "",
                          grade_level: guide.grade_level ?? "",
                          subject_id: guide.subject_id ?? "",
                          is_shared: guide.is_shared,
                        });
                      }}
                    >
                      Edit
                    </button>
                  </div>
                </article>
              ))}
            </div>
          ) : (
            <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
              No pacing guides yet. Create one to start organizing instructional context.
            </div>
          )}
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">
            {editingItemId ? "Edit pacing item" : "Add pacing item"}
          </h2>
          <p className="mt-1 text-sm text-slate-600">
            {selectedGuide
              ? `Adding items to ${selectedGuide.title}.`
              : "Select a pacing guide first."}
          </p>
          <form
            className="mt-5 space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              if (!selectedGuideId) {
                setError("Select or create a pacing guide before saving pacing items.");
                return;
              }
              void runSave(
                "item",
                async () => {
                  const body = {
                    grading_period_id: itemForm.grading_period_id || null,
                    subject_id: itemForm.subject_id || null,
                    week_number: toOptionalInt(itemForm.week_number),
                    day_number: toOptionalInt(itemForm.day_number),
                    instructional_date: itemForm.instructional_date || null,
                    title: itemForm.title,
                    instructional_focus: itemForm.instructional_focus || null,
                    objectives: itemForm.objectives || null,
                    notes: itemForm.notes || null,
                    sort_order: toOptionalInt(itemForm.sort_order),
                  };
                  if (editingItemId) {
                    await updatePacingItem(editingItemId, body);
                  } else {
                    await createPacingGuideItem(selectedGuideId, body);
                  }
                  setEditingItemId(null);
                  setItemForm({
                    ...emptyItemForm(),
                    grading_period_id: itemForm.grading_period_id,
                    subject_id: itemForm.subject_id,
                  });
                },
                editingItemId ? "Pacing item updated." : "Pacing item added.",
              );
            }}
          >
            <label className="space-y-2">
              <span className="ta-label">Title</span>
              <input
                value={itemForm.title}
                onChange={(event) => setItemForm((current) => ({ ...current, title: event.target.value }))}
                className="ta-input"
                placeholder="Place value review"
              />
            </label>
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="ta-label">Grading period</span>
                <select
                  value={itemForm.grading_period_id}
                  onChange={(event) =>
                    setItemForm((current) => ({ ...current, grading_period_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional</option>
                  {(snapshot?.gradingPeriods ?? []).map((period) => (
                    <option key={period.id} value={period.id}>
                      {period.title}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Subject</span>
                <select
                  value={itemForm.subject_id}
                  onChange={(event) =>
                    setItemForm((current) => ({ ...current, subject_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Optional</option>
                  {(snapshot?.subjects ?? []).map((subject) => (
                    <option key={subject.id} value={subject.id}>
                      {subject.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>
            <div className="grid gap-4 md:grid-cols-4">
              <label className="space-y-2">
                <span className="ta-label">Week</span>
                <input
                  value={itemForm.week_number}
                  onChange={(event) =>
                    setItemForm((current) => ({ ...current, week_number: event.target.value }))
                  }
                  className="ta-input"
                />
              </label>
              <label className="space-y-2">
                <span className="ta-label">Day</span>
                <input
                  value={itemForm.day_number}
                  onChange={(event) =>
                    setItemForm((current) => ({ ...current, day_number: event.target.value }))
                  }
                  className="ta-input"
                />
              </label>
              <label className="space-y-2 md:col-span-2">
                <span className="ta-label">Instructional date</span>
                <input
                  type="date"
                  value={itemForm.instructional_date}
                  onChange={(event) =>
                    setItemForm((current) => ({ ...current, instructional_date: event.target.value }))
                  }
                  className="ta-input"
                />
              </label>
            </div>
            <label className="space-y-2">
              <span className="ta-label">Instructional focus</span>
              <textarea
                value={itemForm.instructional_focus}
                onChange={(event) =>
                  setItemForm((current) => ({ ...current, instructional_focus: event.target.value }))
                }
                className="ta-input min-h-24"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">Objectives</span>
              <textarea
                value={itemForm.objectives}
                onChange={(event) =>
                  setItemForm((current) => ({ ...current, objectives: event.target.value }))
                }
                className="ta-input min-h-24"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">Notes</span>
              <textarea
                value={itemForm.notes}
                onChange={(event) => setItemForm((current) => ({ ...current, notes: event.target.value }))}
                className="ta-input min-h-24"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">Sort order</span>
              <input
                value={itemForm.sort_order}
                onChange={(event) =>
                  setItemForm((current) => ({ ...current, sort_order: event.target.value }))
                }
                className="ta-input"
              />
            </label>
            <div className="flex flex-wrap gap-3">
              <button type="submit" disabled={savingKey === "item"} className="ta-button-primary">
                {savingKey === "item"
                  ? "Saving..."
                  : editingItemId
                    ? "Update pacing item"
                    : "Add pacing item"}
              </button>
              {editingItemId ? (
                <button
                  type="button"
                  className="ta-button-secondary"
                  onClick={() => {
                    setEditingItemId(null);
                    setItemForm(emptyItemForm());
                  }}
                >
                  Cancel edit
                </button>
              ) : null}
            </div>
          </form>
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Pacing items</h2>
          <p className="mt-1 text-sm text-slate-600">
            Attach standards and resources after each pacing item is created.
          </p>
          {!selectedGuide ? (
            <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
              Select a pacing guide to manage items.
            </div>
          ) : itemsLoading ? (
            <p className="mt-5 text-sm text-slate-600">Loading pacing items...</p>
          ) : items.length === 0 ? (
            <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
              No pacing items yet for {selectedGuide.title}. Add the first pacing item on the left.
            </div>
          ) : (
            <div className="mt-5 space-y-4">
              {items.map((item) => (
                <article key={item.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                    <div>
                      <p className="text-base font-semibold text-slate-900">{item.title}</p>
                      <p className="mt-2 text-sm text-slate-600">
                        {item.instructional_focus || item.objectives || item.notes || "No detail saved yet."}
                      </p>
                      <p className="mt-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                        {gradingPeriodTitleById.get(item.grading_period_id ?? "") || "No grading period"} · Week{" "}
                        {item.week_number ?? "-"} · Day {item.day_number ?? "-"} · {formatDate(item.instructional_date)}
                      </p>
                    </div>
                    <button
                      type="button"
                      className="ta-button-secondary"
                      onClick={() => {
                        setEditingItemId(item.id);
                        setItemForm({
                          grading_period_id: item.grading_period_id ?? "",
                          subject_id: item.subject_id ?? "",
                          week_number: item.week_number?.toString() ?? "",
                          day_number: item.day_number?.toString() ?? "",
                          instructional_date: item.instructional_date ?? "",
                          title: item.title,
                          instructional_focus: item.instructional_focus ?? "",
                          objectives: item.objectives ?? "",
                          notes: item.notes ?? "",
                          sort_order: item.sort_order?.toString() ?? "",
                        });
                      }}
                    >
                      Edit
                    </button>
                  </div>

                  <div className="mt-4 grid gap-4 lg:grid-cols-2">
                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                        Standards
                      </p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {item.standard_ids.length > 0 ? (
                          item.standard_ids.map((standardId) => (
                            <span
                              key={standardId}
                              className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                            >
                              {standardCodeById.get(standardId) ?? "Standard"}
                            </span>
                          ))
                        ) : (
                          <span className="text-sm text-slate-500">No standards attached yet.</span>
                        )}
                      </div>
                      <div className="mt-3 flex gap-2">
                        <select
                          value={standardSelections[item.id] ?? ""}
                          onChange={(event) =>
                            setStandardSelections((current) => ({
                              ...current,
                              [item.id]: event.target.value,
                            }))
                          }
                          className="ta-input"
                        >
                          <option value="">Select standard</option>
                          {(snapshot?.standards ?? []).map((standard) => (
                            <option key={standard.id} value={standard.id}>
                              {standard.code}
                            </option>
                          ))}
                        </select>
                        <button
                          type="button"
                          className="ta-button-secondary"
                          onClick={() => {
                            const selectedStandardId = standardSelections[item.id];
                            if (!selectedStandardId) return;
                            void runSave(
                              `standard-${item.id}`,
                              async () => {
                                await attachPacingItemStandard(item.id, selectedStandardId);
                              },
                              "Standard attached to pacing item.",
                            );
                          }}
                        >
                          Attach
                        </button>
                      </div>
                    </div>

                    <div>
                      <p className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                        Resources
                      </p>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {item.resource_ids.length > 0 ? (
                          item.resource_ids.map((resourceId) => (
                            <span
                              key={resourceId}
                              className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-700"
                            >
                              {resourceTitleById.get(resourceId) ?? "Resource"}
                            </span>
                          ))
                        ) : (
                          <span className="text-sm text-slate-500">No resources attached yet.</span>
                        )}
                      </div>
                      <div className="mt-3 flex gap-2">
                        <select
                          value={resourceSelections[item.id] ?? ""}
                          onChange={(event) =>
                            setResourceSelections((current) => ({
                              ...current,
                              [item.id]: event.target.value,
                            }))
                          }
                          className="ta-input"
                        >
                          <option value="">Select resource</option>
                          {(snapshot?.resources ?? []).map((resource) => (
                            <option key={resource.id} value={resource.id}>
                              {resource.title}
                            </option>
                          ))}
                        </select>
                        <button
                          type="button"
                          className="ta-button-secondary"
                          onClick={() => {
                            const selectedResourceId = resourceSelections[item.id];
                            if (!selectedResourceId) return;
                            void runSave(
                              `resource-${item.id}`,
                              async () => {
                                await attachPacingItemResource(item.id, selectedResourceId);
                              },
                              "Resource attached to pacing item.",
                            );
                          }}
                        >
                          Attach
                        </button>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 text-sm text-slate-600">
                    Subject: {subjectNameById.get(item.subject_id ?? "") ?? "Not assigned"} · Sort order:{" "}
                    {item.sort_order ?? "Auto"}
                  </div>
                </article>
              ))}
            </div>
          )}
        </article>
      </section>
    </div>
  );
}
