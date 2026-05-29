"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  commitMasteryEvaluation,
  createMasteryEvaluation,
  createMasteryEvaluationCorrection,
  createMasteryEvaluationReversal,
  createMasteryMatrix,
  createReteachPlan,
  fetchClasses,
  fetchGradingPeriods,
  fetchMasteryEvaluationDetail,
  fetchMasteryMatrix,
  fetchMasteryMatrixHeatmap,
  fetchMasteryMatrixReteachInsights,
  fetchMasteryMatrixReteachSummary,
  fetchMasteryMatrixStandardsSummary,
  fetchMasteryMatrixStudentsSummary,
  fetchMasteryMatrixSummary,
  fetchMasteryMatrices,
  fetchMasteryDashboard,
  fetchSchoolYears,
  fetchStandards,
  fetchStudentMasterySummary,
  fetchSubjects,
  fetchTeacherAssistOptions,
} from "@/lib/teacher-assist-api";
import { TeacherAssistCrossLinks } from "@/components/teacher-assist/teacher-assist-cross-links";
import { TeacherAssistMasteryHeatmap } from "@/components/teacher-assist/teacher-assist-mastery-heatmap";
import type {
  GradingPeriod,
  MasteryEvaluationDetail,
  MasteryHeatmapCell,
  MasteryLevel,
  MasteryMatrix,
  MasteryMatrixHeatmap,
  MasteryMatrixReteachInsights,
  MasteryMatrixReteachSummary,
  MasteryMatrixStandardsSummary,
  MasteryMatrixStudentsSummary,
  MasteryMatrixSummary,
  MasteryDashboard,
  SchoolYear,
  Standard,
  StudentMasterySummary,
  Subject,
  TeacherAssistOptions,
  TeacherClass,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function levelClasses(level: string) {
  switch (level) {
    case "advanced":
      return "bg-emerald-100 text-emerald-900";
    case "mastery":
      return "bg-sky-100 text-sky-900";
    case "developing":
      return "bg-amber-100 text-amber-900";
    case "beginning":
      return "bg-rose-100 text-rose-900";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

function SummaryCard({
  label,
  value,
  detail,
}: {
  label: string;
  value: number | string;
  detail: string;
}) {
  return (
    <article className="ta-panel p-5">
      <p className="text-sm font-semibold text-slate-500">{label}</p>
      <p className="mt-3 text-3xl font-semibold text-slate-900">{value}</p>
      <p className="mt-2 text-sm text-slate-600">{detail}</p>
    </article>
  );
}

export function TeacherAssistMasteryScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const requestedMatrixId = searchParams.get("id");

  const [options, setOptions] = useState<TeacherAssistOptions | null>(null);
  const [matrices, setMatrices] = useState<MasteryMatrix[]>([]);
  const [selectedMatrixId, setSelectedMatrixId] = useState<string | null>(null);
  const [matrix, setMatrix] = useState<MasteryMatrix | null>(null);
  const [summary, setSummary] = useState<MasteryMatrixSummary | null>(null);
  const [standardsSummary, setStandardsSummary] = useState<MasteryMatrixStandardsSummary | null>(null);
  const [studentsSummary, setStudentsSummary] = useState<MasteryMatrixStudentsSummary | null>(null);
  const [reteachSummary, setReteachSummary] = useState<MasteryMatrixReteachSummary | null>(null);
  const [heatmap, setHeatmap] = useState<MasteryMatrixHeatmap | null>(null);
  const [reteachInsights, setReteachInsights] = useState<MasteryMatrixReteachInsights | null>(null);
  const [dashboard, setDashboard] = useState<MasteryDashboard | null>(null);
  const [studentSummary, setStudentSummary] = useState<StudentMasterySummary | null>(null);
  const [listFilters, setListFilters] = useState({
    school_year_id: "",
    grading_period_id: "",
    class_id: "",
    subject_id: "",
  });
  const [selectedEvaluationId, setSelectedEvaluationId] = useState<string | null>(null);
  const [evaluationDetail, setEvaluationDetail] = useState<MasteryEvaluationDetail | null>(null);
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [gradingPeriods, setGradingPeriods] = useState<GradingPeriod[]>([]);
  const [classes, setClasses] = useState<TeacherClass[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [standards, setStandards] = useState<Standard[]>([]);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [savingMatrix, setSavingMatrix] = useState(false);
  const [savingEvaluation, setSavingEvaluation] = useState(false);
  const [committing, setCommitting] = useState(false);
  const [matrixForm, setMatrixForm] = useState({
    school_year_id: "",
    grading_period_id: "",
    class_id: "",
    subject_id: "",
    title: "",
    standard_id: "",
  });
  const [evaluationForm, setEvaluationForm] = useState({
    student_number: "1",
    standard_id: "",
    mastery_level: "developing" as MasteryLevel,
    confidence_level: "medium",
    evidence_source_type: "manual_observation",
    teacher_notes: "",
  });
  const [correctionForm, setCorrectionForm] = useState({
    mastery_level: "developing" as MasteryLevel,
    commit_reason: "",
  });
  const [reversalReason, setReversalReason] = useState("");
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [pageError, setPageError] = useState<string | null>(null);
  const [reteachActionBusyStandardId, setReteachActionBusyStandardId] = useState<string | null>(null);

  const selectedMatrix = useMemo(
    () => matrices.find((row) => row.id === selectedMatrixId) ?? matrix,
    [matrices, matrix, selectedMatrixId],
  );

  const loadMatrices = useCallback(async () => {
    setLoading(true);
    try {
      const [
        nextOptions,
        nextMatrices,
        nextSchoolYears,
        nextGradingPeriods,
        nextClasses,
        nextSubjects,
        nextStandards,
        nextDashboard,
      ] = await Promise.all([
        fetchTeacherAssistOptions(),
        fetchMasteryMatrices({
          school_year_id: listFilters.school_year_id || undefined,
          grading_period_id: listFilters.grading_period_id || undefined,
          class_id: listFilters.class_id || undefined,
          subject_id: listFilters.subject_id || undefined,
        }),
        fetchSchoolYears(),
        fetchGradingPeriods(),
        fetchClasses(),
        fetchSubjects(),
        fetchStandards(),
        fetchMasteryDashboard({
          school_year_id: listFilters.school_year_id || undefined,
          grading_period_id: listFilters.grading_period_id || undefined,
          class_id: listFilters.class_id || undefined,
          subject_id: listFilters.subject_id || undefined,
        }),
      ]);
      setOptions(nextOptions);
      setMatrices(nextMatrices);
      setSchoolYears(nextSchoolYears);
      setGradingPeriods(nextGradingPeriods);
      setClasses(nextClasses);
      setSubjects(nextSubjects);
      setStandards(nextStandards);
      setDashboard(nextDashboard);
      setSelectedMatrixId((current) => {
        if (requestedMatrixId && nextMatrices.some((row) => row.id === requestedMatrixId)) {
          return requestedMatrixId;
        }
        if (current && nextMatrices.some((row) => row.id === current)) return current;
        return nextMatrices[0]?.id ?? null;
      });
      setPageError(null);
    } catch (nextError) {
      setPageError(nextError instanceof Error ? nextError.message : "Could not load mastery matrices.");
    } finally {
      setLoading(false);
    }
  }, [requestedMatrixId, listFilters]);

  const loadMatrixDetail = useCallback(async (matrixId: string) => {
    setDetailLoading(true);
    try {
      const [nextMatrix, nextSummary, nextStandards, nextStudents, nextReteach, nextHeatmap, nextReteachInsights] =
        await Promise.all([
        fetchMasteryMatrix(matrixId),
        fetchMasteryMatrixSummary(matrixId),
        fetchMasteryMatrixStandardsSummary(matrixId),
        fetchMasteryMatrixStudentsSummary(matrixId),
        fetchMasteryMatrixReteachSummary(matrixId),
        fetchMasteryMatrixHeatmap(matrixId),
        fetchMasteryMatrixReteachInsights(matrixId),
      ]);
      setMatrix(nextMatrix);
      setSummary(nextSummary);
      setStandardsSummary(nextStandards);
      setStudentsSummary(nextStudents);
      setReteachSummary(nextReteach);
      setHeatmap(nextHeatmap);
      setReteachInsights(nextReteachInsights);
      setStudentSummary(null);
      setEvaluationForm((current) => ({
        ...current,
        standard_id: current.standard_id || nextMatrix.standards[0]?.standard_id || "",
      }));
      setPageError(null);
    } catch (nextError) {
      setSectionAlert(
        "masteryMatrix",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load mastery matrix detail.",
          "Unable to load matrix",
        ),
      );
      setMatrix(null);
      setSummary(null);
      setStandardsSummary(null);
      setStudentsSummary(null);
      setReteachSummary(null);
      setHeatmap(null);
      setReteachInsights(null);
      setStudentSummary(null);
    } finally {
      setDetailLoading(false);
    }
  }, []);

  const loadStudentSummary = useCallback(
    async (matrixId: string, studentNumber: number) => {
      try {
        const summaryResult = await fetchStudentMasterySummary(matrixId, studentNumber);
        setStudentSummary(summaryResult);
      } catch (nextError) {
        setSectionAlert(
          "masteryMatrix",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not load student mastery summary.",
            "Unable to load student summary",
          ),
        );
        setStudentSummary(null);
      }
    },
    [],
  );

  const handleSelectStudent = useCallback(
    (studentNumber: number) => {
      if (!selectedMatrixId) return;
      void loadStudentSummary(selectedMatrixId, studentNumber);
    },
    [loadStudentSummary, selectedMatrixId],
  );

  const handleSelectCell = useCallback(
    (studentNumber: number, cell: MasteryHeatmapCell) => {
      void handleSelectStudent(studentNumber);
      if (cell.evaluation_id) {
        setSelectedEvaluationId(cell.evaluation_id);
      }
    },
    [handleSelectStudent],
  );

  const handleCreateReteachPlan = useCallback(
    async (standardId: string) => {
      if (!selectedMatrixId) return;
      setReteachActionBusyStandardId(standardId);
      clearSectionAlert("masteryMatrix");
      try {
        const plan = await createReteachPlan({
          mastery_matrix_id: selectedMatrixId,
          standard_id: standardId,
        });
        setSectionAlert(
          "masteryMatrix",
          sectionSuccess("Reteach plan draft created. Generate an AI draft on the next screen.", "Reteach plan created"),
        );
        router.push(`/teacher-assist/reteach-plans?id=${plan.id}`);
      } catch (nextError) {
        setSectionAlert(
          "masteryMatrix",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not create reteach plan.",
            "Unable to create reteach plan",
          ),
        );
      } finally {
        setReteachActionBusyStandardId(null);
      }
    },
    [clearSectionAlert, router, selectedMatrixId, setSectionAlert],
  );

  const loadEvaluationDetail = useCallback(async (evaluationId: string) => {
    try {
      const detail = await fetchMasteryEvaluationDetail(evaluationId);
      setEvaluationDetail(detail);
      setCorrectionForm({
        mastery_level: detail.evaluation.mastery_level,
        commit_reason: "",
      });
    } catch (nextError) {
      setSectionAlert(
        "masteryEvaluation",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load mastery evaluation detail.",
          "Unable to load evaluation",
        ),
      );
      setEvaluationDetail(null);
    }
  }, [setSectionAlert]);

  useEffect(() => {
    void loadMatrices();
  }, [loadMatrices]);

  useEffect(() => {
    if (!selectedMatrixId) {
      setMatrix(null);
      setSummary(null);
      setStandardsSummary(null);
      setStudentsSummary(null);
      setReteachSummary(null);
      return;
    }
    void loadMatrixDetail(selectedMatrixId);
  }, [loadMatrixDetail, selectedMatrixId]);

  useEffect(() => {
    if (!selectedEvaluationId) {
      setEvaluationDetail(null);
      return;
    }
    void loadEvaluationDetail(selectedEvaluationId);
  }, [loadEvaluationDetail, selectedEvaluationId]);

  const handleCreateMatrix = useCallback(async () => {
    if (!matrixForm.standard_id) {
      setSectionAlert("masteryMatrices", {
        type: "error",
        title: "Unable to create matrix",
        description: "Select at least one standard for the mastery matrix.",
      });
      return;
    }
    setSavingMatrix(true);
    clearSectionAlert("masteryMatrices");
    try {
      const created = await createMasteryMatrix({
        school_year_id: matrixForm.school_year_id,
        grading_period_id: matrixForm.grading_period_id || null,
        class_id: matrixForm.class_id,
        subject_id: matrixForm.subject_id,
        title: matrixForm.title.trim(),
        standard_ids: [matrixForm.standard_id],
        target_mastery_level: "mastery",
      });
      setMatrices((current) => [created, ...current]);
      setSelectedMatrixId(created.id);
      setSectionAlert("masteryMatrices", sectionSuccess("Mastery matrix created.", "Matrix created"));
    } catch (nextError) {
      setSectionAlert(
        "masteryMatrices",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not create mastery matrix.",
          "Unable to create matrix",
        ),
      );
    } finally {
      setSavingMatrix(false);
    }
  }, [clearSectionAlert, matrixForm, setSectionAlert]);

  const handleCreateEvaluation = useCallback(async () => {
    if (!selectedMatrixId || !evaluationForm.standard_id) return;
    setSavingEvaluation(true);
    clearSectionAlert("masteryEvaluation");
    try {
      const created = await createMasteryEvaluation({
        mastery_matrix_id: selectedMatrixId,
        student_number: Number(evaluationForm.student_number),
        standard_id: evaluationForm.standard_id,
        mastery_level: evaluationForm.mastery_level,
        confidence_level: evaluationForm.confidence_level,
        evidence_source_type: evaluationForm.evidence_source_type,
        teacher_notes: evaluationForm.teacher_notes.trim() || null,
      });
      setSelectedEvaluationId(created.id);
      await loadMatrixDetail(selectedMatrixId);
      setSectionAlert(
        "masteryEvaluation",
        sectionSuccess(
          "Draft mastery evaluation created. Teacher commit is required before it counts as active.",
          "Evaluation created",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "masteryEvaluation",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not create mastery evaluation.",
          "Unable to create evaluation",
        ),
      );
    } finally {
      setSavingEvaluation(false);
    }
  }, [clearSectionAlert, evaluationForm, loadMatrixDetail, selectedMatrixId, setSectionAlert]);

  const handleCommitEvaluation = useCallback(async () => {
    if (!selectedEvaluationId || !selectedMatrixId) return;
    setCommitting(true);
    clearSectionAlert("masteryEvaluation");
    try {
      const result = await commitMasteryEvaluation(selectedEvaluationId, {
        commit_reason: "Teacher confirmed mastery update.",
      });
      setEvaluationDetail({ evaluation: result.evaluation, commits: evaluationDetail?.commits ?? [] });
      await loadMatrixDetail(selectedMatrixId);
      await loadEvaluationDetail(selectedEvaluationId);
      setSectionAlert("masteryEvaluation", sectionSuccess(result.message, "Evaluation committed"));
    } catch (nextError) {
      setSectionAlert(
        "masteryEvaluation",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not commit mastery evaluation.",
          "Unable to commit evaluation",
        ),
      );
    } finally {
      setCommitting(false);
    }
  }, [clearSectionAlert, evaluationDetail?.commits, loadEvaluationDetail, loadMatrixDetail, selectedEvaluationId, selectedMatrixId, setSectionAlert]);

  const handleCorrection = useCallback(async () => {
    if (!selectedEvaluationId || !selectedMatrixId) return;
    setCommitting(true);
    clearSectionAlert("masteryEvaluation");
    try {
      const result = await createMasteryEvaluationCorrection(selectedEvaluationId, {
        mastery_level: correctionForm.mastery_level,
        commit_reason: correctionForm.commit_reason.trim(),
      });
      await loadMatrixDetail(selectedMatrixId);
      await loadEvaluationDetail(selectedEvaluationId);
      setEvaluationDetail({ evaluation: result.evaluation, commits: evaluationDetail?.commits ?? [] });
      setSectionAlert("masteryEvaluation", sectionSuccess(result.message, "Correction committed"));
    } catch (nextError) {
      setSectionAlert(
        "masteryEvaluation",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not commit mastery correction.",
          "Unable to commit correction",
        ),
      );
    } finally {
      setCommitting(false);
    }
  }, [clearSectionAlert, correctionForm, evaluationDetail?.commits, loadEvaluationDetail, loadMatrixDetail, selectedEvaluationId, selectedMatrixId, setSectionAlert]);

  const handleReversal = useCallback(async () => {
    if (!selectedEvaluationId || !selectedMatrixId) return;
    setCommitting(true);
    clearSectionAlert("masteryEvaluation");
    try {
      const result = await createMasteryEvaluationReversal(selectedEvaluationId, {
        commit_reason: reversalReason.trim(),
      });
      await loadMatrixDetail(selectedMatrixId);
      await loadEvaluationDetail(selectedEvaluationId);
      setEvaluationDetail({ evaluation: result.evaluation, commits: evaluationDetail?.commits ?? [] });
      setSectionAlert("masteryEvaluation", sectionSuccess(result.message, "Evaluation reversed"));
    } catch (nextError) {
      setSectionAlert(
        "masteryEvaluation",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not reverse mastery evaluation.",
          "Unable to reverse evaluation",
        ),
      );
    } finally {
      setCommitting(false);
    }
  }, [evaluationDetail?.commits, loadEvaluationDetail, loadMatrixDetail, reversalReason, selectedEvaluationId, selectedMatrixId]);

  const matrixStandards = matrix?.standards ?? [];

  return (
    <div className="space-y-8">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Mastery
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Standards mastery tracking
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Teacher-confirmed mastery only. Anonymous STUDENT # tracking, evidence linkage, correction/reversal
            lineage, and reteach visibility — without automatic gradebook, parent, or LMS side effects.
          </p>
        </div>
      </section>

      <TeacherAssistAlert
        variant="info"
        description="Mastery updates require explicit teacher commit. Grading confirmation and gradebook commits do not auto-update mastery in this phase."
      />

      <TeacherAssistFormErrorSummary title="Unable to load mastery workspace" message={pageError} />

      <section className="ta-panel p-6">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Operational mastery dashboard</h2>
            <p className="mt-1 text-sm text-slate-600">
              Read-only analytics from committed mastery only. Draft evaluations are excluded.
            </p>
          </div>
          <div className="grid gap-2 md:grid-cols-4">
            <select
              className="ta-input"
              value={listFilters.school_year_id}
              onChange={(event) =>
                setListFilters((current) => ({ ...current, school_year_id: event.target.value }))
              }
            >
              <option value="">All school years</option>
              {schoolYears.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.title}
                </option>
              ))}
            </select>
            <select
              className="ta-input"
              value={listFilters.grading_period_id}
              onChange={(event) =>
                setListFilters((current) => ({ ...current, grading_period_id: event.target.value }))
              }
            >
              <option value="">All grading periods</option>
              {gradingPeriods.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.title}
                </option>
              ))}
            </select>
            <select
              className="ta-input"
              value={listFilters.class_id}
              onChange={(event) => setListFilters((current) => ({ ...current, class_id: event.target.value }))}
            >
              <option value="">All classes</option>
              {classes.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.name}
                </option>
              ))}
            </select>
            <select
              className="ta-input"
              value={listFilters.subject_id}
              onChange={(event) => setListFilters((current) => ({ ...current, subject_id: event.target.value }))}
            >
              <option value="">All subjects</option>
              {subjects.map((row) => (
                <option key={row.id} value={row.id}>
                  {row.name}
                </option>
              ))}
            </select>
          </div>
        </div>
        {dashboard ? (
          <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <SummaryCard label="Active matrices" value={dashboard.matrix_count} detail="Filtered mastery matrices." />
            <SummaryCard
              label="Reteach recommended"
              value={dashboard.reteach_recommended_standards.length}
              detail="Standards below monitor thresholds."
            />
            <SummaryCard
              label="Low mastery alerts"
              value={dashboard.low_mastery_alerts.length}
              detail="Critical attention standards."
            />
            <SummaryCard
              label="Unassessed standards"
              value={dashboard.unassessed_standards.length}
              detail="No committed evaluations yet."
            />
          </div>
        ) : null}
      </section>

      <section className="grid gap-6 xl:grid-cols-[0.9fr_1.1fr]">
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Mastery matrices</h2>
          <p className="mt-1 text-sm text-slate-600">Select a class/subject matrix or create a new one.</p>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("masteryMatrices")}
            onDismiss={() => clearSectionAlert("masteryMatrices")}
            className="mt-4"
          />
          <TeacherAssistInlineAlert
            alert={getSectionAlert("masteryMatrix")}
            onDismiss={() => clearSectionAlert("masteryMatrix")}
            className="mt-4"
          />
          <div className="mt-4 space-y-2">
            {loading ? (
              <p className="text-sm text-slate-500">Loading matrices...</p>
            ) : matrices.length === 0 ? (
              <p className="text-sm text-slate-500">No mastery matrices yet.</p>
            ) : (
              matrices.map((row) => (
                <button
                  key={row.id}
                  type="button"
                  onClick={() => {
                    setSelectedMatrixId(row.id);
                    setSelectedEvaluationId(null);
                  }}
                  className={`w-full rounded-2xl border px-4 py-3 text-left ${
                    selectedMatrixId === row.id
                      ? "border-sky-300 bg-sky-50"
                      : "border-slate-200 bg-white hover:border-slate-300"
                  }`}
                >
                  <p className="font-semibold text-slate-900">{row.title}</p>
                  <p className="mt-1 text-sm text-slate-600">
                    {row.standards.length} standard(s) · {labelize(row.status)}
                  </p>
                </button>
              ))
            )}
          </div>

          <div className="mt-6 border-t border-slate-200 pt-6">
            <h3 className="text-lg font-semibold text-slate-900">Create matrix</h3>
            <div className="mt-4 grid gap-3">
              <input
                className="ta-input"
                placeholder="Matrix title"
                value={matrixForm.title}
                onChange={(event) => setMatrixForm((current) => ({ ...current, title: event.target.value }))}
              />
              <select
                className="ta-input"
                value={matrixForm.school_year_id}
                onChange={(event) => setMatrixForm((current) => ({ ...current, school_year_id: event.target.value }))}
              >
                <option value="">School year</option>
                {schoolYears.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.title}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={matrixForm.grading_period_id}
                onChange={(event) =>
                  setMatrixForm((current) => ({ ...current, grading_period_id: event.target.value }))
                }
              >
                <option value="">Grading period (optional)</option>
                {gradingPeriods.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.title}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={matrixForm.class_id}
                onChange={(event) => setMatrixForm((current) => ({ ...current, class_id: event.target.value }))}
              >
                <option value="">Class</option>
                {classes.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={matrixForm.subject_id}
                onChange={(event) => setMatrixForm((current) => ({ ...current, subject_id: event.target.value }))}
              >
                <option value="">Subject</option>
                {subjects.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={matrixForm.standard_id}
                onChange={(event) => setMatrixForm((current) => ({ ...current, standard_id: event.target.value }))}
              >
                <option value="">Standard</option>
                {standards.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.code} · {row.description}
                  </option>
                ))}
              </select>
              <button
                type="button"
                className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                disabled={savingMatrix}
                onClick={() => void handleCreateMatrix()}
              >
                {savingMatrix ? "Creating..." : "Create Mastery Matrix"}
              </button>
            </div>
          </div>
        </article>

        <article className="ta-panel p-6">
          {!selectedMatrix ? (
            <p className="text-sm text-slate-500">Select a mastery matrix to view summaries and drill-down work.</p>
          ) : detailLoading ? (
            <p className="text-sm text-slate-500">Loading mastery workspace...</p>
          ) : (
            <div className="space-y-6">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">{selectedMatrix.title}</h2>
                <p className="mt-1 text-sm text-slate-600">
                  {summary?.tracked_standard_count ?? 0} tracked standards · {summary?.student_count ?? 0} student
                  numbers with evaluations
                </p>
              </div>

              {summary ? (
                <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                  <SummaryCard label="Active evaluations" value={summary.active_evaluation_count} detail="Teacher-confirmed mastery records." />
                  <SummaryCard label="Draft evaluations" value={summary.draft_evaluation_count} detail="Awaiting teacher commit." />
                  <SummaryCard label="Reteach candidates" value={summary.reteach_candidate_count} detail="Below target mastery level." />
                  <SummaryCard label="Unassessed standards" value={summary.unassessed_standard_count} detail="No committed evaluations yet." />
                  <SummaryCard label="Reversed" value={summary.reversed_evaluation_count} detail="Reversed mastery commits." />
                </div>
              ) : null}

              {standardsSummary ? (
                <div>
                  <h3 className="text-lg font-semibold text-slate-900">Standards mastery cards</h3>
                  <div className="mt-3 grid gap-3">
                    {standardsSummary.standards.map((row) => (
                      <article key={row.standard_id} className="rounded-2xl border border-slate-200 bg-white p-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <p className="font-semibold text-slate-900">{row.standard_code ?? "Standard"}</p>
                            <p className="mt-1 text-sm text-slate-600">{row.standard_description}</p>
                          </div>
                          <span className={`rounded-full px-3 py-1 text-xs font-semibold ${levelClasses(row.target_mastery_level)}`}>
                            Target {labelize(row.target_mastery_level)}
                          </span>
                        </div>
                        <div className="mt-3 flex flex-wrap gap-2 text-xs text-slate-600">
                          <span className="rounded-full bg-slate-50 px-2.5 py-1">Active {row.active_evaluation_count}</span>
                          <span className="rounded-full bg-slate-50 px-2.5 py-1">Reteach {row.reteach_candidate_count}</span>
                          {row.is_unassessed ? (
                            <span className="rounded-full bg-amber-50 px-2.5 py-1 text-amber-800">Unassessed</span>
                          ) : null}
                        </div>
                      </article>
                    ))}
                  </div>
                </div>
              ) : null}

              <div>
                <h3 className="text-lg font-semibold text-slate-900">Mastery heatmap</h3>
                <p className="mt-1 text-sm text-slate-600">
                  Anonymous STUDENT # rows with standards columns. Hover for evaluation metadata.
                </p>
                {selectedMatrixId ? (
                  <div className="mt-3">
                    <TeacherAssistCrossLinks
                      links={[
                        {
                          label: "Today workspace",
                          href: "/teacher-assist/today",
                          detail: "See mastery alerts in your daily queue",
                        },
                        {
                          label: "Actions",
                          href: "/teacher-assist/actions",
                          detail: "Operational mastery follow-ups",
                        },
                        {
                          label: "Assignments",
                          href: "/teacher-assist/assignments",
                          detail: "Review assignment effectiveness evidence",
                        },
                      ]}
                    />
                  </div>
                ) : null}
                <div className="mt-4">
                  <TeacherAssistMasteryHeatmap
                    heatmap={heatmap}
                    reteachInsights={reteachInsights}
                    studentSummary={studentSummary}
                    onSelectStudent={handleSelectStudent}
                    onSelectCell={handleSelectCell}
                    onCreateReteachPlan={handleCreateReteachPlan}
                    reteachActionBusyStandardId={reteachActionBusyStandardId}
                  />
                </div>
              </div>

              {reteachSummary && reteachSummary.reteach_items.length > 0 ? (
                <div>
                  <h3 className="text-lg font-semibold text-slate-900">Reteach visibility</h3>
                  <div className="mt-3 space-y-2">
                    {reteachSummary.reteach_items.map((item) => (
                      <button
                        key={item.evaluation_id}
                        type="button"
                        onClick={() => setSelectedEvaluationId(item.evaluation_id)}
                        className="w-full rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-left"
                      >
                        <p className="font-semibold text-amber-950">
                          STUDENT #{item.student_number} · {item.standard_code ?? "Standard"}
                        </p>
                        <p className="mt-1 text-sm text-amber-900">
                          Current {labelize(item.current_mastery_level)} · Target {labelize(item.target_mastery_level)}
                        </p>
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}

              {studentsSummary ? (
                <div>
                  <h3 className="text-lg font-semibold text-slate-900">Student-number matrix</h3>
                  <div className="mt-3 space-y-3">
                    {studentsSummary.students.map((student) => (
                      <article key={student.student_number} className="rounded-2xl border border-slate-200 bg-white p-4">
                        <div className="flex items-center justify-between gap-3">
                          <p className="font-semibold text-slate-900">STUDENT #{student.student_number}</p>
                          <span className="text-xs text-slate-500">
                            Reteach {student.reteach_candidate_count} · Draft {student.draft_evaluation_count}
                          </span>
                        </div>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {student.cells.map((cell) => (
                            <button
                              key={cell.evaluation_id}
                              type="button"
                              onClick={() => setSelectedEvaluationId(cell.evaluation_id)}
                              className={`rounded-full px-3 py-1 text-xs font-semibold ${levelClasses(cell.mastery_level)}`}
                            >
                              {labelize(cell.mastery_level)} · {labelize(cell.evaluation_status)}
                            </button>
                          ))}
                        </div>
                      </article>
                    ))}
                  </div>
                </div>
              ) : null}
            </div>
          )}
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-2">
        <TeacherAssistInlineAlert
          alert={getSectionAlert("masteryEvaluation")}
          onDismiss={() => clearSectionAlert("masteryEvaluation")}
          className="xl:col-span-2"
        />
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Create draft evaluation</h2>
          <p className="mt-1 text-sm text-slate-600">
            Draft evaluations stay inactive until you explicitly commit mastery.
          </p>
          <div className="mt-4 grid gap-3">
            <input
              className="ta-input"
              type="number"
              min={1}
              placeholder="Student number"
              value={evaluationForm.student_number}
              onChange={(event) =>
                setEvaluationForm((current) => ({ ...current, student_number: event.target.value }))
              }
            />
            <select
              className="ta-input"
              value={evaluationForm.standard_id}
              onChange={(event) =>
                setEvaluationForm((current) => ({ ...current, standard_id: event.target.value }))
              }
            >
              <option value="">Standard</option>
              {matrixStandards.map((row) => (
                <option key={row.standard_id} value={row.standard_id}>
                  {row.standard_code ?? row.standard_id}
                </option>
              ))}
            </select>
            <select
              className="ta-input"
              value={evaluationForm.mastery_level}
              onChange={(event) =>
                setEvaluationForm((current) => ({
                  ...current,
                  mastery_level: event.target.value as MasteryLevel,
                }))
              }
            >
              {(options?.mastery_levels ?? ["beginning", "developing", "mastery", "advanced"]).map((level) => (
                <option key={level} value={level}>
                  {labelize(level)}
                </option>
              ))}
            </select>
            <textarea
              className="ta-input min-h-24"
              placeholder="Teacher notes (anonymous, no PII)"
              value={evaluationForm.teacher_notes}
              onChange={(event) =>
                setEvaluationForm((current) => ({ ...current, teacher_notes: event.target.value }))
              }
            />
            <button
              type="button"
              className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
              disabled={!selectedMatrixId || savingEvaluation}
              onClick={() => void handleCreateEvaluation()}
            >
              {savingEvaluation ? "Saving..." : "Create Draft Evaluation"}
            </button>
          </div>
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Evaluation drill-down</h2>
          {!evaluationDetail ? (
            <p className="mt-3 text-sm text-slate-500">Select a student cell or reteach item to inspect commit lineage.</p>
          ) : (
            <div className="mt-4 space-y-4">
              <div className="rounded-2xl border border-slate-200 bg-white p-4">
                <p className="font-semibold text-slate-900">
                  STUDENT #{evaluationDetail.evaluation.student_number}
                </p>
                <p className="mt-1 text-sm text-slate-600">
                  {labelize(evaluationDetail.evaluation.evaluation_status)} ·{" "}
                  {labelize(evaluationDetail.evaluation.mastery_level)}
                </p>
                <p className="mt-2 text-xs text-slate-500">
                  Updated {formatDateTime(evaluationDetail.evaluation.updated_at)}
                </p>
              </div>

              {evaluationDetail.evaluation.evaluation_status === "draft" ? (
                <button
                  type="button"
                  className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                  disabled={committing}
                  onClick={() => void handleCommitEvaluation()}
                >
                  {committing ? "Committing..." : "Commit Mastery"}
                </button>
              ) : null}

              {evaluationDetail.evaluation.evaluation_status === "active" ? (
                <div className="space-y-3">
                  <select
                    className="ta-input"
                    value={correctionForm.mastery_level}
                    onChange={(event) =>
                      setCorrectionForm((current) => ({
                        ...current,
                        mastery_level: event.target.value as MasteryLevel,
                      }))
                    }
                  >
                    {(options?.mastery_levels ?? ["beginning", "developing", "mastery", "advanced"]).map((level) => (
                      <option key={level} value={level}>
                        {labelize(level)}
                      </option>
                    ))}
                  </select>
                  <input
                    className="ta-input"
                    placeholder="Correction reason"
                    value={correctionForm.commit_reason}
                    onChange={(event) =>
                      setCorrectionForm((current) => ({ ...current, commit_reason: event.target.value }))
                    }
                  />
                  <button
                    type="button"
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                    disabled={committing}
                    onClick={() => void handleCorrection()}
                  >
                    Commit Correction
                  </button>
                  <input
                    className="ta-input"
                    placeholder="Reversal reason"
                    value={reversalReason}
                    onChange={(event) => setReversalReason(event.target.value)}
                  />
                  <button
                    type="button"
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                    disabled={committing}
                    onClick={() => void handleReversal()}
                  >
                    Reverse Mastery
                  </button>
                </div>
              ) : null}

              <div>
                <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-500">Commit history</h3>
                <div className="mt-3 space-y-2">
                  {evaluationDetail.commits.map((commit) => (
                    <article key={commit.id} className="rounded-2xl border border-slate-200 bg-white p-3 text-sm">
                      <p className="font-semibold text-slate-900">
                        {labelize(commit.commit_type)} · {labelize(commit.commit_status)}
                      </p>
                      <p className="mt-1 text-slate-600">
                        {commit.previous_mastery_level ? labelize(commit.previous_mastery_level) : "None"} →{" "}
                        {labelize(commit.new_mastery_level)}
                      </p>
                      {commit.commit_reason ? (
                        <p className="mt-1 text-slate-500">{commit.commit_reason}</p>
                      ) : null}
                    </article>
                  ))}
                </div>
              </div>
            </div>
          )}
        </article>
      </section>

      <section className="flex flex-wrap gap-3">
        <Link href="/teacher-assist/actions" className="ta-button-secondary">
          Open Actions Workspace
        </Link>
        <Link href="/teacher-assist/gradebook" className="ta-button-secondary">
          Open Gradebook
        </Link>
      </section>
    </div>
  );
}
