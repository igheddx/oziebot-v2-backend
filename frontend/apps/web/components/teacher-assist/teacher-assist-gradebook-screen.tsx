"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  createGradebookRecordCorrection,
  createGradebookRecordReversal,
  fetchAssignmentGradebookExport,
  fetchAssignmentGradebookRecords,
  fetchAssignments,
  fetchGradebookRecordDetail,
} from "@/lib/teacher-assist-api";
import type {
  Assignment,
  AssignmentGradeRecord,
  AssignmentGradeRecordDetail,
  AssignmentGradebookExportView,
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

function statusClasses(status: string | null | undefined) {
  switch (status) {
    case "active":
      return "bg-emerald-50 text-emerald-800";
    case "reversed":
      return "bg-rose-50 text-rose-800";
    case "superseded":
      return "bg-amber-50 text-amber-800";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

export function TeacherAssistGradebookScreen() {
  const searchParams = useSearchParams();
  const requestedAssignmentId = searchParams.get("assignment_id");
  const [assignments, setAssignments] = useState<Assignment[]>([]);
  const [selectedAssignmentId, setSelectedAssignmentId] = useState<string | null>(null);
  const [records, setRecords] = useState<AssignmentGradeRecord[]>([]);
  const [selectedRecordId, setSelectedRecordId] = useState<string | null>(null);
  const [detail, setDetail] = useState<AssignmentGradeRecordDetail | null>(null);
  const [exportView, setExportView] = useState<AssignmentGradebookExportView | null>(null);
  const [loading, setLoading] = useState(true);
  const [recordsLoading, setRecordsLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [savingCorrection, setSavingCorrection] = useState(false);
  const [savingReversal, setSavingReversal] = useState(false);
  const [correctionForm, setCorrectionForm] = useState({
    committed_score: "",
    max_score: "",
    committed_feedback: "",
    reason: "",
  });
  const [reversalReason, setReversalReason] = useState("");
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [pageError, setPageError] = useState<string | null>(null);

  const selectedAssignment = useMemo(
    () => assignments.find((assignment) => assignment.id === selectedAssignmentId) ?? null,
    [assignments, selectedAssignmentId],
  );
  const selectedRecord = useMemo(
    () => records.find((record) => record.id === selectedRecordId) ?? null,
    [records, selectedRecordId],
  );

  const loadAssignments = useCallback(async () => {
    setLoading(true);
    try {
      const rows = await fetchAssignments({});
      setAssignments(rows);
      setSelectedAssignmentId((current) => {
        if (requestedAssignmentId && rows.some((row) => row.id === requestedAssignmentId)) {
          return requestedAssignmentId;
        }
        if (current && rows.some((row) => row.id === current)) return current;
        return rows[0]?.id ?? null;
      });
      setPageError(null);
    } catch (nextError) {
      setPageError(nextError instanceof Error ? nextError.message : "Could not load assignments.");
      setAssignments([]);
      setSelectedAssignmentId(null);
    } finally {
      setLoading(false);
    }
  }, [requestedAssignmentId]);

  const loadRecords = useCallback(async (assignmentId: string) => {
    setRecordsLoading(true);
    try {
      const rows = await fetchAssignmentGradebookRecords(assignmentId);
      setRecords(rows);
      setSelectedRecordId((current) => {
        if (current && rows.some((row) => row.id === current)) return current;
        return rows[0]?.id ?? null;
      });
      setPageError(null);
    } catch (nextError) {
      setSectionAlert(
        "gradeRecords",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load grade records.",
          "Unable to load grade records",
        ),
      );
      setRecords([]);
      setSelectedRecordId(null);
    } finally {
      setRecordsLoading(false);
    }
  }, []);

  const loadDetail = useCallback(async (gradeRecordId: string) => {
    setDetailLoading(true);
    try {
      const nextDetail = await fetchGradebookRecordDetail(gradeRecordId);
      setDetail(nextDetail);
      setCorrectionForm({
        committed_score: nextDetail.record.committed_score?.toString() ?? "",
        max_score: nextDetail.record.max_score?.toString() ?? "",
        committed_feedback: nextDetail.record.committed_feedback ?? "",
        reason: "",
      });
      setReversalReason("");
      setPageError(null);
    } catch (nextError) {
      setSectionAlert(
        "gradeRecordDetail",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load grade record detail.",
          "Unable to load record detail",
        ),
      );
      setDetail(null);
    } finally {
      setDetailLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadAssignments();
  }, [loadAssignments]);

  useEffect(() => {
    if (!selectedAssignmentId) {
      setRecords([]);
      setSelectedRecordId(null);
      setExportView(null);
      return;
    }
    void loadRecords(selectedAssignmentId);
  }, [loadRecords, selectedAssignmentId]);

  useEffect(() => {
    if (!selectedRecordId) {
      setDetail(null);
      return;
    }
    void loadDetail(selectedRecordId);
  }, [loadDetail, selectedRecordId]);

  const handleExport = useCallback(async () => {
    if (!selectedAssignmentId) return;
    setExportLoading(true);
    clearSectionAlert("gradeRecords");
    try {
      const payload = await fetchAssignmentGradebookExport(selectedAssignmentId);
      setExportView(payload);
      setSectionAlert(
        "gradeRecords",
        sectionSuccess("Export-ready gradebook view generated.", "Export view ready"),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradeRecords",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not generate gradebook export view.",
          "Export failed",
        ),
      );
    } finally {
      setExportLoading(false);
    }
  }, [clearSectionAlert, selectedAssignmentId, setSectionAlert]);

  const handleCorrection = useCallback(async () => {
    if (!selectedRecordId) return;
    setSavingCorrection(true);
    clearSectionAlert("gradeRecordDetail");
    try {
      await createGradebookRecordCorrection(selectedRecordId, {
        committed_score: correctionForm.committed_score.trim()
          ? Number(correctionForm.committed_score)
          : null,
        max_score: correctionForm.max_score.trim() ? Number(correctionForm.max_score) : null,
        committed_feedback: correctionForm.committed_feedback.trim() || null,
        reason: correctionForm.reason.trim(),
      });
      if (selectedAssignmentId) {
        await loadRecords(selectedAssignmentId);
      }
      await loadDetail(selectedRecordId);
      setSectionAlert(
        "gradeRecordDetail",
        sectionSuccess("Grade correction committed with audit trail.", "Correction committed"),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradeRecordDetail",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not commit grade correction.",
          "Unable to commit correction",
        ),
      );
    } finally {
      setSavingCorrection(false);
    }
  }, [clearSectionAlert, correctionForm, loadDetail, loadRecords, selectedAssignmentId, selectedRecordId, setSectionAlert]);

  const handleReversal = useCallback(async () => {
    if (!selectedRecordId) return;
    setSavingReversal(true);
    clearSectionAlert("gradeRecordDetail");
    try {
      await createGradebookRecordReversal(selectedRecordId, { reason: reversalReason.trim() });
      if (selectedAssignmentId) {
        await loadRecords(selectedAssignmentId);
      }
      await loadDetail(selectedRecordId);
      setSectionAlert(
        "gradeRecordDetail",
        sectionSuccess("Grade reversed with audit trail.", "Grade reversed"),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradeRecordDetail",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not reverse grade.",
          "Unable to reverse grade",
        ),
      );
    } finally {
      setSavingReversal(false);
    }
  }, [clearSectionAlert, loadDetail, loadRecords, reversalReason, selectedAssignmentId, selectedRecordId, setSectionAlert]);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-slate-900">Gradebook</h1>
            <p className="mt-2 max-w-3xl text-sm text-slate-600">
              Teacher-confirmed grade commits only. Corrections and reversals preserve commit history and audit
              events. Mastery updates, parent communication, LMS sync, and SIS integration remain disabled.
            </p>
          </div>
          <Link href="/teacher-assist/assignments" className="ta-button-secondary">
            Back to Assignments
          </Link>
        </div>
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load gradebook" message={pageError} />

      <section className="grid gap-6 xl:grid-cols-[320px_minmax(0,1fr)]">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Assignments</h2>
          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading assignments...</p>
          ) : assignments.length === 0 ? (
            <p className="mt-4 text-sm text-slate-500">No assignments available yet.</p>
          ) : (
            <div className="mt-4 space-y-2">
              {assignments.map((assignment) => (
                <button
                  key={assignment.id}
                  type="button"
                  onClick={() => setSelectedAssignmentId(assignment.id)}
                  className={`w-full rounded-2xl border p-4 text-left transition ${
                    assignment.id === selectedAssignmentId
                      ? "border-sky-300 bg-sky-50"
                      : "border-slate-200 bg-white hover:border-sky-200"
                  }`}
                >
                  <p className="text-sm font-semibold text-slate-900">{assignment.title}</p>
                  <p className="mt-1 text-xs text-slate-500">{labelize(assignment.assignment_type)}</p>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-5">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <h2 className="text-lg font-semibold text-slate-900">
                {selectedAssignment?.title ?? "Grade records"}
              </h2>
              <p className="mt-1 text-sm text-slate-500">
                Committed grades for anonymous STUDENT # entries only.
              </p>
            </div>
            <button
              type="button"
              onClick={() => void handleExport()}
              disabled={!selectedAssignmentId || exportLoading}
              className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
            >
              {exportLoading ? "Generating..." : "Generate Export View"}
            </button>
          </div>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("gradeRecords")}
            onDismiss={() => clearSectionAlert("gradeRecords")}
            className="mt-4"
          />

          {recordsLoading ? (
            <p className="mt-4 text-sm text-slate-500">Loading grade records...</p>
          ) : records.length === 0 ? (
            <p className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
              No gradebook commits yet for this assignment. Confirm a grading review in Assignments, then use
              Commit to Gradebook.
            </p>
          ) : (
            <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.2fr)]">
              <div className="space-y-3">
                {records.map((record) => (
                  <button
                    key={record.id}
                    type="button"
                    onClick={() => setSelectedRecordId(record.id)}
                    className={`w-full rounded-2xl border p-4 text-left transition ${
                      record.id === selectedRecordId
                        ? "border-sky-300 bg-sky-50"
                        : "border-slate-200 bg-white hover:border-sky-200"
                    }`}
                  >
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-sm font-semibold text-slate-900">
                        STUDENT #{record.student_number}
                      </span>
                      <span
                        className={`rounded-full px-2.5 py-1 text-xs font-semibold ${statusClasses(record.record_status)}`}
                      >
                        {labelize(record.record_status)}
                      </span>
                    </div>
                    <p className="mt-2 text-sm text-slate-600">
                      Score: {record.committed_score ?? "—"}
                      {record.max_score != null ? ` / ${record.max_score}` : ""}
                    </p>
                    <p className="mt-1 text-xs text-slate-500">Updated {formatDateTime(record.updated_at)}</p>
                  </button>
                ))}
              </div>

              <div className="rounded-2xl border border-slate-200 p-4">
                {!selectedRecord ? (
                  <p className="text-sm text-slate-500">Select a grade record to inspect commit history.</p>
                ) : detailLoading || !detail ? (
                  <p className="text-sm text-slate-500">Loading record detail...</p>
                ) : (
                  <div className="space-y-4">
                    <TeacherAssistInlineAlert
                      alert={getSectionAlert("gradeRecordDetail")}
                      onDismiss={() => clearSectionAlert("gradeRecordDetail")}
                    />
                    <div>
                      <p className="text-sm font-semibold text-slate-900">
                        STUDENT #{detail.record.student_number}
                      </p>
                      <p className="mt-1 text-sm text-slate-600">{detail.record.committed_feedback ?? "No feedback"}</p>
                    </div>

                    <div>
                      <p className="text-sm font-semibold text-slate-900">Commit history</p>
                      <div className="mt-3 space-y-2">
                        {detail.commits.map((commit) => (
                          <div key={commit.id} className="rounded-xl border border-slate-200 bg-slate-50 p-3 text-sm">
                            <div className="flex flex-wrap items-center gap-2">
                              <span className="font-semibold text-slate-900">{labelize(commit.commit_type)}</span>
                              <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${statusClasses(commit.commit_status)}`}>
                                {labelize(commit.commit_status)}
                              </span>
                            </div>
                            <p className="mt-2 text-slate-600">
                              Score: {commit.committed_score ?? "—"}
                              {commit.max_score != null ? ` / ${commit.max_score}` : ""}
                            </p>
                            {commit.reason ? <p className="mt-1 text-slate-500">Reason: {commit.reason}</p> : null}
                            <p className="mt-1 text-xs text-slate-500">{formatDateTime(commit.created_at)}</p>
                          </div>
                        ))}
                      </div>
                    </div>

                    <div>
                      <p className="text-sm font-semibold text-slate-900">Audit trail</p>
                      <div className="mt-3 space-y-2">
                        {detail.audit_events.length === 0 ? (
                          <p className="text-sm text-slate-500">No audit events recorded.</p>
                        ) : (
                          detail.audit_events.map((event) => (
                            <div key={event.id} className="rounded-xl border border-slate-200 p-3 text-sm">
                              <p className="font-semibold text-slate-900">{labelize(event.event_type)}</p>
                              <p className="mt-1 text-slate-600">{event.summary_text}</p>
                              <p className="mt-1 text-xs text-slate-500">{formatDateTime(event.created_at)}</p>
                            </div>
                          ))
                        )}
                      </div>
                    </div>

                    {detail.record.record_status === "active" ? (
                      <>
                        <div className="rounded-2xl border border-slate-200 p-4">
                          <p className="text-sm font-semibold text-slate-900">Grade correction</p>
                          <div className="mt-3 grid gap-3">
                            <input
                              className="ta-input"
                              value={correctionForm.committed_score}
                              onChange={(event) =>
                                setCorrectionForm((current) => ({
                                  ...current,
                                  committed_score: event.target.value,
                                }))
                              }
                              placeholder="Committed score"
                            />
                            <input
                              className="ta-input"
                              value={correctionForm.max_score}
                              onChange={(event) =>
                                setCorrectionForm((current) => ({ ...current, max_score: event.target.value }))
                              }
                              placeholder="Max score"
                            />
                            <textarea
                              className="ta-input min-h-20"
                              value={correctionForm.committed_feedback}
                              onChange={(event) =>
                                setCorrectionForm((current) => ({
                                  ...current,
                                  committed_feedback: event.target.value,
                                }))
                              }
                              placeholder="Committed feedback"
                            />
                            <textarea
                              className="ta-input min-h-20"
                              value={correctionForm.reason}
                              onChange={(event) =>
                                setCorrectionForm((current) => ({ ...current, reason: event.target.value }))
                              }
                              placeholder="Correction reason (required)"
                            />
                            <button
                              type="button"
                              onClick={() => void handleCorrection()}
                              disabled={savingCorrection}
                              className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              {savingCorrection ? "Committing..." : "Commit Correction"}
                            </button>
                          </div>
                        </div>

                        <div className="rounded-2xl border border-rose-200 bg-rose-50/40 p-4">
                          <p className="text-sm font-semibold text-rose-900">Grade reversal</p>
                          <textarea
                            className="ta-input mt-3 min-h-20"
                            value={reversalReason}
                            onChange={(event) => setReversalReason(event.target.value)}
                            placeholder="Reversal reason (required)"
                          />
                          <button
                            type="button"
                            onClick={() => void handleReversal()}
                            disabled={savingReversal}
                            className="mt-3 ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {savingReversal ? "Reversing..." : "Reverse Grade"}
                          </button>
                        </div>
                      </>
                    ) : null}
                  </div>
                )}
              </div>
            </div>
          )}

          {exportView ? (
            <div className="mt-6 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-sm font-semibold text-slate-900">Export-ready gradebook view</p>
              <p className="mt-1 text-sm text-slate-600">
                {exportView.active_record_count} active of {exportView.record_count} records · Generated{" "}
                {formatDateTime(exportView.generated_at)}
              </p>
              <pre className="mt-4 max-h-80 overflow-auto rounded-xl bg-white p-4 text-xs text-slate-700">
                {JSON.stringify(exportView, null, 2)}
              </pre>
            </div>
          ) : null}
        </article>
      </section>
    </div>
  );
}
