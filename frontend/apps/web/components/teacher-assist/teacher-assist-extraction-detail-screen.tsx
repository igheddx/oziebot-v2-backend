"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  cancelExtractionJob,
  fetchExtractedTextDetail,
  fetchExtractedTextHistory,
  fetchExtractionJob,
  fetchExtractionSummaries,
  retryExtractionJob,
  updateExtractedTextApprovedContent,
  updateExtractedTextReviewStatus,
} from "@/lib/teacher-assist-api";
import type {
  TeacherAssistExtractedTextDetailAggregate,
  TeacherAssistExtractedTextHistory,
  TeacherAssistExtractionJobDetail,
  TeacherAssistExtractionSummary,
} from "@/lib/teacher-assist-types";

function formatDateTime(value: string | null) {
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

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function confidenceClasses(level: string | null | undefined) {
  if (level === "low") return "border-rose-200 bg-rose-50 text-rose-900";
  if (level === "medium") return "border-amber-200 bg-amber-50 text-amber-900";
  if (level === "high") return "border-emerald-200 bg-emerald-50 text-emerald-900";
  return "border-slate-200 bg-slate-50 text-slate-700";
}

function statusClasses(status: string) {
  if (status === "failed" || status === "teacher_rejected") return "border-rose-200 bg-rose-50 text-rose-900";
  if (status === "completed" || status === "teacher_approved") return "border-emerald-200 bg-emerald-50 text-emerald-900";
  if (status === "running" || status === "teacher_reviewing") return "border-sky-200 bg-sky-50 text-sky-900";
  if (status === "queued" || status === "pending_review") return "border-amber-200 bg-amber-50 text-amber-900";
  return "border-slate-200 bg-slate-50 text-slate-700";
}

function ExtractionList({
  summaries,
  onOpen,
}: {
  summaries: TeacherAssistExtractionSummary[];
  onOpen: (extractedTextId: string) => void;
}) {
  return (
    <section className="space-y-4">
      {summaries.length === 0 ? (
        <article className="ta-panel p-6 text-sm text-slate-600">
          No extraction jobs yet. Queue extraction from Resources or Assignments to populate this workspace.
        </article>
      ) : (
        summaries.map((summary) => {
          const record = summary.extracted_text;
          const detailId = record?.id ?? null;
          return (
            <article key={summary.job.id} className="ta-panel p-5">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(summary.job.status)}`}>
                      Job {labelize(summary.job.status)}
                    </span>
                    {record ? (
                      <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(record.review_status)}`}>
                        Review {labelize(record.review_status)}
                      </span>
                    ) : null}
                    {record ? (
                      <span className={`rounded-full px-3 py-1 text-xs font-semibold ${confidenceClasses(record.confidence_level)}`}>
                        Confidence {labelize(record.confidence_level)}
                      </span>
                    ) : null}
                  </div>
                  <h2 className="text-lg font-semibold text-slate-900">
                    {summary.job.artifact_type === "student_work"
                      ? `STUDENT #${summary.job.student_number ?? "?"} extraction`
                      : "Resource extraction"}
                  </h2>
                  <p className="text-sm text-slate-600">
                    Attempt {summary.job.attempt_number} · Provider {summary.job.provider_name ?? "mock"} · Updated{" "}
                    {formatDateTime(summary.job.updated_at)}
                  </p>
                  {record?.preview_text ? (
                    <p className="rounded-2xl border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
                      {record.preview_text}
                    </p>
                  ) : summary.job.error_message ? (
                    <p className="rounded-2xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
                      {summary.job.error_message}
                    </p>
                  ) : null}
                </div>
                <div className="flex flex-wrap gap-2">
                  {detailId ? (
                    <button type="button" className="ta-button-primary" onClick={() => onOpen(detailId)}>
                      Open review
                    </button>
                  ) : (
                    <Link href={`/teacher-assist/extractions?jobId=${summary.job.id}`} className="ta-button-secondary">
                      View job
                    </Link>
                  )}
                </div>
              </div>
            </article>
          );
        })
      )}
    </section>
  );
}

function ExtractionDetail({
  detail,
  history,
  onReload,
}: {
  detail: TeacherAssistExtractedTextDetailAggregate;
  history: TeacherAssistExtractedTextHistory | null;
  onReload: () => Promise<void>;
}) {
  const [correctedText, setCorrectedText] = useState(detail.record.teacher_corrected_text ?? detail.record.extracted_text);
  const [approvedText, setApprovedText] = useState(detail.record.approved_text ?? "");
  const [issueReason, setIssueReason] = useState(detail.record.teacher_issue_reason ?? "");
  const [reviewNotes, setReviewNotes] = useState(detail.record.teacher_review_notes ?? "");
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setCorrectedText(detail.record.teacher_corrected_text ?? detail.record.extracted_text);
    setApprovedText(detail.record.approved_text ?? "");
    setIssueReason(detail.record.teacher_issue_reason ?? "");
    setReviewNotes(detail.record.teacher_review_notes ?? "");
  }, [detail]);

  const runAction = useCallback(
    async (actionKey: string, action: () => Promise<void>) => {
      setBusyAction(actionKey);
      setError(null);
      try {
        await action();
        await onReload();
        setNotice("Extraction review updated.");
      } catch (nextError) {
        setError(nextError instanceof Error ? nextError.message : "Could not update extraction review.");
      } finally {
        setBusyAction(null);
      }
    },
    [onReload],
  );

  const lowConfidence = detail.record.confidence_level === "low";

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">Extraction Review</p>
            <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900">
              {detail.record.artifact_type === "student_work"
                ? `STUDENT #${detail.record.student_number ?? "?"}`
                : "Resource artifact"}
            </h1>
            <div className="mt-4 flex flex-wrap gap-2">
              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(detail.job.status)}`}>
                Job {labelize(detail.job.status)}
              </span>
              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(detail.record.review_status)}`}>
                Review {labelize(detail.record.review_status)}
              </span>
              <span className={`rounded-full px-3 py-1 text-xs font-semibold ${confidenceClasses(detail.record.confidence_level)}`}>
                Confidence {labelize(detail.record.confidence_level)}
                {detail.record.provider_confidence_score != null
                  ? ` (${Math.round(detail.record.provider_confidence_score * 100)}%)`
                  : ""}
              </span>
            </div>
          </div>
          <Link href="/teacher-assist/extractions" className="ta-button-secondary">
            Back to extractions
          </Link>
        </div>
      </section>

      {lowConfidence ? (
        <section className="ta-alert ta-alert-error">
          Provider confidence is low. Review the extracted text carefully before approving it for downstream use.
        </section>
      ) : null}
      {notice ? <section className="ta-alert ta-alert-info">{notice}</section> : null}
      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}

      <section className="ta-alert ta-alert-info">
        AI grading, mastery updates, and gradebook commits remain disabled. Extraction review is teacher-controlled
        and does not trigger AI usage.
      </section>

      {detail.record.teacher_issue_reason ? (
        <section className="ta-alert ta-alert-error">
          Teacher issue flagged: {detail.record.teacher_issue_reason}
        </section>
      ) : null}

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Original extracted text</h2>
          <pre className="mt-3 max-h-96 overflow-auto whitespace-pre-wrap rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-800">
            {detail.record.extracted_text}
          </pre>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Teacher corrected text</h2>
          <textarea
            className="ta-input mt-3 min-h-64 w-full"
            value={correctedText}
            onChange={(event) => setCorrectedText(event.target.value)}
          />
          <label className="mt-4 block text-sm font-semibold text-slate-700" htmlFor="approved-text">
            Approved text
          </label>
          <textarea
            id="approved-text"
            className="ta-input mt-2 min-h-32 w-full"
            value={approvedText}
            onChange={(event) => setApprovedText(event.target.value)}
            placeholder="Optional explicit approved text for downstream phases"
          />
          <label className="mt-4 block text-sm font-semibold text-slate-700" htmlFor="review-notes">
            Teacher review notes
          </label>
          <textarea
            id="review-notes"
            className="ta-input mt-2 min-h-24 w-full"
            value={reviewNotes}
            onChange={(event) => setReviewNotes(event.target.value)}
          />
          <label className="mt-4 block text-sm font-semibold text-slate-700" htmlFor="issue-reason">
            Issue reason
          </label>
          <textarea
            id="issue-reason"
            className="ta-input mt-2 min-h-24 w-full"
            value={issueReason}
            onChange={(event) => setIssueReason(event.target.value)}
            placeholder="Describe extraction quality issues before flagging"
          />
        </article>
      </section>

      <section className="ta-panel p-5">
        <h2 className="text-lg font-semibold text-slate-900">Review actions</h2>
        <div className="mt-4 flex flex-wrap gap-3">
          <button
            type="button"
            className="ta-button-secondary"
            disabled={busyAction !== null || detail.record.review_status !== "pending_review"}
            onClick={() =>
              void runAction("start", async () => {
                await updateExtractedTextReviewStatus(detail.record.id, {
                  review_status: "teacher_reviewing",
                  teacher_review_notes: reviewNotes || null,
                });
              })
            }
          >
            Start Review
          </button>
          <button
            type="button"
            className="ta-button-secondary"
            disabled={
              busyAction !== null ||
              !["pending_review", "teacher_reviewing", "issue_flagged"].includes(detail.record.review_status)
            }
            onClick={() =>
              void runAction("reviewed", async () => {
                await updateExtractedTextReviewStatus(detail.record.id, {
                  review_status: "reviewed",
                  teacher_review_notes: reviewNotes || null,
                });
              })
            }
          >
            Mark Reviewed
          </button>
          <button
            type="button"
            className="ta-button-primary"
            disabled={busyAction !== null || !["teacher_reviewing", "pending_review"].includes(detail.record.review_status)}
            onClick={() =>
              void runAction("approve", async () => {
                await updateExtractedTextApprovedContent(detail.record.id, {
                  teacher_corrected_text: correctedText,
                  approved_text: approvedText || correctedText,
                });
                await updateExtractedTextReviewStatus(detail.record.id, {
                  review_status: "teacher_approved",
                  teacher_review_notes: reviewNotes || null,
                });
              })
            }
          >
            Approve Extraction
          </button>
          <button
            type="button"
            className="ta-button-secondary"
            disabled={busyAction !== null || !["teacher_reviewing", "pending_review"].includes(detail.record.review_status)}
            onClick={() =>
              void runAction("reject", async () => {
                await updateExtractedTextReviewStatus(detail.record.id, {
                  review_status: "teacher_rejected",
                  teacher_review_notes: reviewNotes || null,
                });
              })
            }
          >
            Reject Extraction
          </button>
          <button
            type="button"
            className="ta-button-secondary"
            disabled={
              busyAction !== null ||
              !["pending_review", "teacher_reviewing", "issue_flagged"].includes(detail.record.review_status)
            }
            onClick={() =>
              void runAction("flag", async () => {
                await updateExtractedTextReviewStatus(detail.record.id, {
                  review_status: "issue_flagged",
                  teacher_issue_reason: issueReason || "Teacher flagged extraction quality issue.",
                  teacher_review_notes: reviewNotes || null,
                });
              })
            }
          >
            Flag Issue
          </button>
          <button
            type="button"
            className="ta-button-secondary"
            disabled={busyAction !== null}
            onClick={() =>
              void runAction("save", async () => {
                await updateExtractedTextApprovedContent(detail.record.id, {
                  teacher_corrected_text: correctedText,
                  approved_text: approvedText || null,
                });
              })
            }
          >
            Save Corrected Text
          </button>
          {detail.retry_eligible ? (
            <button
              type="button"
              className="ta-button-secondary"
              disabled={busyAction !== null}
              onClick={() =>
                void runAction("retry", async () => {
                  await retryExtractionJob(detail.job.id);
                })
              }
            >
              Retry Extraction
            </button>
          ) : null}
          {detail.cancel_eligible ? (
            <button
              type="button"
              className="ta-button-secondary"
              disabled={busyAction !== null}
              onClick={() =>
                void runAction("cancel", async () => {
                  await cancelExtractionJob(detail.job.id);
                })
              }
            >
              Cancel Extraction
            </button>
          ) : null}
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-2">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Job metadata</h2>
          <dl className="mt-4 space-y-2 text-sm text-slate-700">
            <div className="flex justify-between gap-4">
              <dt>Attempt</dt>
              <dd>{detail.job.attempt_number}</dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt>Retry count</dt>
              <dd>{detail.job.retry_count}</dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt>Processing duration</dt>
              <dd>{detail.processing_duration_seconds != null ? `${detail.processing_duration_seconds}s` : "N/A"}</dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt>Last heartbeat</dt>
              <dd>{formatDateTime(detail.job.heartbeat_at)}</dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt>Last error</dt>
              <dd>{detail.job.error_code ?? "None"}</dd>
            </div>
          </dl>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Retry history</h2>
          <div className="mt-4 space-y-3">
            {(history?.attempt_jobs ?? detail.lineage_jobs).map((job) => (
              <div key={job.id} className="rounded-2xl border border-slate-200 p-3 text-sm">
                <p className="font-semibold text-slate-900">
                  Attempt {job.attempt_number} · {labelize(job.status)}
                </p>
                <p className="mt-1 text-slate-600">
                  Created {formatDateTime(job.created_at)}
                  {job.error_message ? ` · ${job.error_message}` : ""}
                </p>
              </div>
            ))}
          </div>
        </article>
      </section>

      <section className="ta-panel p-5">
        <h2 className="text-lg font-semibold text-slate-900">Activity timeline</h2>
        <div className="mt-4 space-y-3">
          {(history?.activity_events ?? detail.activity_events).length === 0 ? (
            <p className="text-sm text-slate-600">No activity events recorded yet.</p>
          ) : (
            (history?.activity_events ?? detail.activity_events).map((event) => (
              <div key={event.id} className="rounded-2xl border border-slate-200 p-3">
                <p className="text-sm font-semibold text-slate-900">{labelize(event.event_type)}</p>
                <p className="mt-1 text-sm text-slate-600">{event.summary_text}</p>
                <p className="mt-1 text-xs text-slate-500">{formatDateTime(event.timestamp)}</p>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

export function TeacherAssistExtractionDetailScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const extractedTextId = searchParams.get("id");
  const extractionJobId = searchParams.get("jobId");
  const [summaries, setSummaries] = useState<TeacherAssistExtractionSummary[]>([]);
  const [detail, setDetail] = useState<TeacherAssistExtractedTextDetailAggregate | null>(null);
  const [jobDetail, setJobDetail] = useState<TeacherAssistExtractionJobDetail | null>(null);
  const [history, setHistory] = useState<TeacherAssistExtractedTextHistory | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const loadList = useCallback(async () => {
    const nextSummaries = await fetchExtractionSummaries();
    setSummaries(nextSummaries);
  }, []);

  const loadDetail = useCallback(async (id: string) => {
    const [nextDetail, nextHistory] = await Promise.all([
      fetchExtractedTextDetail(id),
      fetchExtractedTextHistory(id),
    ]);
    setDetail(nextDetail);
    setHistory(nextHistory);
  }, []);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      if (extractedTextId) {
        await loadDetail(extractedTextId);
        setJobDetail(null);
      } else if (extractionJobId) {
        const nextJobDetail = await fetchExtractionJob(extractionJobId);
        setJobDetail(nextJobDetail);
        setDetail(null);
        setHistory(null);
      } else {
        setDetail(null);
        setJobDetail(null);
        setHistory(null);
        await loadList();
      }
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load extraction workspace.");
    } finally {
      setLoading(false);
    }
  }, [extractedTextId, extractionJobId, loadDetail, loadList]);

  useEffect(() => {
    void load();
  }, [load]);

  const dashboardCards = useMemo(
    () => [
      {
        label: "Extraction failures",
        value: summaries.filter((item) => item.job.status === "failed").length,
      },
      {
        label: "Low confidence",
        value: summaries.filter((item) => item.extracted_text?.confidence_level === "low").length,
      },
      {
        label: "Awaiting review",
        value: summaries.filter(
          (item) =>
            item.extracted_text?.review_status === "pending_review" ||
            item.extracted_text?.review_status === "teacher_reviewing",
        ).length,
      },
      {
        label: "Retry required",
        value: summaries.filter((item) => item.extracted_text?.review_status === "needs_retry").length,
      },
      {
        label: "Stale jobs",
        value: summaries.filter((item) => item.job.status === "running" && !item.job.heartbeat_at).length,
      },
      {
        label: "Recently approved",
        value: summaries.filter((item) => item.extracted_text?.review_status === "teacher_approved").length,
      },
    ],
    [summaries],
  );

  if (loading) {
    return <section className="ta-panel p-6 text-sm text-slate-600">Loading extraction workspace...</section>;
  }

  if (error) {
    return <section className="ta-alert ta-alert-error">{error}</section>;
  }

  if (extractedTextId && detail) {
    return <ExtractionDetail detail={detail} history={history} onReload={load} />;
  }

  if (extractionJobId && jobDetail) {
    return (
      <div className="space-y-6">
        <section className="ta-panel p-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">Extraction Job</p>
              <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900">
                {jobDetail.source_artifact.artifact_type === "student_work"
                  ? `STUDENT #${jobDetail.source_artifact.student_number ?? "?"}`
                  : jobDetail.source_artifact.original_filename}
              </h1>
              <p className="mt-2 text-sm text-slate-600">
                Provider {jobDetail.job.provider_name ?? "mock"} · Attempt {jobDetail.job.attempt_number} ·{" "}
                {labelize(jobDetail.job.status)}
              </p>
            </div>
            <Link href="/teacher-assist/extractions" className="ta-button-secondary">
              Back to extractions
            </Link>
          </div>
        </section>
        <section className="ta-alert ta-alert-info">
          AI grading remains disabled. This drill-down shows extraction job status only.
        </section>
        {jobDetail.extracted_text ? (
          <section className="ta-panel p-5">
            <p className="text-sm text-slate-600">Extracted text is available for teacher review.</p>
            <button
              type="button"
              className="ta-button-primary mt-4"
              onClick={() => router.push(`/teacher-assist/extractions?id=${jobDetail.extracted_text?.id}`)}
            >
              Open extraction review
            </button>
          </section>
        ) : (
          <section className="ta-panel p-5">
            <p className="text-sm text-slate-600">
              {jobDetail.job.error_message ?? "Extraction has not produced extracted text yet."}
            </p>
          </section>
        )}
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">TeacherAssist Extractions</p>
        <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900">Extraction remediation workspace</h1>
        <p className="mt-3 max-w-3xl text-base leading-7 text-slate-600">
          Review extracted text, inspect confidence metadata, retry failed or low-confidence jobs, and approve
          teacher-controlled extraction output before any downstream AI usage.
        </p>
      </section>

      <section className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
        {dashboardCards.map((card) => (
          <article key={card.label} className="ta-panel p-5">
            <p className="text-sm font-semibold text-slate-500">{card.label}</p>
            <p className="mt-3 text-3xl font-semibold text-slate-900">{card.value}</p>
          </article>
        ))}
      </section>

      <ExtractionList
        summaries={summaries}
        onOpen={(id) => {
          router.push(`/teacher-assist/extractions?id=${id}`);
        }}
      />
    </div>
  );
}
