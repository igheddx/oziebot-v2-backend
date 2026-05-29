"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { fetchTeacherAssistWorkspace } from "@/lib/teacher-assist-api";
import type {
  TeacherAssistClassWorkspace,
  TeacherAssistWorkspace,
  TeacherAssistWorkspaceNeedsAttention,
} from "@/lib/teacher-assist-types";

const POLL_INTERVAL_MS = 60_000;
const SEVERITY_ORDER: Array<TeacherAssistWorkspaceNeedsAttention["severity"]> = [
  "critical",
  "warning",
  "info",
];

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

function severityClasses(severity: TeacherAssistWorkspaceNeedsAttention["severity"]) {
  if (severity === "critical") {
    return "border-rose-200 bg-rose-50 text-rose-900";
  }
  if (severity === "warning") {
    return "border-amber-200 bg-amber-50 text-amber-900";
  }
  return "border-sky-200 bg-sky-50 text-sky-900";
}

function itemLink(entityType: string, entityId?: string) {
  if (entityType === "weekly_plan") return "/teacher-assist/weekly-planning/plans";
  if (entityType === "assignment" || entityType === "grading_review" || entityType === "student_work_submission") {
    return "/teacher-assist/assignments";
  }
  if (entityType === "extracted_text" && entityId) {
    return `/teacher-assist/extractions?id=${entityId}`;
  }
  if (entityType === "extraction_job") return "/teacher-assist/extractions";
  if (entityType === "workflow") return "/teacher-assist/weekly-planning";
  return "/teacher-assist";
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

function ClassWorkspaceCard({ workspace }: { workspace: TeacherAssistClassWorkspace }) {
  return (
    <article className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm shadow-slate-950/5">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <h3 className="text-lg font-semibold text-slate-900">{workspace.class.name}</h3>
          <p className="mt-1 text-sm text-slate-600">
            Grade {workspace.class.grade_level} · {workspace.class.student_count} students
          </p>
        </div>
        <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-800">
          {workspace.needs_attention_count} attention item{workspace.needs_attention_count === 1 ? "" : "s"}
        </span>
      </div>

      <div className="mt-4 grid gap-3 sm:grid-cols-3">
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Plans</p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{workspace.active_plans.length}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Assignments</p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">{workspace.assignments.length}</p>
        </div>
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Pending reviews</p>
          <p className="mt-2 text-2xl font-semibold text-slate-900">
            {workspace.pending_grading_reviews.length}
          </p>
        </div>
      </div>

      <div className="mt-5 grid gap-4 lg:grid-cols-2">
        <div>
          <p className="text-sm font-semibold text-slate-900">Active plans</p>
          {workspace.active_plans.length === 0 ? (
            <p className="mt-2 text-sm text-slate-500">No active plans tied to this class yet.</p>
          ) : (
            <div className="mt-3 space-y-2">
              {workspace.active_plans.slice(0, 3).map((plan) => (
                <div key={plan.id} className="rounded-2xl border border-slate-200 p-3">
                  <p className="text-sm font-semibold text-slate-900">{plan.title}</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {labelize(plan.planning_scope)} · {labelize(plan.status)} · Updated {formatDateTime(plan.updated_at)}
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>

        <div>
          <p className="text-sm font-semibold text-slate-900">Recent submissions</p>
          {workspace.recent_submissions.length === 0 ? (
            <p className="mt-2 text-sm text-slate-500">No recent uploads for this class yet.</p>
          ) : (
            <div className="mt-3 space-y-2">
              {workspace.recent_submissions.slice(0, 3).map((submission) => (
                <div key={submission.id} className="rounded-2xl border border-slate-200 p-3">
                  <p className="text-sm font-semibold text-slate-900">
                    STUDENT #{submission.student_number}
                  </p>
                  <p className="mt-1 text-sm text-slate-600">{submission.original_filename}</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {labelize(submission.processing_status)}
                    {submission.latest_extraction_status
                      ? ` · Extraction ${labelize(submission.latest_extraction_status)}`
                      : " · Extraction Not Started"}
                    {submission.extraction_ready_for_teacher_review ? " · Teacher review ready" : ""}
                    {" · "}
                    {formatDateTime(submission.updated_at)}
                  </p>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="mt-5 flex flex-wrap gap-3">
        <Link href="/teacher-assist/weekly-planning/plans" className="ta-button-secondary">
          Open Plans
        </Link>
        <Link href="/teacher-assist/assignments" className="ta-button-primary">
          Open Assignments
        </Link>
      </div>
    </article>
  );
}

export function TeacherAssistWorkspaceScreen() {
  const [workspace, setWorkspace] = useState<TeacherAssistWorkspace | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const nextWorkspace = await fetchTeacherAssistWorkspace();
      setWorkspace(nextWorkspace);
      setError(null);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load TeacherAssist workspace.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const interval = window.setInterval(() => {
      void load();
    }, POLL_INTERVAL_MS);
    return () => window.clearInterval(interval);
  }, [load]);

  const needsAttentionBySeverity = useMemo(() => {
    const grouped: Record<TeacherAssistWorkspaceNeedsAttention["severity"], TeacherAssistWorkspaceNeedsAttention[]> = {
      critical: [],
      warning: [],
      info: [],
    };
    (workspace?.needs_attention ?? []).forEach((item) => {
      grouped[item.severity].push(item);
    });
    return grouped;
  }, [workspace]);

  const emptyWorkspace =
    !loading &&
    !error &&
    workspace &&
    workspace.class_workspaces.length === 0 &&
    workspace.needs_attention.length === 0 &&
    workspace.recent_activity.length === 0;

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Workspace
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            What needs your attention this week
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            This workspace brings plans, assignments, workflows, grading reviews, printable packets,
            student-work submissions, and recent activity into one teacher-centered operational view.
          </p>
          <div className="mt-4 flex flex-wrap gap-2">
            <span className="rounded-full bg-slate-100 px-3 py-1 text-sm font-medium text-slate-700">
              School Year: {workspace?.current_school_year?.title ?? "Loading..."}
            </span>
            <span className="rounded-full bg-sky-50 px-3 py-1 text-sm font-medium text-sky-800">
              Grading Period: {workspace?.active_grading_period?.title ?? "Not set"}
            </span>
          </div>
        </div>
      </section>

      <section className="ta-alert ta-alert-info">
        The workspace is backend-composed and remains software-first. OCR, AI grading, mastery
        commit, parent communication, and external notifications stay disabled.
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}

      {workspace?.mastery_insights ? (
        <section className="ta-panel p-6">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Mastery visibility</h2>
              <p className="mt-1 text-sm text-slate-600">
                Read-only committed mastery analytics. No draft evaluations or automated changes.
              </p>
            </div>
            <Link href="/teacher-assist/today" className="ta-button-secondary">
              Open today workspace
            </Link>
            <Link href="/teacher-assist/mastery" className="ta-button-secondary">
              Open mastery workspace
            </Link>
          </div>
          <div className="mt-4 grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <SummaryCard
              label="Reteach recommended"
              value={workspace.mastery_insights.reteach_recommended_count}
              detail="Standards below configured mastery thresholds."
            />
            <SummaryCard
              label="Low mastery alerts"
              value={workspace.mastery_insights.low_mastery_alert_count}
              detail="Critical attention standards across matrices."
            />
            <SummaryCard
              label="Improving standards"
              value={workspace.mastery_insights.improving_standard_count}
              detail="Deterministic recent trend improvement."
            />
            <SummaryCard
              label="Unassessed standards"
              value={workspace.mastery_insights.unassessed_standard_count}
              detail="Tracked standards without committed evaluations."
            />
          </div>
        </section>
      ) : null}

      <section className="grid gap-4 lg:grid-cols-3 xl:grid-cols-3">
        <SummaryCard
          label="Active workflows"
          value={loading ? "..." : workspace?.today_summary.active_workflows_count ?? 0}
          detail="Queued or running TeacherAssist jobs."
        />
        <SummaryCard
          label="Plans needing review"
          value={loading ? "..." : workspace?.today_summary.plans_needing_review_count ?? 0}
          detail="Plans still waiting on teacher review."
        />
        <SummaryCard
          label="Pending grading reviews"
          value={loading ? "..." : workspace?.today_summary.grading_reviews_pending_confirmation_count ?? 0}
          detail="Manual grading reviews still awaiting confirmation."
        />
        <SummaryCard
          label="Recent uploads"
          value={loading ? "..." : workspace?.today_summary.recent_uploads_count ?? 0}
          detail="Anonymous student-work uploads in the recent window."
        />
        <SummaryCard
          label="Workflow failures"
          value={loading ? "..." : workspace?.today_summary.workflow_failures_count ?? 0}
          detail="Failures that need teacher or operator attention."
        />
        <SummaryCard
          label="Assignments in review"
          value={loading ? "..." : workspace?.workspace_stats.assignments_in_review_count ?? 0}
          detail="Assignments currently in collected/review flow."
        />
        <SummaryCard
          label="Extraction failures"
          value={loading ? "..." : workspace?.today_summary.extraction_failures_count ?? 0}
          detail="Extraction jobs that need retry or manual follow-up."
        />
        <SummaryCard
          label="Ready for extraction"
          value={loading ? "..." : workspace?.today_summary.student_work_ready_for_extraction_count ?? 0}
          detail="Student-work uploads still waiting on extraction."
        />
        <SummaryCard
          label="Extraction review ready"
          value={
            loading ? "..." : workspace?.today_summary.extracted_artifacts_ready_for_teacher_review_count ?? 0
          }
          detail="Extracted artifacts ready for teacher review."
        />
        <SummaryCard
          label="Awaiting extraction review"
          value={loading ? "..." : workspace?.today_summary.awaiting_teacher_review_count ?? 0}
          detail="Extractions waiting on teacher approval."
        />
        <SummaryCard
          label="Low confidence extractions"
          value={loading ? "..." : workspace?.today_summary.low_confidence_extractions_count ?? 0}
          detail="Extracted text flagged with low provider confidence."
        />
        <SummaryCard
          label="Retry required"
          value={loading ? "..." : workspace?.today_summary.retry_required_extractions_count ?? 0}
          detail="Rejected extractions that need remediation."
        />
        <SummaryCard
          label="Stale extraction jobs"
          value={loading ? "..." : workspace?.today_summary.stale_extraction_jobs_count ?? 0}
          detail="Running jobs with expired leases or stale heartbeats."
        />
        <SummaryCard
          label="Recently approved"
          value={loading ? "..." : workspace?.today_summary.recently_approved_extractions_count ?? 0}
          detail="Teacher-approved extractions in the recent window."
        />
      </section>

      <section className="ta-panel flex flex-col gap-4 p-6 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">Operational action workspace</h2>
          <p className="mt-1 text-sm text-slate-600">
            Open the unified command center for failed jobs, review queues, gradebook-ready work, and exports.
          </p>
        </div>
        <Link href="/teacher-assist/actions" className="ta-button-primary shrink-0">
          Open Actions Workspace
        </Link>
      </section>

      <section className="flex flex-wrap gap-3">
        <Link href="/teacher-assist/extractions" className="ta-button-secondary">
          Open Extractions Workspace
        </Link>
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Needs attention</h2>
              <p className="mt-1 text-sm text-slate-600">
                Operational issues are grouped by severity so you can resolve blockers first.
              </p>
            </div>
            <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
              {workspace?.needs_attention.length ?? 0} open item{workspace?.needs_attention.length === 1 ? "" : "s"}
            </span>
          </div>

          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading workspace alerts...</p>
          ) : workspace && workspace.needs_attention.length === 0 ? (
            <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Nothing urgent is waiting right now.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
              {SEVERITY_ORDER.map((severity) => {
                const items = needsAttentionBySeverity[severity];
                if (items.length === 0) return null;
                return (
                  <div key={severity}>
                    <p className="text-sm font-semibold uppercase tracking-wide text-slate-500">
                      {labelize(severity)}
                    </p>
                    <div className="mt-3 space-y-3">
                      {items.map((item) => (
                        <Link
                          key={`${item.entity_type}-${item.entity_id}-${item.type}`}
                          href={itemLink(item.entity_type, item.entity_id)}
                          className={`block rounded-2xl border p-4 transition hover:shadow-sm ${severityClasses(item.severity)}`}
                        >
                          <p className="text-sm font-semibold">{item.title}</p>
                          <p className="mt-2 text-sm leading-6">{item.message}</p>
                          <p className="mt-2 text-xs opacity-80">
                            {labelize(item.entity_type)} · {formatDateTime(item.created_at)}
                          </p>
                        </Link>
                      ))}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Review queue</h2>
            <p className="mt-1 text-sm text-slate-600">
              Plans, grading reviews, and extracted work that still need teacher confirmation.
            </p>
          </div>

          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading review queue...</p>
          ) : workspace && workspace.review_required_items.length === 0 ? (
            <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              No pending review items right now.
            </div>
          ) : (
            <div className="mt-5 space-y-3">
              {(workspace?.review_required_items ?? []).slice(0, 8).map((item) => (
                <Link
                  key={`${item.entity_type}-${item.entity_id}`}
                  href={itemLink(item.entity_type, item.entity_id)}
                  className="block rounded-2xl border border-slate-200 bg-white p-4 transition hover:border-sky-300 hover:bg-sky-50/40"
                >
                  <p className="text-sm font-semibold text-slate-900">{item.title}</p>
                  <p className="mt-2 text-sm text-slate-600">{item.review_reason}</p>
                  <p className="mt-2 text-xs text-slate-500">
                    {labelize(item.status)} · Updated {formatDateTime(item.updated_at)}
                  </p>
                </Link>
              ))}
            </div>
          )}

          <div className="mt-6">
            <h3 className="text-base font-semibold text-slate-900">Active workflows</h3>
            {loading ? (
              <p className="mt-3 text-sm text-slate-500">Loading workflows...</p>
            ) : workspace && workspace.active_workflows.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">No active workflows right now.</p>
            ) : (
              <div className="mt-3 space-y-3">
                {(workspace?.active_workflows ?? []).slice(0, 5).map((workflow) => (
                  <div key={workflow.id} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-sm font-semibold text-slate-900">
                        {labelize(workflow.workflow_type)}
                      </span>
                      <span className="rounded-full bg-white px-2.5 py-1 text-xs font-semibold text-slate-700">
                        {labelize(workflow.status)}
                      </span>
                    </div>
                    <p className="mt-2 text-sm text-slate-600">
                      Progress {workflow.progress_percent}% · Retry {workflow.retry_count}/{workflow.max_retries}
                    </p>
                    <p className="mt-1 text-xs text-slate-500">
                      Provider {workflow.provider_name ?? "none"} · Updated {formatDateTime(workflow.updated_at)}
                    </p>
                  </div>
                ))}
              </div>
            )}
          </div>
        </article>
      </section>

      <section className="ta-panel p-6">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Class workspaces</h2>
            <p className="mt-1 text-sm text-slate-600">
              Operational context is grouped by class so teachers can scan work where it actually happens.
            </p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Link href="/teacher-assist/weekly-planning" className="ta-button-secondary">
              Weekly Planning
            </Link>
            <Link href="/teacher-assist/assignments" className="ta-button-primary">
              Assignments
            </Link>
          </div>
        </div>

        {loading ? (
          <p className="mt-4 text-sm text-slate-500">Loading class workspaces...</p>
        ) : workspace && workspace.class_workspaces.length === 0 ? (
          <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
            No class-scoped TeacherAssist activity has been created yet.
          </div>
        ) : (
          <div className="mt-5 grid gap-4 xl:grid-cols-2">
            {(workspace?.class_workspaces ?? []).map((classWorkspace) => (
              <ClassWorkspaceCard key={classWorkspace.class.id} workspace={classWorkspace} />
            ))}
          </div>
        )}
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Recent activity</h2>
          <p className="mt-1 text-sm text-slate-600">
            A timeline of the latest TeacherAssist actions recorded for this teacher.
          </p>

          {loading ? (
            <p className="mt-4 text-sm text-slate-500">Loading recent activity...</p>
          ) : workspace && workspace.recent_activity.length === 0 ? (
            <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Activity events will appear here as you generate plans, upload work, and review items.
            </div>
          ) : (
            <div className="mt-5 space-y-4">
              {(workspace?.recent_activity ?? []).map((event) => (
                <div key={event.id} className="flex gap-3">
                  <div className="mt-1 h-3 w-3 rounded-full bg-sky-500" />
                  <div className="min-w-0 flex-1 rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-sm font-semibold text-slate-900">{event.summary_text}</span>
                      <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                        {labelize(event.event_type)}
                      </span>
                    </div>
                    <p className="mt-2 text-xs text-slate-500">
                      {labelize(event.event_category)} · {labelize(event.entity_type)} · {formatDateTime(event.timestamp)}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Workspace stats</h2>
          <p className="mt-1 text-sm text-slate-600">
            Fast counts for the current operational footprint.
          </p>

          <div className="mt-5 grid gap-3">
            {[
              ["Active plans", workspace?.workspace_stats.active_plans_count ?? 0],
              ["Plans in review", workspace?.workspace_stats.plans_in_review_count ?? 0],
              ["Pending grading reviews", workspace?.workspace_stats.pending_grading_reviews_count ?? 0],
              ["Recent uploads", workspace?.workspace_stats.recent_upload_count ?? 0],
              ["Workflow failures", workspace?.workspace_stats.workflow_failure_count ?? 0],
              ["Assignments in review", workspace?.workspace_stats.assignments_in_review_count ?? 0],
              ["Extraction failures", workspace?.workspace_stats.extraction_failure_count ?? 0],
              ["Ready for extraction", workspace?.workspace_stats.student_work_ready_for_extraction_count ?? 0],
              [
                "Extraction review ready",
                workspace?.workspace_stats.extracted_artifacts_ready_for_teacher_review_count ?? 0,
              ],
            ].map(([label, value]) => (
              <div
                key={label}
                className="flex items-center justify-between rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3"
              >
                <span className="text-sm font-medium text-slate-700">{label}</span>
                <span className="text-lg font-semibold text-slate-900">{loading ? "..." : value}</span>
              </div>
            ))}
          </div>
        </article>
      </section>

      {emptyWorkspace ? (
        <section className="ta-panel p-6 text-sm text-slate-600">
          The unified workspace is ready, but this tenant does not have TeacherAssist plans,
          assignments, student-work uploads, workflows, or review activity yet.
        </section>
      ) : null}
    </div>
  );
}
