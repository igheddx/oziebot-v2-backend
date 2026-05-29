"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import {
  createWeeklyPlanExport,
  fetchWeeklyPlan,
  fetchWeeklyPlanVersions,
  regenerateWeeklyPlanSection,
  updateWeeklyPlan,
} from "@/lib/teacher-assist-api";
import type {
  TeacherAssistExportArtifactType,
  WeeklyPlan,
  WeeklyPlanContent,
  WeeklyPlanSectionKey,
  WeeklyPlanVersion,
} from "@/lib/teacher-assist-types";

function statusLabel(status: WeeklyPlan["status"]) {
  return status === "completed" ? "Completed" : "In Progress";
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatCurrencyCents(value: number | null | undefined) {
  return `$${((value ?? 0) / 100).toFixed(2)}`;
}

function formatPlanningScope(value: string | null | undefined) {
  return (value ?? "weekly").replaceAll("_", " ");
}

function readPlanContent(plan: WeeklyPlan | null): WeeklyPlanContent {
  return plan?.content_json ?? {};
}

function splitLines(value: string) {
  return value
    .split("\n")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

export function TeacherAssistWeeklyPlanViewer() {
  const searchParams = useSearchParams();
  const planId = searchParams.get("id");

  const [plan, setPlan] = useState<WeeklyPlan | null>(null);
  const [versions, setVersions] = useState<WeeklyPlanVersion[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);
  const [title, setTitle] = useState("");
  const [overview, setOverview] = useState("");
  const [weeklyObjectivesText, setWeeklyObjectivesText] = useState("");
  const [reviewNotes, setReviewNotes] = useState("");
  const [status, setStatus] = useState<WeeklyPlan["status"]>("in_progress");
  const [changeReason, setChangeReason] = useState("");
  const [regenOpen, setRegenOpen] = useState(false);
  const [regenSectionKey, setRegenSectionKey] = useState<WeeklyPlanSectionKey>("overview");
  const [regenSectionPath, setRegenSectionPath] = useState<string | null>(null);
  const [regenLabel, setRegenLabel] = useState("Overview");
  const [regenInstruction, setRegenInstruction] = useState("");
  const [regenProviderMode, setRegenProviderMode] = useState<"mock" | "real" | "">("");
  const [regenPreserveExistingContext, setRegenPreserveExistingContext] = useState(true);
  const [regenerating, setRegenerating] = useState(false);
  const [exportingType, setExportingType] = useState<TeacherAssistExportArtifactType | null>(null);
  const [exportMessage, setExportMessage] = useState<string | null>(null);

  useEffect(() => {
    if (!plan) return;
    const content = readPlanContent(plan);
    setTitle(plan.title);
    setOverview(content.overview ?? "");
    setWeeklyObjectivesText((content.weekly_objectives ?? []).join("\n"));
    setReviewNotes(content.review_notes ?? "");
    setStatus(plan.status);
  }, [plan]);

  useEffect(() => {
    if (!planId) {
      setLoading(false);
      setError("Weekly plan id is required.");
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    void Promise.all([fetchWeeklyPlan(planId), fetchWeeklyPlanVersions(planId)])
      .then(([planResult, versionResults]) => {
        if (cancelled) return;
        setPlan(planResult);
        setVersions(versionResults);
      })
      .catch((nextError) => {
        if (cancelled) return;
        setError(nextError instanceof Error ? nextError.message : "Could not load weekly plan.");
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [planId]);

  const content = useMemo(() => readPlanContent(plan), [plan]);
  const usageEvent = plan?.latest_usage_event ?? null;
  const providerMode = content.metadata?.provider_mode ?? (content.metadata?.is_mock ? "mock" : "real");
  const providerModel = content.metadata?.provider_model ?? usageEvent?.model ?? "mock";
  const promptVersion = content.metadata?.prompt_version ?? "instructional-plan-v2";

  function openRegeneration(
    sectionKey: WeeklyPlanSectionKey,
    label: string,
    sectionPath?: string | null,
  ) {
    setRegenSectionKey(sectionKey);
    setRegenSectionPath(sectionPath ?? null);
    setRegenLabel(label);
    setRegenInstruction("");
    setRegenProviderMode("");
    setRegenPreserveExistingContext(true);
    setRegenOpen(true);
  }

  async function handleSave(nextStatus?: WeeklyPlan["status"]) {
    if (!plan) return;
    setSaving(true);
    setError(null);
    setSaveMessage(null);
    try {
      const nextPlan = await updateWeeklyPlan(plan.id, {
        title: title.trim() || plan.title,
        status: nextStatus ?? status,
        content_json: {
          ...content,
          overview,
          weekly_objectives: splitLines(weeklyObjectivesText),
          review_notes: reviewNotes,
        },
        change_reason:
          changeReason.trim() ||
          (nextStatus === "completed" ? "Teacher marked plan completed." : "Teacher review update."),
      });
      const nextVersions = await fetchWeeklyPlanVersions(plan.id);
      setPlan(nextPlan);
      setVersions(nextVersions);
      setStatus(nextPlan.status);
      setChangeReason("");
      setSaveMessage(
        nextPlan.status === "completed"
          ? "Weekly plan saved and marked completed."
          : "Weekly plan review updates saved.",
      );
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not save weekly plan.");
    } finally {
      setSaving(false);
    }
  }

  async function handleRegenerateSection() {
    if (!plan) return;
    setRegenerating(true);
    setError(null);
    setSaveMessage(null);
    try {
      const nextPlan = await regenerateWeeklyPlanSection(plan.id, {
        section_key: regenSectionKey,
        section_path: regenSectionPath,
        teacher_instruction: regenInstruction.trim() || null,
        provider_mode: regenProviderMode || null,
        preserve_existing_context: regenPreserveExistingContext,
      });
      const nextVersions = await fetchWeeklyPlanVersions(plan.id);
      setPlan(nextPlan);
      setVersions(nextVersions);
      setStatus(nextPlan.status);
      setChangeReason("");
      setSaveMessage(`${regenLabel} regenerated as a new plan version.`);
      setRegenOpen(false);
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not regenerate this section.");
    } finally {
      setRegenerating(false);
    }
  }

  async function handleCreateExport(artifactType: TeacherAssistExportArtifactType) {
    if (!plan) return;
    setExportingType(artifactType);
    setError(null);
    setExportMessage(null);
    try {
      const created = await createWeeklyPlanExport(plan.id, {
        artifact_type: artifactType,
        provider_mode: "mock",
      });
      setExportMessage(
        `Queued ${artifactType.replaceAll("_", " ")} export. Track progress in Exports while the worker generates the file.`,
      );
      void created;
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not queue export.");
    } finally {
      setExportingType(null);
    }
  }

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="flex flex-wrap items-start justify-between gap-4">
          <div className="max-w-4xl">
            <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
              TeacherAssist Instructional Plan
            </p>
            <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
              {plan?.title ?? "Instructional plan viewer"}
            </h1>
            <p className="mt-3 text-base leading-7 text-slate-600">
              Review, refine, and version the persisted instructional-plan artifact before classroom use.
            </p>
          </div>
          <div className="flex items-center gap-3">
            <span
              className={`rounded-full px-3 py-1 text-xs font-semibold ${
                plan?.status === "completed"
                  ? "bg-emerald-100 text-emerald-700"
                  : "bg-sky-100 text-sky-700"
              }`}
            >
              {plan ? statusLabel(plan.status) : "Loading"}
            </span>
            <Link href="/teacher-assist/weekly-planning" className="ta-button-secondary">
              Back to Instructional Planning
            </Link>
          </div>
        </div>
      </section>

      {content.review_required ? (
        <section className="ta-alert border-amber-200 bg-amber-50 text-amber-900">
          Teacher review is required before classroom use. Generated plans stay <strong>In Progress</strong>{" "}
          until you finish review and mark them completed.
        </section>
      ) : (
        <section className="ta-alert ta-alert-info">
          Review and edit the instructional plan before classroom use.
        </section>
      )}

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {saveMessage ? <section className="ta-alert ta-alert-success">{saveMessage}</section> : null}
      {exportMessage ? <section className="ta-alert ta-alert-success">{exportMessage}</section> : null}

      {loading ? (
        <section className="ta-panel p-6">
          <p className="text-sm text-slate-600">Loading weekly plan...</p>
        </section>
      ) : plan ? (
        <>
          <section className="ta-panel p-6">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">Instructional exports</h2>
                <p className="mt-2 text-sm text-slate-600">
                  Queue async slide and quiz exports from this plan. PPTX downloads can be opened in PowerPoint
                  or imported manually into Google Slides — no Google OAuth in this phase.
                </p>
              </div>
              <Link href="/teacher-assist/exports" className="ta-button-secondary">
                Open export workspace
              </Link>
            </div>
            <div className="mt-4 flex flex-wrap gap-3">
              {(
                [
                  ["lesson_slides", "Generate Slides"],
                  ["guided_notes", "Generate Guided Notes"],
                  ["multiple_choice_quiz", "Generate Quiz"],
                  ["exit_ticket", "Generate Exit Ticket"],
                ] as const
              ).map(([artifactType, label]) => (
                <button
                  key={artifactType}
                  type="button"
                  className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  disabled={exportingType !== null}
                  onClick={() => void handleCreateExport(artifactType)}
                >
                  {exportingType === artifactType ? "Queueing..." : label}
                </button>
              ))}
            </div>
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.25fr_0.75fr]">
            <article className="ta-panel p-6">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <h2 className="text-xl font-semibold text-slate-900">Teacher review</h2>
                  <p className="mt-2 text-sm text-slate-600">
                    Save teacher-facing edits without overwriting prior versions.
                  </p>
                </div>
                <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                  Version {plan.current_version_number}
                </span>
              </div>
              <div className="mt-5 grid gap-4">
                <label className="grid gap-2">
                  <span className="text-sm font-semibold text-slate-900">Plan title</span>
                  <input
                    value={title}
                    onChange={(event) => setTitle(event.target.value)}
                    className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                  />
                </label>
                <label className="grid gap-2">
                  <span className="text-sm font-semibold text-slate-900">Overview</span>
                  <textarea
                    rows={5}
                    value={overview}
                    onChange={(event) => setOverview(event.target.value)}
                    className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                  />
                </label>
                <label className="grid gap-2">
                  <span className="text-sm font-semibold text-slate-900">
                    Weekly objectives (one per line)
                  </span>
                  <textarea
                    rows={5}
                    value={weeklyObjectivesText}
                    onChange={(event) => setWeeklyObjectivesText(event.target.value)}
                    className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                  />
                </label>
                <label className="grid gap-2">
                  <span className="text-sm font-semibold text-slate-900">Review notes</span>
                  <textarea
                    rows={4}
                    value={reviewNotes}
                    onChange={(event) => setReviewNotes(event.target.value)}
                    className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                  />
                </label>
                <div className="grid gap-4 md:grid-cols-2">
                  <label className="grid gap-2">
                    <span className="text-sm font-semibold text-slate-900">Plan status</span>
                    <select
                      value={status}
                      onChange={(event) => setStatus(event.target.value as WeeklyPlan["status"])}
                      className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                    >
                      <option value="in_progress">In Progress</option>
                      <option value="completed">Completed</option>
                    </select>
                  </label>
                  <label className="grid gap-2">
                    <span className="text-sm font-semibold text-slate-900">Change reason</span>
                    <input
                      value={changeReason}
                      onChange={(event) => setChangeReason(event.target.value)}
                      placeholder="Teacher review pass"
                      className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                    />
                  </label>
                </div>
                <div className="flex flex-wrap gap-3">
                  <button
                    type="button"
                    onClick={() => void handleSave()}
                    disabled={saving}
                    className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {saving ? "Saving..." : "Save review changes"}
                  </button>
                  <button
                    type="button"
                    onClick={() => void handleSave("completed")}
                    disabled={saving}
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Mark plan completed
                  </button>
                </div>
              </div>
            </article>

            <div className="space-y-6">
              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">AI usage</h2>
                <dl className="mt-4 space-y-3 text-sm text-slate-600">
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Provider mode</dt>
                    <dd>{providerMode}</dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Provider</dt>
                    <dd>{usageEvent?.provider ?? content.metadata?.generator ?? "mock"}</dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Model</dt>
                    <dd>{providerModel}</dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Prompt version</dt>
                    <dd>{promptVersion}</dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Estimated cost</dt>
                    <dd>{formatCurrencyCents(usageEvent?.estimated_cost_cents)}</dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Tokens</dt>
                    <dd>
                      {usageEvent?.input_tokens ?? 0} input / {usageEvent?.output_tokens ?? 0} output
                    </dd>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <dt className="font-semibold text-slate-900">Generated</dt>
                    <dd>{formatDateTime(content.metadata?.generated_at)}</dd>
                  </div>
                </dl>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">Version history</h2>
                <div className="mt-4 space-y-3">
                  {versions.length > 0 ? (
                    versions.map((version) => (
                      <div
                        key={version.id}
                        className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3"
                      >
                        <p className="text-sm font-semibold text-slate-900">
                          Version {version.version_number}
                        </p>
                        <p className="mt-1 text-xs text-slate-500">
                          {formatDateTime(version.created_at)}
                        </p>
                        <p className="mt-2 text-sm text-slate-600">
                          {version.change_reason || "Saved version snapshot"}
                        </p>
                      </div>
                    ))
                  ) : (
                    <p className="text-sm text-slate-500">No saved versions yet.</p>
                  )}
                </div>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">Quality review</h2>
                <div className="mt-4 space-y-4">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Standards alignment summary</p>
                    <p className="mt-2 text-sm leading-7 text-slate-600">
                      {content.standards_alignment_summary ||
                        "Standards alignment summary was not captured for this plan."}
                    </p>
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Quality flags</p>
                    <div className="mt-3 flex flex-wrap gap-2">
                      {(content.quality_flags ?? []).length > 0 ? (
                        content.quality_flags?.map((flag) => (
                          <span
                            key={flag}
                            className="rounded-full bg-amber-100 px-3 py-1 text-xs font-semibold text-amber-800"
                          >
                            {flag}
                          </span>
                        ))
                      ) : (
                        <span className="text-sm text-slate-500">No quality flags were added.</span>
                      )}
                    </div>
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Missing context warnings</p>
                    {(content.missing_context_warnings ?? []).length > 0 ? (
                      <ul className="mt-3 space-y-2 text-sm text-slate-600">
                        {content.missing_context_warnings?.map((warning) => (
                          <li key={warning}>{warning}</li>
                        ))}
                      </ul>
                    ) : (
                      <p className="mt-2 text-sm text-slate-500">
                        No missing-context warnings were recorded for this plan.
                      </p>
                    )}
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Teacher review checklist</p>
                    <ul className="mt-3 space-y-2 text-sm text-slate-600">
                      {(content.teacher_review_checklist ?? []).map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  </div>
                </div>
              </article>
            </div>
          </section>

          <section className="ta-panel p-6">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <h2 className="text-xl font-semibold text-slate-900">Generated overview</h2>
              <button
                type="button"
                onClick={() => openRegeneration("overview", "Overview")}
                className="ta-button-secondary"
              >
                Regenerate Overview
              </button>
            </div>
            <p className="mt-4 text-sm leading-7 text-slate-700">
              {content.overview ?? "No mock overview was stored for this instructional plan."}
            </p>
          </section>

          <section className="ta-panel p-6">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <h2 className="text-xl font-semibold text-slate-900">Instructional arc</h2>
              <button
                type="button"
                onClick={() => openRegeneration("instructional_arc", "Instructional Arc")}
                className="ta-button-secondary"
              >
                Regenerate Arc
              </button>
            </div>
            <ul className="mt-4 space-y-2 text-sm text-slate-700">
              {(content.instructional_arc ?? []).map((item, index) => (
                <li key={`${item}-${index}`}>{item}</li>
              ))}
            </ul>
          </section>

          <section className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Weekly objectives</h2>
            <ul className="mt-4 space-y-2 text-sm text-slate-700">
              {(content.weekly_objectives ?? []).map((objective) => (
                <li key={objective}>{objective}</li>
              ))}
            </ul>
          </section>

          <section className="ta-panel p-6">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <h2 className="text-xl font-semibold text-slate-900">Weekly segments</h2>
              <button
                type="button"
                onClick={() => openRegeneration("weekly_segments", "Weekly Segments")}
                className="ta-button-secondary"
              >
                Regenerate All Segments
              </button>
            </div>
            {(content.weekly_segments ?? []).length > 0 ? (
              <div className="mt-4 space-y-3">
                {content.weekly_segments?.map((segment, index) => (
                  <div
                    key={`${segment.segment_label ?? "segment"}-${index}`}
                    className="rounded-2xl border border-slate-200 bg-slate-50 p-4"
                  >
                    <div className="flex flex-wrap items-start justify-between gap-3">
                      <div>
                        <p className="text-sm font-semibold text-slate-900">
                          {segment.segment_label ?? `Week ${index + 1}`}
                        </p>
                        <p className="mt-1 text-sm text-slate-600">
                          {segment.focus ?? "No segment focus stored."}
                        </p>
                      </div>
                      <button
                        type="button"
                        onClick={() =>
                          openRegeneration(
                            "weekly_segments",
                            segment.segment_label ?? `Week ${index + 1}`,
                            `weekly_segments.${index}`,
                          )
                        }
                        className="ta-button-secondary"
                      >
                        Regenerate Segment
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            ) : (
              <p className="mt-4 text-sm text-slate-500">No weekly segments were stored for this plan.</p>
            )}
          </section>

          <section className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Plan metadata</h2>
            <dl className="mt-4 grid gap-4 text-sm text-slate-600 md:grid-cols-2">
              <div>
                <dt className="font-semibold text-slate-900">Planning scope</dt>
                <dd className="mt-1">{formatPlanningScope(content.planning_scope ?? plan.planning_scope)}</dd>
              </div>
              <div>
                <dt className="font-semibold text-slate-900">Duration</dt>
                <dd className="mt-1">{content.duration?.summary ?? "No duration summary recorded."}</dd>
              </div>
            </dl>
          </section>

          <section className="grid gap-6 lg:grid-cols-[1.1fr_0.9fr]">
            <article className="ta-panel p-6">
              <h2 className="text-xl font-semibold text-slate-900">Subject sections</h2>
              <div className="mt-5 space-y-5">
                {(content.subjects ?? []).length > 0 ? (
                  content.subjects?.map((subject, index) => (
                    <section
                      key={`${subject.subject_id ?? subject.subject_name ?? "subject"}-${index}`}
                      className="rounded-2xl border border-slate-200 p-5"
                    >
                      <h3 className="text-lg font-semibold text-slate-900">
                        {subject.subject_name ?? "Subject"}
                      </h3>

                      <div className="mt-4">
                        <p className="text-sm font-semibold text-slate-900">Objectives</p>
                        <ul className="mt-2 space-y-2 text-sm text-slate-600">
                          {(subject.objectives ?? []).map((objective) => (
                            <li key={objective}>{objective}</li>
                          ))}
                        </ul>
                      </div>

                      <div className="mt-4">
                        <p className="text-sm font-semibold text-slate-900">Standards</p>
                        <ul className="mt-2 space-y-2 text-sm text-slate-600">
                          {(subject.standards ?? []).map((standard, standardIndex) => (
                            <li key={`${standard.code ?? "standard"}-${standardIndex}`}>
                              <span className="font-medium text-slate-800">
                                {standard.code ?? "Standard"}
                              </span>
                              {standard.description ? ` - ${standard.description}` : ""}
                            </li>
                          ))}
                        </ul>
                      </div>

                      <div className="mt-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <p className="text-sm font-semibold text-slate-900">Vocabulary</p>
                          <button
                            type="button"
                            onClick={() => openRegeneration("vocabulary", "Vocabulary")}
                            className="ta-button-secondary"
                          >
                            Regenerate
                          </button>
                        </div>
                        <ul className="mt-2 space-y-2 text-sm text-slate-600">
                          {(subject.vocabulary ?? []).map((entry) => (
                            <li key={entry}>{entry}</li>
                          ))}
                        </ul>
                      </div>

                      <div className="mt-4">
                        <p className="text-sm font-semibold text-slate-900">Daily breakdown</p>
                        <div className="mt-3 space-y-3">
                          {(subject.daily_breakdown ?? []).map((day, dayIndex) => (
                            <div
                              key={`${subject.subject_name ?? "subject"}-day-${day.day ?? dayIndex}`}
                              className="rounded-2xl border border-slate-100 bg-slate-50 p-4"
                            >
                              <p className="text-sm font-semibold text-slate-900">
                                {day.day_label ?? `Day ${day.day ?? dayIndex + 1}`}:{" "}
                                {day.focus ?? "Mock focus"}
                              </p>
                              <div className="mt-3 grid gap-4 md:grid-cols-3">
                                <div>
                                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                                    Teacher actions
                                  </p>
                                  <ul className="mt-2 space-y-2 text-sm text-slate-600">
                                    {(day.teacher_actions ?? []).map((entry) => (
                                      <li key={entry}>{entry}</li>
                                    ))}
                                  </ul>
                                </div>
                                <div>
                                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                                    Student activities
                                  </p>
                                  <ul className="mt-2 space-y-2 text-sm text-slate-600">
                                    {(day.student_activities ?? []).map((entry) => (
                                      <li key={entry}>{entry}</li>
                                    ))}
                                  </ul>
                                </div>
                                <div>
                                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                                    Checks for understanding
                                  </p>
                                  <ul className="mt-2 space-y-2 text-sm text-slate-600">
                                    {(day.checks_for_understanding ?? []).map((entry) => (
                                      <li key={entry}>{entry}</li>
                                    ))}
                                  </ul>
                                </div>
                              </div>
                              <div className="mt-4">
                                <button
                                  type="button"
                                  onClick={() =>
                                    openRegeneration(
                                      "daily_breakdown",
                                      `${subject.subject_name ?? "Subject"} ${day.day_label ?? `Day ${dayIndex + 1}`}`,
                                      `subjects.${index}.daily_breakdown.${dayIndex}`,
                                    )
                                  }
                                  className="ta-button-secondary"
                                >
                                  Regenerate This Day
                                </button>
                              </div>
                              <div className="mt-4">
                                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                                  Materials needed
                                </p>
                                <ul className="mt-2 space-y-2 text-sm text-slate-600">
                                  {(day.materials_needed ?? []).map((entry) => (
                                    <li key={entry}>{entry}</li>
                                  ))}
                                </ul>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>

                      <div className="mt-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <p className="text-sm font-semibold text-slate-900">Differentiation</p>
                          <button
                            type="button"
                            onClick={() => openRegeneration("differentiation", "Differentiation")}
                            className="ta-button-secondary"
                          >
                            Regenerate
                          </button>
                        </div>
                        <div className="mt-3 grid gap-4 md:grid-cols-3">
                          <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                              Support
                            </p>
                            <ul className="mt-2 space-y-2 text-sm text-slate-600">
                              {(subject.differentiation?.support ?? []).map((entry) => (
                                <li key={entry}>{entry}</li>
                              ))}
                            </ul>
                          </div>
                          <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                              Extension
                            </p>
                            <ul className="mt-2 space-y-2 text-sm text-slate-600">
                              {(subject.differentiation?.extension ?? []).map((entry) => (
                                <li key={entry}>{entry}</li>
                              ))}
                            </ul>
                          </div>
                          <div>
                            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                              Visual supports
                            </p>
                            <ul className="mt-2 space-y-2 text-sm text-slate-600">
                              {(subject.differentiation?.visual_supports ?? []).map((entry) => (
                                <li key={entry}>{entry}</li>
                              ))}
                            </ul>
                          </div>
                        </div>
                      </div>

                      <div className="mt-4">
                        <p className="text-sm font-semibold text-slate-900">Suggested artifacts</p>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {(subject.suggested_artifacts ?? []).map((artifact) => (
                            <span
                              key={artifact}
                              className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-700"
                            >
                              {artifact}
                            </span>
                          ))}
                        </div>
                      </div>

                    </section>
                  ))
                ) : (
                  <p className="text-sm text-slate-500">No subject sections were stored for this plan.</p>
                )}
              </div>
            </article>

            <div className="space-y-6">
              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">Resources used</h2>
                <div className="mt-4 flex flex-wrap gap-2">
                  {(content.resources_used ?? []).length > 0 ? (
                    content.resources_used?.map((resource, index) => (
                      <span
                        key={`${resource.id ?? resource.title ?? "resource"}-${index}`}
                        className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                      >
                        {resource.title ?? "Resource"}
                      </span>
                    ))
                  ) : (
                    <span className="text-sm text-slate-500">No resources were listed in the plan output.</span>
                  )}
                </div>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">Teacher notes used</h2>
                <p className="mt-4 text-sm leading-7 text-slate-600">
                  {content.teacher_notes_used || "No teacher notes were included in the saved context."}
                </p>
              </article>

              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <h2 className="text-xl font-semibold text-slate-900">Materials needed</h2>
                  <button
                    type="button"
                    onClick={() => openRegeneration("materials_needed", "Materials Needed")}
                    className="ta-button-secondary"
                  >
                    Regenerate
                  </button>
                </div>
                <ul className="mt-4 space-y-2 text-sm text-slate-600">
                  {(content.materials_needed ?? []).map((entry) => (
                    <li key={entry}>{entry}</li>
                  ))}
                </ul>
              </article>

              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <h2 className="text-xl font-semibold text-slate-900">Assessment checkpoints</h2>
                  <button
                    type="button"
                    onClick={() => openRegeneration("assessment_checkpoints", "Assessment Checkpoints")}
                    className="ta-button-secondary"
                  >
                    Regenerate
                  </button>
                </div>
                <ul className="mt-4 space-y-2 text-sm text-slate-600">
                  {(content.assessment_checkpoints ?? []).map((entry) => (
                    <li key={entry}>{entry}</li>
                  ))}
                </ul>
              </article>

              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <h2 className="text-xl font-semibold text-slate-900">Standards progression</h2>
                  <button
                    type="button"
                    onClick={() => openRegeneration("standards_progression", "Standards Progression")}
                    className="ta-button-secondary"
                  >
                    Regenerate
                  </button>
                </div>
                <ul className="mt-4 space-y-2 text-sm text-slate-600">
                  {(content.standards_progression ?? []).map((entry, index) => (
                    <li key={`${entry.code ?? "standards-progression"}-${index}`}>
                      <span className="font-medium text-slate-800">{entry.code ?? "Standard"}</span>
                      {entry.phase ? ` - ${entry.phase}` : ""}
                    </li>
                  ))}
                </ul>
              </article>

              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <h2 className="text-xl font-semibold text-slate-900">Review notes</h2>
                  <button
                    type="button"
                    onClick={() => openRegeneration("review_notes", "Review Notes")}
                    className="ta-button-secondary"
                  >
                    Regenerate
                  </button>
                </div>
                <p className="mt-4 text-sm leading-7 text-slate-600">
                  {content.review_notes || "No teacher review notes have been saved yet."}
                </p>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-xl font-semibold text-slate-900">Future artifact generation</h2>
                <p className="mt-2 text-sm text-slate-600">
                  These remain visible as placeholders only in Phase 10.
                </p>
                <div className="mt-4 flex flex-wrap gap-3">
                  <button type="button" disabled className="ta-button-secondary cursor-not-allowed opacity-60">
                    Generate Daily Deck
                  </button>
                  <button type="button" disabled className="ta-button-secondary cursor-not-allowed opacity-60">
                    Generate Quiz
                  </button>
                  <button type="button" disabled className="ta-button-secondary cursor-not-allowed opacity-60">
                    Generate Guided Notes
                  </button>
                </div>
              </article>
            </div>
          </section>
        </>
      ) : null}
      {regenOpen ? (
        <section className="fixed inset-0 z-30 flex items-center justify-center bg-slate-950/40 p-4">
          <div className="w-full max-w-2xl rounded-3xl bg-white p-6 shadow-2xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">Regenerate {regenLabel}</h2>
                <p className="mt-2 text-sm text-slate-600">
                  This creates a new version, keeps the plan in progress, and stays teacher-review-first.
                </p>
              </div>
              <button
                type="button"
                onClick={() => setRegenOpen(false)}
                className="text-sm font-semibold text-slate-500"
              >
                Close
              </button>
            </div>
            <label className="mt-6 grid gap-2">
              <span className="text-sm font-semibold text-slate-900">Teacher instruction</span>
              <textarea
                rows={5}
                value={regenInstruction}
                onChange={(event) => setRegenInstruction(event.target.value)}
                placeholder="Optional guidance for how this section should change."
                className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
              />
            </label>
            <div className="mt-4 grid gap-4 md:grid-cols-2">
              <label className="grid gap-2">
                <span className="text-sm font-semibold text-slate-900">Provider mode</span>
                <select
                  value={regenProviderMode}
                  onChange={(event) => setRegenProviderMode(event.target.value as "mock" | "real" | "")}
                  className="rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-400 focus:ring-2 focus:ring-sky-100"
                >
                  <option value="">Use default</option>
                  <option value="mock">Mock</option>
                  <option value="real">Real provider</option>
                </select>
              </label>
              <label className="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-700">
                <input
                  type="checkbox"
                  checked={regenPreserveExistingContext}
                  onChange={(event) => setRegenPreserveExistingContext(event.target.checked)}
                />
                Preserve existing section context
              </label>
            </div>
            <p className="mt-4 text-xs text-slate-500">
              Decks, quizzes, grading, QR, newsletters, and mastery artifacts remain disabled placeholders.
            </p>
            <div className="mt-6 flex justify-end gap-3">
              <button type="button" onClick={() => setRegenOpen(false)} className="ta-button-secondary">
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void handleRegenerateSection()}
                disabled={regenerating}
                className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
              >
                {regenerating ? "Regenerating..." : "Regenerate Section"}
              </button>
            </div>
          </div>
        </section>
      ) : null}
    </div>
  );
}
