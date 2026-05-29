"use client";

import Link from "next/link";
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
  createReteachPlanVersion,
  fetchReteachPlan,
  fetchReteachPlanVersions,
  fetchReteachPlans,
  generateReteachPlanAIDraft,
  updateReteachPlan,
} from "@/lib/teacher-assist-api";
import type { ReteachPlan, ReteachPlanContent, ReteachPlanVersion } from "@/lib/teacher-assist-types";

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

function ContentSection({ title, items }: { title: string; items: string[] | undefined }) {
  if (!items || items.length === 0) return null;
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4">
      <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-700">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </section>
  );
}

export function TeacherAssistReteachPlansScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const requestedPlanId = searchParams.get("id");
  const requestedMatrixId = searchParams.get("matrix_id");

  const [plans, setPlans] = useState<ReteachPlan[]>([]);
  const [selectedPlanId, setSelectedPlanId] = useState<string | null>(requestedPlanId);
  const [plan, setPlan] = useState<ReteachPlan | null>(null);
  const [versions, setVersions] = useState<ReteachPlanVersion[]>([]);
  const [selectedVersionId, setSelectedVersionId] = useState<string | null>(null);
  const [teacherInstructions, setTeacherInstructions] = useState("");
  const [changeReason, setChangeReason] = useState("");
  const [editContent, setEditContent] = useState<ReteachPlanContent | null>(null);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [pageError, setPageError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const selectedVersion = useMemo(
    () => versions.find((row) => row.id === selectedVersionId) ?? versions.at(-1) ?? null,
    [selectedVersionId, versions],
  );

  const loadPlans = useCallback(async () => {
    const rows = await fetchReteachPlans(
      requestedMatrixId ? { mastery_matrix_id: requestedMatrixId } : {},
    );
    setPlans(rows);
    if (!selectedPlanId && rows.length > 0) {
      setSelectedPlanId(rows[0].id);
    }
  }, [requestedMatrixId, selectedPlanId]);

  const loadPlanDetail = useCallback(async (planId: string) => {
    const [planRow, versionRows] = await Promise.all([
      fetchReteachPlan(planId),
      fetchReteachPlanVersions(planId),
    ]);
    setPlan(planRow);
    setVersions(versionRows);
    const current =
      versionRows.find((row) => row.id === planRow.current_version_id) ?? versionRows.at(-1) ?? null;
    setSelectedVersionId(current?.id ?? null);
    setEditContent(current?.content_json ?? null);
  }, []);

  useEffect(() => {
    loadPlans().catch((error: Error) => setPageError(error.message));
  }, [loadPlans]);

  useEffect(() => {
    if (requestedPlanId) {
      setSelectedPlanId(requestedPlanId);
    }
  }, [requestedPlanId]);

  useEffect(() => {
    if (!selectedPlanId) {
      setPlan(null);
      setVersions([]);
      setEditContent(null);
      return;
    }
    loadPlanDetail(selectedPlanId).catch((error: Error) => setPageError(error.message));
  }, [loadPlanDetail, selectedPlanId]);

  async function handleGenerateDraft() {
    if (!plan) return;
    setBusy(true);
    clearSectionAlert("reteachPlan");
    try {
      const payload = await generateReteachPlanAIDraft(plan.id, {
        provider_mode: "mock",
        teacher_instructions: teacherInstructions.trim() || undefined,
      });
      setPlan(payload.plan);
      await loadPlanDetail(plan.id);
      setSectionAlert("reteachPlan", sectionSuccess(payload.message, "AI draft generated"));
    } catch (error) {
      setSectionAlert(
        "reteachPlan",
        sectionError(error instanceof Error ? error.message : "Unable to generate AI draft.", "AI draft failed"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveTeacherVersion() {
    if (!plan || !editContent) return;
    setBusy(true);
    clearSectionAlert("reteachPlan");
    try {
      const saved = await createReteachPlanVersion(plan.id, {
        content_json: {
          ...editContent,
          teacher_review_required: true,
        },
        change_reason: changeReason.trim() || "Teacher reviewed reteach plan draft.",
      });
      await loadPlanDetail(plan.id);
      setSelectedVersionId(saved.id);
      setSectionAlert(
        "reteachPlan",
        sectionSuccess("Teacher-reviewed version saved. Nothing was published automatically.", "Version saved"),
      );
    } catch (error) {
      setSectionAlert(
        "reteachPlan",
        sectionError(error instanceof Error ? error.message : "Unable to save teacher version.", "Unable to save"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleArchivePlan() {
    if (!plan) return;
    setBusy(true);
    clearSectionAlert("reteachPlan");
    try {
      const updated = await updateReteachPlan(plan.id, { status: "archived" });
      setPlan(updated);
      await loadPlans();
      setSectionAlert("reteachPlan", sectionSuccess("Reteach plan archived.", "Plan archived"));
    } catch (error) {
      setSectionAlert(
        "reteachPlan",
        sectionError(error instanceof Error ? error.message : "Unable to archive plan.", "Unable to archive"),
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-8">
      <header className="space-y-2">
        <p className="text-sm font-semibold uppercase tracking-wide text-sky-700">TeacherAssist · Mastery</p>
        <h1 className="text-3xl font-semibold text-slate-900">Reteach Plans</h1>
        <p className="max-w-3xl text-sm text-slate-600">
          AI may suggest reteach strategies from mastery gaps. You review, edit, and decide what to use.
          Drafts never publish automatically and do not update mastery.
        </p>
      </header>

      <TeacherAssistFormErrorSummary title="Unable to load reteach plans" message={pageError} />

      <section className="grid gap-6 xl:grid-cols-[18rem_minmax(0,1fr)]">
        <aside className="ta-panel p-4">
          <h2 className="text-sm font-semibold text-slate-900">Your plans</h2>
          {plans.length === 0 ? (
            <p className="mt-3 text-sm text-slate-500">
              No reteach plans yet. Create one from a weak standard on the{" "}
              <Link href="/teacher-assist/mastery" className="font-semibold text-sky-700 hover:text-sky-900">
                Mastery
              </Link>{" "}
              dashboard.
            </p>
          ) : (
            <ul className="mt-3 space-y-2">
              {plans.map((row) => (
                <li key={row.id}>
                  <button
                    type="button"
                    onClick={() => {
                      setSelectedPlanId(row.id);
                      router.replace(`/teacher-assist/reteach-plans?id=${row.id}`);
                    }}
                    className={`w-full rounded-xl border px-3 py-2 text-left text-sm ${
                      row.id === selectedPlanId
                        ? "border-sky-300 bg-sky-50"
                        : "border-slate-200 bg-white hover:bg-slate-50"
                    }`}
                  >
                    <p className="font-semibold text-slate-900">{row.title}</p>
                    <p className="mt-1 text-xs text-slate-500">
                      {row.standard_code ?? "Standard"} · {labelize(row.status)}
                    </p>
                  </button>
                </li>
              ))}
            </ul>
          )}
        </aside>

        <div className="space-y-6">
          {!plan ? (
            <article className="ta-panel p-6 text-sm text-slate-500">Select a reteach plan to review.</article>
          ) : (
            <>
              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-900">{plan.title}</h2>
                    <p className="mt-1 text-sm text-slate-600">
                      {plan.standard_code ?? plan.standard_id}
                      {plan.standard_description ? ` · ${plan.standard_description}` : ""}
                    </p>
                  </div>
                  <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                    {labelize(plan.status)}
                  </span>
                </div>
                <TeacherAssistInlineAlert
                  alert={getSectionAlert("reteachPlan")}
                  onDismiss={() => clearSectionAlert("reteachPlan")}
                  className="mt-4"
                />
                <p className="mt-3 text-xs text-slate-500">
                  Updated {formatDateTime(plan.updated_at)} · Matrix {plan.mastery_matrix_id.slice(0, 8)}…
                </p>
                <div className="mt-4 flex flex-wrap gap-2">
                  <Link
                    href={`/teacher-assist/mastery?id=${plan.mastery_matrix_id}`}
                    className="ta-button-secondary"
                  >
                    Open mastery matrix
                  </Link>
                  {plan.status !== "archived" ? (
                    <button type="button" className="ta-button-secondary" disabled={busy} onClick={handleArchivePlan}>
                      Archive plan
                    </button>
                  ) : null}
                </div>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-lg font-semibold text-slate-900">Generate AI draft</h2>
                <p className="mt-1 text-sm text-slate-600">
                  Uses standards, mastery levels, class summaries, and reteach insights. Only STUDENT # summaries are
                  sent — no student names.
                </p>
                <textarea
                  className="ta-input mt-4 min-h-24"
                  placeholder="Optional teacher instructions for the draft"
                  value={teacherInstructions}
                  onChange={(event) => setTeacherInstructions(event.target.value)}
                />
                <button
                  type="button"
                  className="ta-button mt-4"
                  disabled={busy || plan.status === "archived"}
                  onClick={handleGenerateDraft}
                >
                  {busy ? "Working…" : "Generate draft"}
                </button>
              </article>

              {versions.length > 0 ? (
                <article className="ta-panel p-6">
                  <h2 className="text-lg font-semibold text-slate-900">Version history</h2>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {versions.map((row) => (
                      <button
                        key={row.id}
                        type="button"
                        onClick={() => {
                          setSelectedVersionId(row.id);
                          setEditContent(row.content_json);
                        }}
                        className={`rounded-full px-3 py-1 text-xs font-semibold ${
                          row.id === selectedVersion?.id
                            ? "bg-sky-100 text-sky-900"
                            : "bg-slate-100 text-slate-700"
                        }`}
                      >
                        v{row.version_number} · {labelize(row.version_source)}
                      </button>
                    ))}
                  </div>
                </article>
              ) : null}

              {selectedVersion && editContent ? (
                <>
                  <div className="grid gap-4 md:grid-cols-2">
                    <ContentSection title="Reteach objectives" items={editContent.reteach_objectives} />
                    <ContentSection title="Instructional strategies" items={editContent.instructional_strategies} />
                    <ContentSection title="Small-group recommendations" items={editContent.small_group_recommendations} />
                    <ContentSection title="Intervention ideas" items={editContent.intervention_ideas} />
                    <ContentSection title="Vocabulary focus" items={editContent.vocabulary_focus} />
                    <ContentSection title="Assessment checks" items={editContent.assessment_checks} />
                  </div>

                  <article className="ta-panel p-6">
                    <h2 className="text-lg font-semibold text-slate-900">Save teacher-reviewed version</h2>
                    <p className="mt-1 text-sm text-slate-600">
                      Saving creates a new version and moves the plan to teacher review. Mastery is not updated.
                    </p>
                    <textarea
                      className="ta-input mt-4 min-h-20"
                      placeholder="Change reason (optional)"
                      value={changeReason}
                      onChange={(event) => setChangeReason(event.target.value)}
                    />
                    <button
                      type="button"
                      className="ta-button mt-4"
                      disabled={busy || plan.status === "archived"}
                      onClick={handleSaveTeacherVersion}
                    >
                      Save teacher version
                    </button>
                  </article>
                </>
              ) : (
                <article className="ta-panel p-6 text-sm text-slate-500">
                  Generate an AI draft to review reteach objectives, strategies, and checks.
                </article>
              )}
            </>
          )}
        </div>
      </section>
    </div>
  );
}
