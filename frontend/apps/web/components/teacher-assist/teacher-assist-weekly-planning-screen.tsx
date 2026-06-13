"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  attachPlanningDraftResource,
  createPlanningDraft,
  fetchClasses,
  fetchGradingPeriods,
  fetchPacingGuideItems,
  fetchPacingGuides,
  fetchPlanningDraftContextPreview,
  fetchPlanningDrafts,
  fetchResources,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherAssistWorkflows,
  fetchWeeklyPlans,
  startWeeklyPlanWorkflow,
  updatePlanningDraft,
  updatePlanningDraftStatus,
  uploadResourceFile,
} from "@/lib/teacher-assist-api";
import type {
  GradingPeriod,
  PacingGuide,
  PacingItem,
  PlanningDraft,
  PlanningDraftContextPreview,
  ResourceLibraryItem,
  SchoolYear,
  Standard,
  Subject,
  TeacherAssistWorkflow,
  TeacherClass,
  WeeklyPlan,
} from "@/lib/teacher-assist-types";

type Snapshot = {
  schoolYears: SchoolYear[];
  gradingPeriods: GradingPeriod[];
  classes: TeacherClass[];
  subjects: Subject[];
  standards: Standard[];
  resources: ResourceLibraryItem[];
  drafts: PlanningDraft[];
  pacingGuides: PacingGuide[];
  pacingItems: PacingItem[];
  workflows: TeacherAssistWorkflow[];
  weeklyPlans: WeeklyPlan[];
};

type DraftForm = {
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  school_year_id: string;
  grading_period_id: string;
  class_id: string;
  title: string;
  module_title: string;
  start_date: string;
  end_date: string;
  estimated_weeks: string;
  instructional_days_count: string;
  notes: string;
  status: "draft" | "ready";
  subject_ids: string[];
  pacing_item_ids: string[];
  standard_ids: string[];
  resource_ids: string[];
};

type WorkspaceMessage = {
  tone: "info" | "success" | "warning" | "error";
  text: string;
};

function emptyDraftForm(): DraftForm {
  return {
    planning_scope: "weekly",
    school_year_id: "",
    grading_period_id: "",
    class_id: "",
    title: "",
    module_title: "",
    start_date: "",
    end_date: "",
    estimated_weeks: "",
    instructional_days_count: "",
    notes: "",
    status: "draft",
    subject_ids: [],
    pacing_item_ids: [],
    standard_ids: [],
    resource_ids: [],
  };
}

function draftFormFromDraft(draft: PlanningDraft): DraftForm {
  return {
    planning_scope: draft.planning_scope,
    school_year_id: draft.school_year_id ?? "",
    grading_period_id: draft.grading_period_id ?? "",
    class_id: draft.class_id ?? "",
    title: draft.plan_title ?? draft.title ?? "",
    module_title: draft.module_title ?? "",
    start_date: draft.start_date ?? "",
    end_date: draft.end_date ?? "",
    estimated_weeks: draft.estimated_weeks ? String(draft.estimated_weeks) : "",
    instructional_days_count: draft.instructional_days_count
      ? String(draft.instructional_days_count)
      : "",
    notes: draft.notes ?? "",
    status: draft.status === "ready" ? "ready" : "draft",
    subject_ids: draft.subject_ids,
    pacing_item_ids: draft.pacing_item_ids,
    standard_ids: draft.standard_ids,
    resource_ids: draft.resource_ids,
  };
}

function formatDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatPlanningScope(value: string) {
  return value.replaceAll("_", " ");
}

function formatCurrencyCents(value: number | null | undefined) {
  return `$${((value ?? 0) / 100).toFixed(2)}`;
}

function toggleSelection(values: string[], value: string) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}

function sortByTitle<T extends { title?: string | null; name?: string | null }>(rows: T[]) {
  return [...rows].sort((left, right) =>
    (left.title ?? left.name ?? "").localeCompare(right.title ?? right.name ?? ""),
  );
}

const planningScopeOptions: Array<{ value: DraftForm["planning_scope"]; label: string }> = [
  { value: "weekly", label: "Weekly View" },
  { value: "multi_week", label: "Multi-Week" },
  { value: "module", label: "Module" },
  { value: "unit", label: "Unit" },
  { value: "grading_period", label: "Grading Period" },
];

function workflowStatusClass(status: TeacherAssistWorkflow["status"]) {
  switch (status) {
    case "completed":
      return "border-emerald-200 bg-emerald-50 text-emerald-900";
    case "failed":
      return "border-rose-200 bg-rose-50 text-rose-900";
    case "cancelled":
      return "border-slate-300 bg-slate-100 text-slate-700";
    case "queued":
    case "running":
    default:
      return "border-sky-200 bg-sky-50 text-sky-900";
  }
}

function workflowStatusMessage(workflow: TeacherAssistWorkflow) {
  switch (workflow.status) {
    case "queued":
      return workflow.retry_count > 0
        ? `Instructional-plan workflow queued for retry ${workflow.retry_count} of ${workflow.max_retries}.`
        : "Instructional-plan workflow queued. Mock generation will start shortly.";
    case "running":
      return `Instructional-plan workflow running. Progress: ${workflow.progress_percent}%.`;
    case "completed":
      return "Mock instructional plan completed. Review the generated plan output.";
    case "failed":
      return workflow.error_message || "Instructional-plan workflow failed.";
    case "cancelled":
      return "Instructional-plan workflow was cancelled.";
    default:
      return "Workflow status updated.";
  }
}

export function TeacherAssistWeeklyPlanningScreen() {
  const searchParams = useSearchParams();
  const pacingPeriodId = searchParams.get("pacing_period_id") ?? searchParams.get("period_id");
  const focusResources = searchParams.get("focus") === "resources";
  const pacingPrefillRef = useRef<string | null>(null);
  const resourcesSectionRef = useRef<HTMLDivElement | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [message, setMessage] = useState<WorkspaceMessage | null>(null);
  const [savingKey, setSavingKey] = useState<string | null>(null);
  const [draftForm, setDraftForm] = useState<DraftForm>(emptyDraftForm());
  const [activeDraftId, setActiveDraftId] = useState<string | null>(null);
  const [contextPreview, setContextPreview] = useState<PlanningDraftContextPreview | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [dragActive, setDragActive] = useState(false);
  const [uploadingResources, setUploadingResources] = useState(false);

  const load = useCallback(
    async (preferredDraftId?: string | null, options?: { silent?: boolean }) => {
      const silent = options?.silent ?? false;
      if (!silent) {
        setLoading(true);
        setPageError(null);
      }
      try {
        const [
          schoolYears,
          gradingPeriods,
          classes,
          subjects,
          standards,
          resources,
          drafts,
          pacingGuides,
          workflows,
          weeklyPlans,
        ] = await Promise.all([
          fetchSchoolYears(),
          fetchGradingPeriods(),
          fetchClasses(),
          fetchSubjects(),
          fetchStandards(),
          fetchResources(),
          fetchPlanningDrafts(),
          fetchPacingGuides(),
          fetchTeacherAssistWorkflows(),
          fetchWeeklyPlans(),
        ]);
        const pacingItems = (
          await Promise.all(pacingGuides.map((guide) => fetchPacingGuideItems(guide.id)))
        ).flat();
        setSnapshot({
          schoolYears,
          gradingPeriods,
          classes,
          subjects,
          standards,
          resources,
          drafts,
          pacingGuides,
          pacingItems,
          workflows,
          weeklyPlans,
        });
        if (!silent) {
          if (preferredDraftId) {
            const matchingDraft = drafts.find((draft) => draft.id === preferredDraftId);
            if (matchingDraft) {
              setActiveDraftId(matchingDraft.id);
              setDraftForm(draftFormFromDraft(matchingDraft));
            }
          } else if (!activeDraftId && drafts.length === 0) {
            setDraftForm((current) => ({
              ...current,
              school_year_id:
                current.school_year_id ||
                schoolYears.find((schoolYear) => schoolYear.is_active)?.id ||
                schoolYears[0]?.id ||
                "",
              grading_period_id: current.grading_period_id || gradingPeriods[0]?.id || "",
              class_id: current.class_id || classes[0]?.id || "",
            }));
          }
        }
      } catch (nextError) {
        if (silent) {
          throw nextError;
        }
        setPageError(nextError instanceof Error ? nextError.message : "Could not load planning workspace.");
      } finally {
        if (!silent) {
          setLoading(false);
        }
      }
    },
    [activeDraftId],
  );

  const loadPreview = useCallback(async (draftId: string, options?: { silent?: boolean }) => {
    const silent = options?.silent ?? false;
    if (!silent) {
      setPreviewLoading(true);
    }
    try {
      const preview = await fetchPlanningDraftContextPreview(draftId);
      setContextPreview(preview);
    } catch (nextError) {
      if (!silent) {
        setSectionAlert(
          "draftWorkspace",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not load draft context preview.",
            "Unable to load context preview",
          ),
        );
      }
    } finally {
      if (!silent) {
        setPreviewLoading(false);
      }
    }
  }, [setSectionAlert]);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (!focusResources || loading) return;
    resourcesSectionRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [focusResources, loading]);

  const handleResourceUpload = useCallback(
    async (files: File[]) => {
      if (files.length === 0) return;
      setUploadingResources(true);
      clearSectionAlert("resourceUpload");
      try {
        const uploadedIds: string[] = [];
        for (const file of files) {
          const resource = await uploadResourceFile(file);
          uploadedIds.push(resource.id);
        }
        await load(activeDraftId, { silent: true });
        setDraftForm((current) => ({
          ...current,
          resource_ids: [...new Set([...current.resource_ids, ...uploadedIds])],
        }));
        setSectionAlert(
          "resourceUpload",
          sectionSuccess(
            `${files.length} file${files.length === 1 ? "" : "s"} uploaded. Select them below or save your draft.`,
            "Upload complete",
          ),
        );
      } catch (nextError) {
        setSectionAlert(
          "resourceUpload",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not upload curriculum files.",
            "Upload failed",
          ),
        );
      } finally {
        setUploadingResources(false);
      }
    },
    [activeDraftId, clearSectionAlert, load, setSectionAlert],
  );

  useEffect(() => {
    if (!pacingPeriodId || loading || activeDraftId || pacingPrefillRef.current === pacingPeriodId) {
      return;
    }
    pacingPrefillRef.current = pacingPeriodId;
    let active = true;
    void createPlanningDraft({ pacing_guide_period_id: pacingPeriodId })
      .then(async (savedDraft) => {
        if (!active) return;
        setActiveDraftId(savedDraft.id);
        setDraftForm(draftFormFromDraft(savedDraft));
        setMessage({
          tone: "info",
          text: `Pre-filled from pacing week${savedDraft.title ? `: ${savedDraft.title}` : "."}`,
        });
        await load(savedDraft.id);
        await loadPreview(savedDraft.id);
      })
      .catch((nextError) => {
        if (!active) return;
        pacingPrefillRef.current = null;
        setSectionAlert(
          "draftWorkspace",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not pre-fill planning draft from pacing week.",
            "Unable to pre-fill draft",
          ),
        );
      });
    return () => {
      active = false;
    };
  }, [activeDraftId, load, loadPreview, loading, pacingPeriodId, setSectionAlert]);

  useEffect(() => {
    if (!activeDraftId) {
      setContextPreview(null);
      return;
    }
    void loadPreview(activeDraftId);
  }, [activeDraftId, loadPreview]);

  useEffect(() => {
    if (!snapshot?.workflows.some((workflow) => workflow.status === "queued" || workflow.status === "running")) {
      return;
    }
    const interval = window.setInterval(() => {
      void load(null, { silent: true });
      if (activeDraftId) {
        void loadPreview(activeDraftId, { silent: true });
      }
    }, 3000);
    return () => window.clearInterval(interval);
  }, [activeDraftId, load, loadPreview, snapshot?.workflows]);

  const schoolYearTitleById = useMemo(
    () => new Map((snapshot?.schoolYears ?? []).map((row) => [row.id, row.title])),
    [snapshot?.schoolYears],
  );
  const gradingPeriodTitleById = useMemo(
    () => new Map((snapshot?.gradingPeriods ?? []).map((row) => [row.id, row.title])),
    [snapshot?.gradingPeriods],
  );
  const classNameById = useMemo(
    () => new Map((snapshot?.classes ?? []).map((row) => [row.id, row.name])),
    [snapshot?.classes],
  );
  const subjectNameById = useMemo(
    () => new Map((snapshot?.subjects ?? []).map((row) => [row.id, row.name])),
    [snapshot?.subjects],
  );
  const resourceTitleById = useMemo(
    () => new Map((snapshot?.resources ?? []).map((row) => [row.id, row.title])),
    [snapshot?.resources],
  );
  const pacingGuideById = useMemo(
    () => new Map((snapshot?.pacingGuides ?? []).map((row) => [row.id, row])),
    [snapshot?.pacingGuides],
  );
  const weeklyPlanById = useMemo(
    () => new Map((snapshot?.weeklyPlans ?? []).map((row) => [row.id, row])),
    [snapshot?.weeklyPlans],
  );

  const activeDraft = useMemo(
    () => (snapshot?.drafts ?? []).find((draft) => draft.id === activeDraftId) ?? null,
    [activeDraftId, snapshot?.drafts],
  );
  const activeDraftWorkflow = useMemo(
    () =>
      [...(snapshot?.workflows ?? [])]
        .filter((workflow) => workflow.planning_input_draft_id === activeDraftId)
        .sort((left, right) => Date.parse(right.created_at) - Date.parse(left.created_at))[0] ?? null,
    [activeDraftId, snapshot?.workflows],
  );
  const selectedClass = useMemo(
    () => (snapshot?.classes ?? []).find((teacherClass) => teacherClass.id === draftForm.class_id) ?? null,
    [draftForm.class_id, snapshot?.classes],
  );

  const availableSubjects = useMemo(() => {
    const allSubjects = snapshot?.subjects ?? [];
    if (!selectedClass || selectedClass.subject_ids.length === 0) return allSubjects;
    return allSubjects.filter((subject) => selectedClass.subject_ids.includes(subject.id));
  }, [selectedClass, snapshot?.subjects]);

  const availablePacingItems = useMemo(() => {
    return (snapshot?.pacingItems ?? []).filter((item) => {
      const guide = pacingGuideById.get(item.pacing_guide_id);
      if (draftForm.school_year_id && guide?.school_year_id !== draftForm.school_year_id) return false;
      if (
        draftForm.grading_period_id &&
        item.grading_period_id &&
        item.grading_period_id !== draftForm.grading_period_id
      ) {
        return false;
      }
      if (
        draftForm.subject_ids.length > 0 &&
        item.subject_id &&
        !draftForm.subject_ids.includes(item.subject_id)
      ) {
        return false;
      }
      return true;
    });
  }, [
    draftForm.grading_period_id,
    draftForm.school_year_id,
    draftForm.subject_ids,
    pacingGuideById,
    snapshot?.pacingItems,
  ]);

  const availableStandards = useMemo(() => {
    return (snapshot?.standards ?? []).filter((standard) => {
      if (
        draftForm.school_year_id &&
        standard.school_year_id &&
        standard.school_year_id !== draftForm.school_year_id
      ) {
        return false;
      }
      if (
        draftForm.subject_ids.length > 0 &&
        standard.subject_id &&
        !draftForm.subject_ids.includes(standard.subject_id)
      ) {
        return false;
      }
      return true;
    });
  }, [draftForm.school_year_id, draftForm.subject_ids, snapshot?.standards]);

  const readinessAlert = useMemo(() => {
    if (contextPreview?.readiness.is_ready) {
      return {
        tone: "success" as const,
        text: "Instructional planning context is ready for generation.",
      };
    }
    if (contextPreview && contextPreview.readiness.missing_items.length > 0) {
      return {
        tone: "warning" as const,
        text: contextPreview.readiness.missing_items.join(" "),
      };
    }
    return message;
  }, [contextPreview, message]);

  const workflowAlert = useMemo(() => {
    if (!activeDraftWorkflow) return null;
    return {
      tone:
        activeDraftWorkflow.status === "completed"
          ? ("success" as const)
          : activeDraftWorkflow.status === "failed"
            ? ("error" as const)
            : activeDraftWorkflow.status === "cancelled"
              ? ("warning" as const)
              : ("info" as const),
      text: workflowStatusMessage(activeDraftWorkflow),
    };
  }, [activeDraftWorkflow]);

  const primaryAlert = workflowAlert ?? readinessAlert;
  const canGenerate =
    Boolean(activeDraftId) &&
    draftForm.status === "ready" &&
    Boolean(contextPreview?.readiness.is_ready) &&
    activeDraftWorkflow?.status !== "queued" &&
    activeDraftWorkflow?.status !== "running";

  const persistDraft = useCallback(
    async (targetStatus: "draft" | "ready" = "draft") => {
      setSavingKey(targetStatus);
      clearSectionAlert("draftWorkspace");
      setMessage(null);
      try {
        const body = {
          school_year_id: draftForm.school_year_id || null,
          grading_period_id: draftForm.grading_period_id || null,
          class_id: draftForm.class_id || null,
          subject_id: draftForm.subject_ids[0] ?? null,
          subject_ids: draftForm.subject_ids,
          pacing_item_ids: draftForm.pacing_item_ids,
          standard_ids: draftForm.standard_ids,
          planning_scope: draftForm.planning_scope,
          title: draftForm.title || null,
          plan_title: draftForm.title || null,
          module_title: draftForm.module_title || null,
          start_date: draftForm.start_date || null,
          end_date: draftForm.end_date || null,
          estimated_weeks: draftForm.estimated_weeks ? Number(draftForm.estimated_weeks) : null,
          instructional_days_count: draftForm.instructional_days_count
            ? Number(draftForm.instructional_days_count)
            : null,
          notes: draftForm.notes || null,
          status: "draft",
        };

        let savedDraft = activeDraftId
          ? await updatePlanningDraft(activeDraftId, body)
          : await createPlanningDraft(body);

        const missingResourceIds = draftForm.resource_ids.filter(
          (resourceId) => !savedDraft.resource_ids.includes(resourceId),
        );
        for (const resourceId of missingResourceIds) {
          savedDraft = await attachPlanningDraftResource(savedDraft.id, resourceId);
        }

        if (targetStatus === "ready") {
          savedDraft = await updatePlanningDraftStatus(savedDraft.id, "ready");
        }

        setActiveDraftId(savedDraft.id);
        setDraftForm(draftFormFromDraft(savedDraft));
        setMessage(
          targetStatus === "ready"
            ? {
                tone: "success",
                text: "Instructional planning context is ready for generation.",
              }
            : {
                tone: "info",
                text: "Draft saved. Review context before generation.",
              },
        );
        await load(savedDraft.id);
        await loadPreview(savedDraft.id);
      } catch (nextError) {
        setSectionAlert(
          "draftWorkspace",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not save planning draft.",
            "Unable to save draft",
          ),
        );
      } finally {
        setSavingKey(null);
      }
    },
    [activeDraftId, clearSectionAlert, draftForm, load, loadPreview, setSectionAlert],
  );

  const handleGenerate = useCallback(async () => {
    if (!activeDraftId) return;
    setSavingKey("generate");
    clearSectionAlert("draftWorkspace");
    setMessage(null);
    try {
      await startWeeklyPlanWorkflow(activeDraftId);
      setMessage({
        tone: "info",
        text: "Instructional-plan workflow queued. Mock generation output will appear in the workflows panel.",
      });
      await load(activeDraftId, { silent: true });
      await loadPreview(activeDraftId, { silent: true });
    } catch (nextError) {
      setSectionAlert(
        "draftWorkspace",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not start instructional-plan workflow.",
          "Workflow failed",
        ),
      );
    } finally {
      setSavingKey(null);
    }
  }, [activeDraftId, clearSectionAlert, load, loadPreview, setSectionAlert]);

  const resetWorkspace = () => {
    setActiveDraftId(null);
    setContextPreview(null);
    setMessage(null);
    setDraftForm({
      ...emptyDraftForm(),
      school_year_id: snapshot?.schoolYears.find((schoolYear) => schoolYear.is_active)?.id ?? "",
      grading_period_id: snapshot?.gradingPeriods[0]?.id ?? "",
      class_id: snapshot?.classes[0]?.id ?? "",
    });
  };

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">Weekly Planning</p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            {pacingPeriodId
              ? "Upload resources, then generate this week’s plan"
              : "Plan instruction week by week"}
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Drag curriculum files into your draft, add reference links from the Resource Library, save
            the draft, mark it ready, then click Generate Plan to produce lesson plans, Google Slides,
            quizzes, and assignments.
          </p>
        </div>
      </section>

      <TeacherAssistAlert
        variant="info"
        description="Newsletters and parent communication come later — this screen focuses on instructional artifacts for the selected pacing week."
      />

      <TeacherAssistFormErrorSummary title="Unable to load planning workspace" message={pageError} />

      <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
        <article className="ta-panel p-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">
                {activeDraftId ? "Planning draft workspace" : "Create planning draft"}
              </h2>
              <p className="mt-1 text-sm text-slate-600">
                Save as draft first, then mark ready before starting an instructional-plan workflow.
              </p>
            </div>
            <div className="flex items-center gap-2">
              <span
                className={`rounded-full px-3 py-1 text-xs font-semibold ${
                  draftForm.status === "ready"
                    ? "bg-emerald-100 text-emerald-700"
                    : "bg-sky-100 text-sky-700"
                }`}
              >
                {draftForm.status}
              </span>
              <button type="button" className="ta-button-secondary" onClick={resetWorkspace}>
                New draft
              </button>
            </div>
          </div>

          <TeacherAssistInlineAlert
            alert={getSectionAlert("draftWorkspace")}
            onDismiss={() => clearSectionAlert("draftWorkspace")}
            className="mt-4"
          />
          {primaryAlert ? (
            <TeacherAssistAlert variant={primaryAlert.tone} description={primaryAlert.text} className="mt-4" />
          ) : null}

          <form
            className="mt-6 space-y-5"
            onSubmit={(event) => {
              event.preventDefault();
              void persistDraft("draft");
            }}
          >
            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="ta-label">Planning scope</span>
                <select
                  value={draftForm.planning_scope}
                  onChange={(event) =>
                    setDraftForm((current) => ({
                      ...current,
                      planning_scope: event.target.value as DraftForm["planning_scope"],
                    }))
                  }
                  className="ta-input"
                >
                  {planningScopeOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Plan title</span>
                <input
                  value={draftForm.title}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, title: event.target.value }))
                  }
                  className="ta-input"
                  placeholder="Week 1 Reading Plan"
                />
              </label>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="ta-label">Module / unit title</span>
                <input
                  value={draftForm.module_title}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, module_title: event.target.value }))
                  }
                  className="ta-input"
                  placeholder="Argument Writing Module"
                />
              </label>
              <div className="grid gap-4 md:grid-cols-2">
                <label className="space-y-2">
                  <span className="ta-label">Start date</span>
                  <input
                    type="date"
                    value={draftForm.start_date}
                    onChange={(event) =>
                      setDraftForm((current) => ({ ...current, start_date: event.target.value }))
                    }
                    className="ta-input"
                  />
                </label>
                <label className="space-y-2">
                  <span className="ta-label">End date</span>
                  <input
                    type="date"
                    value={draftForm.end_date}
                    onChange={(event) =>
                      setDraftForm((current) => ({ ...current, end_date: event.target.value }))
                    }
                    className="ta-input"
                  />
                </label>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <label className="space-y-2">
                <span className="ta-label">Estimated weeks</span>
                <input
                  type="number"
                  min={1}
                  value={draftForm.estimated_weeks}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, estimated_weeks: event.target.value }))
                  }
                  className="ta-input"
                  placeholder="1"
                />
              </label>
              <label className="space-y-2">
                <span className="ta-label">Instructional days</span>
                <input
                  type="number"
                  min={1}
                  value={draftForm.instructional_days_count}
                  onChange={(event) =>
                    setDraftForm((current) => ({
                      ...current,
                      instructional_days_count: event.target.value,
                    }))
                  }
                  className="ta-input"
                  placeholder="5"
                />
              </label>
            </div>

            <label className="space-y-2">
              <span className="ta-label">Teacher notes</span>
              <textarea
                value={draftForm.notes}
                onChange={(event) =>
                  setDraftForm((current) => ({ ...current, notes: event.target.value }))
                }
                className="ta-input min-h-32"
                placeholder="Save instructions, constraints, reminders, accommodations, pacing notes, or instructional priorities."
              />
            </label>

            <div className="grid gap-4 md:grid-cols-3">
              <label className="space-y-2">
                <span className="ta-label">School year</span>
                <select
                  value={draftForm.school_year_id}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, school_year_id: event.target.value }))
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
                <span className="ta-label">Grading period</span>
                <select
                  value={draftForm.grading_period_id}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, grading_period_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Select grading period</option>
                  {(snapshot?.gradingPeriods ?? []).map((period) => (
                    <option key={period.id} value={period.id}>
                      {period.title}
                    </option>
                  ))}
                </select>
              </label>
              <label className="space-y-2">
                <span className="ta-label">Class</span>
                <select
                  value={draftForm.class_id}
                  onChange={(event) =>
                    setDraftForm((current) => ({ ...current, class_id: event.target.value }))
                  }
                  className="ta-input"
                >
                  <option value="">Select class</option>
                  {(snapshot?.classes ?? []).map((teacherClass) => (
                    <option key={teacherClass.id} value={teacherClass.id}>
                      {teacherClass.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="space-y-3">
              <div>
                <p className="ta-label">Subjects</p>
                <p className="mt-1 text-sm text-slate-500">
                  Select one or more subjects. If the class already has subject assignments, this
                  list is filtered to that class.
                </p>
              </div>
              <div className="grid gap-2 md:grid-cols-2">
                {availableSubjects.length > 0 ? (
                  sortByTitle(availableSubjects).map((subject) => (
                    <label
                      key={subject.id}
                      className="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-700"
                    >
                      <input
                        type="checkbox"
                        checked={draftForm.subject_ids.includes(subject.id)}
                        onChange={() =>
                          setDraftForm((current) => ({
                            ...current,
                            subject_ids: toggleSelection(current.subject_ids, subject.id),
                          }))
                        }
                      />
                      <span>{subject.name}</span>
                    </label>
                  ))
                ) : (
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-500">
                    Add subjects in TeacherAssist Settings before building a draft.
                  </div>
                )}
              </div>
            </div>

            <div className="space-y-3">
              <div>
                <p className="ta-label">Pacing items</p>
                <p className="mt-1 text-sm text-slate-500">
                  Attach pacing items that define the instructional sequence for this planning window.
                </p>
              </div>
              <div className="max-h-56 space-y-2 overflow-y-auto rounded-2xl border border-slate-200 p-3">
                {availablePacingItems.length > 0 ? (
                  availablePacingItems.map((item) => {
                    const guide = pacingGuideById.get(item.pacing_guide_id);
                    return (
                      <label
                        key={item.id}
                        className="flex items-start gap-3 rounded-2xl border border-slate-100 px-4 py-3 text-sm text-slate-700"
                      >
                        <input
                          type="checkbox"
                          checked={draftForm.pacing_item_ids.includes(item.id)}
                          onChange={() =>
                            setDraftForm((current) => ({
                              ...current,
                              pacing_item_ids: toggleSelection(current.pacing_item_ids, item.id),
                            }))
                          }
                        />
                        <div>
                          <p className="font-semibold text-slate-900">{item.title}</p>
                          <p className="text-xs uppercase tracking-[0.16em] text-slate-500">
                            {guide?.title ?? "Pacing guide"} · Week {item.week_number ?? "-"} · Day{" "}
                            {item.day_number ?? "-"}
                          </p>
                        </div>
                      </label>
                    );
                  })
                ) : (
                  <p className="px-1 py-2 text-sm text-slate-500">
                    No pacing items match the current school year, grading period, and subject scope.
                  </p>
                )}
              </div>
            </div>

            <div className="space-y-3">
              <div>
                <p className="ta-label">Standards / TEKS</p>
                <p className="mt-1 text-sm text-slate-500">
                  Attach the standards the teacher wants included in future planning context.
                </p>
              </div>
              <div className="max-h-56 space-y-2 overflow-y-auto rounded-2xl border border-slate-200 p-3">
                {availableStandards.length > 0 ? (
                  availableStandards.map((standard) => (
                    <label
                      key={standard.id}
                      className="flex items-start gap-3 rounded-2xl border border-slate-100 px-4 py-3 text-sm text-slate-700"
                    >
                      <input
                        type="checkbox"
                        checked={draftForm.standard_ids.includes(standard.id)}
                        onChange={() =>
                          setDraftForm((current) => ({
                            ...current,
                            standard_ids: toggleSelection(current.standard_ids, standard.id),
                          }))
                        }
                      />
                      <div>
                        <p className="font-semibold text-slate-900">{standard.code}</p>
                        <p className="text-sm text-slate-600">{standard.description}</p>
                      </div>
                    </label>
                  ))
                ) : (
                  <p className="px-1 py-2 text-sm text-slate-500">
                    No standards match the current school year and subject scope yet.
                  </p>
                )}
              </div>
            </div>

            <div className="space-y-3" ref={resourcesSectionRef}>
              <div>
                <p className="ta-label">Curriculum resources</p>
                <p className="mt-1 text-sm text-slate-500">
                  Drag files here or browse to upload. Uploaded files attach to this planning draft.
                </p>
              </div>
              <TeacherAssistInlineAlert
                alert={getSectionAlert("resourceUpload")}
                onDismiss={() => clearSectionAlert("resourceUpload")}
              />
              <button
                type="button"
                disabled={uploadingResources}
                onClick={() => fileInputRef.current?.click()}
                onDragEnter={(event) => {
                  event.preventDefault();
                  setDragActive(true);
                }}
                onDragOver={(event) => {
                  event.preventDefault();
                  setDragActive(true);
                }}
                onDragLeave={(event) => {
                  event.preventDefault();
                  setDragActive(false);
                }}
                onDrop={(event) => {
                  event.preventDefault();
                  setDragActive(false);
                  const files = Array.from(event.dataTransfer.files);
                  if (files.length > 0) {
                    void handleResourceUpload(files);
                  }
                }}
                className={`flex min-h-36 w-full flex-col items-center justify-center rounded-2xl border border-dashed px-6 py-8 text-center transition ${
                  dragActive
                    ? "border-sky-400 bg-sky-50"
                    : "border-slate-300 bg-slate-50 hover:border-sky-300 hover:bg-sky-50/50"
                }`}
              >
                <span className="text-sm font-semibold text-slate-900">
                  {uploadingResources ? "Uploading..." : "Drag and drop curriculum files here"}
                </span>
                <span className="mt-2 text-xs leading-5 text-slate-600">
                  PDFs, slides, docs, images, and other supporting materials
                </span>
              </button>
              <input
                ref={fileInputRef}
                type="file"
                multiple
                className="hidden"
                onChange={(event) => {
                  const files = event.target.files ? Array.from(event.target.files) : [];
                  if (files.length > 0) {
                    void handleResourceUpload(files);
                  }
                  event.target.value = "";
                }}
              />
              <div className="flex flex-wrap gap-2">
                <Link href="/teacher-assist/resources" className="ta-button-secondary text-xs">
                  Add reference links
                </Link>
              </div>
              <div className="grid gap-2 md:grid-cols-2">
                {(snapshot?.resources ?? []).length > 0 ? (
                  sortByTitle(snapshot?.resources ?? []).map((resource) => (
                    <label
                      key={resource.id}
                      className="flex items-center gap-3 rounded-2xl border border-slate-200 px-4 py-3 text-sm text-slate-700"
                    >
                      <input
                        type="checkbox"
                        checked={draftForm.resource_ids.includes(resource.id)}
                        onChange={() =>
                          setDraftForm((current) => ({
                            ...current,
                            resource_ids: toggleSelection(current.resource_ids, resource.id),
                          }))
                        }
                      />
                      <span>{resource.title}</span>
                    </label>
                  ))
                ) : (
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-500">
                    Upload curriculum files or add resource links in the Resource Library first.
                  </div>
                )}
              </div>
            </div>

            <div className="flex flex-wrap gap-3">
              <button type="submit" disabled={savingKey === "draft"} className="ta-button-primary">
                {savingKey === "draft" ? "Saving..." : "Save draft"}
              </button>
              <button
                type="button"
                disabled={savingKey === "ready"}
                className="ta-button-secondary"
                onClick={() => {
                  void persistDraft("ready");
                }}
              >
                {savingKey === "ready" ? "Checking readiness..." : "Mark ready"}
              </button>
              {activeDraftId && draftForm.status === "ready" ? (
                <button
                  type="button"
                  className="ta-button-secondary"
                  onClick={() => {
                    void (async () => {
                      setSavingKey("return-to-draft");
                      clearSectionAlert("draftWorkspace");
                      try {
                        const updated = await updatePlanningDraftStatus(activeDraftId, "draft");
                        setDraftForm(draftFormFromDraft(updated));
                        setMessage({
                          tone: "info",
                          text: "Draft returned to draft status. Review context before marking ready again.",
                        });
                        await load(activeDraftId);
                        await loadPreview(activeDraftId);
                      } catch (nextError) {
                        setSectionAlert(
                          "draftWorkspace",
                          sectionError(
                            nextError instanceof Error
                              ? nextError.message
                              : "Could not move the draft back to draft status.",
                            "Unable to update draft",
                          ),
                        );
                      } finally {
                        setSavingKey(null);
                      }
                    })();
                  }}
                >
                  Return to draft
                </button>
              ) : null}
            </div>
          </form>
        </article>

        <div className="space-y-6">
          <article className="ta-panel p-6">
            <div className="flex items-center justify-between gap-3">
              <div>
                <h2 className="text-xl font-semibold text-slate-900">Context preview</h2>
                <p className="mt-1 text-sm text-slate-600">
                  This saved preview becomes the input snapshot for instructional-plan workflows.
                </p>
              </div>
              <button
                type="button"
                className={
                  canGenerate ? "ta-button-primary" : "ta-button-secondary cursor-not-allowed opacity-60"
                }
                disabled={!canGenerate || savingKey === "generate"}
                onClick={() => {
                  void handleGenerate();
                }}
              >
                {savingKey === "generate" ? "Generating..." : "Generate Plan"}
              </button>
            </div>
            <p className="mt-2 text-sm text-slate-500">
              Copying, branching, and manual editing remain software-only. Real-provider execution, if
              enabled, is still subject to teacher review before use.
            </p>

            {activeDraftWorkflow &&
            (activeDraftWorkflow.status === "queued" || activeDraftWorkflow.status === "running") ? (
              <TeacherAssistAlert
                variant="info"
                title="Plan generation in progress"
                description={
                  <>
                    {workflowStatusMessage(activeDraftWorkflow)} This page refreshes workflow status
                    automatically every few seconds. You can keep working while generation runs.
                  </>
                }
                className="mt-4"
              />
            ) : null}

            {previewLoading ? (
              <p className="mt-5 text-sm text-slate-600">Loading preview...</p>
            ) : contextPreview ? (
              <div className="mt-5 space-y-4">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
                  <p>
                    <span className="font-semibold text-slate-900">Planning scope:</span>{" "}
                    {formatPlanningScope(contextPreview.draft.planning_scope)}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">Plan title:</span>{" "}
                    {contextPreview.draft.plan_title ?? "Not set"}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">Module / unit title:</span>{" "}
                    {contextPreview.draft.module_title ?? "Not set"}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">Duration:</span>{" "}
                    {contextPreview.duration_summary.summary}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">School year:</span>{" "}
                    {contextPreview.school_year?.title ?? "Not selected"}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">Grading period:</span>{" "}
                    {contextPreview.grading_period?.title ?? "Not selected"}
                  </p>
                  <p className="mt-2">
                    <span className="font-semibold text-slate-900">Class:</span>{" "}
                    {contextPreview.class?.name ?? "Not selected"}
                  </p>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Subjects</p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {contextPreview.subjects.length > 0 ? (
                      contextPreview.subjects.map((subject) => (
                        <span
                          key={subject.id}
                          className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-700"
                        >
                          {subject.name}
                        </span>
                      ))
                    ) : (
                      <span className="text-sm text-slate-500">No subjects attached yet.</span>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Pacing by week / date</p>
                  <div className="mt-3 space-y-2">
                    {contextPreview.pacing_groups.length > 0 ? (
                      contextPreview.pacing_groups.map((group) => (
                        <div key={group.group_key} className="rounded-2xl border border-slate-100 px-4 py-3">
                          <p className="font-semibold text-slate-900">{group.label}</p>
                          <div className="mt-2 space-y-2">
                            {group.pacing_items.map((item) => (
                              <div key={item.id}>
                                <p className="text-sm font-semibold text-slate-800">{item.title}</p>
                                <p className="text-sm text-slate-600">
                                  Week {item.week_number ?? "-"} · Day {item.day_number ?? "-"} ·{" "}
                                  {item.objectives ?? item.notes ?? "No extra pacing details saved."}
                                </p>
                              </div>
                            ))}
                          </div>
                        </div>
                      ))
                    ) : (
                      <p className="text-sm text-slate-500">No pacing groups attached yet.</p>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Provider metadata</p>
                  <dl className="mt-3 space-y-2 text-sm text-slate-600">
                    <div className="flex items-center justify-between gap-4">
                      <dt className="font-semibold text-slate-900">Provider mode</dt>
                      <dd>{activeDraftWorkflow?.provider_name ?? "mock"}</dd>
                    </div>
                    <div className="flex items-center justify-between gap-4">
                      <dt className="font-semibold text-slate-900">Model</dt>
                      <dd>{activeDraftWorkflow?.provider_model ?? "mock"}</dd>
                    </div>
                    <div className="flex items-center justify-between gap-4">
                      <dt className="font-semibold text-slate-900">Prompt version</dt>
                      <dd>{activeDraftWorkflow?.prompt_version ?? "instructional-plan-v2"}</dd>
                    </div>
                    <div className="flex items-center justify-between gap-4">
                      <dt className="font-semibold text-slate-900">Tokens / estimated cost</dt>
                      <dd>
                        {activeDraftWorkflow?.input_tokens_total ?? 0} input /{" "}
                        {activeDraftWorkflow?.output_tokens_total ?? 0} output /{" "}
                        {formatCurrencyCents(activeDraftWorkflow?.estimated_cost_cents_total)}
                      </dd>
                    </div>
                    <div className="flex items-center justify-between gap-4">
                      <dt className="font-semibold text-slate-900">Provider seam</dt>
                      <dd>Real provider stays disabled unless explicitly enabled in config.</dd>
                    </div>
                  </dl>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Standards / TEKS</p>
                  <div className="mt-3 space-y-2">
                    {contextPreview.standards.length > 0 ? (
                      contextPreview.standards.map((standard) => (
                        <div key={standard.id} className="rounded-2xl border border-slate-100 px-4 py-3">
                          <p className="font-semibold text-slate-900">{standard.code}</p>
                          <p className="mt-1 text-sm text-slate-600">{standard.description}</p>
                        </div>
                      ))
                    ) : (
                      <p className="text-sm text-slate-500">No standards attached yet.</p>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Resources</p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    {contextPreview.resources.length > 0 ? (
                      contextPreview.resources.map((resource) => (
                        <span
                          key={resource.id}
                          className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                        >
                          {resource.title}
                        </span>
                      ))
                    ) : (
                      <span className="text-sm text-slate-500">No resources attached yet.</span>
                    )}
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 p-4">
                  <p className="text-sm font-semibold text-slate-900">Teacher notes</p>
                  <p className="mt-3 text-sm leading-6 text-slate-600">
                    {contextPreview.teacher_notes || "No teacher notes saved yet."}
                  </p>
                </div>

                {contextPreview.reflection_hints ? (
                  <div className="rounded-2xl border border-violet-200 bg-violet-50 p-4">
                    <p className="text-sm font-semibold text-violet-950">Planning reflection hints</p>
                    <p className="mt-1 text-xs text-violet-800">
                      Read-only notes from prior reflections and lesson effectiveness.
                    </p>
                    {contextPreview.reflection_hints.last_year_notes.length > 0 ? (
                      <ul className="mt-3 list-disc space-y-1 pl-5 text-sm text-violet-950">
                        {contextPreview.reflection_hints.last_year_notes.map((note) => (
                          <li key={note}>{note}</li>
                        ))}
                      </ul>
                    ) : null}
                  </div>
                ) : null}

                {contextPreview.readiness.warnings.length > 0 ? (
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <p className="text-sm font-semibold text-slate-900">Readiness warnings</p>
                    <ul className="mt-3 space-y-2 text-sm text-slate-600">
                      {contextPreview.readiness.warnings.map((warning) => (
                        <li key={warning}>{warning}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </div>
            ) : (
              <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
                 Save a planning draft to review the generated context preview. Start by selecting a
                 scope, plan title, duration, class, subjects, pacing items, standards, resources,
                 and teacher notes.
              </div>
            )}
          </article>

          <article className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Recent workflows</h2>
            <p className="mt-1 text-sm text-slate-600">
               Teacher-triggered workflows stay persisted even when generation is mock-only.
            </p>
            {snapshot && snapshot.workflows.length > 0 ? (
              <div className="mt-5 space-y-3">
                {snapshot.workflows.slice(0, 6).map((workflow) => {
                  const linkedWeeklyPlan =
                    workflow.output_ref_id ? weeklyPlanById.get(workflow.output_ref_id) : null;
                  return (
                    <article
                      key={workflow.id}
                      className={`rounded-2xl border px-4 py-4 ${workflowStatusClass(workflow.status)}`}
                    >
                      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                        <div>
                          <p className="text-sm font-semibold uppercase tracking-[0.18em]">
                            {workflow.workflow_type.replaceAll("_", " ")}
                          </p>
                          <p className="mt-2 text-sm">
                            Draft: {activeDraft && workflow.planning_input_draft_id === activeDraft.id ? "Current draft" : workflow.planning_input_draft_id ?? "None"}
                          </p>
                          <p className="mt-1 text-sm">
                            Status: {workflow.status} · Progress: {workflow.progress_percent}%
                          </p>
                          <p className="mt-1 text-sm">
                            Provider: {workflow.provider_name ?? "mock"} · Model:{" "}
                            {workflow.provider_model ?? "mock"} · Prompt:{" "}
                            {workflow.prompt_version ?? "instructional-plan-v2"}
                          </p>
                          <p className="mt-1 text-sm">
                            Retries: {workflow.retry_count} / {workflow.max_retries} · Cost:{" "}
                            {formatCurrencyCents(workflow.estimated_cost_cents_total)}
                          </p>
                          <p className="mt-1 text-sm">
                            Worker: {workflow.leased_by_worker ?? "Waiting for worker"} · Last heartbeat:{" "}
                            {workflow.heartbeat_at ? formatDateTime(workflow.heartbeat_at) : "Not started"}
                          </p>
                          {workflow.status === "failed" ? (
                            <p className="mt-2 text-sm">
                              Failure explanation: {workflow.error_message ?? "No failure details recorded yet."}
                            </p>
                          ) : workflow.status === "queued" && workflow.retry_count > 0 ? (
                            <p className="mt-2 text-sm">
                              Retry scheduled after worker failure. Last error code:{" "}
                              {workflow.last_error_code ?? "pending"}.
                            </p>
                          ) : workflow.status === "running" ? (
                            <p className="mt-2 text-sm">
                              Worker timeout window:{" "}
                              {workflow.timeout_at ? formatDateTime(workflow.timeout_at) : "Pending"}.
                            </p>
                          ) : (
                            <p className="mt-2 text-sm">
                              Retry and failure detail placeholders remain visible here for worker-managed
                              executions.
                            </p>
                          )}
                          <p className="mt-1 text-xs opacity-80">
                            Created {formatDateTime(workflow.created_at)}
                          </p>
                          {workflow.error_message ? (
                            <p className="mt-2 text-sm font-medium">{workflow.error_message}</p>
                          ) : null}
                        </div>
                        {workflow.status === "completed" && linkedWeeklyPlan ? (
                          <Link
                            href={`/teacher-assist/weekly-planning/plans?id=${linkedWeeklyPlan.id}`}
                            className="ta-button-secondary"
                          >
                            View plan artifact
                          </Link>
                        ) : null}
                      </div>
                    </article>
                  );
                })}
              </div>
            ) : (
              <div className="mt-5 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-600">
                No TeacherAssist workflows have been started yet.
              </div>
            )}
          </article>

          <article className="ta-panel p-6">
            <h2 className="text-xl font-semibold text-slate-900">Saved planning drafts</h2>
            {loading ? (
              <p className="mt-5 text-sm text-slate-600">Loading planning drafts...</p>
            ) : snapshot && snapshot.drafts.length > 0 ? (
              <div className="mt-5 space-y-4">
                {snapshot.drafts.map((draft) => (
                  <article key={draft.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                    <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                      <div>
                        <p className="text-base font-semibold text-slate-900">
                            {draft.plan_title || draft.title || "Untitled planning draft"}
                        </p>
                        <p className="mt-2 text-sm leading-6 text-slate-600">
                          {draft.notes || "No teacher notes saved yet."}
                        </p>
                        <p className="mt-3 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                           {formatPlanningScope(draft.planning_scope)} ·{" "}
                           {schoolYearTitleById.get(draft.school_year_id ?? "") || "No school year"} ·{" "}
                           {gradingPeriodTitleById.get(draft.grading_period_id ?? "") || "No grading period"} ·{" "}
                           {classNameById.get(draft.class_id ?? "") || "No class"}
                        </p>
                        <p className="mt-2 text-xs text-slate-500">
                          {draft.module_title || "No module title"} ·{" "}
                          {draft.start_date && draft.end_date
                            ? `${formatDate(draft.start_date)} - ${formatDate(draft.end_date)}`
                            : "No date range"}{" "}
                          · {draft.estimated_weeks ?? "-"} week(s) ·{" "}
                          {draft.instructional_days_count ?? "-"} instructional day(s)
                        </p>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {draft.subject_ids.length > 0 ? (
                            draft.subject_ids.map((subjectId) => (
                              <span
                                key={subjectId}
                                className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-700"
                              >
                                {subjectNameById.get(subjectId) ?? "Subject"}
                              </span>
                            ))
                          ) : (
                            <span className="text-sm text-slate-500">No subjects attached yet.</span>
                          )}
                        </div>
                        <div className="mt-3 flex flex-wrap gap-2">
                          {draft.resource_ids.length > 0
                            ? draft.resource_ids.map((resourceId) => (
                                <span
                                  key={resourceId}
                                  className="rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-700"
                                >
                                  {resourceTitleById.get(resourceId) ?? "Resource"}
                                </span>
                              ))
                            : null}
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <span
                          className={`rounded-full px-3 py-1 text-xs font-semibold ${
                            draft.status === "ready"
                              ? "bg-emerald-100 text-emerald-700"
                              : "bg-amber-100 text-amber-700"
                          }`}
                        >
                          {draft.status}
                        </span>
                        <button
                          type="button"
                          className="ta-button-secondary"
                          onClick={() => {
                            setActiveDraftId(draft.id);
                            setDraftForm(draftFormFromDraft(draft));
                            setMessage({
                              tone: "info",
                              text: "Draft saved. Review context before generation.",
                            });
                          }}
                        >
                          Open
                        </button>
                      </div>
                    </div>
                    <p className="mt-4 text-xs text-slate-500">Updated {formatDate(draft.updated_at)}</p>
                  </article>
                ))}
              </div>
            ) : (
              <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
                 Create a draft after you upload curriculum resources, organize pacing guides, and
                 select scope, duration, standards, notes, and subjects for the planning window.
              </div>
            )}
          </article>
        </div>
      </section>
    </div>
  );
}
