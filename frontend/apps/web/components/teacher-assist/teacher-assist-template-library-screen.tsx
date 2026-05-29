"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { applyWeekTemplate, fetchWeekTemplates } from "@/lib/pacing-guide-api";

type TemplateRow = {
  id: string;
  name: string;
  description?: string | null;
  subject?: string | null;
  grade_level?: string | null;
  artifact_type: string;
  template_type: string;
  visibility: string;
  updated_at: string;
  template_data?: Record<string, unknown>;
};

export function TeacherAssistTemplateLibraryScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [templates, setTemplates] = useState<TemplateRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [busyId, setBusyId] = useState<string | null>(null);
  const [filters, setFilters] = useState({
    subject: "",
    grade_level: "",
    artifact_type: "",
    visibility: "",
  });
  const [applyPeriodId, setApplyPeriodId] = useState("");
  const [previewId, setPreviewId] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("templateLibrary");
    try {
      const params = Object.fromEntries(
        Object.entries(filters).filter(([, value]) => value.trim().length > 0),
      );
      const rows = (await fetchWeekTemplates(params)) as TemplateRow[];
      setTemplates(rows);
    } catch (nextError) {
      setSectionAlert(
        "templateLibrary",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load templates.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, filters, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const preview = useMemo(
    () => templates.find((row) => row.id === previewId) ?? null,
    [previewId, templates],
  );

  async function handleApply(templateId: string) {
    if (!applyPeriodId.trim()) {
      setSectionAlert("templateLibrary", sectionError("Enter a target pacing week ID to apply this template.", "Target week required"));
      return;
    }
    setBusyId(templateId);
    clearSectionAlert("templateLibrary");
    try {
      await applyWeekTemplate(templateId, applyPeriodId.trim());
      setSectionAlert("templateLibrary", sectionSuccess("Template applied to the selected week.", "Template applied"));
    } catch (nextError) {
      setSectionAlert(
        "templateLibrary",
        sectionError(nextError instanceof Error ? nextError.message : "Could not apply template.", "Apply failed"),
      );
    } finally {
      setBusyId(null);
    }
  }

  return (
    <div className="space-y-4">
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Template Library</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">Reusable instructional templates</h1>
        <p className="mt-1 text-sm text-slate-600">
          Preview, copy, and apply week, assignment, quiz, rubric, and newsletter templates without starting from scratch.
        </p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Link href="/teacher-assist/home" className="ta-button-secondary text-xs">
            Home
          </Link>
          <Link href="/teacher-assist/planning/pacing-guides/workspace" className="ta-button-secondary text-xs">
            Pacing workspace
          </Link>
        </div>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("templateLibrary")} onDismiss={() => clearSectionAlert("templateLibrary")} />

      <section className="ta-panel grid gap-3 p-4 md:grid-cols-4">
        <label className="text-xs">
          <span className="font-semibold text-slate-600">Subject</span>
          <input
            className="ta-input mt-1 w-full"
            value={filters.subject}
            onChange={(event) => setFilters((current) => ({ ...current, subject: event.target.value }))}
            placeholder="Math"
          />
        </label>
        <label className="text-xs">
          <span className="font-semibold text-slate-600">Grade</span>
          <input
            className="ta-input mt-1 w-full"
            value={filters.grade_level}
            onChange={(event) => setFilters((current) => ({ ...current, grade_level: event.target.value }))}
            placeholder="5"
          />
        </label>
        <label className="text-xs">
          <span className="font-semibold text-slate-600">Artifact type</span>
          <select
            className="ta-input mt-1 w-full"
            value={filters.artifact_type}
            onChange={(event) => setFilters((current) => ({ ...current, artifact_type: event.target.value }))}
          >
            <option value="">All</option>
            <option value="WEEK">Week</option>
            <option value="ASSIGNMENT">Assignment</option>
            <option value="QUIZ">Quiz</option>
            <option value="RUBRIC">Rubric</option>
            <option value="NEWSLETTER">Newsletter</option>
          </select>
        </label>
        <label className="text-xs">
          <span className="font-semibold text-slate-600">Visibility</span>
          <select
            className="ta-input mt-1 w-full"
            value={filters.visibility}
            onChange={(event) => setFilters((current) => ({ ...current, visibility: event.target.value }))}
          >
            <option value="">All</option>
            <option value="PRIVATE">Private</option>
            <option value="TEAM">Team</option>
            <option value="SCHOOL">School</option>
            <option value="DISTRICT">District</option>
          </select>
        </label>
      </section>

      <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
        <article className="ta-panel p-4">
          <h2 className="text-base font-semibold text-slate-900">Templates</h2>
          {loading ? (
            <p className="mt-2 text-sm text-slate-600">Loading templates...</p>
          ) : templates.length === 0 ? (
            <p className="mt-2 text-sm text-slate-500">No templates match these filters yet.</p>
          ) : (
            <div className="mt-3 space-y-2">
              {templates.map((row) => (
                <div key={row.id} className="rounded-xl border border-slate-200 px-3 py-2.5">
                  <div className="flex flex-wrap items-start justify-between gap-2">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{row.name}</p>
                      <p className="text-xs text-slate-500">
                        {row.artifact_type} · {row.template_type} · {row.visibility}
                        {row.subject ? ` · ${row.subject}` : ""}
                        {row.grade_level ? ` · Grade ${row.grade_level}` : ""}
                      </p>
                    </div>
                    <div className="flex flex-wrap gap-2">
                      <button type="button" className="ta-button-secondary text-xs" onClick={() => setPreviewId(row.id)}>
                        Preview
                      </button>
                      <button
                        type="button"
                        className="ta-button-primary text-xs"
                        disabled={busyId === row.id}
                        onClick={() => void handleApply(row.id)}
                      >
                        {busyId === row.id ? "Applying..." : "Apply"}
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel space-y-3 p-4">
          <h2 className="text-base font-semibold text-slate-900">Apply to week</h2>
          <label className="block text-xs">
            <span className="font-semibold text-slate-600">Target pacing week ID</span>
            <input
              className="ta-input mt-1 w-full"
              value={applyPeriodId}
              onChange={(event) => setApplyPeriodId(event.target.value)}
              placeholder="Paste period_id from week workspace URL"
            />
          </label>
          <p className="text-xs text-slate-500">
            Open a week workspace and copy the `period_id` query parameter, then apply a template here.
          </p>
          {preview ? (
            <div className="rounded-xl border border-slate-200 p-3">
              <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Preview</p>
              <p className="mt-1 text-sm font-semibold text-slate-900">{preview.name}</p>
              {preview.description ? <p className="mt-1 text-sm text-slate-600">{preview.description}</p> : null}
              <pre className="mt-2 max-h-64 overflow-auto rounded-lg bg-slate-50 p-2 text-[11px] text-slate-700">
                {JSON.stringify(preview.template_data ?? {}, null, 2)}
              </pre>
            </div>
          ) : (
            <p className="text-sm text-slate-500">Select a template to preview stored week context.</p>
          )}
        </article>
      </section>
    </div>
  );
}
