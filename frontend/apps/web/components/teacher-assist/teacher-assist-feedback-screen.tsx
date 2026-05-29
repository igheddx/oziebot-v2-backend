"use client";

import { useCallback, useEffect, useState } from "react";

import { TeacherAssistDashboardHeader } from "@/components/teacher-assist/teacher-assist-dashboard-header";
import {
  TeacherAssistInlineAlert,
  sectionError,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { createPilotFeedback, fetchPilotFeedback } from "@/lib/pilot-api";

const CATEGORIES = ["bug", "usability", "feature_request", "performance", "data", "documentation", "other"];
const SEVERITIES = ["low", "medium", "high", "critical"];

type FeedbackRow = {
  id?: string;
  category?: string;
  severity?: string;
  feature_area?: string;
  description?: string;
  requested_improvement?: string | null;
  status?: string;
  created_at?: string;
};

export function TeacherAssistFeedbackScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [rows, setRows] = useState<FeedbackRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [category, setCategory] = useState("usability");
  const [severity, setSeverity] = useState("medium");
  const [featureArea, setFeatureArea] = useState("");
  const [description, setDescription] = useState("");
  const [improvement, setImprovement] = useState("");

  const refresh = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("feedback");
    try {
      const payload = await fetchPilotFeedback(true);
      setRows(payload as FeedbackRow[]);
    } catch (nextError) {
      setSectionAlert(
        "feedback",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load feedback.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const submit = async () => {
    if (!featureArea.trim() || !description.trim()) return;
    setBusy(true);
    clearSectionAlert("feedback");
    try {
      await createPilotFeedback({
        category,
        severity,
        feature_area: featureArea.trim(),
        description: description.trim(),
        requested_improvement: improvement.trim() || undefined,
      });
      setFeatureArea("");
      setDescription("");
      setImprovement("");
      await refresh();
    } catch (nextError) {
      setSectionAlert(
        "feedback",
        sectionError(nextError instanceof Error ? nextError.message : "Could not submit feedback.", "Submit failed"),
      );
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-4">
      <TeacherAssistDashboardHeader
        eyebrow="Pilot"
        title="Teacher feedback"
        description="Share bugs, usability issues, and improvement ideas during the pilot. Your feedback helps us prioritize fixes."
      />

      <TeacherAssistInlineAlert alert={getSectionAlert("feedback")} onDismiss={() => clearSectionAlert("feedback")} />

      <section className="ta-panel grid gap-4 p-4 lg:grid-cols-2">
        <article>
          <h2 className="text-base font-semibold text-slate-900">Submit feedback</h2>
          <div className="mt-3 space-y-3">
            <label className="block text-sm">
              <span className="font-medium text-slate-700">Category</span>
              <select className="ta-input mt-1 w-full" value={category} onChange={(e) => setCategory(e.target.value)}>
                {CATEGORIES.map((value) => (
                  <option key={value} value={value}>
                    {value.replaceAll("_", " ")}
                  </option>
                ))}
              </select>
            </label>
            <label className="block text-sm">
              <span className="font-medium text-slate-700">Severity</span>
              <select className="ta-input mt-1 w-full" value={severity} onChange={(e) => setSeverity(e.target.value)}>
                {SEVERITIES.map((value) => (
                  <option key={value} value={value}>
                    {value}
                  </option>
                ))}
              </select>
            </label>
            <label className="block text-sm">
              <span className="font-medium text-slate-700">Feature area</span>
              <input
                className="ta-input mt-1 w-full"
                value={featureArea}
                onChange={(e) => setFeatureArea(e.target.value)}
                placeholder="e.g. Copilot, Instructional Week, Gradebook"
              />
            </label>
            <label className="block text-sm">
              <span className="font-medium text-slate-700">Description</span>
              <textarea
                className="ta-input mt-1 min-h-24 w-full"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What happened? What did you expect?"
              />
            </label>
            <label className="block text-sm">
              <span className="font-medium text-slate-700">Requested improvement (optional)</span>
              <textarea
                className="ta-input mt-1 min-h-20 w-full"
                value={improvement}
                onChange={(e) => setImprovement(e.target.value)}
              />
            </label>
            <button type="button" className="ta-button-primary" disabled={busy} onClick={() => void submit()}>
              {busy ? "Submitting..." : "Submit feedback"}
            </button>
          </div>
        </article>

        <article>
          <h2 className="text-base font-semibold text-slate-900">Your submissions</h2>
          <div className="mt-3 space-y-2">
            {loading ? (
              <p className="text-sm text-slate-500">Loading feedback...</p>
            ) : rows.length === 0 ? (
              <p className="text-sm text-slate-500">No feedback submitted yet.</p>
            ) : (
              rows.map((row) => (
                <div key={row.id} className="rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <p className="font-semibold text-slate-900">{row.feature_area}</p>
                    <span className="text-xs uppercase tracking-wide text-slate-500">{row.status}</span>
                  </div>
                  <p className="mt-1 text-xs text-slate-500">
                    {row.category} · {row.severity}
                  </p>
                  <p className="mt-1 text-slate-700">{row.description}</p>
                </div>
              ))
            )}
          </div>
        </article>
      </section>
    </div>
  );
}
