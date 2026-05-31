"use client";

import { useCallback, useEffect, useState } from "react";

import { TeacherAssistDashboardHeader } from "@/components/teacher-assist/teacher-assist-dashboard-header";
import {
  TeacherAssistInlineAlert,
  sectionError,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { fetchSeedValidation, fetchSystemHealth } from "@/lib/pilot-api";

export function TeacherAssistSystemHealthScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [health, setHealth] = useState<Record<string, unknown> | null>(null);
  const [seed, setSeed] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("health");
    try {
      const [healthPayload, seedPayload] = await Promise.all([fetchSystemHealth(), fetchSeedValidation()]);
      setHealth(healthPayload);
      setSeed(seedPayload);
    } catch (nextError) {
      setSectionAlert(
        "health",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load system health.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const users = (health?.users as Record<string, number>) ?? {};
  const jobs = (health?.jobs as Record<string, number>) ?? {};
  const storage = (health?.storage as Record<string, unknown>) ?? {};
  const copilot = (health?.copilot_usage as Record<string, number>) ?? {};
  const checks = (seed?.checks as Array<Record<string, unknown>>) ?? [];

  return (
    <div className="space-y-4">
      <TeacherAssistDashboardHeader
        eyebrow="Administration"
        title="System health"
        description="Operational summary for pilot monitoring — users, jobs, storage, copilot usage, and seed validation."
      />

      <TeacherAssistInlineAlert alert={getSectionAlert("health")} onDismiss={() => clearSectionAlert("health")} />

      {loading ? (
        <p className="text-sm text-slate-600">Loading system health...</p>
      ) : (
        <>
          <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
            {[
              { label: "Users", value: users.total ?? 0 },
              { label: "Active workflows", value: jobs.workflows_active ?? 0 },
              { label: "Failed workflows (30d)", value: jobs.workflows_failed_30d ?? 0 },
              { label: "Open pilot feedback", value: Number(health?.open_pilot_feedback ?? 0) },
            ].map((card) => (
              <article key={card.label} className="ta-panel p-4">
                <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{card.label}</p>
                <p className="mt-1 text-2xl font-semibold text-slate-900">{card.value}</p>
              </article>
            ))}
          </section>

          <section className="grid gap-4 lg:grid-cols-2">
            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">Storage & jobs</h2>
              <dl className="mt-3 space-y-2 text-sm">
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">Storage backend</dt>
                  <dd className="font-medium text-slate-900">{String(storage.backend ?? "—")}</dd>
                </div>
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">S3 bucket configured</dt>
                  <dd className="font-medium text-slate-900">{storage.s3_bucket_configured ? "Yes" : "No"}</dd>
                </div>
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">Failed extractions (30d)</dt>
                  <dd className="font-medium text-slate-900">{jobs.extractions_failed_30d ?? 0}</dd>
                </div>
              </dl>
            </article>

            <article className="ta-panel p-4">
              <h2 className="text-base font-semibold text-slate-900">Copilot usage (30d)</h2>
              <dl className="mt-3 space-y-2 text-sm">
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">Sessions</dt>
                  <dd className="font-medium text-slate-900">{copilot.sessions_30d ?? 0}</dd>
                </div>
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">Teacher messages</dt>
                  <dd className="font-medium text-slate-900">{copilot.teacher_messages_30d ?? 0}</dd>
                </div>
                <div className="flex justify-between gap-2">
                  <dt className="text-slate-500">AI usage events</dt>
                  <dd className="font-medium text-slate-900">{copilot.ai_usage_events_30d ?? 0}</dd>
                </div>
              </dl>
            </article>
          </section>

          <article className="ta-panel p-4">
            <h2 className="text-base font-semibold text-slate-900">Seed validation (Texas / LISD / Mason)</h2>
            <div className="mt-3 space-y-2">
              {checks.length === 0 ? (
                <p className="text-sm text-slate-500">No seed checks returned.</p>
              ) : (
                checks.map((row) => (
                  <div
                    key={String(row.name)}
                    className={`rounded-xl border px-3 py-2.5 text-sm ${
                      row.ok ? "border-emerald-100 bg-emerald-50" : "border-amber-100 bg-amber-50"
                    }`}
                  >
                    <p className="font-semibold text-slate-900">{String(row.name)}</p>
                    <p className="text-xs text-slate-600">{String(row.detail)}</p>
                  </div>
                ))
              )}
            </div>
          </article>
        </>
      )}
    </div>
  );
}
