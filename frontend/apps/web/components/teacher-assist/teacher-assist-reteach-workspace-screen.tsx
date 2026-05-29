"use client";

import Link from "next/link";
import { useCallback, useEffect, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { fetchReteachWorkspace } from "@/lib/instructional-loop-api";

type ObjectiveRow = {
  objective_code?: string;
  mastery_pct?: number;
  students_assessed?: number;
};

export function TeacherAssistReteachWorkspaceScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [workspace, setWorkspace] = useState<Awaited<ReturnType<typeof fetchReteachWorkspace>> | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("reteachWorkspace");
    try {
      const payload = await fetchReteachWorkspace();
      setWorkspace(payload);
    } catch (nextError) {
      setSectionAlert(
        "reteachWorkspace",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load reteach workspace.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const objectives = (workspace?.objectives_requiring_reteach ?? []) as ObjectiveRow[];
  const students = (workspace?.students_impacted ?? []) as Array<{ student_identifier?: string }>;
  const suggestedGroups = (workspace?.suggested_groups ?? []) as Array<{ title?: string; suggested_activities?: string[] }>;
  const supportGroups = (workspace?.support_groups ?? []) as Array<{ id?: string; title?: string; status?: string; member_count?: number }>;
  const plans = (workspace?.reteach_plans ?? []) as Array<{ id?: string; title?: string; status?: string }>;
  const history = (workspace?.prior_reteach_history ?? []) as Array<{
    before_mastery_pct?: number;
    after_mastery_pct?: number;
    improvement_pct?: number;
  }>;

  return (
    <div className="space-y-4">
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Instructional Loop</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">Reteach Workspace</h1>
        <p className="mt-1 text-sm text-slate-600">
          Review objectives needing support, suggested student groups, and open reteach plans. Nothing is applied automatically.
        </p>
        <div className="mt-3 flex flex-wrap gap-2">
          <Link href="/teacher-assist/mastery" className="ta-button-secondary text-xs">
            Mastery dashboard
          </Link>
          <Link href="/teacher-assist/reteach-plans" className="ta-button-secondary text-xs">
            Reteach plans
          </Link>
        </div>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("reteachWorkspace")} onDismiss={() => clearSectionAlert("reteachWorkspace")} />

      {loading || !workspace ? (
        <p className="text-sm text-slate-600">Loading reteach workspace...</p>
      ) : (
        <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Objectives requiring reteach</h2>
            {objectives.length === 0 ? (
              <p className="text-sm text-slate-500">No objectives below the mastery threshold yet.</p>
            ) : (
              objectives.map((row, index) => (
                <div key={`${row.objective_code}-${index}`} className="rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <p className="font-semibold text-slate-900">{row.objective_code ?? "Objective"}</p>
                  <p className="text-xs text-slate-500">
                    Mastery {row.mastery_pct ?? 0}% · {row.students_assessed ?? 0} students assessed
                  </p>
                </div>
              ))
            )}
            <h3 className="text-sm font-semibold text-slate-900">Students impacted</h3>
            {students.length === 0 ? (
              <p className="text-sm text-slate-500">No support signals yet.</p>
            ) : (
              <div className="flex flex-wrap gap-1.5">
                {students.map((row, index) => (
                  <span key={`${row.student_identifier}-${index}`} className="rounded-full bg-amber-50 px-2 py-0.5 text-xs font-semibold text-amber-800">
                    Student {row.student_identifier}
                  </span>
                ))}
              </div>
            )}
          </article>

          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Suggested groups & plans</h2>
            {suggestedGroups.map((row, index) => (
              <div key={`${row.title}-${index}`} className="rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                <p className="font-semibold text-slate-900">{row.title}</p>
                <p className="text-xs text-slate-500">{(row.suggested_activities ?? []).join(" · ")}</p>
              </div>
            ))}
            <h3 className="text-sm font-semibold text-slate-900">Saved support groups</h3>
            {supportGroups.length === 0 ? (
              <p className="text-sm text-slate-500">Create support groups from mastery data when ready.</p>
            ) : (
              supportGroups.map((row) => (
                <div key={row.id} className="rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <p className="font-semibold text-slate-900">{row.title}</p>
                  <p className="text-xs text-slate-500">
                    {row.status} · {row.member_count ?? 0} students
                  </p>
                </div>
              ))
            )}
            <h3 className="text-sm font-semibold text-slate-900">Open reteach plans</h3>
            {plans.length === 0 ? (
              <p className="text-sm text-slate-500">No open reteach plans.</p>
            ) : (
              plans.map((row) => (
                <div key={row.id} className="flex items-center justify-between gap-2 rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <span className="font-semibold text-slate-900">{row.title}</span>
                  <span className="text-xs uppercase text-slate-500">{row.status}</span>
                </div>
              ))
            )}
            {history.length > 0 ? (
              <>
                <h3 className="text-sm font-semibold text-slate-900">Prior reteach effectiveness</h3>
                {history.slice(0, 3).map((row, index) => (
                  <div key={`history-${index}`} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                    Before {row.before_mastery_pct ?? "—"}% → After {row.after_mastery_pct ?? "—"}% ({row.improvement_pct ?? "—"}% change)
                  </div>
                ))}
              </>
            ) : null}
          </article>
        </section>
      )}
    </div>
  );
}
