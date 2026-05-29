import Link from "next/link";

import type { TeacherAssistOnboardingChecklist } from "@/lib/teacher-assist-types";

export function TeacherAssistOnboardingChecklistPanel({
  checklist,
}: {
  checklist: TeacherAssistOnboardingChecklist | null;
}) {
  if (!checklist || checklist.is_complete) {
    return null;
  }

  return (
    <article className="ta-panel p-5 sm:p-6">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">Setup checklist</h2>
          <p className="mt-1 text-sm text-slate-600">
            Complete these steps to unlock the full TeacherAssist workflow.
          </p>
        </div>
        <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
          {checklist.completed_count}/{checklist.total_count}
        </span>
      </div>
      <ul className="mt-4 space-y-2">
        {checklist.items.map((item) => (
          <li
            key={item.key}
            className={`flex flex-wrap items-center justify-between gap-3 rounded-xl border px-3 py-3 ${
              item.complete ? "border-emerald-200 bg-emerald-50" : "border-slate-200 bg-white"
            }`}
          >
            <div className="flex items-center gap-3">
              <span
                className={`inline-flex h-6 w-6 items-center justify-center rounded-full text-xs font-bold ${
                  item.complete ? "bg-emerald-600 text-white" : "bg-slate-200 text-slate-600"
                }`}
              >
                {item.complete ? "✓" : "•"}
              </span>
              <span className="text-sm font-medium text-slate-900">{item.title}</span>
            </div>
            {!item.complete ? (
              <Link href={item.navigation_href} className="ta-button-secondary text-sm">
                {item.navigation_label}
              </Link>
            ) : null}
          </li>
        ))}
      </ul>
    </article>
  );
}
