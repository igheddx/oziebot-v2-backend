import Link from "next/link";

import type { TeacherAssistWorkflowProgressCard } from "@/lib/teacher-assist-types";

const STEP_LABELS: Record<string, string> = {
  lesson_plan: "Lesson Plan",
  assignment: "Assignment",
  student_work: "Student Work",
  grading_review: "Grading Review",
  gradebook: "Gradebook",
  mastery: "Mastery",
};

function stepClasses(status: string) {
  switch (status) {
    case "complete":
      return "border-emerald-200 bg-emerald-50 text-emerald-900";
    case "in_progress":
      return "border-amber-200 bg-amber-50 text-amber-900";
    default:
      return "border-slate-200 bg-white text-slate-500";
  }
}

export function TeacherAssistWorkflowProgressCardView({ card }: { card: TeacherAssistWorkflowProgressCard }) {
  const steps = Object.entries(card.steps);
  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4 sm:p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-base font-semibold text-slate-900">{card.assignment_title}</h3>
          {card.source_plan_title ? (
            <p className="mt-1 text-sm text-slate-600">From plan: {card.source_plan_title}</p>
          ) : null}
        </div>
        <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-800">
          {card.progress_percent}% complete
        </span>
      </div>
      <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
        {steps.map(([key, status]) => (
          <div
            key={key}
            className={`rounded-xl border px-2 py-2 text-center text-[11px] font-semibold sm:text-xs ${stepClasses(status)}`}
          >
            {STEP_LABELS[key] ?? key}
          </div>
        ))}
      </div>
      <div className="mt-4 flex flex-wrap gap-2">
        <Link href={card.navigation_href} className="ta-button-secondary text-sm">
          Open assignment
        </Link>
        {card.source_plan_id ? (
          <Link
            href={`/teacher-assist/weekly-planning/plans?id=${card.source_plan_id}`}
            className="ta-button-secondary text-sm"
          >
            Open plan
          </Link>
        ) : null}
        <Link href={`/teacher-assist/gradebook?assignment_id=${card.assignment_id}`} className="ta-button-secondary text-sm">
          Gradebook
        </Link>
        <Link href={`/teacher-assist/mastery`} className="ta-button-secondary text-sm">
          Mastery impact
        </Link>
      </div>
    </article>
  );
}
