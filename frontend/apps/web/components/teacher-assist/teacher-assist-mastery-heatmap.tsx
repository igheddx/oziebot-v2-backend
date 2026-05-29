"use client";

import { useMemo, useState } from "react";

import type {
  MasteryHeatmapCell,
  MasteryMatrixHeatmap,
  MasteryMatrixReteachInsights,
  MasteryStandardInsight,
  StudentMasterySummary,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "Not confirmed";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function cellClasses(level: string) {
  switch (level) {
    case "advanced":
    case "mastery":
      return "bg-emerald-100 text-emerald-950 border-emerald-200";
    case "developing":
      return "bg-amber-100 text-amber-950 border-amber-200";
    case "beginning":
      return "bg-rose-100 text-rose-950 border-rose-200";
    default:
      return "bg-slate-100 text-slate-600 border-slate-200";
  }
}

function statusBadgeClasses(status: string) {
  switch (status) {
    case "healthy":
      return "bg-emerald-50 text-emerald-800";
    case "monitor":
      return "bg-sky-50 text-sky-800";
    case "reteach_recommended":
      return "bg-amber-50 text-amber-900";
    case "critical_attention":
      return "bg-rose-50 text-rose-900";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

function InsightCard({
  title,
  items,
  emptyLabel,
  showReteachActions = false,
  onCreateReteachPlan,
  reteachActionBusyStandardId,
}: {
  title: string;
  items: MasteryStandardInsight[];
  emptyLabel: string;
  showReteachActions?: boolean;
  onCreateReteachPlan?: (standardId: string) => void;
  reteachActionBusyStandardId?: string | null;
}) {
  return (
    <article className="rounded-2xl border border-slate-200 bg-white p-4">
      <h4 className="text-sm font-semibold text-slate-900">{title}</h4>
      {items.length === 0 ? (
        <p className="mt-2 text-sm text-slate-500">{emptyLabel}</p>
      ) : (
        <ul className="mt-3 space-y-2">
          {items.slice(0, 5).map((item) => (
            <li key={item.standard_id} className="rounded-xl bg-slate-50 px-3 py-2 text-sm">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span className="font-medium text-slate-900">{item.standard_code ?? "Standard"}</span>
                <span className={`rounded-full px-2 py-0.5 text-xs font-semibold ${statusBadgeClasses(item.operational_status)}`}>
                  {labelize(item.operational_status)}
                </span>
              </div>
              <p className="mt-1 text-xs text-slate-600">
                {Math.round(item.mastery_percentage * 100)}% mastery · {item.total_committed_evaluations} committed
              </p>
              {showReteachActions && onCreateReteachPlan ? (
                <button
                  type="button"
                  className="mt-2 rounded-lg bg-sky-700 px-3 py-1.5 text-xs font-semibold text-white hover:bg-sky-800 disabled:opacity-60"
                  disabled={reteachActionBusyStandardId === item.standard_id}
                  onClick={() => onCreateReteachPlan(item.standard_id)}
                >
                  {reteachActionBusyStandardId === item.standard_id ? "Creating…" : "Create reteach plan"}
                </button>
              ) : null}
            </li>
          ))}
        </ul>
      )}
    </article>
  );
}

export function TeacherAssistMasteryHeatmap({
  heatmap,
  reteachInsights,
  studentSummary,
  onSelectStudent,
  onSelectCell,
  onCreateReteachPlan,
  reteachActionBusyStandardId,
}: {
  heatmap: MasteryMatrixHeatmap | null;
  reteachInsights: MasteryMatrixReteachInsights | null;
  studentSummary: StudentMasterySummary | null;
  onSelectStudent: (studentNumber: number) => void;
  onSelectCell: (studentNumber: number, cell: MasteryHeatmapCell) => void;
  onCreateReteachPlan?: (standardId: string) => void;
  reteachActionBusyStandardId?: string | null;
}) {
  const [hoveredCell, setHoveredCell] = useState<{
    studentNumber: number;
    cell: MasteryHeatmapCell;
    standardCode: string | null;
  } | null>(null);

  const standardById = useMemo(() => {
    const map = new Map<string, { code: string | null; operational_status: string }>();
    heatmap?.standards.forEach((standard) => {
      map.set(standard.standard_id, {
        code: standard.standard_code,
        operational_status: standard.operational_status,
      });
    });
    return map;
  }, [heatmap]);

  if (!heatmap) {
    return <p className="text-sm text-slate-500">Select a matrix to load the mastery heatmap.</p>;
  }

  return (
    <div className="space-y-6">
      {reteachInsights ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <InsightCard
            title="Standards needing reteach"
            items={reteachInsights.panels.standards_needing_reteach}
            emptyLabel="No reteach-recommended standards right now."
            showReteachActions
            onCreateReteachPlan={onCreateReteachPlan}
            reteachActionBusyStandardId={reteachActionBusyStandardId}
          />
          <InsightCard
            title="Strongest standards"
            items={reteachInsights.panels.strongest_standards}
            emptyLabel="No assessed standards yet."
          />
          <InsightCard
            title="Weakest standards"
            items={reteachInsights.panels.weakest_standards}
            emptyLabel="No assessed standards yet."
            showReteachActions
            onCreateReteachPlan={onCreateReteachPlan}
            reteachActionBusyStandardId={reteachActionBusyStandardId}
          />
          <InsightCard
            title="Improving standards"
            items={reteachInsights.panels.improving_standards}
            emptyLabel="No improving trend detected yet."
          />
          <InsightCard
            title="Declining standards"
            items={reteachInsights.panels.declining_standards}
            emptyLabel="No declining trend detected yet."
          />
          <InsightCard
            title="Unassessed standards"
            items={reteachInsights.panels.unassessed_standards}
            emptyLabel="All tracked standards have committed evaluations."
          />
        </div>
      ) : null}

      <div className="overflow-x-auto rounded-2xl border border-slate-200 bg-white">
        <table className="min-w-full border-collapse text-left text-sm">
          <thead>
            <tr className="border-b border-slate-200 bg-slate-50">
              <th className="sticky left-0 z-10 bg-slate-50 px-3 py-3 font-semibold text-slate-700">Student</th>
              {heatmap.standards.map((standard) => (
                <th key={standard.standard_id} className="min-w-[7rem] px-2 py-3 align-top">
                  <div className="space-y-1">
                    <p className="font-semibold text-slate-900">{standard.standard_code ?? "Standard"}</p>
                    <span
                      className={`inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold ${statusBadgeClasses(standard.operational_status)}`}
                    >
                      {labelize(standard.operational_status)}
                    </span>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {heatmap.rows.length === 0 ? (
              <tr>
                <td colSpan={Math.max(heatmap.standards.length, 1) + 1} className="px-3 py-6 text-slate-500">
                  No committed mastery evaluations yet. Draft evaluations are excluded from analytics.
                </td>
              </tr>
            ) : (
              heatmap.rows.map((row) => (
                <tr key={row.student_number} className="border-b border-slate-100">
                  <td className="sticky left-0 z-10 bg-white px-3 py-2">
                    <button
                      type="button"
                      className="font-semibold text-sky-700 hover:text-sky-900"
                      onClick={() => onSelectStudent(row.student_number)}
                    >
                      STUDENT #{row.student_number}
                    </button>
                  </td>
                  {row.cells.map((cell) => {
                    const standardMeta = standardById.get(cell.standard_id);
                    return (
                      <td key={`${row.student_number}-${cell.standard_id}`} className="px-2 py-2">
                        <button
                          type="button"
                          className={`h-10 w-full rounded-lg border text-xs font-semibold ${cellClasses(cell.mastery_level)}`}
                          onMouseEnter={() =>
                            setHoveredCell({
                              studentNumber: row.student_number,
                              cell,
                              standardCode: standardMeta?.code ?? null,
                            })
                          }
                          onMouseLeave={() => setHoveredCell(null)}
                          onClick={() => onSelectCell(row.student_number, cell)}
                        >
                          {cell.mastery_level === "not_assessed" ? "—" : labelize(cell.mastery_level)}
                        </button>
                      </td>
                    );
                  })}
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {hoveredCell ? (
        <article className="rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
          <p className="font-semibold text-slate-900">
            STUDENT #{hoveredCell.studentNumber} · {hoveredCell.standardCode ?? "Standard"}
          </p>
          <p className="mt-1">Level: {labelize(hoveredCell.cell.mastery_level)}</p>
          <p className="mt-1">Confirmed: {formatDateTime(hoveredCell.cell.confirmed_at)}</p>
          <p className="mt-1">Evaluations counted: {hoveredCell.cell.evaluation_count}</p>
          {hoveredCell.cell.needs_reteach ? (
            <p className="mt-2 font-medium text-amber-800">Below target mastery — review reteach insight.</p>
          ) : null}
        </article>
      ) : null}

      {studentSummary ? (
        <article className="rounded-2xl border border-sky-200 bg-sky-50 p-4">
          <h4 className="text-sm font-semibold text-sky-950">
            STUDENT #{studentSummary.student_number} drill-down
          </h4>
          <p className="mt-1 text-sm text-sky-900">
            Trend {labelize(studentSummary.trend)} · {studentSummary.active_evaluation_count} active evaluations ·{" "}
            {studentSummary.standards_needing_attention.length} standard(s) needing attention
          </p>
        </article>
      ) : null}
    </div>
  );
}
