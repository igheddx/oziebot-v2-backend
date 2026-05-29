"use client";

type TeacherAssistDashboardHeaderProps = {
  eyebrow?: string;
  title: string;
  description?: string;
  actions?: React.ReactNode;
};

export function TeacherAssistDashboardHeader({
  eyebrow,
  title,
  description,
  actions,
}: TeacherAssistDashboardHeaderProps) {
  return (
    <header className="ta-panel flex flex-wrap items-start justify-between gap-3 p-4">
      <div className="min-w-0 flex-1">
        {eyebrow ? (
          <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">{eyebrow}</p>
        ) : null}
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">{title}</h1>
        {description ? <p className="mt-1 text-sm text-slate-600">{description}</p> : null}
      </div>
      {actions ? <div className="flex flex-wrap items-center gap-2">{actions}</div> : null}
    </header>
  );
}
