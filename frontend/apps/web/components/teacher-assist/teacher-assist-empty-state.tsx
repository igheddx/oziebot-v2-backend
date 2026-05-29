import Link from "next/link";

export function TeacherAssistEmptyState({
  title,
  description,
  whyItMatters,
  actionLabel,
  actionHref,
}: {
  title: string;
  description: string;
  whyItMatters?: string;
  actionLabel?: string;
  actionHref?: string;
}) {
  return (
    <article className="rounded-2xl border border-dashed border-slate-300 bg-slate-50/80 px-5 py-6 text-center sm:px-8">
      <h3 className="text-lg font-semibold text-slate-900">{title}</h3>
      <p className="mx-auto mt-2 max-w-xl text-sm leading-6 text-slate-600">{description}</p>
      {whyItMatters ? (
        <p className="mx-auto mt-3 max-w-xl text-sm leading-6 text-slate-500">
          <span className="font-medium text-slate-700">Why it matters:</span> {whyItMatters}
        </p>
      ) : null}
      {actionLabel && actionHref ? (
        <div className="mt-5">
          <Link href={actionHref} className="ta-button-primary inline-flex">
            {actionLabel}
          </Link>
        </div>
      ) : null}
    </article>
  );
}
