import Link from "next/link";

export type TeacherAssistCrossLink = {
  label: string;
  href: string;
  detail?: string;
};

export function TeacherAssistCrossLinks({
  title = "Related workflows",
  links,
}: {
  title?: string;
  links: TeacherAssistCrossLink[];
}) {
  if (links.length === 0) return null;
  return (
    <section className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
      <h3 className="text-sm font-semibold uppercase tracking-wide text-slate-500">{title}</h3>
      <div className="mt-3 flex flex-wrap gap-2">
        {links.map((link) => (
          <Link
            key={`${link.href}-${link.label}`}
            href={link.href}
            className="inline-flex flex-col rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm font-semibold text-sky-800 transition hover:border-sky-200 hover:bg-sky-50"
            title={link.detail}
          >
            {link.label}
            {link.detail ? <span className="mt-0.5 text-xs font-normal text-slate-500">{link.detail}</span> : null}
          </Link>
        ))}
      </div>
    </section>
  );
}
