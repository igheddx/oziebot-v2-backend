type TeacherAssistPageProps = {
  title: string;
  description: string;
};

export function TeacherAssistPage({ title, description }: TeacherAssistPageProps) {
  return (
    <div className="space-y-5">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist AI
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            {title}
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">{description}</p>
          <div className="mt-6 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
            The current TeacherAssist foundation stops at structured context and setup. Generation,
            grading, OCR, exports, and workflow jobs still arrive in later phases.
          </div>
        </div>
      </section>

      <section className="grid gap-4 lg:grid-cols-3">
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">What exists now</h2>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              Product access, the separate TeacherAssist shell, academic setup, resource metadata,
              pacing guides, and planning draft save are in place.
            </p>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">What is intentionally missing</h2>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              AI generation, OCR, grading, mastery, exports, newsletters, and workflow jobs are all
              deferred to later phases.
            </p>
        </article>
        <article className="ta-panel p-5">
          <h2 className="text-lg font-semibold text-slate-900">Current expectation</h2>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              Use this workspace to prepare structured instructional context without affecting the
              trading experience or triggering generation.
            </p>
          </article>
        </section>
      </div>
  );
}
