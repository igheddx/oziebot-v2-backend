"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import { fetchMyCatalogContext } from "@/lib/education-catalog-api";
import type { TeacherCatalogContext } from "@/lib/education-catalog-types";

export function TeacherAssistEducationCatalogViewScreen() {
  const [context, setContext] = useState<TeacherCatalogContext | null>(null);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);

  useEffect(() => {
    void fetchMyCatalogContext()
      .then(setContext)
      .catch((nextError) => {
        setPageError(nextError instanceof Error ? nextError.message : "Could not load education catalog.");
      })
      .finally(() => setLoading(false));
  }, []);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-5 sm:p-6">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.18em] text-sky-700">Education Catalog</p>
          <h1 className="mt-2 text-2xl font-semibold text-slate-900">Your school instructional catalog</h1>
          <p className="mt-1 text-sm text-slate-600">
            Read-only view of state, district, school, grades, subjects, objectives, and curriculum resources assigned to you.
          </p>
        </div>
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load catalog" message={pageError} />

      {loading ? (
        <section className="ta-panel p-6 text-sm text-slate-600">Loading catalog context...</section>
      ) : !context?.assignment ? (
        <section className="ta-panel p-6 text-sm text-slate-600">
          No active school assignment found. Contact a TeacherAssist administrator to be assigned to a school.
        </section>
      ) : (
        <>
          <section className="ta-panel p-6">
            <h2 className="text-lg font-semibold text-slate-900">Assignment</h2>
            <p className="mt-2 text-sm text-slate-700">
              {context.assignment.state.name} · {context.assignment.district.name} · {context.assignment.school.name}
            </p>
          </section>

          <section className="grid gap-4 lg:grid-cols-2">
            <article className="ta-panel p-5">
              <h2 className="text-lg font-semibold text-slate-900">Grades</h2>
              <ul className="mt-3 space-y-2 text-sm text-slate-700">
                {context.grades.map((grade) => (
                  <li key={grade.id}>{grade.display_name} ({grade.grade_code})</li>
                ))}
              </ul>
            </article>
            <article className="ta-panel p-5">
              <h2 className="text-lg font-semibold text-slate-900">Subjects</h2>
              <ul className="mt-3 space-y-2 text-sm text-slate-700">
                {context.subjects.map((subject) => (
                  <li key={subject.id}>{subject.display_name} ({subject.subject_code})</li>
                ))}
              </ul>
            </article>
          </section>

          <section className="ta-panel p-6">
            <h2 className="text-lg font-semibold text-slate-900">Learning objectives</h2>
            <div className="mt-4 space-y-3">
              {context.objectives.map((objective) => (
                <div key={objective.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <p className="font-semibold text-slate-900">{objective.objective_id}</p>
                  <p className="mt-1 text-slate-600">
                    Grade {objective.grade_level} · {objective.subject_code} · {objective.coverage_type}
                  </p>
                  <p className="mt-2 text-slate-700">{objective.description}</p>
                </div>
              ))}
            </div>
          </section>

          <section className="ta-panel p-6">
            <h2 className="text-lg font-semibold text-slate-900">Curriculum resources</h2>
            <div className="mt-4 space-y-3">
              {context.resources.map((resource) => (
                <div key={resource.id} className="rounded-xl border border-slate-200 px-4 py-3 text-sm">
                  <p className="font-semibold text-slate-900">{resource.title}</p>
                  <p className="mt-1 text-slate-600">
                    {resource.resource_type} · Grade {resource.grade_level} · {resource.subject_code}
                  </p>
                  {resource.description ? <p className="mt-2 text-slate-700">{resource.description}</p> : null}
                </div>
              ))}
            </div>
          </section>
        </>
      )}

      <p className="text-sm text-slate-600">
        Administrators manage catalog data in{" "}
        <Link href="/teacher-assist/administration/education-catalog" className="font-medium text-sky-700">
          Education Catalog administration
        </Link>
        .
      </p>
    </div>
  );
}
