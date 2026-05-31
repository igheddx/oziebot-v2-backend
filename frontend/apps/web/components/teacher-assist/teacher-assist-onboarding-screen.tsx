"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistEmptyState } from "@/components/teacher-assist/teacher-assist-empty-state";
import { fetchTeacherAssistHomeWorkspace } from "@/lib/teacher-assist-api";
import type { TeacherAssistOnboardingProgress } from "@/lib/teacher-assist-types";

function OnboardingProgressBar({ progress }: { progress: TeacherAssistOnboardingProgress }) {
  return (
    <div>
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="font-medium text-slate-700">Setup progress</span>
        <span className="font-semibold text-sky-700">{progress.progress_percent}%</span>
      </div>
      <div className="mt-2 h-3 overflow-hidden rounded-full bg-slate-200">
        <div
          className="h-full rounded-full bg-sky-600 transition-all"
          style={{ width: `${progress.progress_percent}%` }}
        />
      </div>
      <p className="mt-2 text-xs text-slate-500">
        {progress.completed_count} of {progress.total_count} steps complete
      </p>
    </div>
  );
}

export function TeacherAssistOnboardingScreen() {
  const [onboarding, setOnboarding] = useState<TeacherAssistOnboardingProgress | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    fetchTeacherAssistHomeWorkspace()
      .then((data) => {
        if (active) setOnboarding(data.onboarding);
      })
      .catch((err: Error) => {
        if (active) setError(err.message);
      });
    return () => {
      active = false;
    };
  }, []);

  if (error) {
    return (
      <TeacherAssistAlert
        variant="error"
        title="Unable to load onboarding"
        description={error}
        actionLabel="Retry"
        onAction={() => window.location.reload()}
      />
    );
  }
  if (!onboarding) {
    return <p className="text-sm text-slate-600">Loading onboarding...</p>;
  }

  if (onboarding.is_complete) {
    return (
      <div className="space-y-6">
        <header className="ta-panel p-6 text-center">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-emerald-700">Setup complete</p>
          <h1 className="mt-2 text-3xl font-semibold text-slate-900">Your classroom is ready</h1>
          <p className="mx-auto mt-3 max-w-xl text-sm text-slate-600">
            Next: browse district pacing guides for your grade, copy one to your library, then start weekly planning
            for each subject.
          </p>
        </header>
        <section className="grid gap-4 md:grid-cols-3">
          {[
            {
              label: "1. Browse pacing guides",
              href: "/teacher-assist/pacing-guides",
              detail: "Find the district guide for your grade and subject.",
            },
            {
              label: "2. Open pacing workspace",
              href: "/teacher-assist/planning/pacing-guides/workspace",
              detail: "Set your copied guide active and pick the current week.",
            },
            {
              label: "3. Generate this week’s plan",
              href: "/teacher-assist/planning/weeks",
              detail: "Upload curriculum, attach resources, then click Generate Plan.",
            },
          ].map((action) => (
            <Link
              key={action.href}
              href={action.href}
              className="ta-panel p-5 transition hover:border-sky-300"
            >
              <span className="text-sm font-semibold text-sky-700">{action.label}</span>
              <p className="mt-2 text-xs leading-5 text-slate-600">{action.detail}</p>
            </Link>
          ))}
        </section>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <header className="ta-panel p-6">
        <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Get started</p>
        <h1 className="mt-2 text-2xl font-semibold text-slate-900">Guided onboarding</h1>
        <p className="mt-2 text-sm text-slate-600">
          Complete setup in three steps on this page, then browse pacing guides to begin weekly planning.
        </p>
        <div className="mt-5">
          <OnboardingProgressBar progress={onboarding} />
        </div>
      </header>

      <section className="ta-panel p-5">
        <h2 className="text-lg font-semibold text-slate-900">Setup steps</h2>
        <ul className="mt-4 space-y-3">
          {onboarding.steps.map((step, index) => (
            <li
              key={step.key}
              className={`rounded-2xl border px-4 py-4 ${
                step.complete ? "border-emerald-200 bg-emerald-50" : "border-slate-200 bg-white"
              }`}
            >
              <div className="flex flex-wrap items-start justify-between gap-3">
                <div className="flex gap-3">
                  <span
                    className={`inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full text-sm font-bold ${
                      step.complete ? "bg-emerald-600 text-white" : "bg-slate-200 text-slate-700"
                    }`}
                  >
                    {step.complete ? "✓" : index + 1}
                  </span>
                  <div>
                    <p className="font-semibold text-slate-900">{step.title}</p>
                    <p className="mt-1 text-sm text-slate-600">{step.description}</p>
                  </div>
                </div>
                {!step.complete ? (
                  <Link href={step.navigation_href} className="ta-button-secondary text-sm">
                    {step.navigation_label}
                  </Link>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      </section>

      {onboarding.completed_count === 0 ? (
        <TeacherAssistEmptyState
          title="Welcome to TeacherAssist"
          description="Start with school placement, school year, and homeroom setup below."
          whyItMatters="Setup aligns TeacherAssist with your district catalog — grade, subjects, and pacing come from there."
          actionLabel="Begin step 1"
          actionHref="/teacher-assist/settings#school-setup"
        />
      ) : null}

      <section className="ta-panel p-5">
        <h2 className="text-lg font-semibold text-slate-900">Setup workspace</h2>
        <p className="mt-1 text-sm text-slate-600">
          All three setup steps live on the Settings page — use the links above or open Settings directly.
        </p>
        <Link href="/teacher-assist/settings" className="ta-button-secondary mt-4 inline-flex text-sm">
          Open Settings
        </Link>
      </section>
    </div>
  );
}
