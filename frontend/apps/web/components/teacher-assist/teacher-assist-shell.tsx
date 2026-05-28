"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { AppSwitcher } from "@/components/platform/app-switcher";
import { useAuth } from "@/components/providers/auth-provider";
import { TEACHER_ASSIST_NAV_LINKS } from "@/components/teacher-assist/teacher-assist-nav";

export function TeacherAssistShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { logoutUser, user } = useAuth();

  return (
    <div className="teacher-assist-theme min-h-dvh bg-background text-foreground">
      <div className="mx-auto flex min-h-dvh w-full max-w-7xl flex-col px-4 py-4 sm:px-6 sm:py-6 lg:px-8">
        <header className="ta-panel sticky top-4 z-30 px-5 py-5 sm:px-6">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="max-w-2xl">
              <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
                TeacherAssist AI
              </p>
              <h1 className="mt-2 text-2xl font-semibold tracking-tight text-slate-900 sm:text-3xl">
                Educator-focused workspace
              </h1>
              <p className="mt-2 text-sm leading-6 text-slate-600 sm:text-[15px]">
                Calm, desktop-first navigation for the TeacherAssist product module. Trading remains
                isolated in its own dark shell.
              </p>
            </div>
            <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-end">
              <AppSwitcher />
              <div className="rounded-2xl border border-slate-200 bg-white/70 px-4 py-3">
                <p className="text-sm font-semibold text-slate-900">{user?.full_name ?? "Teacher"}</p>
                <p className="text-xs text-slate-500">{user?.email ?? "Signed in"}</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  void logoutUser();
                }}
                className="inline-flex h-11 items-center rounded-2xl border border-slate-200 bg-white px-4 text-sm font-semibold text-slate-600 transition hover:bg-slate-50"
              >
                Logout
              </button>
            </div>
          </div>

          <nav className="mt-5 flex flex-wrap gap-2">
            {TEACHER_ASSIST_NAV_LINKS.map((item) => {
              const active =
                item.href === "/teacher-assist"
                  ? pathname === item.href
                  : pathname === item.href || pathname.startsWith(`${item.href}/`);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  className={`inline-flex h-11 items-center rounded-2xl border px-4 text-sm font-semibold transition ${
                    active
                      ? "border-sky-300 bg-sky-50 text-sky-900"
                      : "border-slate-200 bg-white/80 text-slate-600 hover:bg-slate-50 hover:text-slate-900"
                  }`}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>
        </header>
        <main className="flex-1 py-6">{children}</main>
      </div>
    </div>
  );
}
