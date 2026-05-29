"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useMemo, useState } from "react";

import { AppSwitcher } from "@/components/platform/app-switcher";
import { useAuth } from "@/components/providers/auth-provider";
import {
  TEACHER_ASSIST_NAV_GROUPS,
  TEACHER_ASSIST_PRIMARY_LINKS,
  TEACHER_ASSIST_QUICK_CREATE_LINKS,
  type TeacherAssistNavLink,
} from "@/components/teacher-assist/teacher-assist-nav";

function isActivePath(pathname: string, href: string) {
  if (href === "/teacher-assist/home") {
    return pathname === "/teacher-assist" || pathname === href || pathname.startsWith(`${href}/`);
  }
  return pathname === href || pathname.startsWith(`${href}/`);
}

function NavLink({ item, pathname }: { item: TeacherAssistNavLink; pathname: string }) {
  const active = isActivePath(pathname, item.href);
  return (
    <Link
      href={item.href}
      className={`inline-flex min-h-10 items-center rounded-2xl border px-3 py-2 text-sm font-semibold transition ${
        active
          ? "border-sky-300 bg-sky-50 text-sky-900"
          : "border-slate-200 bg-white/80 text-slate-600 hover:bg-slate-50 hover:text-slate-900"
      }`}
    >
      {item.label}
    </Link>
  );
}

function QuickCreateMenu() {
  const [open, setOpen] = useState(false);

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        className="ta-button-primary inline-flex h-10 items-center gap-2 px-4 text-sm"
      >
        Quick create
        <span className="text-xs">{open ? "▲" : "▼"}</span>
      </button>
      {open ? (
        <div className="absolute right-0 z-40 mt-2 min-w-52 rounded-2xl border border-slate-200 bg-white p-2 shadow-lg">
          {TEACHER_ASSIST_QUICK_CREATE_LINKS.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className="block rounded-xl px-3 py-2 text-sm font-medium text-slate-700 hover:bg-sky-50 hover:text-sky-900"
              onClick={() => setOpen(false)}
            >
              {item.label}
            </Link>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export function TeacherAssistShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const { logoutUser, user } = useAuth();
  const [mobileNavOpen, setMobileNavOpen] = useState(false);

  const activeGroupKey = useMemo(() => {
    for (const group of TEACHER_ASSIST_NAV_GROUPS) {
      if (group.links.some((link) => isActivePath(pathname, link.href))) {
        return group.key;
      }
    }
    return TEACHER_ASSIST_NAV_GROUPS[0]?.key ?? "instruction";
  }, [pathname]);

  const [expandedGroup, setExpandedGroup] = useState<string>(activeGroupKey);

  return (
    <div className="teacher-assist-theme min-h-dvh bg-background text-foreground">
      <div className="mx-auto flex min-h-dvh w-full max-w-7xl flex-col px-3 py-4 sm:px-6 sm:py-6 lg:px-8">
        <header className="ta-panel sticky top-3 z-30 px-4 py-4 sm:top-4 sm:px-6 sm:py-5">
          <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
            <div className="max-w-2xl">
              <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
                TeacherAssist AI
              </p>
              <h1 className="mt-2 text-xl font-semibold tracking-tight text-slate-900 sm:text-3xl">
                Educator-focused workspace
              </h1>
              <p className="mt-2 text-sm leading-6 text-slate-600 sm:text-[15px]">
                Start on Home, use Work Queue for operational tasks, and manage each class from one place.
              </p>
            </div>
            <div className="flex flex-col items-start gap-3 sm:flex-row sm:items-center sm:justify-end">
              <QuickCreateMenu />
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

          <div className="mt-5 flex flex-wrap items-center gap-2">
            {TEACHER_ASSIST_PRIMARY_LINKS.map((item) => (
              <NavLink key={item.href} item={item} pathname={pathname} />
            ))}
            <button
              type="button"
              className="inline-flex h-10 items-center rounded-2xl border border-slate-200 bg-white px-3 text-sm font-semibold text-slate-700 lg:hidden"
              onClick={() => setMobileNavOpen((current) => !current)}
            >
              {mobileNavOpen ? "Hide menu" : "Browse areas"}
            </button>
          </div>

          <nav className={`mt-4 ${mobileNavOpen ? "block" : "hidden lg:block"}`}>
            <div className="grid gap-3 lg:grid-cols-3 xl:grid-cols-5">
              {TEACHER_ASSIST_NAV_GROUPS.map((group) => {
                const isExpanded = expandedGroup === group.key || mobileNavOpen;
                return (
                  <section key={group.key} className="rounded-2xl border border-slate-200 bg-white/70 p-3">
                    <button
                      type="button"
                      className="flex w-full items-center justify-between text-left"
                      onClick={() => setExpandedGroup((current) => (current === group.key ? "" : group.key))}
                    >
                      <span className="text-xs font-semibold uppercase tracking-wide text-slate-500">
                        {group.label}
                      </span>
                      <span className="text-xs text-slate-400 lg:hidden">{isExpanded ? "−" : "+"}</span>
                    </button>
                    <div className={`mt-2 flex flex-wrap gap-2 ${isExpanded ? "flex" : "hidden lg:flex"}`}>
                      {group.links.map((item) => (
                        <NavLink key={item.href} item={item} pathname={pathname} />
                      ))}
                    </div>
                  </section>
                );
              })}
            </div>
          </nav>
        </header>
        <main className="flex-1 py-4 sm:py-6">{children}</main>
      </div>
    </div>
  );
}
