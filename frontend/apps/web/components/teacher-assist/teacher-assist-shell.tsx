"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useMemo, useState } from "react";

import { AppSwitcher } from "@/components/platform/app-switcher";
import { useAuth } from "@/components/providers/auth-provider";
import {
  TEACHER_ASSIST_NAV_GROUPS,
  TEACHER_ASSIST_PRIMARY_LINKS,
  TEACHER_ASSIST_QUICK_CREATE_LINKS,
  type TeacherAssistNavGroup,
  type TeacherAssistNavLink,
} from "@/components/teacher-assist/teacher-assist-nav";

function isActivePath(pathname: string, href: string) {
  if (href === "/teacher-assist/home") {
    return pathname === "/teacher-assist" || pathname === href || pathname.startsWith(`${href}/`);
  }
  return pathname === href || pathname.startsWith(`${href}/`);
}

function isPrimaryActive(pathname: string) {
  return TEACHER_ASSIST_PRIMARY_LINKS.some((link) => isActivePath(pathname, link.href));
}

function pillClass(active: boolean, compact = false) {
  const base = compact
    ? "inline-flex h-8 shrink-0 items-center rounded-full border px-3 text-xs font-semibold transition"
    : "inline-flex h-9 shrink-0 items-center rounded-full border px-3.5 text-sm font-semibold transition";
  return active
    ? `${base} border-sky-300 bg-sky-50 text-sky-900`
    : `${base} border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:text-slate-900`;
}

function QuickCreateMenu() {
  const [open, setOpen] = useState(false);

  return (
    <div className="relative">
      <button
        type="button"
        onClick={() => setOpen((current) => !current)}
        className="inline-flex h-8 items-center gap-1 rounded-full border border-sky-300 bg-sky-500 px-3 text-xs font-semibold text-slate-950 hover:bg-sky-400"
      >
        Quick create
        <span className="text-[10px]">{open ? "▲" : "▼"}</span>
      </button>
      {open ? (
        <div className="absolute right-0 z-40 mt-1 min-w-48 rounded-xl border border-slate-200 bg-white p-1.5 shadow-lg">
          {TEACHER_ASSIST_QUICK_CREATE_LINKS.map((item) => (
            <Link
              key={item.href}
              href={item.href}
              className="block rounded-lg px-2.5 py-2 text-xs font-medium text-slate-700 hover:bg-sky-50 hover:text-sky-900"
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

function ChildNavRow({
  group,
  pathname,
  isRootAdmin,
}: {
  group: TeacherAssistNavGroup;
  pathname: string;
  isRootAdmin: boolean;
}) {
  const links = group.links.filter((item) => !item.rootAdminOnly || isRootAdmin);
  return (
    <div className="flex gap-1.5 overflow-x-auto pb-0.5 [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
      {links.map((item) => (
        <Link key={item.href} href={item.href} className={pillClass(isActivePath(pathname, item.href), true)}>
          {item.label}
        </Link>
      ))}
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
    return "instruction";
  }, [pathname]);

  const [selectedCategory, setSelectedCategory] = useState(activeGroupKey);

  useEffect(() => {
    setSelectedCategory(activeGroupKey);
  }, [activeGroupKey]);

  const selectedGroup =
    TEACHER_ASSIST_NAV_GROUPS.find((group) => group.key === selectedCategory) ?? TEACHER_ASSIST_NAV_GROUPS[0];
  const isRootAdmin = Boolean(user?.is_root_admin);

  return (
    <div className="teacher-assist-theme min-h-dvh bg-background text-foreground">
      <div className="mx-auto flex min-h-dvh w-full max-w-7xl flex-col px-3 py-3 sm:px-5 sm:py-4">
        <header className="sticky top-0 z-30 border-b border-slate-200/80 bg-[color-mix(in_srgb,var(--background)_92%,white)] pb-2 pt-1 backdrop-blur-sm">
          {/* Row 1 – compact app header */}
          <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
            <Link href="/teacher-assist/home" className="text-xs font-bold uppercase tracking-[0.18em] text-sky-700">
              TeacherAssist AI
            </Link>
            <div className="hidden h-4 w-px bg-slate-200 sm:block" />
            <div className="flex flex-wrap items-center gap-1.5">
              {TEACHER_ASSIST_PRIMARY_LINKS.map((item: TeacherAssistNavLink) => (
                <Link key={item.href} href={item.href} className={pillClass(isActivePath(pathname, item.href), true)}>
                  {item.label}
                </Link>
              ))}
              <QuickCreateMenu />
            </div>
            <div className="ml-auto flex flex-wrap items-center gap-2">
              <AppSwitcher />
              <div className="hidden text-right sm:block">
                <p className="max-w-[160px] truncate text-xs font-semibold text-slate-900">
                  {user?.full_name ?? "Teacher"}
                </p>
                <p className="max-w-[160px] truncate text-[11px] text-slate-500">{user?.email ?? ""}</p>
              </div>
              <button
                type="button"
                onClick={() => {
                  void logoutUser();
                }}
                className="inline-flex h-8 items-center rounded-full border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-600 hover:bg-slate-50"
              >
                Logout
              </button>
              <button
                type="button"
                className="inline-flex h-8 items-center rounded-full border border-slate-200 bg-white px-3 text-xs font-semibold text-slate-700 lg:hidden"
                onClick={() => setMobileNavOpen((current) => !current)}
              >
                {mobileNavOpen ? "Close" : "Menu"}
              </button>
            </div>
          </div>

          {/* Row 2 – category pills */}
          <nav
            className={`mt-2 flex gap-1.5 overflow-x-auto [-ms-overflow-style:none] [scrollbar-width:none] [&::-webkit-scrollbar]:hidden ${mobileNavOpen ? "flex" : "hidden lg:flex"}`}
            aria-label="TeacherAssist categories"
          >
            {TEACHER_ASSIST_NAV_GROUPS.map((group) => {
              const categoryActive =
                selectedCategory === group.key ||
                (!isPrimaryActive(pathname) && activeGroupKey === group.key);
              return (
                <button
                  key={group.key}
                  type="button"
                  onClick={() => setSelectedCategory(group.key)}
                  className={pillClass(categoryActive, true)}
                >
                  {group.label}
                </button>
              );
            })}
          </nav>

          {/* Row 3 – child navigation */}
          <div className={`mt-1.5 ${mobileNavOpen ? "block" : "hidden lg:block"}`}>
            <ChildNavRow group={selectedGroup} pathname={pathname} isRootAdmin={isRootAdmin} />
          </div>
        </header>

        <main className="flex-1 py-4 sm:py-5">{children}</main>
      </div>
    </div>
  );
}
