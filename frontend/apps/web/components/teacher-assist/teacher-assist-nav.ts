export type TeacherAssistNavLink = {
  href: string;
  label: string;
  rootAdminOnly?: boolean;
};

export type TeacherAssistNavGroup = {
  key: string;
  label: string;
  links: TeacherAssistNavLink[];
};

/** Primary teacher workflow — pacing guide first, then weekly planning. */
export const TEACHER_ASSIST_PRIMARY_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/home", label: "Home" },
  { href: "/teacher-assist/get-started", label: "Setup" },
  { href: "/teacher-assist/pacing-guides", label: "Pacing Guides" },
  { href: "/teacher-assist/planning/pacing-guides/workspace", label: "Pacing Workspace" },
  { href: "/teacher-assist/planning/weeks", label: "Weekly Planning" },
  { href: "/teacher-assist/resources", label: "Resources" },
];

export const TEACHER_ASSIST_NAV_GROUPS: TeacherAssistNavGroup[] = [
  {
    key: "instruction",
    label: "Instruction",
    links: [
      { href: "/teacher-assist/assignments", label: "Assignments" },
      { href: "/teacher-assist/mastery", label: "Mastery" },
      { href: "/teacher-assist/plans", label: "Plan Library" },
      { href: "/teacher-assist/weekly-planning", label: "Legacy Planning" },
    ],
  },
  {
    key: "operations",
    label: "Operations",
    links: [
      { href: "/teacher-assist/work-queue", label: "Work Queue" },
      { href: "/teacher-assist/planning/templates", label: "Template Library" },
      { href: "/teacher-assist/catalog", label: "Catalog Browse" },
      { href: "/teacher-assist/extractions", label: "Student Work" },
      { href: "/teacher-assist/gradebook", label: "Gradebook" },
    ],
  },
  {
    key: "communication",
    label: "Communication",
    links: [
      { href: "/teacher-assist/newsletters", label: "Newsletters" },
      { href: "/teacher-assist/communication", label: "Communication Hub" },
    ],
  },
  {
    key: "insights",
    label: "Insights",
    links: [
      { href: "/teacher-assist/reteach", label: "Reteach Workspace" },
      { href: "/teacher-assist/reteach-plans", label: "Reteach Plans" },
      { href: "/teacher-assist/reflections", label: "Reflections" },
      { href: "/teacher-assist/copilot", label: "Copilot" },
    ],
  },
  {
    key: "content",
    label: "Content",
    links: [{ href: "/teacher-assist/exports", label: "Exports" }],
  },
  {
    key: "administration",
    label: "Administration",
    links: [
      { href: "/teacher-assist/settings", label: "Settings" },
      { href: "/teacher-assist/feedback", label: "Pilot Feedback" },
      { href: "/teacher-assist/administration/education-catalog", label: "Catalog Admin", rootAdminOnly: true },
      { href: "/teacher-assist/administration/system-health", label: "System Health", rootAdminOnly: true },
    ],
  },
];

export const TEACHER_ASSIST_ROOT_ADMIN_LINKS = TEACHER_ASSIST_NAV_GROUPS.find(
  (group) => group.key === "administration",
)?.links.filter((link) => link.rootAdminOnly) ?? [];

/** Workflow-aligned shortcuts — no standalone generate actions. */
export const TEACHER_ASSIST_QUICK_CREATE_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/pacing-guides", label: "Browse pacing guides" },
  { href: "/teacher-assist/planning/pacing-guides/workspace", label: "Open pacing workspace" },
  { href: "/teacher-assist/planning/weeks", label: "Weekly planning" },
];

export const TEACHER_ASSIST_NAV_LINKS: TeacherAssistNavLink[] = [
  ...TEACHER_ASSIST_PRIMARY_LINKS,
  ...TEACHER_ASSIST_NAV_GROUPS.flatMap((group) => group.links),
];

/** @deprecated Use TEACHER_ASSIST_PRIMARY_LINKS[0] */
export const TEACHER_ASSIST_PRIMARY_LINK = TEACHER_ASSIST_PRIMARY_LINKS[0];
