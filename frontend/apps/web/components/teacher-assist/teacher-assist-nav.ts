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

/** Primary teacher workflow — ordered for pilot clarity. */
export const TEACHER_ASSIST_PRIMARY_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/home", label: "Home" },
  { href: "/teacher-assist/planning/weeks", label: "Instructional Weeks" },
  { href: "/teacher-assist/pacing-guides", label: "Pacing Guides" },
  { href: "/teacher-assist/assignments", label: "Assignments" },
  { href: "/teacher-assist/mastery", label: "Mastery" },
  { href: "/teacher-assist/resources", label: "Resources" },
  { href: "/teacher-assist/newsletters", label: "Communication" },
  { href: "/teacher-assist/copilot", label: "Copilot" },
];

export const TEACHER_ASSIST_NAV_GROUPS: TeacherAssistNavGroup[] = [
  {
    key: "operations",
    label: "Operations",
    links: [
      { href: "/teacher-assist/work-queue", label: "Work Queue" },
      { href: "/teacher-assist/planning/pacing-guides/workspace", label: "Current Week" },
      { href: "/teacher-assist/planning/templates", label: "Template Library" },
      { href: "/teacher-assist/catalog", label: "Catalog Browse" },
    ],
  },
  {
    key: "instruction",
    label: "Instruction",
    links: [
      { href: "/teacher-assist/plans", label: "Plans" },
      { href: "/teacher-assist/extractions", label: "Student Work" },
      { href: "/teacher-assist/gradebook", label: "Gradebook" },
    ],
  },
  {
    key: "insights",
    label: "Insights",
    links: [
      { href: "/teacher-assist/reteach", label: "Reteach Workspace" },
      { href: "/teacher-assist/reteach-plans", label: "Reteach Plans" },
      { href: "/teacher-assist/reflections", label: "Reflections" },
      { href: "/teacher-assist/actions", label: "Reviews" },
    ],
  },
  {
    key: "content",
    label: "Content",
    links: [
      { href: "/teacher-assist/exports", label: "Exports" },
      { href: "/teacher-assist/communication", label: "Communication Hub" },
    ],
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

export const TEACHER_ASSIST_QUICK_CREATE_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/planning/weeks", label: "Open instructional week" },
  { href: "/teacher-assist/weekly-planning", label: "Create lesson" },
  { href: "/teacher-assist/assignments", label: "Create assignment" },
  { href: "/teacher-assist/reteach-plans", label: "Create reteach plan" },
  { href: "/teacher-assist/newsletters", label: "Create newsletter" },
];

export const TEACHER_ASSIST_NAV_LINKS: TeacherAssistNavLink[] = [
  ...TEACHER_ASSIST_PRIMARY_LINKS,
  ...TEACHER_ASSIST_NAV_GROUPS.flatMap((group) => group.links),
];

/** @deprecated Use TEACHER_ASSIST_PRIMARY_LINKS[0] */
export const TEACHER_ASSIST_PRIMARY_LINK = TEACHER_ASSIST_PRIMARY_LINKS[0];
