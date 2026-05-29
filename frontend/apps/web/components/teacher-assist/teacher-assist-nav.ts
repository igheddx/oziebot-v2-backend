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

export const TEACHER_ASSIST_PRIMARY_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/home", label: "Home" },
  { href: "/teacher-assist/work-queue", label: "Work Queue" },
  { href: "/teacher-assist/catalog", label: "Catalog" },
];

export const TEACHER_ASSIST_NAV_GROUPS: TeacherAssistNavGroup[] = [
  {
    key: "instruction",
    label: "Instruction",
    links: [
      { href: "/teacher-assist/plans", label: "Plans" },
      { href: "/teacher-assist/assignments", label: "Assignments" },
      { href: "/teacher-assist/resources", label: "Resources" },
    ],
  },
  {
    key: "assessment",
    label: "Assessment",
    links: [
      { href: "/teacher-assist/extractions", label: "Student Work" },
      { href: "/teacher-assist/actions", label: "Reviews" },
      { href: "/teacher-assist/gradebook", label: "Gradebook" },
    ],
  },
  {
    key: "insights",
    label: "Insights",
    links: [
      { href: "/teacher-assist/mastery", label: "Mastery" },
      { href: "/teacher-assist/reteach-plans", label: "Reteach" },
      { href: "/teacher-assist/reflections", label: "Reflections" },
    ],
  },
  {
    key: "content",
    label: "Content",
    links: [
      { href: "/teacher-assist/newsletters", label: "Newsletters" },
      { href: "/teacher-assist/exports", label: "Exports" },
    ],
  },
  {
    key: "administration",
    label: "Administration",
    links: [
      { href: "/teacher-assist/settings", label: "Settings" },
      { href: "/teacher-assist/administration/education-catalog", label: "Catalog Admin", rootAdminOnly: true },
    ],
  },
];

export const TEACHER_ASSIST_ROOT_ADMIN_LINKS = TEACHER_ASSIST_NAV_GROUPS.find(
  (group) => group.key === "administration",
)?.links.filter((link) => link.rootAdminOnly) ?? [];

export const TEACHER_ASSIST_QUICK_CREATE_LINKS: TeacherAssistNavLink[] = [
  { href: "/teacher-assist/weekly-planning", label: "Create lesson" },
  { href: "/teacher-assist/assignments", label: "Create assignment" },
  { href: "/teacher-assist/exports", label: "Create quiz" },
  { href: "/teacher-assist/reteach-plans", label: "Create reteach plan" },
  { href: "/teacher-assist/newsletters", label: "Create newsletter" },
];

export const TEACHER_ASSIST_NAV_LINKS: TeacherAssistNavLink[] = [
  ...TEACHER_ASSIST_PRIMARY_LINKS,
  ...TEACHER_ASSIST_NAV_GROUPS.flatMap((group) => group.links),
];

/** @deprecated Use TEACHER_ASSIST_PRIMARY_LINKS[0] */
export const TEACHER_ASSIST_PRIMARY_LINK = TEACHER_ASSIST_PRIMARY_LINKS[0];
