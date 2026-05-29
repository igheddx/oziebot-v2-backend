export type TeacherAssistNavLink = {
  href: string;
  label: string;
};

export type TeacherAssistNavGroup = {
  key: string;
  label: string;
  links: TeacherAssistNavLink[];
};

export const TEACHER_ASSIST_PRIMARY_LINK: TeacherAssistNavLink = {
  href: "/teacher-assist/today",
  label: "Today",
};

export const TEACHER_ASSIST_NAV_GROUPS: TeacherAssistNavGroup[] = [
  {
    key: "planning",
    label: "Planning",
    links: [
      { href: "/teacher-assist/weekly-planning", label: "Weekly Planning" },
      { href: "/teacher-assist/plans", label: "Plans" },
      { href: "/teacher-assist/pacing-guides", label: "Pacing Guides" },
      { href: "/teacher-assist/resources", label: "Resources" },
      { href: "/teacher-assist/curriculum-rollover", label: "Curriculum Rollover" },
    ],
  },
  {
    key: "instruction",
    label: "Instruction",
    links: [
      { href: "/teacher-assist/assignments", label: "Assignments" },
      { href: "/teacher-assist/assignments/print-packets", label: "Print Packets" },
      { href: "/teacher-assist/daily-teaching", label: "Daily Teaching" },
    ],
  },
  {
    key: "assessment",
    label: "Assessment",
    links: [
      { href: "/teacher-assist/extractions", label: "Extractions" },
      { href: "/teacher-assist/gradebook", label: "Gradebook" },
      { href: "/teacher-assist/assessments", label: "Assessments" },
    ],
  },
  {
    key: "mastery",
    label: "Mastery",
    links: [
      { href: "/teacher-assist/mastery", label: "Mastery" },
      { href: "/teacher-assist/reteach-plans", label: "Reteach Plans" },
    ],
  },
  {
    key: "operations",
    label: "Operations",
    links: [
      { href: "/teacher-assist/actions", label: "Actions" },
      { href: "/teacher-assist/workspace", label: "Workspace" },
      { href: "/teacher-assist/exports", label: "Exports" },
    ],
  },
  {
    key: "settings",
    label: "Settings",
    links: [{ href: "/teacher-assist/settings", label: "Settings" }],
  },
];

export const TEACHER_ASSIST_NAV_LINKS: TeacherAssistNavLink[] = [
  TEACHER_ASSIST_PRIMARY_LINK,
  ...TEACHER_ASSIST_NAV_GROUPS.flatMap((group) => group.links),
];
