import { readJson } from "@/lib/api-client";

export async function fetchReteachWorkspace(params?: {
  class_id?: string;
  subject_id?: string;
  instructional_week_id?: string;
}) {
  const query = new URLSearchParams();
  if (params?.class_id) query.set("class_id", params.class_id);
  if (params?.subject_id) query.set("subject_id", params.subject_id);
  if (params?.instructional_week_id) query.set("instructional_week_id", params.instructional_week_id);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/reteach-workspace${suffix}`);
}

export async function fetchMasteryDashboardV2(params?: {
  class_id?: string;
  subject_id?: string;
  instructional_week_id?: string;
  school_year_id?: string;
  grading_period_id?: string;
}) {
  const query = new URLSearchParams();
  if (params?.class_id) query.set("class_id", params.class_id);
  if (params?.subject_id) query.set("subject_id", params.subject_id);
  if (params?.instructional_week_id) query.set("instructional_week_id", params.instructional_week_id);
  if (params?.school_year_id) query.set("school_year_id", params.school_year_id);
  if (params?.grading_period_id) query.set("grading_period_id", params.grading_period_id);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/mastery-dashboard/v2${suffix}`);
}

export async function fetchInstructionalHealthReport(params?: {
  class_id?: string;
  subject_id?: string;
  instructional_week_id?: string;
}) {
  const query = new URLSearchParams();
  if (params?.class_id) query.set("class_id", params.class_id);
  if (params?.subject_id) query.set("subject_id", params.subject_id);
  if (params?.instructional_week_id) query.set("instructional_week_id", params.instructional_week_id);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/instructional-health-report${suffix}`);
}

export async function fetchAssignmentCoverage(params?: {
  assignment_id?: string;
  instructional_week_id?: string;
  class_id?: string;
}) {
  const query = new URLSearchParams();
  if (params?.assignment_id) query.set("assignment_id", params.assignment_id);
  if (params?.instructional_week_id) query.set("instructional_week_id", params.instructional_week_id);
  if (params?.class_id) query.set("class_id", params.class_id);
  const suffix = query.toString() ? `?${query.toString()}` : "";
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/assignment-coverage${suffix}`);
}

export async function createSupportGroup(body: Record<string, unknown>) {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/support-groups", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function updateWeekClosureChecklist(weekId: string, checklist: Record<string, boolean>) {
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/instructional-weeks/${weekId}/closure`, {
    method: "PATCH",
    body: JSON.stringify({ checklist }),
  });
}

export async function generateWeekSummary(weekId: string) {
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/instructional-weeks/${weekId}/summary`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export async function upsertInstructionalReflection(body: Record<string, unknown>) {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/instructional-reflections", {
    method: "PUT",
    body: JSON.stringify(body),
  });
}
