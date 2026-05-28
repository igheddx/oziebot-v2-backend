import {
  authFetch,
  buildApiUrl,
  getStoredAccessToken,
  parseErrorMessage,
  refreshTokens,
} from "@/lib/auth-service";
import type {
  Assignment,
  AssignmentInput,
  AssignmentPrintPacket,
  AssignmentPrintPacketInput,
  AssignmentPrintPage,
  AssignmentStudentWorkSubmission,
  CurriculumRolloverCandidates,
  CurriculumRolloverCopyInput,
  CurriculumRolloverCopyResult,
  GradingPeriod,
  InstructionalPlanLibraryItem,
  PacingGuide,
  PacingItem,
  PlanningDraft,
  PlanningDraftContextPreview,
  ResourceLibraryItem,
  SchoolYear,
  Standard,
  Subject,
  TeacherAssistWorkflow,
  TeacherAssistWorkflowDetail,
  TeacherAssistOptions,
  TeacherClass,
  TeacherProfile,
  WeeklyPlan,
  WeeklyPlanCopyInput,
  WeeklyPlanSectionRegenerationInput,
  WeeklyPlanSharingUpdateInput,
  WeeklyPlanUpdateInput,
  WeeklyPlanVersion,
} from "@/lib/teacher-assist-types";

async function readJson<T>(path: string): Promise<T> {
  const res = await authFetch(path);
  if (!res || !res.ok) {
    throw new Error(res ? await parseErrorMessage(res) : "Could not reach API");
  }
  return (await res.json()) as T;
}

async function writeJson<T>(
  path: string,
  method: "POST" | "PUT" | "PATCH",
  body: Record<string, unknown>,
): Promise<T> {
  const res = await authFetch(path, {
    method,
    body: JSON.stringify(body),
  });
  if (!res || !res.ok) {
    throw new Error(res ? await parseErrorMessage(res) : "Could not reach API");
  }
  return (await res.json()) as T;
}

function parseUploadError(text: string, status: number): string {
  if (!text.trim()) return `Upload failed (${status})`;
  try {
    const payload = JSON.parse(text) as { detail?: string };
    if (typeof payload.detail === "string") return payload.detail;
  } catch {
    // Fall through to raw text.
  }
  return text;
}

export function fetchTeacherAssistOptions() {
  return readJson<TeacherAssistOptions>("/v1/teacher-assist/options");
}

export function fetchTeacherProfile() {
  return readJson<TeacherProfile>("/v1/teacher-assist/profile");
}

export function saveTeacherProfile(body: Record<string, unknown>) {
  return writeJson<TeacherProfile>("/v1/teacher-assist/profile", "PUT", body);
}

export function fetchSchoolYears() {
  return readJson<SchoolYear[]>("/v1/teacher-assist/school-years");
}

export function createSchoolYear(body: Record<string, unknown>) {
  return writeJson<SchoolYear>("/v1/teacher-assist/school-years", "POST", body);
}

export function updateSchoolYear(id: string, body: Record<string, unknown>) {
  return writeJson<SchoolYear>(`/v1/teacher-assist/school-years/${id}`, "PUT", body);
}

export function fetchGradingPeriods() {
  return readJson<GradingPeriod[]>("/v1/teacher-assist/grading-periods");
}

export function createGradingPeriod(body: Record<string, unknown>) {
  return writeJson<GradingPeriod>("/v1/teacher-assist/grading-periods", "POST", body);
}

export function updateGradingPeriod(id: string, body: Record<string, unknown>) {
  return writeJson<GradingPeriod>(`/v1/teacher-assist/grading-periods/${id}`, "PUT", body);
}

export function fetchSubjects() {
  return readJson<Subject[]>("/v1/teacher-assist/subjects");
}

export function createSubject(body: Record<string, unknown>) {
  return writeJson<Subject>("/v1/teacher-assist/subjects", "POST", body);
}

export function fetchClasses() {
  return readJson<TeacherClass[]>("/v1/teacher-assist/classes");
}

export function createClass(body: Record<string, unknown>) {
  return writeJson<TeacherClass>("/v1/teacher-assist/classes", "POST", body);
}

export function updateClass(id: string, body: Record<string, unknown>) {
  return writeJson<TeacherClass>(`/v1/teacher-assist/classes/${id}`, "PUT", body);
}

export function attachClassSubject(body: Record<string, unknown>) {
  return writeJson("/v1/teacher-assist/class-subjects", "POST", body);
}

export function fetchStandards() {
  return readJson<Standard[]>("/v1/teacher-assist/standards");
}

export function createStandard(body: Record<string, unknown>) {
  return writeJson<Standard>("/v1/teacher-assist/standards", "POST", body);
}

export function fetchPacingGuides() {
  return readJson<PacingGuide[]>("/v1/teacher-assist/pacing-guides");
}

export function createPacingGuide(body: Record<string, unknown>) {
  return writeJson<PacingGuide>("/v1/teacher-assist/pacing-guides", "POST", body);
}

export function updatePacingGuide(id: string, body: Record<string, unknown>) {
  return writeJson<PacingGuide>(`/v1/teacher-assist/pacing-guides/${id}`, "PUT", body);
}

export function fetchPacingGuideItems(id: string) {
  return readJson<PacingItem[]>(`/v1/teacher-assist/pacing-guides/${id}/items`);
}

export function createPacingGuideItem(id: string, body: Record<string, unknown>) {
  return writeJson<PacingItem>(`/v1/teacher-assist/pacing-guides/${id}/items`, "POST", body);
}

export function updatePacingItem(id: string, body: Record<string, unknown>) {
  return writeJson<PacingItem>(`/v1/teacher-assist/pacing-items/${id}`, "PUT", body);
}

export function attachPacingItemStandard(id: string, standardId: string) {
  return writeJson<PacingItem>(`/v1/teacher-assist/pacing-items/${id}/standards`, "POST", {
    standard_id: standardId,
  });
}

export function attachPacingItemResource(id: string, resourceLibraryItemId: string) {
  return writeJson<PacingItem>(`/v1/teacher-assist/pacing-items/${id}/resources`, "POST", {
    resource_library_item_id: resourceLibraryItemId,
  });
}

export function fetchResources() {
  return readJson<ResourceLibraryItem[]>("/v1/teacher-assist/resources");
}

export function fetchAssignments(
  filters: {
    school_year_id?: string;
    grading_period_id?: string;
    class_id?: string;
    subject_id?: string;
    status?: string;
    assignment_type?: string;
    q?: string;
  } = {},
) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    params.set(key, String(value));
  });
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return readJson<Assignment[]>(`/v1/teacher-assist/assignments${suffix}`);
}

export function createAssignment(body: AssignmentInput) {
  return writeJson<Assignment>(
    "/v1/teacher-assist/assignments",
    "POST",
    body as Record<string, unknown>,
  );
}

export function updateAssignment(id: string, body: AssignmentInput) {
  return writeJson<Assignment>(
    `/v1/teacher-assist/assignments/${id}`,
    "PUT",
    body as Record<string, unknown>,
  );
}

export function updateAssignmentStatus(id: string, status: Assignment["status"]) {
  return writeJson<Assignment>(`/v1/teacher-assist/assignments/${id}/status`, "PATCH", { status });
}

export function attachAssignmentStandard(id: string, standardId: string) {
  return writeJson<Assignment>(`/v1/teacher-assist/assignments/${id}/standards`, "POST", {
    standard_id: standardId,
  });
}

export function attachAssignmentResource(id: string, resourceLibraryItemId: string) {
  return writeJson<Assignment>(`/v1/teacher-assist/assignments/${id}/resources`, "POST", {
    resource_library_item_id: resourceLibraryItemId,
  });
}

export function createAssignmentPrintPacket(id: string, body: AssignmentPrintPacketInput) {
  return writeJson<AssignmentPrintPacket>(
    `/v1/teacher-assist/assignments/${id}/print-packets`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function fetchAssignmentPrintPackets(id: string) {
  return readJson<AssignmentPrintPacket[]>(`/v1/teacher-assist/assignments/${id}/print-packets`);
}

export function fetchAssignmentPrintPacket(id: string) {
  return readJson<AssignmentPrintPacket>(`/v1/teacher-assist/print-packets/${id}`);
}

export function fetchAssignmentPrintPacketPages(id: string) {
  return readJson<AssignmentPrintPage[]>(`/v1/teacher-assist/print-packets/${id}/pages`);
}

export function fetchAssignmentStudentWork(id: string) {
  return readJson<AssignmentStudentWorkSubmission[]>(`/v1/teacher-assist/assignments/${id}/student-work`);
}

export function fetchAssignmentStudentWorkSubmission(id: string) {
  return readJson<AssignmentStudentWorkSubmission>(`/v1/teacher-assist/student-work/${id}`);
}

export async function uploadAssignmentStudentWork(
  assignmentId: string,
  file: File,
  body: {
    student_number: number;
    assignment_print_packet_id?: string | null;
    assignment_print_page_id?: string | null;
  },
  onProgress?: (progress: number) => void,
): Promise<AssignmentStudentWorkSubmission> {
  let accessToken = getStoredAccessToken();
  if (!accessToken) {
    const refreshed = await refreshTokens();
    accessToken = refreshed ? getStoredAccessToken() : null;
  }
  const formData = new FormData();
  formData.append("file", file);
  formData.append("student_number", String(body.student_number));
  if (body.assignment_print_packet_id) {
    formData.append("assignment_print_packet_id", body.assignment_print_packet_id);
  }
  if (body.assignment_print_page_id) {
    formData.append("assignment_print_page_id", body.assignment_print_page_id);
  }

  return await new Promise<AssignmentStudentWorkSubmission>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", buildApiUrl(`/v1/teacher-assist/assignments/${assignmentId}/student-work`));
    xhr.withCredentials = true;
    if (accessToken) {
      xhr.setRequestHeader("Authorization", `Bearer ${accessToken}`);
    }
    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable || !onProgress) return;
      onProgress(Math.round((event.loaded / event.total) * 100));
    };
    xhr.onerror = () => reject(new Error("Could not reach API"));
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText) as AssignmentStudentWorkSubmission);
        } catch {
          reject(new Error("Upload succeeded but returned invalid JSON"));
        }
        return;
      }
      reject(new Error(parseUploadError(xhr.responseText, xhr.status)));
    };
    xhr.send(formData);
  });
}

export function updateAssignmentStudentWorkStatus(
  id: string,
  processingStatus: AssignmentStudentWorkSubmission["processing_status"],
) {
  return writeJson<AssignmentStudentWorkSubmission>(`/v1/teacher-assist/student-work/${id}/status`, "PATCH", {
    processing_status: processingStatus,
  });
}

export function updateAssignmentStudentWorkPacketContext(
  id: string,
  body: {
    assignment_print_packet_id?: string | null;
    assignment_print_page_id?: string | null;
  },
) {
  return writeJson<AssignmentStudentWorkSubmission>(
    `/v1/teacher-assist/student-work/${id}/packet-context`,
    "PATCH",
    body as Record<string, unknown>,
  );
}

export function createLinkResource(body: Record<string, unknown>) {
  return writeJson<ResourceLibraryItem>("/v1/teacher-assist/resources/link", "POST", body);
}

export async function uploadResourceFile(
  file: File,
  body: { title?: string; description?: string } = {},
  onProgress?: (progress: number) => void,
): Promise<ResourceLibraryItem> {
  let accessToken = getStoredAccessToken();
  if (!accessToken) {
    const refreshed = await refreshTokens();
    accessToken = refreshed ? getStoredAccessToken() : null;
  }
  const formData = new FormData();
  formData.append("file", file);
  if (body.title?.trim()) formData.append("title", body.title.trim());
  if (body.description?.trim()) formData.append("description", body.description.trim());

  return await new Promise<ResourceLibraryItem>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", buildApiUrl("/v1/teacher-assist/resources/upload"));
    xhr.withCredentials = true;
    if (accessToken) {
      xhr.setRequestHeader("Authorization", `Bearer ${accessToken}`);
    }
    xhr.upload.onprogress = (event) => {
      if (!event.lengthComputable || !onProgress) return;
      onProgress(Math.round((event.loaded / event.total) * 100));
    };
    xhr.onerror = () => reject(new Error("Could not reach API"));
    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          resolve(JSON.parse(xhr.responseText) as ResourceLibraryItem);
        } catch {
          reject(new Error("Upload succeeded but returned invalid JSON"));
        }
        return;
      }
      reject(new Error(parseUploadError(xhr.responseText, xhr.status)));
    };
    xhr.send(formData);
  });
}

export function fetchPlanningDrafts() {
  return readJson<PlanningDraft[]>("/v1/teacher-assist/planning-drafts");
}

export function createPlanningDraft(body: Record<string, unknown>) {
  return writeJson<PlanningDraft>("/v1/teacher-assist/planning-drafts", "POST", body);
}

export function updatePlanningDraft(id: string, body: Record<string, unknown>) {
  return writeJson<PlanningDraft>(`/v1/teacher-assist/planning-drafts/${id}`, "PUT", body);
}

export function fetchPlanningDraftContextPreview(id: string) {
  return readJson<PlanningDraftContextPreview>(
    `/v1/teacher-assist/planning-drafts/${id}/context-preview`,
  );
}

export function updatePlanningDraftStatus(id: string, status: "draft" | "ready") {
  return writeJson<PlanningDraft>(`/v1/teacher-assist/planning-drafts/${id}/status`, "PATCH", {
    status,
  });
}

export async function startWeeklyPlanWorkflow(id: string) {
  const res = await authFetch(`/v1/teacher-assist/planning-drafts/${id}/workflows/weekly-plan`, {
    method: "POST",
  });
  if (!res || !res.ok) {
    throw new Error(res ? await parseErrorMessage(res) : "Could not reach API");
  }
  return (await res.json()) as TeacherAssistWorkflow;
}

export function fetchTeacherAssistWorkflows() {
  return readJson<TeacherAssistWorkflow[]>("/v1/teacher-assist/workflows");
}

export function fetchTeacherAssistWorkflow(id: string) {
  return readJson<TeacherAssistWorkflowDetail>(`/v1/teacher-assist/workflows/${id}`);
}

export function cancelTeacherAssistWorkflow(id: string) {
  return writeJson<TeacherAssistWorkflow>(`/v1/teacher-assist/workflows/${id}/cancel`, "PATCH", {
    status: "cancelled",
  });
}

export function fetchWeeklyPlans() {
  return readJson<WeeklyPlan[]>("/v1/teacher-assist/weekly-plans");
}

export function fetchWeeklyPlan(id: string) {
  return readJson<WeeklyPlan>(`/v1/teacher-assist/weekly-plans/${id}`);
}

export function updateWeeklyPlan(id: string, body: WeeklyPlanUpdateInput) {
  return writeJson<WeeklyPlan>(`/v1/teacher-assist/weekly-plans/${id}`, "PUT", body);
}

export function copyWeeklyPlan(id: string, body: WeeklyPlanCopyInput = {}) {
  return writeJson<WeeklyPlan>(`/v1/teacher-assist/weekly-plans/${id}/copy`, "POST", body);
}

export function regenerateWeeklyPlanSection(id: string, body: WeeklyPlanSectionRegenerationInput) {
  return writeJson<WeeklyPlan>(
    `/v1/teacher-assist/weekly-plans/${id}/regenerate-section`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function updateWeeklyPlanSharing(id: string, body: WeeklyPlanSharingUpdateInput) {
  return writeJson<WeeklyPlan>(`/v1/teacher-assist/weekly-plans/${id}/sharing`, "PATCH", body);
}

export function fetchWeeklyPlanVersions(id: string) {
  return readJson<WeeklyPlanVersion[]>(`/v1/teacher-assist/weekly-plans/${id}/versions`);
}

export function attachPlanningDraftResource(id: string, resourceLibraryItemId: string) {
  return writeJson<PlanningDraft>(`/v1/teacher-assist/planning-drafts/${id}/resources`, "POST", {
    resource_library_item_id: resourceLibraryItemId,
  });
}

export function fetchInstructionalPlanLibrary(
  filters: {
    school_year_id?: string;
    subject_id?: string;
    planning_scope?: string;
    visibility_scope?: string;
    reuse_status?: string;
    is_template?: boolean;
    q?: string;
  } = {},
) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    params.set(key, String(value));
  });
  const suffix = params.toString() ? `?${params.toString()}` : "";
  return readJson<InstructionalPlanLibraryItem[]>(
    `/v1/teacher-assist/instructional-plans/library${suffix}`,
  );
}

export function fetchCurriculumRolloverCandidates(filters: {
  source_school_year_id: string;
  target_school_year_id: string;
  subject_id?: string;
  planning_scope?: string;
  reuse_status?: string;
}) {
  const params = new URLSearchParams({
    source_school_year_id: filters.source_school_year_id,
    target_school_year_id: filters.target_school_year_id,
  });
  if (filters.subject_id) params.set("subject_id", filters.subject_id);
  if (filters.planning_scope) params.set("planning_scope", filters.planning_scope);
  if (filters.reuse_status) params.set("reuse_status", filters.reuse_status);
  return readJson<CurriculumRolloverCandidates>(
    `/v1/teacher-assist/curriculum-rollover/candidates?${params.toString()}`,
  );
}

export function createCurriculumRolloverCopy(body: CurriculumRolloverCopyInput) {
  return writeJson<CurriculumRolloverCopyResult>(
    "/v1/teacher-assist/curriculum-rollover/copy",
    "POST",
    body,
  );
}
