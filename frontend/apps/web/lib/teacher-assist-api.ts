import {
  authFetch,
  buildApiUrl,
  getStoredAccessToken,
  parseErrorMessage,
  refreshTokens,
} from "@/lib/auth-service";
import type {
  Assignment,
  AssignmentGradingReview,
  AssignmentGradingReviewAISuggestion,
  AssignmentGradingReviewAISuggestionInput,
  AssignmentGradebookCommitResult,
  AssignmentGradebookExportView,
  AssignmentGradeRecord,
  AssignmentGradeRecordDetail,
  AssignmentGradingReviewCreateInput,
  AssignmentGradingReviewUpdateInput,
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
  StandardImportCommitResult,
  StandardImportPreview,
  Subject,
  TeacherAssistWorkflow,
  TeacherAssistWorkflowDetail,
  TeacherAssistExtractedTextDetailAggregate,
  TeacherAssistExtractedTextHistory,
  TeacherAssistExtractedTextRecord,
  TeacherAssistExtractionJob,
  TeacherAssistExtractionJobDetail,
  TeacherAssistExtractionRun,
  TeacherAssistExtractionSummary,
  TeacherAssistFileDownload,
  TeacherAssistAssignmentGradingPrepSummary,
  TeacherAssistStudentWorkGradingPrepContext,
  TeacherAssistExportArtifact,
  TeacherAssistExportArtifactCreateInput,
  TeacherAssistExportArtifactDetail,
  TeacherAssistExportDownload,
  TeacherAssistActionWorkspace,
  TeacherAssistTodayWorkspace,
  AssignmentEffectiveness,
  MasteryCommitResult,
  MasteryEvaluation,
  MasteryEvaluationDetail,
  MasteryDashboard,
  ReteachPlan,
  ReteachPlanAIDraft,
  Newsletter,
  NewsletterAIDraft,
  NewsletterExport,
  NewsletterExportDownload,
  NewsletterSectionRegenerate,
  NewsletterVersion,
  ReteachPlanVersion,
  MasteryMatrix,
  MasteryMatrixHeatmap,
  LessonEffectiveness,
  LessonEffectivenessHistoricalComparison,
  LessonReflection,
  LessonReflectionAISuggestions,
  LessonReflectionVersion,
  MasteryMatrixReteachInsights,
  MasteryMatrixReteachSummary,
  MasteryMatrixStandardsSummary,
  MasteryMatrixStudentsSummary,
  MasteryMatrixSummary,
  StudentMasterySummary,
  TeacherAssistOptions,
  TeacherAssistWorkspace,
  TeacherAssistHomeWorkspace,
  TeacherAssistWorkQueue,
  TeacherAssistClassOperationalWorkspace,
  TeacherAssistUserPreferences,
  TeacherClass,
  TeacherMyClassroom,
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

export function fetchTeacherAssistWorkspace() {
  return readJson<TeacherAssistWorkspace>("/v1/teacher-assist/workspace");
}

export function fetchTeacherAssistActionWorkspace() {
  return readJson<TeacherAssistActionWorkspace>("/v1/teacher-assist/action-workspace");
}

export function fetchTeacherAssistTodayWorkspace() {
  return readJson<TeacherAssistTodayWorkspace>("/v1/teacher-assist/today");
}

export function fetchTeacherAssistHomeWorkspace() {
  return readJson<TeacherAssistHomeWorkspace>("/v1/teacher-assist/home");
}

export function fetchTeacherAssistWorkQueue() {
  return readJson<TeacherAssistWorkQueue>("/v1/teacher-assist/work-queue");
}

export function fetchTeacherAssistClassOperationalWorkspace(classId: string) {
  return readJson<TeacherAssistClassOperationalWorkspace>(
    `/v1/teacher-assist/classes/${classId}/workspace`,
  );
}

export function fetchTeacherAssistUserPreferences() {
  return readJson<TeacherAssistUserPreferences>("/v1/teacher-assist/user-preferences");
}

export function patchTeacherAssistUserPreferences(body: Record<string, unknown>) {
  return writeJson<TeacherAssistUserPreferences>("/v1/teacher-assist/user-preferences", "PATCH", body);
}

export function fetchTeacherProfile() {
  return readJson<TeacherProfile>("/v1/teacher-assist/profile");
}

export function saveTeacherProfile(body: Record<string, unknown>) {
  return writeJson<TeacherProfile>("/v1/teacher-assist/profile", "PUT", body);
}

export function fetchMyClassroom() {
  return readJson<TeacherMyClassroom>("/v1/teacher-assist/my-classroom");
}

export function saveMyClassroom(body: {
  homeroom_name: string;
  student_count: number;
  timezone?: string | null;
}) {
  return writeJson<TeacherMyClassroom>("/v1/teacher-assist/my-classroom", "PUT", body);
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

export function updateStandard(id: string, body: Record<string, unknown>) {
  return writeJson<Standard>(`/v1/teacher-assist/standards/${id}`, "PUT", body);
}

export function previewStandardsImport(body: { csv_content: string }) {
  return writeJson<StandardImportPreview>("/v1/teacher-assist/standards/import/preview", "POST", body);
}

export function commitStandardsImport(body: {
  rows: Array<{
    code: string;
    standard_type: string;
    subject_id: string;
    description: string;
  }>;
}) {
  return writeJson<StandardImportCommitResult>("/v1/teacher-assist/standards/import/commit", "POST", body);
}

export function fetchPacingGuides() {
  return readJson<PacingGuide[]>("/v1/teacher-assist/legacy/pacing-guides");
}

export function createPacingGuide(body: Record<string, unknown>) {
  return writeJson<PacingGuide>("/v1/teacher-assist/legacy/pacing-guides", "POST", body);
}

export function updatePacingGuide(id: string, body: Record<string, unknown>) {
  return writeJson<PacingGuide>(`/v1/teacher-assist/legacy/pacing-guides/${id}`, "PUT", body);
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

export function fetchResourceDownloadUrl(id: string) {
  return readJson<TeacherAssistFileDownload>(`/v1/teacher-assist/resources/${id}/download-url`);
}

export function createResourceExtractionJob(id: string) {
  return writeJson<TeacherAssistExtractionJob>(`/v1/teacher-assist/resources/${id}/extraction-jobs`, "POST", {});
}

export function fetchResourceExtractions(id: string) {
  return readJson<TeacherAssistExtractionRun[]>(`/v1/teacher-assist/resources/${id}/extractions`);
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

export function fetchAssignmentStudentWorkDownloadUrl(id: string) {
  return readJson<TeacherAssistFileDownload>(`/v1/teacher-assist/student-work/${id}/download-url`);
}

export function createStudentWorkExtractionJob(id: string) {
  return writeJson<TeacherAssistExtractionJob>(`/v1/teacher-assist/student-work/${id}/extraction-jobs`, "POST", {});
}

export function fetchStudentWorkExtractions(id: string) {
  return readJson<TeacherAssistExtractionRun[]>(`/v1/teacher-assist/student-work/${id}/extractions`);
}

export function fetchExtractionJob(id: string) {
  return readJson<TeacherAssistExtractionJobDetail>(`/v1/teacher-assist/extraction-jobs/${id}`);
}

export function cancelExtractionJob(id: string) {
  return writeJson<TeacherAssistExtractionJob>(`/v1/teacher-assist/extraction-jobs/${id}/cancel`, "PATCH", {
    status: "cancelled",
  });
}

export function fetchExtractionSummaries(limit = 100) {
  return readJson<TeacherAssistExtractionSummary[]>(`/v1/teacher-assist/extractions?limit=${limit}`);
}

export function retryExtractionJob(id: string) {
  return writeJson<TeacherAssistExtractionJob>(`/v1/teacher-assist/extraction-jobs/${id}/retry`, "POST", {});
}

export function fetchExtractedTextDetail(id: string) {
  return readJson<TeacherAssistExtractedTextDetailAggregate>(`/v1/teacher-assist/extracted-text/${id}`);
}

export function fetchExtractedTextHistory(id: string) {
  return readJson<TeacherAssistExtractedTextHistory>(`/v1/teacher-assist/extracted-text/${id}/history`);
}

export function updateExtractedTextReviewStatus(
  id: string,
  body: {
    review_status: TeacherAssistExtractedTextRecord["review_status"];
    teacher_review_notes?: string | null;
    teacher_issue_reason?: string | null;
  },
) {
  return writeJson<TeacherAssistExtractedTextRecord>(
    `/v1/teacher-assist/extracted-text/${id}/review-status`,
    "PATCH",
    body,
  );
}

export function updateExtractedTextApprovedContent(
  id: string,
  body: { approved_text?: string | null; teacher_corrected_text?: string | null },
) {
  return writeJson<TeacherAssistExtractedTextRecord>(
    `/v1/teacher-assist/extracted-text/${id}/approved-text`,
    "PUT",
    body,
  );
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

export function fetchAssignmentGradingReviews(id: string) {
  return readJson<AssignmentGradingReview[]>(`/v1/teacher-assist/assignments/${id}/grading-reviews`);
}

export function fetchAssignmentGradingPrepSummary(id: string) {
  return readJson<TeacherAssistAssignmentGradingPrepSummary>(
    `/v1/teacher-assist/assignments/${id}/grading-prep-summary`,
  );
}

export function fetchStudentWorkGradingPrepContext(id: string) {
  return readJson<TeacherAssistStudentWorkGradingPrepContext>(
    `/v1/teacher-assist/student-work/${id}/grading-prep-context`,
  );
}

export function createAssignmentGradingReview(
  studentWorkSubmissionId: string,
  body: AssignmentGradingReviewCreateInput,
) {
  return writeJson<AssignmentGradingReview>(
    `/v1/teacher-assist/student-work/${studentWorkSubmissionId}/grading-review`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function fetchAssignmentGradingReview(id: string) {
  return readJson<AssignmentGradingReview>(`/v1/teacher-assist/grading-reviews/${id}`);
}

export function updateAssignmentGradingReview(id: string, body: AssignmentGradingReviewUpdateInput) {
  return writeJson<AssignmentGradingReview>(
    `/v1/teacher-assist/grading-reviews/${id}`,
    "PUT",
    body as Record<string, unknown>,
  );
}

export function updateAssignmentGradingReviewStatus(
  id: string,
  status: AssignmentGradingReview["status"],
) {
  return writeJson<AssignmentGradingReview>(`/v1/teacher-assist/grading-reviews/${id}/status`, "PATCH", {
    status,
  });
}

export function generateAssignmentGradingReviewAISuggestion(
  id: string,
  body: AssignmentGradingReviewAISuggestionInput = { provider_mode: "mock" },
) {
  return writeJson<AssignmentGradingReviewAISuggestion>(
    `/v1/teacher-assist/grading-reviews/${id}/ai-suggestions`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function commitGradingReviewToGradebook(
  gradingReviewId: string,
  body: { teacher_confirmation_note?: string | null } = {},
) {
  return writeJson<AssignmentGradebookCommitResult>(
    `/v1/teacher-assist/grading-reviews/${gradingReviewId}/gradebook-commit`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function fetchAssignmentGradebookRecords(assignmentId: string, recordStatus?: string) {
  const params = new URLSearchParams();
  if (recordStatus) params.set("record_status", recordStatus);
  const query = params.toString();
  return readJson<AssignmentGradeRecord[]>(
    `/v1/teacher-assist/assignments/${assignmentId}/gradebook-records${query ? `?${query}` : ""}`,
  );
}

export function fetchGradebookRecordDetail(gradeRecordId: string) {
  return readJson<AssignmentGradeRecordDetail>(`/v1/teacher-assist/gradebook/records/${gradeRecordId}`);
}

export function createGradebookRecordCorrection(
  gradeRecordId: string,
  body: {
    committed_score?: number | null;
    max_score?: number | null;
    committed_feedback?: string | null;
    reason: string;
  },
) {
  return writeJson<AssignmentGradebookCommitResult>(
    `/v1/teacher-assist/gradebook/records/${gradeRecordId}/corrections`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function createGradebookRecordReversal(gradeRecordId: string, body: { reason: string }) {
  return writeJson<AssignmentGradebookCommitResult>(
    `/v1/teacher-assist/gradebook/records/${gradeRecordId}/reversals`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function fetchAssignmentGradebookExport(assignmentId: string) {
  return readJson<AssignmentGradebookExportView>(
    `/v1/teacher-assist/assignments/${assignmentId}/gradebook-export`,
  );
}

function buildQuery(params: Record<string, string | undefined>) {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value) search.set(key, value);
  }
  const query = search.toString();
  return query ? `?${query}` : "";
}

export function fetchMasteryMatrices(filters: {
  school_year_id?: string;
  grading_period_id?: string;
  class_id?: string;
  subject_id?: string;
  status?: string;
} = {}) {
  const query = buildQuery({
    school_year_id: filters.school_year_id,
    grading_period_id: filters.grading_period_id,
    class_id: filters.class_id,
    subject_id: filters.subject_id,
    status: filters.status,
  });
  return readJson<MasteryMatrix[]>(`/v1/teacher-assist/mastery-matrices${query}`);
}

export function fetchMasteryMatrix(masteryMatrixId: string) {
  return readJson<MasteryMatrix>(`/v1/teacher-assist/mastery-matrices/${masteryMatrixId}`);
}

export function createMasteryMatrix(body: Record<string, unknown>) {
  return writeJson<MasteryMatrix>("/v1/teacher-assist/mastery-matrices", "POST", body);
}

export function updateMasteryMatrix(masteryMatrixId: string, body: Record<string, unknown>) {
  return writeJson<MasteryMatrix>(`/v1/teacher-assist/mastery-matrices/${masteryMatrixId}`, "PUT", body);
}

export function createMasteryEvaluation(body: Record<string, unknown>) {
  return writeJson<MasteryEvaluation>("/v1/teacher-assist/mastery-evaluations", "POST", body);
}

export function updateMasteryEvaluation(masteryEvaluationId: string, body: Record<string, unknown>) {
  return writeJson<MasteryEvaluation>(
    `/v1/teacher-assist/mastery-evaluations/${masteryEvaluationId}`,
    "PUT",
    body,
  );
}

export function fetchMasteryEvaluationDetail(masteryEvaluationId: string) {
  return readJson<MasteryEvaluationDetail>(
    `/v1/teacher-assist/mastery-evaluations/${masteryEvaluationId}`,
  );
}

export function commitMasteryEvaluation(masteryEvaluationId: string, body: Record<string, unknown> = {}) {
  return writeJson<MasteryCommitResult>(
    `/v1/teacher-assist/mastery-evaluations/${masteryEvaluationId}/commit`,
    "POST",
    body,
  );
}

export function createMasteryEvaluationCorrection(
  masteryEvaluationId: string,
  body: Record<string, unknown>,
) {
  return writeJson<MasteryCommitResult>(
    `/v1/teacher-assist/mastery-evaluations/${masteryEvaluationId}/corrections`,
    "POST",
    body,
  );
}

export function createMasteryEvaluationReversal(
  masteryEvaluationId: string,
  body: Record<string, unknown>,
) {
  return writeJson<MasteryCommitResult>(
    `/v1/teacher-assist/mastery-evaluations/${masteryEvaluationId}/reversals`,
    "POST",
    body,
  );
}

export function fetchMasteryMatrixSummary(masteryMatrixId: string) {
  return readJson<MasteryMatrixSummary>(`/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/summary`);
}

export function fetchMasteryMatrixStandardsSummary(masteryMatrixId: string) {
  return readJson<MasteryMatrixStandardsSummary>(
    `/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/standards`,
  );
}

export function fetchMasteryMatrixStudentsSummary(masteryMatrixId: string) {
  return readJson<MasteryMatrixStudentsSummary>(
    `/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/students`,
  );
}

export function fetchMasteryMatrixReteachSummary(masteryMatrixId: string) {
  return readJson<MasteryMatrixReteachSummary>(
    `/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/reteach-summary`,
  );
}

export function fetchMasteryMatrixHeatmap(masteryMatrixId: string) {
  return readJson<MasteryMatrixHeatmap>(`/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/heatmap`);
}

export function fetchMasteryMatrixReteachInsights(masteryMatrixId: string) {
  return readJson<MasteryMatrixReteachInsights>(
    `/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/reteach-insights`,
  );
}

export function fetchStudentMasterySummary(masteryMatrixId: string, studentNumber: number) {
  return readJson<StudentMasterySummary>(
    `/v1/teacher-assist/mastery-matrices/${masteryMatrixId}/student-summary/${studentNumber}`,
  );
}

export function fetchAssignmentEffectiveness(assignmentId: string) {
  return readJson<AssignmentEffectiveness>(`/v1/teacher-assist/assignments/${assignmentId}/effectiveness`);
}

export function fetchMasteryDashboard(filters: {
  school_year_id?: string;
  grading_period_id?: string;
  class_id?: string;
  subject_id?: string;
} = {}) {
  const params = new URLSearchParams();
  if (filters.school_year_id) params.set("school_year_id", filters.school_year_id);
  if (filters.grading_period_id) params.set("grading_period_id", filters.grading_period_id);
  if (filters.class_id) params.set("class_id", filters.class_id);
  if (filters.subject_id) params.set("subject_id", filters.subject_id);
  const query = params.toString() ? `?${params.toString()}` : "";
  return readJson<MasteryDashboard>(`/v1/teacher-assist/mastery-dashboard${query}`);
}

export function fetchReteachPlans(filters: {
  mastery_matrix_id?: string;
  standard_id?: string;
  status?: string;
} = {}) {
  const params = new URLSearchParams();
  if (filters.mastery_matrix_id) params.set("mastery_matrix_id", filters.mastery_matrix_id);
  if (filters.standard_id) params.set("standard_id", filters.standard_id);
  if (filters.status) params.set("status", filters.status);
  const query = params.toString() ? `?${params.toString()}` : "";
  return readJson<ReteachPlan[]>(`/v1/teacher-assist/reteach-plans${query}`);
}

export function fetchReteachPlan(id: string) {
  return readJson<ReteachPlan>(`/v1/teacher-assist/reteach-plans/${id}`);
}

export function createReteachPlan(body: {
  mastery_matrix_id: string;
  standard_id: string;
  title?: string;
}) {
  return writeJson<ReteachPlan>("/v1/teacher-assist/reteach-plans", "POST", body);
}

export function updateReteachPlan(
  id: string,
  body: { title?: string; status?: string },
) {
  return writeJson<ReteachPlan>(`/v1/teacher-assist/reteach-plans/${id}`, "PUT", body);
}

export function fetchReteachPlanVersions(id: string) {
  return readJson<ReteachPlanVersion[]>(`/v1/teacher-assist/reteach-plans/${id}/versions`);
}

export function createReteachPlanVersion(
  id: string,
  body: { content_json: Record<string, unknown>; change_reason?: string },
) {
  return writeJson<ReteachPlanVersion>(
    `/v1/teacher-assist/reteach-plans/${id}/versions`,
    "POST",
    body,
  );
}

export function generateReteachPlanAIDraft(
  id: string,
  body: { provider_mode?: "mock" | "real"; teacher_instructions?: string } = {},
) {
  return writeJson<ReteachPlanAIDraft>(
    `/v1/teacher-assist/reteach-plans/${id}/ai-draft`,
    "POST",
    body,
  );
}

export function fetchNewsletters(filters: {
  school_year_id?: string;
  grading_period_id?: string;
  class_id?: string;
  subject_id?: string;
  status?: string;
} = {}) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const query = params.toString() ? `?${params.toString()}` : "";
  return readJson<Newsletter[]>(`/v1/teacher-assist/newsletters${query}`);
}

export function fetchNewsletter(id: string) {
  return readJson<Newsletter>(`/v1/teacher-assist/newsletters/${id}`);
}

export function createNewsletter(body: {
  school_year_id: string;
  grading_period_id?: string;
  class_id: string;
  subject_id: string;
  title?: string;
  teacher_notes?: string;
  week_start_date?: string;
  week_end_date?: string;
}) {
  return writeJson<Newsletter>("/v1/teacher-assist/newsletters", "POST", body);
}

export function updateNewsletter(id: string, body: Record<string, unknown>) {
  return writeJson<Newsletter>(`/v1/teacher-assist/newsletters/${id}`, "PUT", body);
}

export function fetchNewsletterVersions(id: string) {
  return readJson<NewsletterVersion[]>(`/v1/teacher-assist/newsletters/${id}/versions`);
}

export function createNewsletterVersion(
  id: string,
  body: { content_json: Record<string, unknown>; change_reason?: string },
) {
  return writeJson<NewsletterVersion>(`/v1/teacher-assist/newsletters/${id}/versions`, "POST", body);
}

export function generateNewsletterAIDraft(
  id: string,
  body: { provider_mode?: "mock" | "real"; teacher_instructions?: string } = {},
) {
  return writeJson<NewsletterAIDraft>(`/v1/teacher-assist/newsletters/${id}/ai-draft`, "POST", body);
}

export function regenerateNewsletterSection(
  id: string,
  body: {
    section: "overview" | "upcoming_learning" | "teacher_message" | "reminders";
    provider_mode?: "mock" | "real";
    teacher_instructions?: string;
  },
) {
  return writeJson<NewsletterSectionRegenerate>(
    `/v1/teacher-assist/newsletters/${id}/regenerate-section`,
    "POST",
    body,
  );
}

export function createNewsletterExport(id: string, exportFormat: "html" | "pdf" | "docx") {
  return writeJson<NewsletterExport>(`/v1/teacher-assist/newsletters/${id}/exports`, "POST", {
    export_format: exportFormat,
  });
}

export function fetchNewsletterExportDownload(newsletterId: string, exportId: string) {
  return readJson<NewsletterExportDownload>(
    `/v1/teacher-assist/newsletters/${newsletterId}/exports/${exportId}/download`,
  );
}

export function fetchLessonEffectiveness(filters: {
  school_year_id?: string;
  grading_period_id?: string;
  class_id?: string;
  subject_id?: string;
  classification?: string;
} = {}) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const query = params.toString() ? `?${params.toString()}` : "";
  return readJson<LessonEffectiveness[]>(`/v1/teacher-assist/lesson-effectiveness${query}`);
}

export function fetchLessonEffectivenessHistoricalComparison(filters: {
  school_year_id: string;
  class_id: string;
  subject_id: string;
  grading_period_id?: string;
}) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  return readJson<LessonEffectivenessHistoricalComparison>(
    `/v1/teacher-assist/lesson-effectiveness/historical-comparison?${params.toString()}`,
  );
}

export function fetchLessonReflections(filters: {
  school_year_id?: string;
  grading_period_id?: string;
  class_id?: string;
  subject_id?: string;
  weekly_plan_id?: string;
  status?: string;
} = {}) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const query = params.toString() ? `?${params.toString()}` : "";
  return readJson<LessonReflection[]>(`/v1/teacher-assist/reflections${query}`);
}

export function fetchLessonReflection(id: string) {
  return readJson<LessonReflection>(`/v1/teacher-assist/reflections/${id}`);
}

export function createLessonReflection(body: {
  school_year_id: string;
  grading_period_id?: string;
  class_id: string;
  subject_id: string;
  weekly_plan_id?: string;
  title?: string;
  lesson_date?: string;
}) {
  return writeJson<LessonReflection>("/v1/teacher-assist/reflections", "POST", body);
}

export function updateLessonReflection(id: string, body: Record<string, unknown>) {
  return writeJson<LessonReflection>(`/v1/teacher-assist/reflections/${id}`, "PUT", body);
}

export function fetchLessonReflectionVersions(id: string) {
  return readJson<LessonReflectionVersion[]>(`/v1/teacher-assist/reflections/${id}/versions`);
}

export function createLessonReflectionVersion(
  id: string,
  body: { content_json: Record<string, unknown>; change_reason?: string },
) {
  return writeJson<LessonReflectionVersion>(`/v1/teacher-assist/reflections/${id}/versions`, "POST", body);
}

export function generateLessonReflectionAISuggestions(
  id: string,
  body: { provider_mode?: string; teacher_instructions?: string } = {},
) {
  return writeJson<LessonReflectionAISuggestions>(
    `/v1/teacher-assist/reflections/${id}/ai-suggestions`,
    "POST",
    body,
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

export function createWeeklyPlanExport(
  planId: string,
  body: TeacherAssistExportArtifactCreateInput,
) {
  return writeJson<TeacherAssistExportArtifact>(
    `/v1/teacher-assist/weekly-plans/${planId}/exports`,
    "POST",
    body as Record<string, unknown>,
  );
}

export function fetchExportArtifacts(filters: {
  artifact_type?: string;
  artifact_status?: string;
  source_plan_id?: string;
  limit?: number;
} = {}) {
  const params = new URLSearchParams();
  Object.entries(filters).forEach(([key, value]) => {
    if (value === undefined || value === null || value === "") return;
    params.set(key, String(value));
  });
  const query = params.toString();
  return readJson<TeacherAssistExportArtifact[]>(
    `/v1/teacher-assist/exports${query ? `?${query}` : ""}`,
  );
}

export function fetchExportArtifactDetail(id: string) {
  return readJson<TeacherAssistExportArtifactDetail>(`/v1/teacher-assist/exports/${id}`);
}

export function fetchExportArtifactDownload(id: string) {
  return readJson<TeacherAssistExportDownload>(`/v1/teacher-assist/exports/${id}/download`);
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
