import { authFetch, parseErrorMessage } from "@/lib/auth-service";
import type {
  CatalogImportPreview,
  EducationCurriculumResource,
  EducationDistrict,
  EducationGrade,
  EducationObjective,
  EducationSchool,
  EducationState,
  EducationSubject,
  TeacherCatalogContext,
  TeacherMySchoolSetup,
  TeacherSchoolAssignment,
  TeacherSchoolAssignmentListItem,
  AvailableTeacher,
  TeacherSchoolAssignmentProvisionResult,
} from "@/lib/education-catalog-types";

const base = "/v1/teacher-assist/education-catalog";

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

export function fetchMyCatalogContext() {
  return readJson<TeacherCatalogContext>(`${base}/my-context`);
}

export function fetchMySchoolSetup() {
  return readJson<TeacherMySchoolSetup>(`${base}/my-school-setup`);
}

export function saveMySchoolSetup(body: {
  state_id: string;
  district_id: string;
  school_id: string;
  catalog_grade_id: string;
  catalog_subject_ids: string[];
}) {
  return writeJson<TeacherMySchoolSetup>(`${base}/my-school-setup`, "PUT", body);
}

export function fetchCatalogStates(q?: string, activeOnly = true) {
  const params = new URLSearchParams();
  if (q) params.set("q", q);
  if (activeOnly) params.set("active_only", "true");
  const query = params.toString();
  return readJson<EducationState[]>(`${base}/states${query ? `?${query}` : ""}`);
}

export function createCatalogState(body: Record<string, unknown>) {
  return writeJson<EducationState>(`${base}/states`, "POST", body);
}

export function updateCatalogState(id: string, body: Record<string, unknown>) {
  return writeJson<EducationState>(`${base}/states/${id}`, "PUT", body);
}

export function fetchCatalogDistricts(stateId?: string, q?: string, activeOnly = true) {
  const params = new URLSearchParams();
  if (stateId) params.set("state_id", stateId);
  if (q) params.set("q", q);
  if (activeOnly) params.set("active_only", "true");
  const query = params.toString();
  return readJson<EducationDistrict[]>(`${base}/districts${query ? `?${query}` : ""}`);
}

export function createCatalogDistrict(body: Record<string, unknown>) {
  return writeJson<EducationDistrict>(`${base}/districts`, "POST", body);
}

export function updateCatalogDistrict(id: string, body: Record<string, unknown>) {
  return writeJson<EducationDistrict>(`${base}/districts/${id}`, "PUT", body);
}

export function fetchCatalogSchools(districtId?: string, q?: string, activeOnly = true) {
  const params = new URLSearchParams();
  if (districtId) params.set("district_id", districtId);
  if (q) params.set("q", q);
  if (activeOnly) params.set("active_only", "true");
  const query = params.toString();
  return readJson<EducationSchool[]>(`${base}/schools${query ? `?${query}` : ""}`);
}

export function createCatalogSchool(body: Record<string, unknown>) {
  return writeJson<EducationSchool>(`${base}/schools`, "POST", body);
}

export function updateCatalogSchool(id: string, body: Record<string, unknown>) {
  return writeJson<EducationSchool>(`${base}/schools/${id}`, "PUT", body);
}

export function fetchCatalogGrades(schoolId?: string) {
  const params = schoolId ? `?school_id=${encodeURIComponent(schoolId)}` : "";
  return readJson<EducationGrade[]>(`${base}/grades${params}`);
}

export function createCatalogGrade(body: Record<string, unknown>) {
  return writeJson<EducationGrade>(`${base}/grades`, "POST", body);
}

export function updateCatalogGrade(id: string, body: Record<string, unknown>) {
  return writeJson<EducationGrade>(`${base}/grades/${id}`, "PUT", body);
}

export function fetchCatalogSubjects(gradeId?: string) {
  const params = gradeId ? `?grade_id=${encodeURIComponent(gradeId)}` : "";
  return readJson<EducationSubject[]>(`${base}/subjects${params}`);
}

export function createCatalogSubject(body: Record<string, unknown>) {
  return writeJson<EducationSubject>(`${base}/subjects`, "POST", body);
}

export function updateCatalogSubject(id: string, body: Record<string, unknown>) {
  return writeJson<EducationSubject>(`${base}/subjects/${id}`, "PUT", body);
}

export function fetchCatalogObjectives(filters?: {
  state_id?: string;
  grade_level?: string;
  subject_code?: string;
  q?: string;
  active_only?: boolean;
}) {
  const params = new URLSearchParams();
  if (filters?.state_id) params.set("state_id", filters.state_id);
  if (filters?.grade_level) params.set("grade_level", filters.grade_level);
  if (filters?.subject_code) params.set("subject_code", filters.subject_code);
  if (filters?.q) params.set("q", filters.q);
  if (filters?.active_only === false) params.set("active_only", "false");
  const query = params.toString();
  return readJson<EducationObjective[]>(`${base}/objectives${query ? `?${query}` : ""}`);
}

export function createCatalogObjective(body: Record<string, unknown>) {
  return writeJson<EducationObjective>(`${base}/objectives`, "POST", body);
}

export function updateCatalogObjective(id: string, body: Record<string, unknown>) {
  return writeJson<EducationObjective>(`${base}/objectives/${id}`, "PUT", body);
}

export function previewCatalogObjectivesImport(body: { csv_content: string }) {
  return writeJson<CatalogImportPreview>(`${base}/objectives/import/preview`, "POST", body);
}

export function commitCatalogObjectivesImport(body: { rows: Array<Record<string, string>> }) {
  return writeJson<{
    created_count: number;
    skipped_duplicate_count: number;
    errors: CatalogImportPreview["errors"];
  }>(`${base}/objectives/import/commit`, "POST", body);
}

export function fetchCatalogCurriculumResources(filters?: {
  school_id?: string;
  grade_level?: string;
  subject_code?: string;
  resource_type?: string;
  active_only?: boolean;
}) {
  const params = new URLSearchParams();
  if (filters?.school_id) params.set("school_id", filters.school_id);
  if (filters?.grade_level) params.set("grade_level", filters.grade_level);
  if (filters?.subject_code) params.set("subject_code", filters.subject_code);
  if (filters?.resource_type) params.set("resource_type", filters.resource_type);
  if (filters?.active_only === false) params.set("active_only", "false");
  const query = params.toString();
  return readJson<EducationCurriculumResource[]>(`${base}/curriculum-resources${query ? `?${query}` : ""}`);
}

export function createCatalogCurriculumResource(body: Record<string, unknown>) {
  return writeJson<EducationCurriculumResource>(`${base}/curriculum-resources`, "POST", body);
}

export function updateCatalogCurriculumResource(id: string, body: Record<string, unknown>) {
  return writeJson<EducationCurriculumResource>(`${base}/curriculum-resources/${id}`, "PUT", body);
}

export function fetchCatalogTeacherAssignments(filters?: {
  userId?: string;
  stateId?: string;
  districtId?: string;
  schoolId?: string;
  activeOnly?: boolean;
}) {
  const params = new URLSearchParams();
  if (filters?.userId) params.set("user_id", filters.userId);
  if (filters?.stateId) params.set("state_id", filters.stateId);
  if (filters?.districtId) params.set("district_id", filters.districtId);
  if (filters?.schoolId) params.set("school_id", filters.schoolId);
  if (filters?.activeOnly) params.set("active_only", "true");
  const query = params.toString();
  return readJson<TeacherSchoolAssignmentListItem[]>(`${base}/teacher-assignments${query ? `?${query}` : ""}`);
}

export function fetchCatalogAvailableTeachers(schoolId: string, q?: string) {
  const params = new URLSearchParams({ school_id: schoolId });
  if (q?.trim()) params.set("q", q.trim());
  return readJson<AvailableTeacher[]>(`${base}/teacher-assignments/available-teachers?${params.toString()}`);
}

export function provisionCatalogTeacherAssignment(body: Record<string, unknown>) {
  return writeJson<TeacherSchoolAssignmentProvisionResult>(`${base}/teacher-assignments/provision`, "POST", body);
}

export function createCatalogTeacherAssignment(body: Record<string, unknown>) {
  return writeJson<TeacherSchoolAssignment>(`${base}/teacher-assignments`, "POST", body);
}

export function updateCatalogTeacherAssignment(id: string, body: Record<string, unknown>) {
  return writeJson<TeacherSchoolAssignment>(`${base}/teacher-assignments/${id}`, "PUT", body);
}
