import { authFetch, parseErrorMessage } from "@/lib/auth-service";
import type {
  CatalogBrowseContext,
  CatalogGradeItem,
  CatalogListFilters,
  CatalogObjectiveItem,
  CatalogPaged,
  CatalogResourceItem,
  CatalogSubjectItem,
} from "@/lib/teacher-catalog-types";

const base = "/v1/teacher-assist/catalog";

function buildQuery(filters?: CatalogListFilters) {
  const params = new URLSearchParams();
  if (!filters) return "";
  if (filters.page) params.set("page", String(filters.page));
  if (filters.page_size) params.set("page_size", String(filters.page_size));
  if (filters.grade_id) params.set("grade_id", filters.grade_id);
  if (filters.grade_level) params.set("grade_level", filters.grade_level);
  if (filters.subject_code) params.set("subject_code", filters.subject_code);
  if (filters.objective_type) params.set("objective_type", filters.objective_type);
  if (filters.coverage_type) params.set("coverage_type", filters.coverage_type);
  if (filters.resource_type) params.set("resource_type", filters.resource_type);
  if (filters.q) params.set("q", filters.q);
  if (filters.state_id) params.set("state_id", filters.state_id);
  if (filters.district_id) params.set("district_id", filters.district_id);
  if (filters.school_id) params.set("school_id", filters.school_id);
  const query = params.toString();
  return query ? `?${query}` : "";
}

async function readJson<T>(path: string): Promise<T> {
  const res = await authFetch(path);
  if (!res || !res.ok) {
    throw new Error(res ? await parseErrorMessage(res) : "Could not reach API");
  }
  return (await res.json()) as T;
}

export function fetchCatalogBrowseContext(filters?: Pick<CatalogListFilters, "state_id" | "district_id" | "school_id">) {
  return readJson<CatalogBrowseContext>(`${base}/context${buildQuery(filters)}`);
}

export function fetchCatalogBrowseGrades(filters?: CatalogListFilters) {
  return readJson<CatalogPaged<CatalogGradeItem>>(`${base}/grades${buildQuery(filters)}`);
}

export function fetchCatalogBrowseSubjects(filters?: CatalogListFilters) {
  return readJson<CatalogPaged<CatalogSubjectItem>>(`${base}/subjects${buildQuery(filters)}`);
}

export function fetchCatalogBrowseObjectives(filters?: CatalogListFilters) {
  return readJson<CatalogPaged<CatalogObjectiveItem>>(`${base}/objectives${buildQuery(filters)}`);
}

export function fetchCatalogBrowseResources(filters?: CatalogListFilters) {
  return readJson<CatalogPaged<CatalogResourceItem>>(`${base}/resources${buildQuery(filters)}`);
}
