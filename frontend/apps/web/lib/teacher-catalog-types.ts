export type CatalogPageMeta = {
  page: number;
  page_size: number;
  total: number;
  total_pages: number;
};

export type CatalogBrowseContext = {
  assignment: {
    id: string | null;
    state: { id: string; name: string; abbreviation: string };
    district: { id: string; name: string };
    school: { id: string; name: string; school_type: string | null };
  } | null;
  missing_assignment: boolean;
  multiple_assignments_detected: boolean;
  can_browse: boolean;
  is_root_unscoped: boolean;
  scope_filters: {
    state_id: string | null;
    district_id: string | null;
    school_id: string | null;
  };
  scope_labels: {
    state_name: string | null;
    district_name: string | null;
    school_name: string | null;
  };
  scope_banner: string | null;
};

export type CatalogGradeItem = {
  id: string;
  grade_code: string;
  display_name: string;
  active: boolean;
  subject_count: number;
};

export type CatalogSubjectItem = {
  id: string;
  grade_id: string | null;
  grade_code: string | null;
  subject_code: string;
  display_name: string;
  active: boolean;
  objective_count: number;
  resource_count: number;
};

export type CatalogResourceLinkItem = {
  id: string;
  link_title: string;
  url: string;
  active: boolean;
};

export type CatalogLinkedResourceItem = {
  id: string;
  title: string;
  resource_type: string;
  reference_links: CatalogResourceLinkItem[];
};

export type CatalogObjectiveItem = {
  id: string;
  objective_id: string;
  objective_type: string;
  description: string;
  coverage_type: string;
  grade_level: string;
  subject_code: string;
  active: boolean;
  linked_resources: CatalogLinkedResourceItem[];
};

export type CatalogResourceItem = {
  id: string;
  title: string;
  resource_type: string;
  description: string | null;
  grade_level: string;
  subject_code: string;
  storage_key: string | null;
  active: boolean;
  reference_links: CatalogResourceLinkItem[];
  associated_objectives: Array<{
    id: string;
    objective_id: string;
    objective_type: string;
    coverage_type: string;
    grade_level: string;
    subject_code: string;
  }>;
};

export type CatalogPaged<T> = {
  items: T[];
  meta: CatalogPageMeta;
};

export type CatalogBrowseSection = "grades" | "subjects" | "objectives" | "resources";

export type CatalogListFilters = {
  page?: number;
  page_size?: number;
  grade_id?: string;
  grade_level?: string;
  subject_code?: string;
  objective_type?: string;
  coverage_type?: string;
  resource_type?: string;
  q?: string;
  state_id?: string;
  district_id?: string;
  school_id?: string;
};

export const CATALOG_GRADE_OPTIONS = ["K", "1", "2", "3", "4", "5"];
export const CATALOG_SUBJECT_OPTIONS = ["ELA", "Math", "Science", "Social Studies"];
export const CATALOG_OBJECTIVE_TYPE_OPTIONS = ["TEKS", "CommonCore", "DistrictObjective", "Custom"];
export const CATALOG_COVERAGE_TYPE_OPTIONS = ["required", "optional", "enrichment"];
export const CATALOG_RESOURCE_TYPE_OPTIONS = ["curriculum", "textbook", "reference"];
