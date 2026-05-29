export type EducationState = {
  id: string;
  name: string;
  abbreviation: string;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type EducationDistrict = {
  id: string;
  state_id: string;
  name: string;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type EducationSchool = {
  id: string;
  district_id: string;
  name: string;
  school_type: string | null;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type EducationGrade = {
  id: string;
  school_id: string | null;
  grade_code: string;
  display_name: string;
  active: boolean;
};

export type EducationSubject = {
  id: string;
  grade_id: string | null;
  subject_code: string;
  display_name: string;
  active: boolean;
};

export type EducationObjective = {
  id: string;
  state_id: string;
  grade_level: string;
  subject_code: string;
  objective_type: string;
  objective_id: string;
  description: string;
  coverage_type: string;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type EducationCurriculumResource = {
  id: string;
  state_id: string | null;
  district_id: string | null;
  school_id: string | null;
  grade_level: string;
  subject_code: string;
  resource_type: string;
  title: string;
  description: string | null;
  storage_key: string | null;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type TeacherSchoolAssignment = {
  id: string;
  user_id: string;
  state_id: string;
  district_id: string;
  school_id: string;
  active: boolean;
  created_at: string;
  updated_at: string;
};

export type CatalogImportPreview = {
  total_rows: number;
  valid_count: number;
  invalid_count: number;
  duplicate_count: number;
  errors: Array<{ row_number: number; message: string; field?: string | null }>;
};

export type TeacherCatalogContext = {
  assignment: {
    id: string;
    state: { id: string; name: string; abbreviation: string };
    district: { id: string; name: string };
    school: { id: string; name: string; school_type: string | null };
  } | null;
  grades: Array<{ id: string; grade_code: string; display_name: string }>;
  subjects: Array<{ id: string; grade_id: string | null; subject_code: string; display_name: string }>;
  objectives: Array<{
    id: string;
    objective_id: string;
    grade_level: string;
    subject_code: string;
    description: string;
    coverage_type: string;
    objective_type: string;
  }>;
  resources: Array<{
    id: string;
    title: string;
    resource_type: string;
    grade_level: string;
    subject_code: string;
    description: string | null;
  }>;
};

export type CatalogSection =
  | "states"
  | "districts"
  | "schools"
  | "grades"
  | "subjects"
  | "objectives"
  | "curriculum"
  | "assignments"
  | "pacing_guides";
