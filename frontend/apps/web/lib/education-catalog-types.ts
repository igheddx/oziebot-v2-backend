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

export type TeacherSchoolAssignmentListItem = TeacherSchoolAssignment & {
  user_email?: string | null;
  user_full_name?: string | null;
  state_name?: string | null;
  district_name?: string | null;
  school_name?: string | null;
};

export type AvailableTeacher = {
  user_id: string;
  email: string;
  full_name?: string | null;
};

export type TeacherSchoolAssignmentProvisionResult = {
  assignment: TeacherSchoolAssignment;
  user_id: string;
  email: string;
  full_name?: string | null;
  created_user: boolean;
  temporary_password?: string | null;
  grade_setup_applied: boolean;
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

export type TeacherMySchoolSetup = {
  assignment: {
    id: string;
    state_id: string;
    state_name: string;
    state_abbreviation: string;
    district_id: string;
    district_name: string;
    school_id: string;
    school_name: string;
    school_type: string | null;
  } | null;
  catalog_grade_id: string | null;
  catalog_grade_code: string | null;
  selected_catalog_subject_ids: string[];
  synced_subjects: Array<{
    catalog_subject_id: string;
    tenant_subject_id: string;
    subject_code: string;
    display_name: string;
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
