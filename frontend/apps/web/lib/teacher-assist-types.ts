export type TeacherProfile = {
  id: string | null;
  preferred_grade_level: string | null;
  default_student_count: number | null;
  preferred_grading_period_type: string | null;
  timezone: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type TeacherAssistOptions = {
  grading_period_types: string[];
  standard_types: string[];
  resource_types: string[];
  assignment_types: string[];
  assignment_statuses: string[];
  assignment_print_packet_statuses: string[];
  assignment_print_template_types: string[];
  assignment_print_output_formats: string[];
  assignment_student_work_upload_statuses: string[];
  assignment_student_work_processing_statuses: string[];
  planning_draft_statuses: string[];
  planning_scopes: string[];
  supported_grade_levels: string[];
};

export type SchoolYear = {
  id: string;
  tenant_id: string;
  title: string;
  start_date: string;
  end_date: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
};

export type GradingPeriod = {
  id: string;
  school_year_id: string;
  title: string;
  grading_period_type: string;
  start_date: string;
  end_date: string;
  sort_order: number;
  created_at: string;
  updated_at: string;
};

export type Subject = {
  id: string;
  tenant_id: string;
  code: string | null;
  name: string;
  created_at: string;
  updated_at: string;
};

export type TeacherClass = {
  id: string;
  tenant_id: string;
  school_year_id: string;
  name: string;
  grade_level: string;
  student_count: number;
  subject_ids: string[];
  student_number_range_start: number;
  student_number_range_end: number;
  created_at: string;
  updated_at: string;
};

export type Standard = {
  id: string;
  tenant_id: string;
  subject_id: string | null;
  standard_type: string;
  code: string;
  description: string;
  grade_level: string | null;
  school_year_id: string | null;
  created_at: string;
  updated_at: string;
};

export type PacingGuide = {
  id: string;
  tenant_id: string;
  school_year_id: string;
  title: string;
  description: string | null;
  grade_level: string | null;
  subject_id: string | null;
  is_shared: boolean;
  created_by_user_id: string;
  item_count: number;
  created_at: string;
  updated_at: string;
};

export type PacingItem = {
  id: string;
  pacing_guide_id: string;
  grading_period_id: string | null;
  subject_id: string | null;
  week_number: number | null;
  day_number: number | null;
  instructional_date: string | null;
  title: string;
  instructional_focus: string | null;
  objectives: string | null;
  notes: string | null;
  sort_order: number | null;
  standard_ids: string[];
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type ResourceLibraryItem = {
  id: string;
  tenant_id: string;
  uploaded_by_user_id: string;
  title: string;
  description: string | null;
  resource_type: string;
  storage_key: string | null;
  original_filename: string | null;
  mime_type: string | null;
  file_size: number | null;
  external_url: string | null;
  uploaded_at: string;
  linked_pacing_items_count: number;
  linked_planning_drafts_count: number;
  created_at: string;
  updated_at: string;
};

export type Assignment = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  description: string | null;
  assignment_type:
    | "writing"
    | "reading_response"
    | "short_answer"
    | "quiz"
    | "exit_ticket"
    | "project"
    | "homework"
    | "other";
  due_date: string | null;
  status:
    | "draft"
    | "ready"
    | "assigned"
    | "collected"
    | "review_in_progress"
    | "reviewed"
    | "archived";
  instructions: string | null;
  rubric_json: Record<string, unknown> | null;
  source_plan_id: string | null;
  source_context_json: Record<string, unknown> | null;
  standard_ids: string[];
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type AssignmentInput = {
  school_year_id: string;
  grading_period_id?: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  description?: string | null;
  assignment_type?: Assignment["assignment_type"];
  due_date?: string | null;
  status?: Assignment["status"];
  instructions?: string | null;
  rubric_json?: Record<string, unknown> | null;
  source_plan_id?: string | null;
  source_context_json?: Record<string, unknown> | null;
  standard_ids?: string[];
  resource_ids?: string[];
};

export type AssignmentPrintPacket = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  class_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  subject_id: string;
  packet_status: "generated" | "archived";
  pages_per_student: number;
  student_count: number;
  template_type: "blank_writing_page" | "lined_writing_page" | "short_answer_page";
  output_format: "html";
  storage_key: string | null;
  total_page_count: number;
  created_at: string;
  updated_at: string;
};

export type AssignmentPrintPacketInput = {
  pages_per_student?: number;
  template_type?: AssignmentPrintPacket["template_type"];
  output_format?: AssignmentPrintPacket["output_format"];
};

export type AssignmentPrintPage = {
  id: string;
  packet_id: string;
  assignment_id: string;
  student_number: number;
  page_number: number;
  qr_payload_json: Record<string, unknown>;
  qr_token: string;
  qr_svg_data_uri: string;
  created_at: string;
};

export type AssignmentStudentWorkSubmission = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  assignment_print_packet_id: string | null;
  assignment_print_page_id: string | null;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  student_number: number;
  original_filename: string;
  mime_type: string;
  file_size: number;
  storage_key: string;
  upload_status: "uploaded" | "archived";
  processing_status: "pending_review" | "ready_for_processing" | "processing_deferred" | "archived";
  created_at: string;
  updated_at: string;
};

export type PlanningDraft = {
  id: string;
  tenant_id: string;
  user_id: string;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  subject_ids: string[];
  pacing_item_ids: string[];
  standard_ids: string[];
  title: string | null;
  plan_title: string | null;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  notes: string | null;
  status: string;
  resource_ids: string[];
  created_at: string;
  updated_at: string;
};

export type PlanningDraftReadiness = {
  is_ready: boolean;
  missing_items: string[];
  warnings: string[];
};

export type PlanningDraftContextPreview = {
  draft: PlanningDraft;
  school_year: SchoolYear | null;
  grading_period: GradingPeriod | null;
  class: TeacherClass | null;
  subjects: Subject[];
  pacing_items: PacingItem[];
  pacing_groups: Array<{
    group_key: string;
    label: string;
    pacing_items: PacingItem[];
  }>;
  standards: Standard[];
  resources: ResourceLibraryItem[];
  teacher_notes: string | null;
  duration_summary: {
    start_date: string | null;
    end_date: string | null;
    estimated_weeks: number | null;
    instructional_days_count: number | null;
    summary: string;
  };
  readiness: PlanningDraftReadiness;
};

export type TeacherAssistWorkflowStep = {
  id: string;
  workflow_id: string;
  step_name: string;
  status: "queued" | "running" | "completed" | "failed" | "skipped";
  metadata_json: Record<string, unknown> | null;
  error_message: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string;
};

export type TeacherAssistWorkflow = {
  id: string;
  tenant_id: string;
  user_id: string;
  planning_input_draft_id: string | null;
  workflow_type:
    | "weekly_plan_generation"
    | "daily_deck_generation"
    | "assessment_generation"
    | "newsletter_generation"
    | "grading_assist";
  status: "queued" | "running" | "completed" | "failed" | "cancelled";
  input_snapshot_json: Record<string, unknown>;
  output_ref_type: string | null;
  output_ref_id: string | null;
  error_message: string | null;
  last_error_code: string | null;
  progress_percent: number;
  leased_by_worker: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  retry_count: number;
  max_retries: number;
  timeout_at: string | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  input_tokens_total: number;
  output_tokens_total: number;
  estimated_cost_cents_total: number;
  execution_log_json: Array<Record<string, unknown>>;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
};

export type TeacherAssistAIUsageEvent = {
  id: string;
  tenant_id: string;
  user_id: string;
  workflow_id: string | null;
  provider: string;
  model: string | null;
  feature: string;
  input_tokens: number | null;
  output_tokens: number | null;
  estimated_cost_cents: number | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
};

export type TeacherAssistWorkflowDetail = TeacherAssistWorkflow & {
  steps: TeacherAssistWorkflowStep[];
  usage_events: TeacherAssistAIUsageEvent[];
};

export type WeeklyPlanContentStandard = {
  id?: string;
  code?: string;
  description?: string;
};

export type WeeklyPlanContentDay = {
  day?: number;
  day_label?: string;
  focus?: string;
  teacher_actions?: string[];
  student_activities?: string[];
  checks_for_understanding?: string[];
  materials_needed?: string[];
};

export type WeeklyPlanContentSubject = {
  subject_id?: string;
  subject_name?: string;
  standards?: WeeklyPlanContentStandard[];
  objectives?: string[];
  vocabulary?: string[];
  daily_breakdown?: WeeklyPlanContentDay[];
  differentiation?: {
    support?: string[];
    extension?: string[];
    visual_supports?: string[];
  };
  suggested_artifacts?: string[];
};

export type WeeklyPlanContent = {
  metadata?: {
    is_mock?: boolean;
    generator?: string;
    provider_mode?: string;
    provider_model?: string | null;
    prompt_version?: string;
    version?: number;
    generated_at?: string;
    planning_draft_id?: string;
    workflow_id?: string | null;
    copied_from_plan_id?: string;
    copied_at?: string;
  };
  planning_scope?: string;
  plan_title?: string;
  module_title?: string | null;
  duration?: {
    start_date?: string | null;
    end_date?: string | null;
    estimated_weeks?: number | null;
    instructional_days_count?: number | null;
    summary?: string;
  };
  overview?: string;
  instructional_arc?: string[];
  weekly_objectives?: string[];
  subjects?: WeeklyPlanContentSubject[];
  weekly_segments?: Array<{
    segment_index?: number;
    segment_label?: string;
    focus?: string;
    objectives?: string[];
    subjects?: Array<{
      subject_id?: string;
      subject_name?: string;
      objectives?: string[];
      daily_breakdown?: WeeklyPlanContentDay[];
    }>;
    daily_breakdown?: WeeklyPlanContentDay[];
    assessment_checkpoints?: string[];
  }>;
  standards_progression?: Array<{ code?: string; description?: string; phase?: string }>;
  vocabulary?: string[];
  materials_needed?: string[];
  differentiation?: {
    support?: string[];
    extension?: string[];
    intervention?: string[];
  };
  assessment_checkpoints?: string[];
  daily_breakdown?: WeeklyPlanContentDay[];
  resources_used?: Array<{ id?: string; title?: string; resource_type?: string }>;
  teacher_notes_used?: string | null;
  review_notes?: string;
  review_required?: boolean;
  quality_flags?: string[];
  missing_context_warnings?: string[];
  standards_alignment_summary?: string;
  teacher_review_checklist?: string[];
};

export type WeeklyPlan = {
  id: string;
  tenant_id: string;
  user_id: string;
  owner_user_id: string;
  planning_input_draft_id: string;
  workflow_id: string | null;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  title: string;
  plan_title: string;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  source_plan_id: string | null;
  derived_from_plan_id: string | null;
  is_template: boolean;
  visibility_scope: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status: "active" | "archived" | "reusable";
  school_year_origin_id: string | null;
  status: "in_progress" | "completed";
  content_json: WeeklyPlanContent;
  source_context_json: Record<string, unknown>;
  current_version_number: number;
  latest_usage_event: TeacherAssistAIUsageEvent | null;
  created_at: string;
  updated_at: string;
};

export type WeeklyPlanVersion = {
  id: string;
  weekly_plan_id: string;
  version_number: number;
  content_json: WeeklyPlanContent;
  source_context_json: Record<string, unknown>;
  created_by_user_id: string;
  created_at: string;
  change_reason: string | null;
};

export type WeeklyPlanUpdateInput = {
  title?: string;
  status?: "in_progress" | "completed";
  content_json?: WeeklyPlanContent;
  change_reason?: string;
};

export type WeeklyPlanSectionKey =
  | "overview"
  | "instructional_arc"
  | "weekly_segments"
  | "daily_breakdown"
  | "vocabulary"
  | "materials_needed"
  | "differentiation"
  | "assessment_checkpoints"
  | "standards_progression"
  | "review_notes";

export type WeeklyPlanSharingUpdateInput = {
  is_template?: boolean;
  visibility_scope?: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status?: "active" | "archived" | "reusable";
};

export type WeeklyPlanCopyInput = {
  target_school_year_id?: string;
  target_grading_period_id?: string;
  target_class_id?: string;
  title_override?: string;
  copy_mode?: "personal_copy" | "rollover_copy" | "template_copy";
};

export type WeeklyPlanSectionRegenerationInput = {
  section_key: WeeklyPlanSectionKey;
  section_path?: string | null;
  teacher_instruction?: string | null;
  provider_mode?: "mock" | "real" | null;
  preserve_existing_context?: boolean;
};

export type InstructionalPlanLibraryItem = {
  id: string;
  tenant_id: string;
  user_id: string;
  owner_user_id: string;
  owner_name: string | null;
  is_owner: boolean;
  planning_input_draft_id: string;
  workflow_id: string | null;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  title: string;
  plan_title: string;
  module_title: string | null;
  start_date: string | null;
  end_date: string | null;
  estimated_weeks: number | null;
  instructional_days_count: number | null;
  source_plan_id: string | null;
  derived_from_plan_id: string | null;
  is_template: boolean;
  visibility_scope: "private" | "shared" | "grade_team" | "school" | "district";
  reuse_status: "active" | "archived" | "reusable";
  school_year_origin_id: string | null;
  source_school_year_id: string | null;
  source_school_year_title: string | null;
  subject_ids: string[];
  subject_names: string[];
  class_id: string | null;
  class_name: string | null;
  grading_period_id: string | null;
  grading_period_title: string | null;
  status: "in_progress" | "completed";
  created_at: string;
  updated_at: string;
};

export type CurriculumRolloverCandidate = InstructionalPlanLibraryItem & {
  already_copied_to_target: boolean;
  existing_target_plan_id: string | null;
};

export type CurriculumRolloverCandidates = {
  items: CurriculumRolloverCandidate[];
  summary_counts_by_planning_scope: Record<string, number>;
  subjects_represented: string[];
  grading_periods_represented: string[];
};

export type CurriculumRolloverCopyInput = {
  source_school_year_id: string;
  target_school_year_id: string;
  plan_ids: string[];
  copy_mode?: "rollover_copy";
  preserve_titles?: boolean;
  title_suffix?: string;
  target_grading_period_mapping?: Record<string, string>;
};

export type CurriculumRolloverCopyResult = {
  copied_plans: WeeklyPlan[];
  warnings: string[];
};
