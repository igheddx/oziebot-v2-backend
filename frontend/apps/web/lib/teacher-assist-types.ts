export type TeacherProfile = {
  id: string | null;
  preferred_grade_level: string | null;
  default_student_count: number | null;
  preferred_grading_period_type: string | null;
  timezone: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type TeacherMyClassroom = {
  grade_level: string | null;
  grade_display_name: string | null;
  homeroom_name: string;
  student_count: number | null;
  timezone: string | null;
  class_id: string | null;
  synced_subjects: Array<{
    catalog_subject_id: string;
    tenant_subject_id: string;
    subject_code: string;
    display_name: string;
  }>;
  has_active_school_year: boolean;
  requires_school_setup: boolean;
  active_school_year_id: string | null;
  active_school_year_title: string | null;
  active_school_year_start_date: string | null;
  active_school_year_end_date: string | null;
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
  assignment_grading_review_statuses: string[];
  assignment_grading_review_sources: string[];
  assignment_grade_record_statuses?: string[];
  assignment_gradebook_commit_types?: string[];
  assignment_gradebook_commit_statuses?: string[];
  assignment_gradebook_audit_event_types?: string[];
  mastery_matrix_statuses?: string[];
  mastery_levels?: string[];
  mastery_evaluation_statuses?: string[];
  mastery_commit_types?: string[];
  mastery_commit_statuses?: string[];
  mastery_evidence_source_types?: string[];
  mastery_confidence_levels?: string[];
  reteach_plan_statuses?: string[];
  reteach_plan_version_sources?: string[];
  newsletter_statuses?: string[];
  newsletter_version_sources?: string[];
  newsletter_regeneratable_sections?: string[];
  newsletter_export_formats?: string[];
  lesson_reflection_statuses?: string[];
  lesson_reflection_version_sources?: string[];
  lesson_effectiveness_classifications?: string[];
  extraction_review_statuses?: string[];
  extraction_confidence_levels?: string[];
  export_artifact_types?: string[];
  export_artifact_statuses?: string[];
  export_formats?: string[];
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

export type StandardImportPreviewRow = {
  row_number: number;
  code: string;
  standard_type: string;
  subject_label: string;
  description: string;
  subject_id: string | null;
  status: "valid" | "invalid" | "duplicate";
};

export type StandardImportRowError = {
  row_number: number;
  message: string;
  field?: string | null;
};

export type StandardImportPreview = {
  total_rows: number;
  valid_count: number;
  invalid_count: number;
  duplicate_count: number;
  rows: StandardImportPreviewRow[];
  errors: StandardImportRowError[];
};

export type StandardImportCommitResult = {
  created_count: number;
  skipped_duplicate_count: number;
  errors: StandardImportRowError[];
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
  latest_extraction_job: TeacherAssistExtractionJob | null;
  latest_extracted_text: TeacherAssistExtractedTextRecord | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistFileDownload = {
  url: string;
  expires_at: string;
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
  latest_extraction_job: TeacherAssistExtractionJob | null;
  latest_extracted_text: TeacherAssistExtractedTextRecord | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistExtractionJob = {
  id: string;
  artifact_type: "resource" | "student_work";
  resource_library_item_id: string | null;
  student_work_submission_id: string | null;
  assignment_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  student_number: number | null;
  status: "queued" | "running" | "completed" | "failed" | "cancelled" | "skipped";
  progress_percent: number;
  provider_name: string | null;
  provider_model: string | null;
  provider_version: string | null;
  provider_mode: "mock" | "real" | null;
  page_count: number | null;
  processing_duration_ms: number | null;
  estimated_cost_cents: number | null;
  error_code: string | null;
  error_message: string | null;
  error_metadata_json: Record<string, unknown> | null;
  retry_count: number;
  max_retries: number;
  parent_extraction_job_id: string | null;
  retry_root_job_id: string | null;
  attempt_number: number;
  leased_by_worker: string | null;
  lease_expires_at: string | null;
  heartbeat_at: string | null;
  execution_log_json: Array<Record<string, unknown>> | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
};

export type ExtractionReviewStatus =
  | "pending_review"
  | "teacher_reviewing"
  | "teacher_approved"
  | "teacher_rejected"
  | "reviewed"
  | "issue_flagged"
  | "needs_retry"
  | "archived";

export type ExtractionConfidenceLevel = "low" | "medium" | "high" | "unknown";

export type TeacherAssistExtractedTextRecord = {
  id: string;
  extraction_job_id: string;
  artifact_type: "resource" | "student_work";
  resource_library_item_id: string | null;
  student_work_submission_id: string | null;
  assignment_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  student_number: number | null;
  preview_text: string;
  text_char_count: number;
  pii_flagged: boolean;
  redaction_applied: boolean;
  review_status: ExtractionReviewStatus;
  provider_confidence_score: number | null;
  confidence_level: ExtractionConfidenceLevel;
  teacher_corrected_text: string | null;
  approved_text: string | null;
  reviewed_at: string | null;
  reviewed_by_user_id: string | null;
  source_extraction_job_id: string | null;
  teacher_review_notes: string | null;
  teacher_issue_reason: string | null;
  metadata_json: Record<string, unknown> | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistExtractedTextDetail = TeacherAssistExtractedTextRecord & {
  extracted_text: string;
};

export type TeacherAssistExtractionSummary = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
  retry_eligible: boolean;
  processing_duration_seconds: number | null;
};

export type TeacherAssistExtractedTextDetailAggregate = {
  record: TeacherAssistExtractedTextDetail;
  job: TeacherAssistExtractionJob;
  lineage_jobs: TeacherAssistExtractionJob[];
  retry_eligible: boolean;
  cancel_eligible: boolean;
  processing_duration_seconds: number | null;
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractionJobDetail = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
  lineage_jobs: TeacherAssistExtractionJob[];
  retry_eligible: boolean;
  cancel_eligible: boolean;
  processing_duration_seconds: number | null;
  execution_timeline: Array<Record<string, unknown>>;
  source_artifact: {
    artifact_type: "resource" | "student_work";
    original_filename: string;
    mime_type: string;
    file_size: number;
    resource_library_item_id: string | null;
    student_work_submission_id: string | null;
    assignment_id: string | null;
    student_number: number | null;
  };
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractedTextHistory = {
  current_record: TeacherAssistExtractedTextDetail;
  current_job: TeacherAssistExtractionJob;
  attempt_jobs: TeacherAssistExtractionJob[];
  attempt_records: TeacherAssistExtractedTextRecord[];
  activity_events: TeacherAssistActivityEvent[];
};

export type TeacherAssistExtractionRun = {
  job: TeacherAssistExtractionJob;
  extracted_text: TeacherAssistExtractedTextRecord | null;
};

export type GradingPrepTextSource = "approved_text" | "teacher_corrected_text" | "extracted_text";

export type TeacherAssistStudentWorkGradingPrepContext = {
  student_work_submission_id: string;
  assignment_id: string;
  student_number: number;
  ready_for_grading_prep: boolean;
  blocked_reason: string | null;
  review_status: ExtractionReviewStatus | null;
  text_source: GradingPrepTextSource | null;
  approved_text: string | null;
  text_char_count: number | null;
  extracted_text_record_id: string | null;
  extraction_job_id: string | null;
  ai_grading_enabled: boolean;
  message: string;
};

export type TeacherAssistAssignmentGradingPrepSubmission = {
  student_work_submission_id: string;
  student_number: number;
  ready_for_grading_prep: boolean;
  blocked_reason: string | null;
  review_status: ExtractionReviewStatus | null;
  text_source: GradingPrepTextSource | null;
  text_char_count: number | null;
  extracted_text_record_id: string | null;
  extraction_job_id: string | null;
};

export type TeacherAssistAssignmentGradingPrepSummary = {
  assignment_id: string;
  assignment_title: string;
  total_submissions: number;
  ready_for_grading_prep_count: number;
  blocked_count: number;
  submissions: TeacherAssistAssignmentGradingPrepSubmission[];
  ai_grading_enabled: boolean;
  message: string;
};

export type TeacherAssistExportArtifactType =
  | "lesson_slides"
  | "guided_notes"
  | "multiple_choice_quiz"
  | "exit_ticket"
  | "short_answer_quiz";

export type TeacherAssistExportArtifactStatus =
  | "queued"
  | "generating"
  | "ready"
  | "failed"
  | "archived";

export type TeacherAssistExportFormat = "pptx" | "json" | "printable_html";

export type TeacherAssistExportArtifact = {
  id: string;
  tenant_id: string;
  user_id: string;
  source_plan_id: string;
  source_assignment_id: string | null;
  workflow_id: string | null;
  artifact_type: TeacherAssistExportArtifactType;
  artifact_status: TeacherAssistExportArtifactStatus;
  title: string;
  export_format: TeacherAssistExportFormat;
  storage_key: string | null;
  preview_json: Record<string, unknown>;
  metadata_json: Record<string, unknown> | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistExportArtifactDetail = {
  artifact: TeacherAssistExportArtifact;
  workflow_status: TeacherAssistWorkflow["status"] | null;
  workflow_progress_percent: number | null;
  workflow_error_message: string | null;
  download_url: string | null;
};

export type TeacherAssistExportArtifactCreateInput = {
  artifact_type: TeacherAssistExportArtifactType;
  export_format?: TeacherAssistExportFormat;
  provider_mode?: "mock" | "real";
};

export type TeacherAssistExportDownload = {
  download_url: string;
  filename: string;
  mime_type: string;
  expires_at: string;
};

export type AssignmentGradingReviewItem = {
  id: string;
  grading_review_id: string;
  criterion_title: string;
  score_suggestion: number | null;
  max_score: number | null;
  feedback_summary: string | null;
  strengths: string[];
  improvement_areas: string[];
  teacher_notes: string | null;
  sort_order: number;
  created_at: string;
  updated_at: string;
};

export type AssignmentGradingReview = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  student_work_submission_id: string;
  student_number: number;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  status:
    | "draft"
    | "ai_suggested"
    | "teacher_reviewing"
    | "teacher_confirmed"
    | "returned_for_revision"
    | "archived";
  review_source: "manual" | "ai_placeholder";
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  ai_usage_event_id: string | null;
  score_suggestion: number | null;
  max_score: number | null;
  feedback_summary: string | null;
  strengths: string[];
  improvement_areas: string[];
  teacher_notes: string | null;
  teacher_confirmed_score: number | null;
  teacher_confirmed_feedback: string | null;
  items: AssignmentGradingReviewItem[];
  created_at: string;
  updated_at: string;
};

export type AssignmentGradingReviewCreateInput = {
  student_number: number;
  score_suggestion?: number | null;
  max_score?: number | null;
  feedback_summary?: string | null;
  strengths?: string[];
  improvement_areas?: string[];
  teacher_notes?: string | null;
  items?: Array<{
    criterion_title: string;
    score_suggestion?: number | null;
    max_score?: number | null;
    feedback_summary?: string | null;
    strengths?: string[];
    improvement_areas?: string[];
    teacher_notes?: string | null;
    sort_order?: number;
  }>;
};

export type AssignmentGradingReviewUpdateInput = {
  status: AssignmentGradingReview["status"];
  score_suggestion?: number | null;
  max_score?: number | null;
  feedback_summary?: string | null;
  strengths?: string[];
  improvement_areas?: string[];
  teacher_notes?: string | null;
  teacher_confirmed_score?: number | null;
  teacher_confirmed_feedback?: string | null;
  items?: AssignmentGradingReviewCreateInput["items"];
};

export type AssignmentGradingReviewAISuggestionInput = {
  provider_mode?: "mock" | "real";
  teacher_instructions?: string | null;
};

export type AssignmentGradingReviewAISuggestion = {
  review: AssignmentGradingReview;
  confidence_level: "low" | "medium" | "high";
  teacher_review_required: boolean;
  rubric_notes: string | null;
  text_source: string | null;
  message: string;
};

export type AssignmentGradeRecord = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  assignment_id: string;
  student_work_submission_id: string;
  grading_review_id: string;
  student_number: number;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  record_status: "active" | "superseded" | "reversed";
  current_commit_id: string | null;
  committed_score: number | null;
  max_score: number | null;
  committed_feedback: string | null;
  created_at: string;
  updated_at: string;
};

export type AssignmentGradebookCommit = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  grade_record_id: string;
  assignment_id: string;
  student_work_submission_id: string;
  grading_review_id: string;
  student_number: number;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  commit_type: "initial_commit" | "correction" | "reversal";
  commit_status: "active" | "superseded" | "reversed";
  committed_score: number | null;
  max_score: number | null;
  committed_feedback: string | null;
  teacher_confirmation_checkpoint_at: string;
  reason: string | null;
  supersedes_commit_id: string | null;
  reversed_by_commit_id: string | null;
  audit_metadata_json: Record<string, unknown> | null;
  created_at: string;
};

export type AssignmentGradebookAuditEvent = {
  id: string;
  tenant_id: string;
  teacher_user_id: string;
  grade_record_id: string | null;
  gradebook_commit_id: string | null;
  assignment_id: string;
  student_number: number;
  event_type: string;
  summary_text: string;
  details_json: Record<string, unknown> | null;
  created_at: string;
};

export type AssignmentGradeRecordDetail = {
  record: AssignmentGradeRecord;
  commits: AssignmentGradebookCommit[];
  audit_events: AssignmentGradebookAuditEvent[];
};

export type AssignmentGradebookCommitResult = {
  grade_record: AssignmentGradeRecord;
  commit: AssignmentGradebookCommit;
  message: string;
};

export type AssignmentGradebookExportView = {
  assignment_id: string;
  assignment_title: string;
  assignment_type: string;
  class_id: string;
  subject_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  generated_at: string;
  record_count: number;
  active_record_count: number;
  records: Array<Record<string, unknown>>;
  commits: Array<Record<string, unknown>>;
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
  reflection_hints?: PlanningReflectionHints | null;
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

export type TeacherAssistActivityEvent = {
  id: string;
  event_category: string;
  event_type: string;
  entity_type: string;
  entity_id: string;
  timestamp: string;
  summary_text: string;
  workflow_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  details_json: Record<string, unknown> | null;
  created_at: string;
};

export type TeacherAssistWorkspacePlanSummary = {
  id: string;
  title: string;
  planning_scope: "weekly" | "multi_week" | "module" | "unit" | "grading_period";
  status: "in_progress" | "completed";
  workflow_id: string | null;
  class_id: string | null;
  school_year_id: string | null;
  review_required: boolean;
  quality_flags: string[];
  missing_context_warnings: string[];
  updated_at: string;
};

export type TeacherAssistWorkspaceAssignmentSummary = {
  id: string;
  class_id: string;
  subject_id: string;
  title: string;
  status: Assignment["status"];
  assignment_type: Assignment["assignment_type"];
  due_date: string | null;
  updated_at: string;
};

export type TeacherAssistWorkspacePacketSummary = {
  id: string;
  assignment_id: string;
  class_id: string;
  packet_status: AssignmentPrintPacket["packet_status"];
  pages_per_student: number;
  student_count: number;
  template_type: AssignmentPrintPacket["template_type"];
  created_at: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceSubmissionSummary = {
  id: string;
  assignment_id: string;
  class_id: string;
  student_number: number;
  original_filename: string;
  upload_status: AssignmentStudentWorkSubmission["upload_status"];
  processing_status: AssignmentStudentWorkSubmission["processing_status"];
  latest_extraction_status: TeacherAssistExtractionJob["status"] | null;
  extraction_ready_for_teacher_review: boolean;
  created_at: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceGradingReviewSummary = {
  id: string;
  assignment_id: string;
  student_work_submission_id: string;
  class_id: string;
  student_number: number;
  status: AssignmentGradingReview["status"];
  teacher_confirmed_score: number | null;
  updated_at: string;
};

export type TeacherAssistWorkspaceWorkflowSummary = {
  id: string;
  workflow_type: TeacherAssistWorkflow["workflow_type"];
  status: TeacherAssistWorkflow["status"];
  class_id: string | null;
  school_year_id: string | null;
  grading_period_id: string | null;
  progress_percent: number;
  retry_count: number;
  max_retries: number;
  provider_name: string | null;
  provider_model: string | null;
  last_error_code: string | null;
  heartbeat_at: string | null;
  created_at: string;
  updated_at: string;
  completed_at: string | null;
  error_message: string | null;
};

export type TeacherAssistWorkspaceNeedsAttention = {
  type: string;
  severity: "info" | "warning" | "critical";
  title: string;
  message: string;
  entity_type: string;
  entity_id: string;
  class_id: string | null;
  created_at: string;
};

export type TeacherAssistWorkspaceReviewRequiredItem = {
  entity_type: string;
  entity_id: string;
  class_id: string | null;
  title: string;
  status: string;
  review_reason: string;
  updated_at: string;
};

export type TeacherAssistWorkspaceTodaySummary = {
  active_grading_period_title: string | null;
  active_workflows_count: number;
  plans_needing_review_count: number;
  grading_reviews_pending_confirmation_count: number;
  recent_uploads_count: number;
  workflow_failures_count: number;
  extraction_failures_count: number;
  student_work_ready_for_extraction_count: number;
  extracted_artifacts_ready_for_teacher_review_count: number;
  low_confidence_extractions_count: number;
  rejected_extractions_count: number;
  retry_required_extractions_count: number;
  awaiting_teacher_review_count: number;
  stale_extraction_jobs_count: number;
  recently_approved_extractions_count: number;
};

export type TeacherAssistClassWorkspace = {
  class: TeacherClass;
  active_plans: TeacherAssistWorkspacePlanSummary[];
  assignments: TeacherAssistWorkspaceAssignmentSummary[];
  pending_grading_reviews: TeacherAssistWorkspaceGradingReviewSummary[];
  recent_submissions: TeacherAssistWorkspaceSubmissionSummary[];
  workflow_summaries: TeacherAssistWorkspaceWorkflowSummary[];
  packet_summaries: TeacherAssistWorkspacePacketSummary[];
  needs_attention_count: number;
};

export type TeacherAssistWorkspaceStats = {
  active_plans_count: number;
  plans_in_review_count: number;
  pending_grading_reviews_count: number;
  recent_upload_count: number;
  workflow_failure_count: number;
  assignments_in_review_count: number;
  extraction_failure_count: number;
  student_work_ready_for_extraction_count: number;
  extracted_artifacts_ready_for_teacher_review_count: number;
  low_confidence_extractions_count: number;
  rejected_extractions_count: number;
  retry_required_extractions_count: number;
  awaiting_teacher_review_count: number;
  stale_extraction_jobs_count: number;
  recently_approved_extractions_count: number;
};

export type TeacherAssistWorkspaceMasteryInsights = {
  matrix_count: number;
  active_evaluation_count: number;
  reteach_recommended_count: number;
  low_mastery_alert_count: number;
  unassessed_standard_count: number;
  improving_standard_count: number;
  declining_standard_count: number;
  reteach_recommended_standards: Array<Record<string, unknown>>;
  standards_needing_attention: Array<Record<string, unknown>>;
  low_mastery_alerts: Array<Record<string, unknown>>;
  improving_standards: Array<Record<string, unknown>>;
  declining_standards: Array<Record<string, unknown>>;
  unassessed_standards: Array<Record<string, unknown>>;
  class_snapshots: Array<Record<string, unknown>>;
};

export type TeacherAssistWorkspace = {
  current_school_year: SchoolYear | null;
  active_grading_period: GradingPeriod | null;
  today_summary: TeacherAssistWorkspaceTodaySummary;
  class_workspaces: TeacherAssistClassWorkspace[];
  needs_attention: TeacherAssistWorkspaceNeedsAttention[];
  recent_activity: TeacherAssistActivityEvent[];
  active_workflows: TeacherAssistWorkspaceWorkflowSummary[];
  review_required_items: TeacherAssistWorkspaceReviewRequiredItem[];
  workspace_stats: TeacherAssistWorkspaceStats;
  mastery_insights: TeacherAssistWorkspaceMasteryInsights | null;
};

export type TeacherAssistActionWorkspaceSeverity =
  | "critical"
  | "warning"
  | "review"
  | "ready"
  | "info";

export type TeacherAssistActionWorkspaceSectionKey =
  | "extractions"
  | "grading"
  | "gradebook"
  | "workflows_exports"
  | "planning_assignments";

export type TeacherAssistActionWorkspaceNavigation = {
  label: string;
  href: string;
};

export type TeacherAssistActionWorkspaceItem = {
  action_key: string;
  action_type: string;
  severity: TeacherAssistActionWorkspaceSeverity;
  title: string;
  description: string;
  tenant_id: string;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  assignment_id: string | null;
  student_work_id: string | null;
  grading_review_id: string | null;
  gradebook_record_id: string | null;
  workflow_id: string | null;
  export_artifact_id: string | null;
  extraction_job_id: string | null;
  extracted_text_id: string | null;
  navigation: TeacherAssistActionWorkspaceNavigation;
  created_at: string | null;
  updated_at: string | null;
};

export type TeacherAssistActionWorkspaceSection = {
  section_key: TeacherAssistActionWorkspaceSectionKey;
  title: string;
  count: number;
  items: TeacherAssistActionWorkspaceItem[];
};

export type TeacherAssistActionWorkspaceSummary = {
  total_open_actions: number;
  critical_count: number;
  warning_count: number;
  review_count: number;
  ready_count: number;
  mastery_alert_count: number;
};

export type TeacherAssistActionWorkspaceClassRollup = {
  class_id: string;
  class_name: string;
  open_action_count: number;
  extraction_count: number;
  grading_count: number;
  gradebook_count: number;
  workflow_export_count: number;
  planning_assignment_count: number;
};

export type TeacherAssistActionWorkspaceActivity = {
  id: string;
  event_category: string;
  event_type: string;
  entity_type: string;
  entity_id: string;
  summary_text: string;
  class_id: string | null;
  created_at: string;
};

export type TeacherAssistActionWorkspace = {
  summary: TeacherAssistActionWorkspaceSummary;
  sections: TeacherAssistActionWorkspaceSection[];
  priority_items: TeacherAssistActionWorkspaceItem[];
  class_rollups: TeacherAssistActionWorkspaceClassRollup[];
  recent_activity: TeacherAssistActionWorkspaceActivity[];
};

export type TeacherAssistTodaySummary = {
  total_open_actions: number;
  critical_count: number;
  warning_count: number;
  review_count: number;
  ready_count: number;
  mastery_alert_count: number;
  today_open_count: number;
  items_needing_review_count: number;
  grading_pending_count: number;
  extraction_pending_count: number;
  gradebook_pending_count: number;
  reteach_plans_pending_count: number;
  mastery_reteach_standard_count: number;
};

export type TeacherAssistTodayPriorityItem = TeacherAssistActionWorkspaceItem & {
  today_category?: string | null;
};

export type TeacherAssistWorkflowStepStatus = "complete" | "in_progress" | "pending";

export type TeacherAssistWorkflowProgressCard = {
  assignment_id: string;
  assignment_title: string;
  class_id: string;
  source_plan_id: string | null;
  source_plan_title: string | null;
  steps: Record<string, TeacherAssistWorkflowStepStatus>;
  completed_step_count: number;
  total_step_count: number;
  progress_percent: number;
  navigation_href: string;
};

export type TeacherAssistOnboardingChecklistItem = {
  key: string;
  title: string;
  complete: boolean;
  navigation_href: string;
  navigation_label: string;
};

export type TeacherAssistOnboardingChecklist = {
  items: TeacherAssistOnboardingChecklistItem[];
  completed_count: number;
  total_count: number;
  is_complete: boolean;
};

export type TeacherAssistTodayWorkspace = {
  summary: TeacherAssistTodaySummary;
  priority_items: TeacherAssistTodayPriorityItem[];
  categories: Record<string, TeacherAssistActionWorkspaceItem[]>;
  workflow_progress_cards: TeacherAssistWorkflowProgressCard[];
  onboarding_checklist: TeacherAssistOnboardingChecklist;
  recent_activity: TeacherAssistActionWorkspaceActivity[];
  current_school_year: SchoolYear | null;
  active_grading_period: GradingPeriod | null;
  mastery_insights: TeacherAssistWorkspaceMasteryInsights | null;
};

export type MasteryLevel =
  | "not_assessed"
  | "beginning"
  | "developing"
  | "mastery"
  | "advanced";

export type MasteryMatrixStatus = "draft" | "active" | "archived";
export type MasteryEvaluationStatus = "draft" | "active" | "reversed";
export type MasteryEvidenceSourceType =
  | "assignment"
  | "grading_review"
  | "gradebook_commit"
  | "manual_observation";

export type MasteryMatrixStandard = {
  id: string;
  standard_id: string;
  display_order: number;
  target_mastery_level: MasteryLevel;
  assessment_count: number;
  standard_code: string | null;
  standard_description: string | null;
};

export type MasteryMatrix = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  status: MasteryMatrixStatus;
  created_at: string;
  updated_at: string;
  standards: MasteryMatrixStandard[];
};

export type MasteryEvaluation = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  mastery_matrix_id: string;
  student_number: number;
  standard_id: string;
  evaluation_status: MasteryEvaluationStatus;
  mastery_level: MasteryLevel;
  confidence_level: "low" | "medium" | "high" | null;
  evidence_source_type: MasteryEvidenceSourceType | null;
  evidence_source_id: string | null;
  teacher_notes: string | null;
  confirmed_by_user_id: string | null;
  confirmed_at: string | null;
  current_commit_id: string | null;
  created_at: string;
  updated_at: string;
};

export type MasteryCommit = {
  id: string;
  mastery_evaluation_id: string;
  mastery_matrix_id: string;
  student_number: number;
  standard_id: string;
  commit_type: "initial_commit" | "correction" | "reversal";
  commit_status: "active" | "superseded" | "reversed";
  previous_mastery_level: MasteryLevel | null;
  new_mastery_level: MasteryLevel;
  confidence_level: "low" | "medium" | "high" | null;
  evidence_source_type: MasteryEvidenceSourceType | null;
  evidence_source_id: string | null;
  teacher_notes: string | null;
  commit_reason: string | null;
  supersedes_commit_id: string | null;
  reversed_by_commit_id: string | null;
  reversed_at: string | null;
  reversed_by_user_id: string | null;
  created_at: string;
};

export type MasteryEvaluationDetail = {
  evaluation: MasteryEvaluation;
  commits: MasteryCommit[];
};

export type MasteryCommitResult = {
  evaluation: MasteryEvaluation;
  commit: MasteryCommit;
  message: string;
};

export type MasteryMatrixSummary = {
  mastery_matrix_id: string;
  title: string;
  status: MasteryMatrixStatus;
  class_id: string;
  subject_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  tracked_standard_count: number;
  active_evaluation_count: number;
  draft_evaluation_count: number;
  reversed_evaluation_count: number;
  student_count: number;
  unassessed_standard_count: number;
  reteach_candidate_count: number;
  mastery_distribution: Record<string, number>;
};

export type MasteryMatrixStandardsSummary = {
  mastery_matrix_id: string;
  standards: Array<{
    matrix_standard_id: string;
    standard_id: string;
    standard_code: string | null;
    standard_description: string | null;
    display_order: number;
    target_mastery_level: MasteryLevel;
    assessment_count: number;
    active_evaluation_count: number;
    reteach_candidate_count: number;
    mastery_distribution: Record<string, number>;
    is_unassessed: boolean;
  }>;
};

export type MasteryMatrixStudentsSummary = {
  mastery_matrix_id: string;
  students: Array<{
    student_number: number;
    active_evaluation_count: number;
    draft_evaluation_count: number;
    reteach_candidate_count: number;
    cells: Array<{
      evaluation_id: string;
      standard_id: string;
      evaluation_status: MasteryEvaluationStatus;
      mastery_level: MasteryLevel;
      target_mastery_level: MasteryLevel;
      needs_reteach: boolean;
      confirmed_at: string | null;
    }>;
  }>;
};

export type MasteryMatrixReteachSummary = {
  mastery_matrix_id: string;
  reteach_candidate_count: number;
  unassessed_standard_count: number;
  reteach_items: Array<{
    evaluation_id: string;
    student_number: number;
    standard_id: string;
    standard_code: string | null;
    current_mastery_level: MasteryLevel;
    target_mastery_level: MasteryLevel;
    evidence_source_type: MasteryEvidenceSourceType | null;
    evidence_source_id: string | null;
    confirmed_at: string | null;
  }>;
  unassessed_standards: Array<{
    standard_id: string;
    standard_code: string | null;
    target_mastery_level: MasteryLevel;
  }>;
};

export type ReteachOperationalStatus =
  | "healthy"
  | "monitor"
  | "reteach_recommended"
  | "critical_attention"
  | "unassessed";

export type AssignmentEffectivenessStatus =
  | "effective"
  | "mixed_results"
  | "reteach_likely"
  | "insufficient_data";

export type StudentMasteryTrend = "improving" | "stable" | "declining" | "insufficient_data";
export type StandardMasteryTrend = "improving" | "stable" | "declining" | "insufficient_data";

export type MasteryHeatmapCell = {
  standard_id: string;
  mastery_level: MasteryLevel;
  evaluation_id: string | null;
  evaluation_count: number;
  confirmed_at: string | null;
  evidence_source_type: MasteryEvidenceSourceType | null;
  evidence_source_id: string | null;
  needs_reteach: boolean;
};

export type MasteryMatrixHeatmap = {
  mastery_matrix_id: string;
  title: string;
  class_id: string;
  subject_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  standards: Array<{
    matrix_standard_id: string;
    standard_id: string;
    standard_code: string | null;
    standard_description: string | null;
    display_order: number;
    target_mastery_level: MasteryLevel;
    operational_status: ReteachOperationalStatus;
    mastery_distribution: Record<string, number>;
    evaluation_count: number;
    last_assessed_at: string | null;
  }>;
  student_numbers: number[];
  rows: Array<{
    student_number: number;
    cells: MasteryHeatmapCell[];
  }>;
  mastery_distribution: Record<string, number>;
  active_evaluation_count: number;
  student_count: number;
};

export type MasteryStandardInsight = {
  matrix_standard_id: string;
  standard_id: string;
  standard_code: string | null;
  standard_description: string | null;
  display_order: number;
  target_mastery_level: MasteryLevel;
  mastery_percentage: number;
  developing_percentage: number;
  beginning_percentage: number;
  not_assessed_percentage: number;
  total_committed_evaluations: number;
  recent_assessment_count: number;
  recent_assignment_count: number;
  last_assessed_at: string | null;
  operational_status: ReteachOperationalStatus;
  trend: StandardMasteryTrend;
  is_unassessed: boolean;
};

export type MasteryMatrixReteachInsights = {
  mastery_matrix_id: string;
  title: string;
  class_id: string;
  subject_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  standard_insights: MasteryStandardInsight[];
  status_counts: Record<string, number>;
  panels: {
    standards_needing_reteach: MasteryStandardInsight[];
    standards_needing_attention: MasteryStandardInsight[];
    strongest_standards: MasteryStandardInsight[];
    weakest_standards: MasteryStandardInsight[];
    improving_standards: MasteryStandardInsight[];
    declining_standards: MasteryStandardInsight[];
    unassessed_standards: MasteryStandardInsight[];
  };
};

export type ReteachPlanStatus = "draft" | "ai_draft" | "teacher_review" | "archived";
export type ReteachPlanVersionSource = "initial" | "ai_draft" | "teacher_edit";

export type ReteachPlanContent = {
  reteach_objectives?: string[];
  instructional_strategies?: string[];
  small_group_recommendations?: string[];
  intervention_ideas?: string[];
  vocabulary_focus?: string[];
  assessment_checks?: string[];
  teacher_review_required?: boolean;
  is_ai_draft?: boolean;
  prompt_version?: string;
  [key: string]: unknown;
};

export type ReteachPlan = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  mastery_matrix_id: string;
  standard_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  status: ReteachPlanStatus;
  current_version_id: string | null;
  latest_ai_usage_event_id: string | null;
  created_at: string;
  updated_at: string;
  standard_code: string | null;
  standard_description: string | null;
};

export type ReteachPlanVersion = {
  id: string;
  reteach_plan_id: string;
  version_number: number;
  version_source: ReteachPlanVersionSource;
  content_json: ReteachPlanContent;
  prompt_context_json: Record<string, unknown> | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  ai_usage_event_id: string | null;
  created_by_user_id: string;
  change_reason: string | null;
  created_at: string;
};

export type ReteachPlanAIDraft = {
  plan: ReteachPlan;
  version: ReteachPlanVersion;
  teacher_review_required: boolean;
  provider_mode: string;
  prompt_version: string;
  message: string;
};

export type NewsletterStatus = "draft" | "review" | "approved" | "archived";
export type NewsletterVersionSource = "initial" | "ai_draft" | "ai_section_regen" | "teacher_edit";
export type NewsletterRegeneratableSection = "overview" | "upcoming_learning" | "teacher_message" | "reminders";
export type NewsletterExportFormat = "html" | "pdf" | "docx";

export type NewsletterContent = {
  overview?: string;
  what_we_learned?: string[];
  standards_covered?: string[];
  upcoming_topics?: string[];
  reminders?: string[];
  celebration_highlights?: string[];
  teacher_message?: string;
  teacher_review_required?: boolean;
  is_ai_draft?: boolean;
  prompt_version?: string;
  last_regenerated_section?: string;
  [key: string]: unknown;
};

export type Newsletter = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  title: string;
  status: NewsletterStatus;
  week_start_date: string | null;
  week_end_date: string | null;
  teacher_notes: string | null;
  current_version_id: string | null;
  latest_ai_usage_event_id: string | null;
  created_at: string;
  updated_at: string;
  subject_name: string | null;
  class_name: string | null;
};

export type NewsletterVersion = {
  id: string;
  newsletter_id: string;
  version_number: number;
  version_source: NewsletterVersionSource;
  content_json: NewsletterContent;
  prompt_context_json: Record<string, unknown> | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  ai_usage_event_id: string | null;
  created_by_user_id: string;
  change_reason: string | null;
  created_at: string;
};

export type NewsletterAIDraft = {
  newsletter: Newsletter;
  version: NewsletterVersion;
  teacher_review_required: boolean;
  provider_mode: string;
  prompt_version: string;
  message: string;
};

export type NewsletterSectionRegenerate = {
  newsletter: Newsletter;
  version: NewsletterVersion;
  teacher_review_required: boolean;
  provider_mode: string;
  prompt_version: string;
  section: string;
  message: string;
};

export type NewsletterExport = {
  id: string;
  newsletter_id: string;
  newsletter_version_id: string | null;
  export_format: NewsletterExportFormat;
  file_size_bytes: number;
  created_at: string;
  download_filename: string;
};

export type NewsletterExportDownload = {
  export_id: string;
  newsletter_id: string;
  export_format: string;
  mime_type: string;
  download_filename: string;
  download_url: string;
};

export type LessonEffectivenessClassification =
  | "highly_effective"
  | "effective"
  | "needs_adjustment"
  | "ineffective"
  | "insufficient_data";

export type LessonEffectiveness = {
  weekly_plan_id: string;
  weekly_plan_title: string;
  planning_scope: string;
  school_year_id: string | null;
  grading_period_id: string | null;
  class_id: string | null;
  subject_id: string | null;
  classification: LessonEffectivenessClassification;
  aggregate_mastery_percentage: number;
  total_committed_evaluations: number;
  assignment_count: number;
  grading_review_count: number;
  gradebook_commit_count: number;
  reteach_plan_count: number;
  mixed_or_reteach_assignments: number;
  assignment_summaries: Array<Record<string, unknown>>;
  read_only: boolean;
};

export type LessonEffectivenessHistoricalComparison = {
  class_id: string;
  subject_id: string;
  current_grading_period: Record<string, unknown>;
  prior_grading_period: Record<string, unknown> | null;
  prior_school_year: Record<string, unknown> | null;
  read_only: boolean;
};

export type LessonReflectionStatus = "draft" | "review" | "archived";
export type LessonReflectionVersionSource = "initial" | "ai_draft" | "teacher_edit";

export type LessonReflectionContent = {
  what_worked: string[];
  what_failed: string[];
  notes_for_next_year: string[];
  strengths: string[];
  weaknesses: string[];
  improvements: string[];
  teacher_review_required: boolean;
  is_ai_draft?: boolean;
};

export type LessonReflection = {
  id: string;
  tenant_id: string;
  owner_user_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  class_id: string;
  subject_id: string;
  weekly_plan_id: string | null;
  title: string;
  status: LessonReflectionStatus;
  lesson_date: string | null;
  current_version_id: string | null;
  latest_ai_usage_event_id: string | null;
  created_at: string;
  updated_at: string;
  subject_name: string | null;
  class_name: string | null;
  weekly_plan_title: string | null;
};

export type LessonReflectionVersion = {
  id: string;
  lesson_reflection_id: string;
  version_number: number;
  version_source: LessonReflectionVersionSource;
  content_json: LessonReflectionContent;
  prompt_context_json: Record<string, unknown> | null;
  provider_name: string | null;
  provider_model: string | null;
  prompt_version: string | null;
  ai_usage_event_id: string | null;
  created_by_user_id: string;
  change_reason: string | null;
  created_at: string;
};

export type LessonReflectionAISuggestions = {
  lesson_reflection: LessonReflection;
  version: LessonReflectionVersion;
  provider_mode: string;
  teacher_review_required: boolean;
  prompt_version: string;
};

export type PlanningReflectionHints = {
  planning_draft_id: string;
  last_year_notes: string[];
  reflection_notes: string[];
  prior_effectiveness: Array<Record<string, unknown>>;
  read_only: boolean;
};

export type StudentMasterySummary = {
  mastery_matrix_id: string;
  student_number: number;
  trend: StudentMasteryTrend;
  active_evaluation_count: number;
  average_mastery_rank: number | null;
  recent_assessment_count: number;
  recent_assignment_count: number;
  mastery_states: Array<Record<string, unknown>>;
  standards_needing_attention: Array<Record<string, unknown>>;
  latest_assignment_evidence: Array<Record<string, unknown>>;
  latest_grading_review_references: Array<Record<string, unknown>>;
  latest_gradebook_commit_references: Array<Record<string, unknown>>;
};

export type AssignmentEffectiveness = {
  assignment_id: string;
  assignment_title: string;
  class_id: string;
  subject_id: string;
  school_year_id: string;
  grading_period_id: string | null;
  linked_standards: Array<Record<string, unknown>>;
  mastery_distribution: Record<string, number>;
  developing_or_beginning_count: number;
  average_mastery_rank: number | null;
  mastery_percentage: number;
  total_committed_evaluations: number;
  grading_review_count: number;
  gradebook_commit_count: number;
  effectiveness_status: AssignmentEffectivenessStatus;
};

export type MasteryDashboard = {
  filters: {
    school_year_id: string | null;
    grading_period_id: string | null;
    class_id: string | null;
    subject_id: string | null;
  };
  matrix_count: number;
  active_evaluation_count: number;
  student_count: number;
  mastery_distribution: Record<string, number>;
  matrix_snapshots: Array<Record<string, unknown>>;
  standards_needing_attention: Array<Record<string, unknown>>;
  reteach_recommended_standards: Array<Record<string, unknown>>;
  low_mastery_alerts: Array<Record<string, unknown>>;
  improving_standards: Array<Record<string, unknown>>;
  declining_standards: Array<Record<string, unknown>>;
  unassessed_standards: Array<Record<string, unknown>>;
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

export type TeacherAssistOnboardingStep = {
  key: string;
  title: string;
  description: string;
  complete: boolean;
  navigation_href: string;
  navigation_label: string;
};

export type TeacherAssistOnboardingProgress = {
  steps: TeacherAssistOnboardingStep[];
  completed_count: number;
  total_count: number;
  progress_percent: number;
  is_complete: boolean;
  completed_at: string | null;
};

export type TeacherAssistUserPreferences = {
  id: string;
  tenant_id: string;
  user_id: string;
  last_class_id: string | null;
  last_grading_period_id: string | null;
  last_subject_id: string | null;
  preferred_landing: string;
  recently_viewed: Array<Record<string, unknown>>;
  onboarding_completed_at: string | null;
  created_at: string;
  updated_at: string;
  onboarding: TeacherAssistOnboardingProgress | null;
};

export type TeacherAssistHomeClassCard = {
  class_id: string;
  class_name: string;
  subject_names: string[];
  student_count: number | null;
  pending_reviews: number;
  pending_grades: number;
  mastery_alerts: number;
  reteach_alerts: number;
  open_action_count: number;
  assignments_due: Array<{
    assignment_id: string;
    title: string;
    due_date: string | null;
  }>;
  navigation_href: string;
  actions: Array<{ label: string; href: string }>;
};

export type TeacherAssistHomePriorityItem = TeacherAssistActionWorkspaceItem & {
  priority_level?: string;
};

export type TeacherAssistHomePriorities = {
  items: TeacherAssistHomePriorityItem[];
  grouped: Record<string, TeacherAssistHomePriorityItem[]>;
  read_only: boolean;
};

export type TeacherAssistHomeTimelineEvent = {
  event_type: string;
  title: string;
  event_date: string | null;
  navigation_href: string;
};

export type TeacherAssistHomeMasteryAlert = {
  alert_type: string;
  title: string;
  description: string | null;
  navigation_href: string;
};

export type TeacherAssistHomeQuickAction = {
  action_key: string;
  label: string;
  navigation_href: string;
};

export type TeacherAssistTeacherShortcuts = {
  most_used_class: {
    class_id: string;
    class_name: string | null;
    navigation_href: string;
  } | null;
  most_used_subject: {
    subject_id: string;
    subject_name: string | null;
  } | null;
  most_used_grading_period_id: string | null;
  recent_assignments: Array<{
    assignment_id: string;
    title: string;
    navigation_href: string;
  }>;
  recent_plans: Array<{
    weekly_plan_id: string;
    title: string;
    navigation_href: string;
  }>;
  recent_reteach_plans: Array<{
    reteach_plan_id: string;
    title: string;
    navigation_href: string;
  }>;
  recently_viewed: Array<Record<string, unknown>>;
};

export type TeacherAssistHomeReuseCandidate = {
  entity_type?: string;
  entity_id?: string;
  title?: string;
  artifact_type?: string;
  source?: string;
  reuse_score?: { score?: number };
  navigation_href?: string;
};

export type TeacherAssistHomeTemplateSummary = {
  id: string;
  name: string;
  artifact_type: string;
  navigation_href: string;
};

export type TeacherAssistHomeWorkspace = {
  summary: Record<string, unknown>;
  priorities: TeacherAssistHomePriorities;
  classes: TeacherAssistHomeClassCard[];
  this_week: {
    assignments_due_count: number;
    completed_plans_count: number;
    week_end_date: string;
  };
  current_week: Record<string, unknown>;
  mastery_alerts: TeacherAssistHomeMasteryAlert[];
  quick_actions: TeacherAssistHomeQuickAction[];
    continue_planning?: {
    current_week_href: string;
    instructional_week_href?: string | null;
    create_instructional_week_href?: string | null;
    generate_next_week_href: string | null;
    upcoming_instructional_week_href?: string | null;
    template_library_href: string;
  };
  instructional_week_id?: string | null;
  upcoming_instructional_week_id?: string | null;
  recently_used_resources?: Array<{
    title: string;
    resource_type?: string | null;
    navigation_href: string;
  }>;
  copilot?: {
    href: string;
    suggested_questions?: string[];
    weekly_summary_href?: string;
    objectives_requiring_attention?: Array<{ objective_code?: string; mastery_pct?: number }>;
    students_needing_support?: Array<{ student_identifier?: string; objective_code?: string; mastery_pct?: number }>;
    suggested_actions?: Array<{ title: string; description?: string; navigation_href?: string }>;
    instructional_health?: {
      objectives_assessed?: number;
      students_needing_support_count?: number;
      open_reteach_plan_count?: number;
    };
  };
  instructional_loop?: {
    students_needing_support?: Array<{ student_identifier?: string }>;
    objectives_requiring_attention?: Array<{ objective_code?: string; mastery_pct?: number }>;
    open_reteach_plans?: Array<{ id: string; title: string; status: string; navigation_href: string }>;
    recent_mastery_changes?: Array<{ objective_code?: string; trend_direction?: string; mastery_pct?: number }>;
    week_closure_status?: { status?: string; checklist?: Record<string, boolean> } | null;
    instructional_health?: {
      objectives_assessed?: number;
      students_needing_support_count?: number;
      open_reteach_plan_count?: number;
    };
    loop_recommendations?: Array<{ title: string; description?: string; navigation_href?: string }>;
  };
  recommended_reuse?: TeacherAssistHomeReuseCandidate[];
  time_savings?: {
    time_saved_this_year_hours?: number;
    time_saved_this_year_minutes?: number;
  };
  efficiency_summary?: {
    estimated_hours_saved?: number;
    reuse_rate_percent?: number;
    recent_templates?: TeacherAssistHomeTemplateSummary[];
  };
  shortcuts: TeacherAssistTeacherShortcuts;
  timeline: TeacherAssistHomeTimelineEvent[];
  recent_activity: TeacherAssistActionWorkspaceActivity[];
  onboarding: TeacherAssistOnboardingProgress;
  preferences: {
    preferred_landing: string;
    last_class_id: string | null;
    last_subject_id: string | null;
    last_grading_period_id: string | null;
    active_pacing_guide_id?: string | null;
    manual_pacing_period_id?: string | null;
  };
  read_only: boolean;
};

export type TeacherAssistWorkQueueSection = {
  section_key: string;
  title: string;
  count: number;
  items: TeacherAssistActionWorkspaceItem[];
};

export type TeacherAssistWorkQueue = {
  summary: {
    total_actionable: number;
    critical_count: number;
    high_count: number;
    medium_count: number;
  };
  sections: TeacherAssistWorkQueueSection[];
  items: TeacherAssistActionWorkspaceItem[];
  read_only: boolean;
};

export type TeacherAssistClassOperationalWorkspace = {
  class_id: string;
  class_name: string;
  grade_level: string | null;
  student_count: number | null;
  school_year_id: string;
  summary: {
    pending_actions_count: number;
    assignment_count: number;
    reteach_plan_count: number;
    reflection_count: number;
    mastery_matrix_count: number;
  };
  tabs: Record<string, unknown>;
  recent_activity: TeacherAssistActionWorkspaceActivity[];
  read_only: boolean;
};
