"use client";

import Image from "next/image";
import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { fetchPeriodLaunchContext } from "@/lib/pacing-guide-api";

import { TeacherAssistAlert } from "@/components/teacher-assist/teacher-assist-alert";
import { TeacherAssistFieldError, fieldErrorInputClass } from "@/components/teacher-assist/teacher-assist-field-error";
import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import { buildApiUrl } from "@/lib/auth-service";
import { withPreservedScroll } from "@/lib/teacher-assist-scroll";
import {
  cancelExtractionJob,
  commitGradingReviewToGradebook,
  createAssignment,
  createAssignmentGradingReview,
  createAssignmentPrintPacket,
  createStudentWorkExtractionJob,
  fetchAssignmentGradingReviews,
  fetchAssignmentGradebookRecords,
  fetchAssignmentGradingPrepSummary,
  fetchAssignmentStudentWork,
  fetchAssignmentPrintPacketPages,
  fetchAssignmentPrintPackets,
  fetchAssignments,
  fetchAssignmentStudentWorkDownloadUrl,
  fetchStudentWorkGradingPrepContext,
  fetchClasses,
  fetchGradingPeriods,
  generateAssignmentGradingReviewAISuggestion,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherAssistOptions,
  updateAssignmentGradingReview,
  updateAssignmentGradingReviewStatus,
  updateAssignmentStudentWorkPacketContext,
  updateAssignmentStudentWorkStatus,
  updateAssignment,
  updateAssignmentStatus,
  uploadAssignmentStudentWork,
} from "@/lib/teacher-assist-api";
import type {
  Assignment,
  AssignmentGradingReview,
  AssignmentGradingReviewAISuggestion,
  AssignmentGradeRecord,
  AssignmentInput,
  AssignmentPrintPacket,
  AssignmentPrintPage,
  AssignmentStudentWorkSubmission,
  GradingPeriod,
  SchoolYear,
  Standard,
  Subject,
  TeacherAssistAssignmentGradingPrepSummary,
  TeacherAssistStudentWorkGradingPrepContext,
  TeacherAssistOptions,
  TeacherClass,
} from "@/lib/teacher-assist-types";

type Filters = {
  school_year_id: string;
  class_id: string;
  subject_id: string;
  status: string;
  assignment_type: string;
  q: string;
};

type AssignmentForm = {
  school_year_id: string;
  grading_period_id: string;
  class_id: string;
  subject_id: string;
  title: string;
  description: string;
  assignment_type: Assignment["assignment_type"];
  due_date: string;
  status: Assignment["status"];
  instructions: string;
  standard_ids: string[];
};

type PacketForm = {
  pages_per_student: number;
  template_type: AssignmentPrintPacket["template_type"];
  output_format: AssignmentPrintPacket["output_format"];
};

type StudentWorkUploadForm = {
  student_number: number;
  assignment_print_packet_id: string;
};

type SubmissionContextForm = {
  processing_status: AssignmentStudentWorkSubmission["processing_status"];
  assignment_print_packet_id: string;
  assignment_print_page_id: string;
};

type GradingReviewForm = {
  status: AssignmentGradingReview["status"];
  score_suggestion: string;
  max_score: string;
  feedback_summary: string;
  strengths: string;
  improvement_areas: string;
  teacher_notes: string;
  teacher_confirmed_score: string;
  teacher_confirmed_feedback: string;
};

const PLACEHOLDER_ACTIONS = [
  "AI Grading",
  "Update Mastery Matrix",
  "Send Parent Communication",
];

function emptyForm(): AssignmentForm {
  return {
    school_year_id: "",
    grading_period_id: "",
    class_id: "",
    subject_id: "",
    title: "",
    description: "",
    assignment_type: "other",
    due_date: "",
    status: "draft",
    instructions: "",
    standard_ids: [],
  };
}

function emptyPacketForm(): PacketForm {
  return {
    pages_per_student: 1,
    template_type: "blank_writing_page",
    output_format: "html",
  };
}

function emptyStudentWorkUploadForm(): StudentWorkUploadForm {
  return {
    student_number: 1,
    assignment_print_packet_id: "",
  };
}

function emptyGradingReviewForm(): GradingReviewForm {
  return {
    status: "draft",
    score_suggestion: "",
    max_score: "",
    feedback_summary: "",
    strengths: "",
    improvement_areas: "",
    teacher_notes: "",
    teacher_confirmed_score: "",
    teacher_confirmed_feedback: "",
  };
}

function formFromAssignment(assignment: Assignment): AssignmentForm {
  return {
    school_year_id: assignment.school_year_id,
    grading_period_id: assignment.grading_period_id ?? "",
    class_id: assignment.class_id,
    subject_id: assignment.subject_id,
    title: assignment.title,
    description: assignment.description ?? "",
    assignment_type: assignment.assignment_type,
    due_date: assignment.due_date ?? "",
    status: assignment.status,
    instructions: assignment.instructions ?? "",
    standard_ids: assignment.standard_ids,
  };
}

function formatDate(value: string | null) {
  if (!value) return "No due date";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

function formatDateTime(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function formatFileSize(value: number) {
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function formatNumber(value: number | null) {
  if (value == null) return "";
  return Number.isInteger(value) ? String(value) : String(value);
}

function labelize(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function extractionStatusClasses(status: string | null | undefined) {
  switch (status) {
    case "completed":
      return "bg-emerald-100 text-emerald-700";
    case "failed":
      return "bg-rose-100 text-rose-700";
    case "running":
      return "bg-sky-100 text-sky-700";
    case "queued":
      return "bg-amber-100 text-amber-700";
    case "cancelled":
      return "bg-slate-200 text-slate-700";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

function listToEditorText(values: string[]) {
  return values.join("\n");
}

function editorTextToList(value: string) {
  return value
    .split("\n")
    .map((item) => item.trim())
    .filter(Boolean);
}

function reviewFormFromReview(review: AssignmentGradingReview): GradingReviewForm {
  return {
    status: review.status,
    score_suggestion: formatNumber(review.score_suggestion),
    max_score: formatNumber(review.max_score),
    feedback_summary: review.feedback_summary ?? "",
    strengths: listToEditorText(review.strengths),
    improvement_areas: listToEditorText(review.improvement_areas),
    teacher_notes: review.teacher_notes ?? "",
    teacher_confirmed_score: formatNumber(review.teacher_confirmed_score),
    teacher_confirmed_feedback: review.teacher_confirmed_feedback ?? "",
  };
}

function maybeNumber(value: string) {
  const normalized = value.trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function toAssignmentInput(form: AssignmentForm): AssignmentInput {
  return {
    school_year_id: form.school_year_id,
    grading_period_id: form.grading_period_id || null,
    class_id: form.class_id,
    subject_id: form.subject_id,
    title: form.title.trim(),
    description: form.description.trim() || null,
    assignment_type: form.assignment_type,
    due_date: form.due_date || null,
    status: form.status,
    instructions: form.instructions.trim() || null,
    standard_ids: form.standard_ids,
  };
}

function PacketPreviewCard({
  packet,
  isSelected,
  onSelect,
}: {
  packet: AssignmentPrintPacket;
  isSelected: boolean;
  onSelect: (packetId: string) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => onSelect(packet.id)}
      className={`w-full rounded-2xl border p-4 text-left transition ${
        isSelected
          ? "border-sky-300 bg-sky-50"
          : "border-slate-200 bg-white hover:border-sky-200 hover:bg-sky-50/40"
      }`}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm font-semibold text-slate-900">
          {labelize(packet.template_type)}
        </span>
        <span className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-semibold text-violet-800">
          {packet.total_page_count} pages
        </span>
      </div>
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-600">
        <span>{packet.student_count} students</span>
        <span>{packet.pages_per_student} page(s) per student</span>
        <span>{labelize(packet.packet_status)}</span>
      </div>
      <p className="mt-2 text-xs text-slate-500">Generated {formatDateTime(packet.created_at)}</p>
    </button>
  );
}

export function TeacherAssistAssignmentsScreen() {
  const searchParams = useSearchParams();
  const requestedAssignmentId = searchParams.get("assignment_id");
  const pacingPeriodId = searchParams.get("pacing_period_id");
  const pacingPrefillRef = useRef<string | null>(null);
  const [options, setOptions] = useState<TeacherAssistOptions | null>(null);
  const [assignments, setAssignments] = useState<Assignment[]>([]);
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [gradingPeriods, setGradingPeriods] = useState<GradingPeriod[]>([]);
  const [classes, setClasses] = useState<TeacherClass[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [standards, setStandards] = useState<Standard[]>([]);
  const [filters, setFilters] = useState<Filters>({
    school_year_id: "",
    class_id: "",
    subject_id: "",
    status: "",
    assignment_type: "",
    q: "",
  });
  const [form, setForm] = useState<AssignmentForm>(emptyForm);
  const [packetForm, setPacketForm] = useState<PacketForm>(emptyPacketForm);
  const [editingAssignmentId, setEditingAssignmentId] = useState<string | null>(null);
  const [packetAssignmentId, setPacketAssignmentId] = useState<string | null>(null);
  const [selectedPacketId, setSelectedPacketId] = useState<string | null>(null);
  const [packets, setPackets] = useState<AssignmentPrintPacket[]>([]);
  const [selectedPacketPages, setSelectedPacketPages] = useState<AssignmentPrintPage[]>([]);
  const [submissionPagesByPacketId, setSubmissionPagesByPacketId] = useState<
    Record<string, AssignmentPrintPage[]>
  >({});
  const [submissions, setSubmissions] = useState<AssignmentStudentWorkSubmission[]>([]);
  const [gradingReviews, setGradingReviews] = useState<AssignmentGradingReview[]>([]);
  const [gradingPrepSummary, setGradingPrepSummary] = useState<TeacherAssistAssignmentGradingPrepSummary | null>(
    null,
  );
  const [gradingPrepContext, setGradingPrepContext] = useState<TeacherAssistStudentWorkGradingPrepContext | null>(
    null,
  );
  const [studentWorkForm, setStudentWorkForm] = useState<StudentWorkUploadForm>(emptyStudentWorkUploadForm);
  const [selectedSubmissionId, setSelectedSubmissionId] = useState<string | null>(null);
  const [selectedGradingReviewId, setSelectedGradingReviewId] = useState<string | null>(null);
  const [selectedSubmissionFile, setSelectedSubmissionFile] = useState<File | null>(null);
  const [submissionContextForm, setSubmissionContextForm] = useState<SubmissionContextForm | null>(null);
  const [gradingReviewForm, setGradingReviewForm] = useState<GradingReviewForm>(emptyGradingReviewForm);
  const [loading, setLoading] = useState(true);
  const [packetsLoading, setPacketsLoading] = useState(false);
  const [packetPagesLoading, setPacketPagesLoading] = useState(false);
  const [submissionsLoading, setSubmissionsLoading] = useState(false);
  const [gradingReviewsLoading, setGradingReviewsLoading] = useState(false);
  const [gradingPrepLoading, setGradingPrepLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [generatingPacket, setGeneratingPacket] = useState(false);
  const [uploadingStudentWork, setUploadingStudentWork] = useState(false);
  const [studentWorkUploadProgress, setStudentWorkUploadProgress] = useState(0);
  const [downloadingSubmissionId, setDownloadingSubmissionId] = useState<string | null>(null);
  const [startingExtractionId, setStartingExtractionId] = useState<string | null>(null);
  const [cancellingExtractionId, setCancellingExtractionId] = useState<string | null>(null);
  const [savingSubmissionStatus, setSavingSubmissionStatus] = useState(false);
  const [savingSubmissionContext, setSavingSubmissionContext] = useState(false);
  const [creatingGradingReview, setCreatingGradingReview] = useState<string | null>(null);
  const [savingGradingReview, setSavingGradingReview] = useState(false);
  const [savingGradingReviewStatus, setSavingGradingReviewStatus] = useState(false);
  const [generatingAISuggestion, setGeneratingAISuggestion] = useState(false);
  const [aiSuggestionMeta, setAiSuggestionMeta] = useState<AssignmentGradingReviewAISuggestion | null>(null);
  const [gradeRecords, setGradeRecords] = useState<AssignmentGradeRecord[]>([]);
  const [gradeRecordsLoading, setGradeRecordsLoading] = useState(false);
  const [committingGradebook, setCommittingGradebook] = useState(false);
  const [updatingStatusId, setUpdatingStatusId] = useState<string | null>(null);
  const [statusDrafts, setStatusDrafts] = useState<Record<string, Assignment["status"]>>({});
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [pageError, setPageError] = useState<string | null>(null);
  const [studentWorkFileError, setStudentWorkFileError] = useState<string | null>(null);
  const [studentWorkUploadError, setStudentWorkUploadError] = useState<string | null>(null);

  const load = useCallback(async (currentFilters: Filters, options?: { silent?: boolean }) => {
    if (!options?.silent) {
      setLoading(true);
      setPageError(null);
    }
    try {
      const [
        nextOptions,
        nextAssignments,
        nextSchoolYears,
        nextGradingPeriods,
        nextClasses,
        nextSubjects,
        nextStandards,
      ] = await Promise.all([
        fetchTeacherAssistOptions(),
        fetchAssignments({
          school_year_id: currentFilters.school_year_id || undefined,
          class_id: currentFilters.class_id || undefined,
          subject_id: currentFilters.subject_id || undefined,
          status: currentFilters.status || undefined,
          assignment_type: currentFilters.assignment_type || undefined,
          q: currentFilters.q.trim() || undefined,
        }),
        fetchSchoolYears(),
        fetchGradingPeriods(),
        fetchClasses(),
        fetchSubjects(),
        fetchStandards(),
      ]);
      setOptions(nextOptions);
      setAssignments(nextAssignments);
      setSchoolYears(nextSchoolYears);
      setGradingPeriods(nextGradingPeriods);
      setClasses(nextClasses);
      setSubjects(nextSubjects);
      setStandards(nextStandards);
      setStatusDrafts(
        Object.fromEntries(nextAssignments.map((assignment) => [assignment.id, assignment.status])),
      );
      if (packetAssignmentId && !nextAssignments.some((assignment) => assignment.id === packetAssignmentId)) {
        setPacketAssignmentId(null);
        setPackets([]);
        setSelectedPacketId(null);
        setSelectedPacketPages([]);
        setSubmissions([]);
        setSelectedSubmissionId(null);
        setSubmissionContextForm(null);
        setGradingReviews([]);
        setSelectedGradingReviewId(null);
        setGradingReviewForm(emptyGradingReviewForm());
      }
    } catch (nextError) {
      if (!options?.silent) {
        setPageError(nextError instanceof Error ? nextError.message : "Could not load assignments.");
      } else {
        throw nextError;
      }
    } finally {
      if (!options?.silent) {
        setLoading(false);
      }
    }
  }, [packetAssignmentId]);

  const loadPackets = useCallback(async (assignmentId: string) => {
    setPacketsLoading(true);
    try {
      const nextPackets = await fetchAssignmentPrintPackets(assignmentId);
      setPackets(nextPackets);
      setSelectedPacketId((current) => {
        if (current && nextPackets.some((packet) => packet.id === current)) return current;
        return nextPackets[0]?.id ?? null;
      });
      setStudentWorkForm((current) => ({
        ...current,
        assignment_print_packet_id:
          current.assignment_print_packet_id &&
          nextPackets.some((packet) => packet.id === current.assignment_print_packet_id)
            ? current.assignment_print_packet_id
            : "",
      }));
    } catch (nextError) {
      setSectionAlert(
        "printPacket",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load printable packets.",
          "Unable to load packets",
        ),
      );
      setPackets([]);
      setSelectedPacketId(null);
    } finally {
      setPacketsLoading(false);
    }
  }, []);

  const loadGradingPrepSummary = useCallback(async (assignmentId: string) => {
    setGradingPrepLoading(true);
    try {
      const nextSummary = await fetchAssignmentGradingPrepSummary(assignmentId);
      setGradingPrepSummary(nextSummary);
    } catch (nextError) {
      setGradingPrepSummary(null);
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load grading prep summary.",
          "Unable to load grading prep",
        ),
      );
    } finally {
      setGradingPrepLoading(false);
    }
  }, []);

  const loadGradingPrepContext = useCallback(async (submissionId: string) => {
    try {
      const nextContext = await fetchStudentWorkGradingPrepContext(submissionId);
      setGradingPrepContext(nextContext);
    } catch {
      setGradingPrepContext(null);
    }
  }, []);

  const loadStudentWork = useCallback(async (assignmentId: string) => {
    setSubmissionsLoading(true);
    try {
      const nextSubmissions = await fetchAssignmentStudentWork(assignmentId);
      setSubmissions(nextSubmissions);
      setSelectedSubmissionId((current) => {
        if (current && nextSubmissions.some((submission) => submission.id === current)) return current;
        return nextSubmissions[0]?.id ?? null;
      });
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load student work.",
          "Unable to load student work",
        ),
      );
      setSubmissions([]);
      setSelectedSubmissionId(null);
    } finally {
      setSubmissionsLoading(false);
    }
    void loadGradingPrepSummary(assignmentId);
  }, [loadGradingPrepSummary]);

  const loadGradingReviews = useCallback(async (assignmentId: string) => {
    setGradingReviewsLoading(true);
    try {
      const nextReviews = await fetchAssignmentGradingReviews(assignmentId);
      setGradingReviews(nextReviews);
      setSelectedGradingReviewId((current) => {
        if (current && nextReviews.some((review) => review.id === current)) return current;
        return nextReviews[0]?.id ?? null;
      });
    } catch (nextError) {
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not load grading reviews.",
          "Unable to load grading reviews",
        ),
      );
      setGradingReviews([]);
      setSelectedGradingReviewId(null);
    } finally {
      setGradingReviewsLoading(false);
    }
    setGradeRecordsLoading(true);
    try {
      const nextRecords = await fetchAssignmentGradebookRecords(assignmentId);
      setGradeRecords(nextRecords);
    } catch {
      setGradeRecords([]);
    } finally {
      setGradeRecordsLoading(false);
    }
  }, []);

  const ensurePacketPages = useCallback(
    async (packetId: string) => {
      if (submissionPagesByPacketId[packetId]) return;
      const pages = await fetchAssignmentPrintPacketPages(packetId);
      setSubmissionPagesByPacketId((current) => ({ ...current, [packetId]: pages }));
    },
    [submissionPagesByPacketId],
  );

  useEffect(() => {
    void load(filters);
  }, [filters, load]);

  useEffect(() => {
    if (
      !pacingPeriodId ||
      loading ||
      editingAssignmentId ||
      pacingPrefillRef.current === pacingPeriodId
    ) {
      return;
    }
    pacingPrefillRef.current = pacingPeriodId;
    void fetchPeriodLaunchContext(pacingPeriodId)
      .then((context) => {
        const assignment = (context.assignment ?? {}) as Record<string, unknown>;
        setForm((current) => ({
          ...current,
          school_year_id: String(assignment.school_year_id ?? current.school_year_id),
          grading_period_id: String(assignment.grading_period_id ?? current.grading_period_id),
          subject_id: String(assignment.subject_id ?? current.subject_id),
          title: String(assignment.title ?? current.title),
          description: String(assignment.description ?? current.description),
          standard_ids: Array.isArray(assignment.standard_ids)
            ? assignment.standard_ids.map(String)
            : current.standard_ids,
        }));
        setSectionAlert(
          "assignmentForm",
          sectionSuccess(
            `Pre-filled from pacing week${context.period_title ? `: ${context.period_title}` : "."}`,
            "Assignment pre-filled",
          ),
        );
      })
      .catch((nextError) => {
        pacingPrefillRef.current = null;
        setSectionAlert(
          "assignmentForm",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not pre-fill assignment from pacing week.",
            "Unable to pre-fill assignment",
          ),
        );
      });
  }, [editingAssignmentId, loading, pacingPeriodId, setSectionAlert]);

  useEffect(() => {
    if (!packetAssignmentId) {
      setPackets([]);
      setSelectedPacketId(null);
      setSelectedPacketPages([]);
      setSubmissionPagesByPacketId({});
      setSubmissions([]);
      setSelectedSubmissionId(null);
      setSubmissionContextForm(null);
      setSelectedSubmissionFile(null);
      setStudentWorkForm(emptyStudentWorkUploadForm());
      setGradingReviews([]);
      setSelectedGradingReviewId(null);
      setGradingPrepSummary(null);
      setGradingPrepContext(null);
      setGradingReviewForm(emptyGradingReviewForm());
      return;
    }
    void loadPackets(packetAssignmentId);
    void loadStudentWork(packetAssignmentId);
    void loadGradingReviews(packetAssignmentId);
  }, [loadGradingReviews, loadPackets, loadStudentWork, packetAssignmentId]);

  useEffect(() => {
    if (!selectedSubmissionId) {
      setGradingPrepContext(null);
      return;
    }
    void loadGradingPrepContext(selectedSubmissionId);
  }, [loadGradingPrepContext, selectedSubmissionId]);

  useEffect(() => {
    if (!selectedPacketId) {
      setSelectedPacketPages([]);
      return;
    }
    setPacketPagesLoading(true);
    setPageError(null);
    void fetchAssignmentPrintPacketPages(selectedPacketId)
      .then((pages) => {
        setSelectedPacketPages(pages);
      })
      .catch((nextError) => {
        setSectionAlert(
          "printPacket",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not load packet pages.",
            "Unable to load packet pages",
          ),
        );
        setSelectedPacketPages([]);
      })
      .finally(() => {
        setPacketPagesLoading(false);
      });
  }, [selectedPacketId]);

  const schoolYearMap = useMemo(
    () => Object.fromEntries(schoolYears.map((item) => [item.id, item])),
    [schoolYears],
  );
  const classMap = useMemo(() => Object.fromEntries(classes.map((item) => [item.id, item])), [classes]);
  const subjectMap = useMemo(
    () => Object.fromEntries(subjects.map((item) => [item.id, item])),
    [subjects],
  );
  const gradingPeriodMap = useMemo(
    () => Object.fromEntries(gradingPeriods.map((item) => [item.id, item])),
    [gradingPeriods],
  );
  const filteredGradingPeriods = useMemo(
    () =>
      gradingPeriods.filter(
        (item) => !form.school_year_id || item.school_year_id === form.school_year_id,
      ),
    [form.school_year_id, gradingPeriods],
  );
  const packetAssignment = useMemo(
    () => assignments.find((assignment) => assignment.id === packetAssignmentId) ?? null,
    [assignments, packetAssignmentId],
  );
  const selectedPacket = useMemo(
    () => packets.find((packet) => packet.id === selectedPacketId) ?? null,
    [packets, selectedPacketId],
  );
  const selectedSubmission = useMemo(
    () => submissions.find((submission) => submission.id === selectedSubmissionId) ?? null,
    [selectedSubmissionId, submissions],
  );
  const selectedGradingReview = useMemo(
    () => gradingReviews.find((review) => review.id === selectedGradingReviewId) ?? null,
    [gradingReviews, selectedGradingReviewId],
  );
  const reviewBySubmissionId = useMemo(
    () => Object.fromEntries(gradingReviews.map((review) => [review.student_work_submission_id, review])),
    [gradingReviews],
  );
  const activeGradeRecordByReviewId = useMemo(
    () =>
      Object.fromEntries(
        gradeRecords
          .filter((record) => record.record_status === "active")
          .map((record) => [record.grading_review_id, record]),
      ),
    [gradeRecords],
  );
  const gradingPrepBySubmissionId = useMemo(
    () =>
      Object.fromEntries(
        (gradingPrepSummary?.submissions ?? []).map((item) => [item.student_work_submission_id, item]),
      ),
    [gradingPrepSummary],
  );

  const availableSubjects = useMemo(() => {
    const selectedClass = form.class_id ? classMap[form.class_id] : null;
    if (!selectedClass || selectedClass.subject_ids.length === 0) {
      return subjects;
    }
    return subjects.filter((subject) => selectedClass.subject_ids.includes(subject.id));
  }, [classMap, form.class_id, subjects]);

  const availableStandards = useMemo(
    () =>
      standards.filter((standard) => {
        if (form.subject_id && standard.subject_id && standard.subject_id !== form.subject_id) {
          return false;
        }
        if (form.school_year_id && standard.school_year_id && standard.school_year_id !== form.school_year_id) {
          return false;
        }
        return true;
      }),
    [form.school_year_id, form.subject_id, standards],
  );
  const selectedSubmissionPages = useMemo(() => {
    if (!selectedSubmission || !submissionContextForm?.assignment_print_packet_id) return [];
    return (submissionPagesByPacketId[submissionContextForm.assignment_print_packet_id] ?? []).filter(
      (page) => page.student_number === selectedSubmission.student_number,
    );
  }, [selectedSubmission, submissionContextForm, submissionPagesByPacketId]);

  const assignmentCounts = useMemo(
    () => ({
      total: assignments.length,
      ready: assignments.filter((assignment) => assignment.status === "ready").length,
      review: assignments.filter((assignment) =>
        ["collected", "review_in_progress"].includes(assignment.status),
      ).length,
    }),
    [assignments],
  );

  const resetForm = useCallback(() => {
    setEditingAssignmentId(null);
    setForm(emptyForm());
  }, []);

  const handleEditAssignment = useCallback((assignment: Assignment) => {
    setEditingAssignmentId(assignment.id);
    setForm(formFromAssignment(assignment));
    clearSectionAlert("assignmentForm");
  }, [clearSectionAlert]);

  const handlePacketAssignment = useCallback((assignmentId: string) => {
    setPacketAssignmentId(assignmentId);
    setSelectedPacketId(null);
    setSelectedPacketPages([]);
    setPacketForm(emptyPacketForm());
    setSubmissions([]);
    setSelectedSubmissionId(null);
    setSubmissionContextForm(null);
    setSelectedSubmissionFile(null);
    setStudentWorkForm(emptyStudentWorkUploadForm());
    setGradingReviews([]);
    setSelectedGradingReviewId(null);
    setGradingReviewForm(emptyGradingReviewForm());
    clearSectionAlert("printPacket");
    clearSectionAlert("studentWork");
    clearSectionAlert("gradingReview");
  }, [clearSectionAlert]);

  useEffect(() => {
    if (!requestedAssignmentId || loading) return;
    if (!assignments.some((assignment) => assignment.id === requestedAssignmentId)) return;
    if (packetAssignmentId === requestedAssignmentId) return;
    handlePacketAssignment(requestedAssignmentId);
  }, [
    assignments,
    handlePacketAssignment,
    loading,
    packetAssignmentId,
    requestedAssignmentId,
  ]);

  const handleToggleStandard = useCallback((standardId: string) => {
    setForm((current) => ({
      ...current,
      standard_ids: current.standard_ids.includes(standardId)
        ? current.standard_ids.filter((id) => id !== standardId)
        : [...current.standard_ids, standardId],
    }));
  }, []);

  const handleSaveAssignment = useCallback(async () => {
    setSaving(true);
    clearSectionAlert("assignmentForm");
    try {
      const payload = toAssignmentInput(form);
      const wasEditing = Boolean(editingAssignmentId);
      const saved = editingAssignmentId
        ? await updateAssignment(editingAssignmentId, payload)
        : await createAssignment(payload);
      await withPreservedScroll(null, () => load(filters, { silent: true }));
      setEditingAssignmentId(saved.id);
      setForm(formFromAssignment(saved));
      setSectionAlert(
        "assignmentForm",
        sectionSuccess(
          wasEditing ? "Your assignment changes were saved." : "The assignment was created successfully.",
          wasEditing ? "Assignment updated" : "Assignment created",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "assignmentForm",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not save assignment.",
          "Unable to save assignment",
        ),
      );
    } finally {
      setSaving(false);
    }
  }, [clearSectionAlert, editingAssignmentId, filters, form, load, setSectionAlert]);

  const handleStatusUpdate = useCallback(
    async (assignmentId: string) => {
      const nextStatus = statusDrafts[assignmentId];
      if (!nextStatus) return;
      setUpdatingStatusId(assignmentId);
      clearSectionAlert("assignmentList");
      try {
        await updateAssignmentStatus(assignmentId, nextStatus);
        await withPreservedScroll(null, () => load(filters, { silent: true }));
        setSectionAlert(
          "assignmentList",
          sectionSuccess("Assignment status updated.", "Status updated"),
        );
      } catch (nextError) {
        setSectionAlert(
          "assignmentList",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not update assignment status.",
            "Unable to update status",
          ),
        );
      } finally {
        setUpdatingStatusId(null);
      }
    },
    [clearSectionAlert, filters, load, setSectionAlert, statusDrafts],
  );

  const handleGeneratePacket = useCallback(async () => {
    if (!packetAssignmentId) return;
    setGeneratingPacket(true);
    clearSectionAlert("printPacket");
    try {
      const created = await createAssignmentPrintPacket(packetAssignmentId, packetForm);
      await loadPackets(packetAssignmentId);
      setSelectedPacketId(created.id);
      setSectionAlert(
        "printPacket",
        sectionSuccess("Printable QR packet generated.", "Packet generated"),
      );
    } catch (nextError) {
      setSectionAlert(
        "printPacket",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not generate printable packet.",
          "Unable to generate packet",
        ),
      );
    } finally {
      setGeneratingPacket(false);
    }
  }, [clearSectionAlert, loadPackets, packetAssignmentId, packetForm, setSectionAlert]);

  const handleUploadStudentWork = useCallback(async () => {
    if (!packetAssignmentId || !selectedSubmissionFile) {
      setStudentWorkFileError("Choose a file before uploading student work.");
      return;
    }
    setUploadingStudentWork(true);
    setStudentWorkUploadProgress(0);
    setStudentWorkFileError(null);
    setStudentWorkUploadError(null);
    clearSectionAlert("studentWork");
    try {
      const created = await uploadAssignmentStudentWork(
        packetAssignmentId,
        selectedSubmissionFile,
        {
          student_number: studentWorkForm.student_number,
          assignment_print_packet_id: studentWorkForm.assignment_print_packet_id || null,
        },
        setStudentWorkUploadProgress,
      );
      await loadStudentWork(packetAssignmentId);
      setSelectedSubmissionId(created.id);
      setSelectedSubmissionFile(null);
      setStudentWorkForm((current) => ({
        ...current,
        assignment_print_packet_id: current.assignment_print_packet_id,
      }));
      setSectionAlert(
        "studentWork",
        sectionSuccess("Student work uploaded.", "Upload complete"),
      );
    } catch (nextError) {
      setStudentWorkUploadError(
        nextError instanceof Error ? nextError.message : "Could not upload student work.",
      );
    } finally {
      setUploadingStudentWork(false);
      setStudentWorkUploadProgress(0);
    }
  }, [clearSectionAlert, loadStudentWork, packetAssignmentId, selectedSubmissionFile, setSectionAlert, studentWorkForm]);

  const handleStartGradingReview = useCallback(
    async (submission: AssignmentStudentWorkSubmission) => {
      const existing = reviewBySubmissionId[submission.id];
      if (existing) {
        setSelectedGradingReviewId(existing.id);
        setSectionAlert(
          "gradingReview",
          sectionSuccess("Opened existing grading review.", "Review opened"),
        );
        return;
      }
      setCreatingGradingReview(submission.id);
      clearSectionAlert("gradingReview");
      try {
        const created = await createAssignmentGradingReview(submission.id, {
          student_number: submission.student_number,
        });
        if (packetAssignmentId) {
          await loadGradingReviews(packetAssignmentId);
        }
        setSelectedGradingReviewId(created.id);
        setSectionAlert(
          "gradingReview",
          sectionSuccess("Grading review created.", "Review created"),
        );
      } catch (nextError) {
        setSectionAlert(
          "gradingReview",
          sectionError(
            nextError instanceof Error ? nextError.message : "Could not start grading review.",
            "Unable to start review",
          ),
        );
      } finally {
        setCreatingGradingReview(null);
      }
    },
    [loadGradingReviews, packetAssignmentId, reviewBySubmissionId, setSectionAlert, clearSectionAlert],
  );

  const handleDownloadSubmission = useCallback(async () => {
    if (!selectedSubmission) return;
    setDownloadingSubmissionId(selectedSubmission.id);
    clearSectionAlert("studentWork");
    try {
      const download = await fetchAssignmentStudentWorkDownloadUrl(selectedSubmission.id);
      const nextUrl = download.url.startsWith("/") ? buildApiUrl(download.url) : download.url;
      window.open(nextUrl, "_blank", "noopener,noreferrer");
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not prepare the student-work download.",
          "Download failed",
        ),
      );
    } finally {
      setDownloadingSubmissionId(null);
    }
  }, [clearSectionAlert, selectedSubmission, setSectionAlert]);

  const handleStartExtraction = useCallback(async () => {
    if (!selectedSubmission || !packetAssignmentId) return;
    clearSectionAlert("studentWork");
    setStartingExtractionId(selectedSubmission.id);
    try {
      await createStudentWorkExtractionJob(selectedSubmission.id);
      await loadStudentWork(packetAssignmentId);
      setSectionAlert(
        "studentWork",
        sectionSuccess(
          `Extraction queued for STUDENT #${selectedSubmission.student_number}.`,
          "Extraction queued",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not queue extraction.",
          "Extraction failed",
        ),
      );
    } finally {
      setStartingExtractionId(null);
    }
  }, [clearSectionAlert, loadStudentWork, packetAssignmentId, selectedSubmission, setSectionAlert]);

  const handleCancelExtraction = useCallback(async () => {
    const extractionJobId = selectedSubmission?.latest_extraction_job?.id;
    if (!selectedSubmission || !extractionJobId || !packetAssignmentId) return;
    clearSectionAlert("studentWork");
    setCancellingExtractionId(extractionJobId);
    try {
      await cancelExtractionJob(extractionJobId);
      await loadStudentWork(packetAssignmentId);
      setSectionAlert(
        "studentWork",
        sectionSuccess(
          `Extraction cancelled for STUDENT #${selectedSubmission.student_number}.`,
          "Extraction cancelled",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not cancel extraction.",
          "Unable to cancel extraction",
        ),
      );
    } finally {
      setCancellingExtractionId(null);
    }
  }, [clearSectionAlert, loadStudentWork, packetAssignmentId, selectedSubmission, setSectionAlert]);

  const handleUpdateSubmissionStatus = useCallback(async () => {
    if (!selectedSubmission || !submissionContextForm) return;
    setSavingSubmissionStatus(true);
    clearSectionAlert("studentWork");
    try {
      await updateAssignmentStudentWorkStatus(selectedSubmission.id, submissionContextForm.processing_status);
      if (packetAssignmentId) {
        await loadStudentWork(packetAssignmentId);
      }
      setSectionAlert(
        "studentWork",
        sectionSuccess("Student work status updated.", "Status updated"),
      );
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not update student work status.",
          "Unable to update status",
        ),
      );
    } finally {
      setSavingSubmissionStatus(false);
    }
  }, [clearSectionAlert, loadStudentWork, packetAssignmentId, selectedSubmission, setSectionAlert, submissionContextForm]);

  const handleUpdateSubmissionContext = useCallback(async () => {
    if (!selectedSubmission || !submissionContextForm) return;
    setSavingSubmissionContext(true);
    clearSectionAlert("studentWork");
    try {
      await updateAssignmentStudentWorkPacketContext(selectedSubmission.id, {
        assignment_print_packet_id: submissionContextForm.assignment_print_packet_id || null,
        assignment_print_page_id: submissionContextForm.assignment_print_page_id || null,
      });
      if (packetAssignmentId) {
        await loadStudentWork(packetAssignmentId);
      }
      setSectionAlert(
        "studentWork",
        sectionSuccess("Student work packet context updated.", "Context updated"),
      );
    } catch (nextError) {
      setSectionAlert(
        "studentWork",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not update packet context.",
          "Unable to update context",
        ),
      );
    } finally {
      setSavingSubmissionContext(false);
    }
  }, [clearSectionAlert, loadStudentWork, packetAssignmentId, selectedSubmission, setSectionAlert, submissionContextForm]);

  const handleSaveGradingReview = useCallback(async () => {
    if (!selectedGradingReview) return;
    setSavingGradingReview(true);
    clearSectionAlert("gradingReview");
    try {
      await updateAssignmentGradingReview(selectedGradingReview.id, {
        status: gradingReviewForm.status,
        score_suggestion: maybeNumber(gradingReviewForm.score_suggestion),
        max_score: maybeNumber(gradingReviewForm.max_score),
        feedback_summary: gradingReviewForm.feedback_summary.trim() || null,
        strengths: editorTextToList(gradingReviewForm.strengths),
        improvement_areas: editorTextToList(gradingReviewForm.improvement_areas),
        teacher_notes: gradingReviewForm.teacher_notes.trim() || null,
        teacher_confirmed_score: maybeNumber(gradingReviewForm.teacher_confirmed_score),
        teacher_confirmed_feedback: gradingReviewForm.teacher_confirmed_feedback.trim() || null,
        items: [],
      });
      if (packetAssignmentId) {
        await loadGradingReviews(packetAssignmentId);
      }
      setSectionAlert(
        "gradingReview",
        sectionSuccess("Grading review saved.", "Review saved"),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not save grading review.",
          "Unable to save review",
        ),
      );
    } finally {
      setSavingGradingReview(false);
    }
  }, [clearSectionAlert, gradingReviewForm, loadGradingReviews, packetAssignmentId, selectedGradingReview, setSectionAlert]);

  const handleUpdateGradingReviewStatus = useCallback(async () => {
    if (!selectedGradingReview) return;
    setSavingGradingReviewStatus(true);
    clearSectionAlert("gradingReview");
    try {
      await updateAssignmentGradingReviewStatus(selectedGradingReview.id, gradingReviewForm.status);
      if (packetAssignmentId) {
        await loadGradingReviews(packetAssignmentId);
      }
      setSectionAlert(
        "gradingReview",
        sectionSuccess("Grading review status updated.", "Status updated"),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not update grading review status.",
          "Unable to update status",
        ),
      );
    } finally {
      setSavingGradingReviewStatus(false);
    }
  }, [clearSectionAlert, gradingReviewForm.status, loadGradingReviews, packetAssignmentId, selectedGradingReview, setSectionAlert]);

  const handleGenerateAISuggestion = useCallback(async () => {
    if (!selectedGradingReview) return;
    setGeneratingAISuggestion(true);
    clearSectionAlert("gradingReview");
    try {
      const result = await generateAssignmentGradingReviewAISuggestion(selectedGradingReview.id, {
        provider_mode: "mock",
      });
      setAiSuggestionMeta(result);
      if (packetAssignmentId) {
        await loadGradingReviews(packetAssignmentId);
      }
      setGradingReviewForm(reviewFormFromReview(result.review));
      setSectionAlert(
        "gradingReview",
        sectionSuccess(
          "AI grading suggestion saved as draft. Review and edit before confirming.",
          "AI suggestion ready",
        ),
      );
    } catch (nextError) {
      setAiSuggestionMeta(null);
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not generate AI grading suggestion.",
          "AI suggestion failed",
        ),
      );
    } finally {
      setGeneratingAISuggestion(false);
    }
  }, [clearSectionAlert, loadGradingReviews, packetAssignmentId, selectedGradingReview, setSectionAlert]);

  const handleCommitToGradebook = useCallback(async () => {
    if (!selectedGradingReview) return;
    setCommittingGradebook(true);
    clearSectionAlert("gradingReview");
    try {
      await commitGradingReviewToGradebook(selectedGradingReview.id, {});
      if (packetAssignmentId) {
        await loadGradingReviews(packetAssignmentId);
      }
      setSectionAlert(
        "gradingReview",
        sectionSuccess(
          "Grade committed to gradebook. Review commit history in the Gradebook workspace.",
          "Grade committed",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "gradingReview",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not commit grade to gradebook.",
          "Unable to commit grade",
        ),
      );
    } finally {
      setCommittingGradebook(false);
    }
  }, [clearSectionAlert, loadGradingReviews, packetAssignmentId, selectedGradingReview, setSectionAlert]);

  useEffect(() => {
    if (!selectedSubmission) {
      setSubmissionContextForm(null);
      return;
    }
    setSubmissionContextForm({
      processing_status: selectedSubmission.processing_status,
      assignment_print_packet_id: selectedSubmission.assignment_print_packet_id ?? "",
      assignment_print_page_id: selectedSubmission.assignment_print_page_id ?? "",
    });
  }, [selectedSubmission]);

  useEffect(() => {
    if (!submissionContextForm?.assignment_print_packet_id) return;
    void ensurePacketPages(submissionContextForm.assignment_print_packet_id);
  }, [ensurePacketPages, submissionContextForm?.assignment_print_packet_id]);

  useEffect(() => {
    if (!selectedGradingReview) {
      setGradingReviewForm(emptyGradingReviewForm());
      setAiSuggestionMeta(null);
      return;
    }
    setGradingReviewForm(reviewFormFromReview(selectedGradingReview));
    if (selectedGradingReview.status !== "ai_suggested") {
      setAiSuggestionMeta(null);
    }
  }, [selectedGradingReview]);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Assignments
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Assignment packets, student work, and grading review foundation
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Create, edit, organize, and move assignments through a teacher-review lifecycle with
            class, subject, standards, printable QR packet context, and anonymous student-work
            uploads. Grading review stays teacher-confirmation-first, while OCR, AI grading, mastery,
            and gradebook commit remain intentionally deferred.
          </p>
        </div>
      </section>

      <TeacherAssistAlert
        variant="info"
        description="Assignment packets, student-work intake, and grading review are software-only in this phase. No provider call, OCR, grading automation, mastery update, gradebook commit, or trading behavior is involved here."
      />

      <TeacherAssistFormErrorSummary title="Unable to load assignments" message={pageError} />

      <section className="grid gap-4 lg:grid-cols-3">
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Assignments</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">{loading ? "..." : assignmentCounts.total}</p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Ready to assign</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">{loading ? "..." : assignmentCounts.ready}</p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">In review flow</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">{loading ? "..." : assignmentCounts.review}</p>
        </article>
      </section>

      <section className="ta-panel p-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Assignment filters</h2>
            <p className="mt-1 text-sm text-slate-600">
              Narrow by school year, class, subject, lifecycle state, assignment type, or a text search.
            </p>
          </div>
          <button type="button" onClick={resetForm} className="ta-button-secondary">
            New Assignment
          </button>
        </div>

        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          <label className="flex flex-col gap-2">
            <span className="ta-label">School year</span>
            <select
              className="ta-input"
              value={filters.school_year_id}
              onChange={(event) =>
                setFilters((current) => ({ ...current, school_year_id: event.target.value }))
              }
            >
              <option value="">All school years</option>
              {schoolYears.map((schoolYear) => (
                <option key={schoolYear.id} value={schoolYear.id}>
                  {schoolYear.title}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2">
            <span className="ta-label">Class</span>
            <select
              className="ta-input"
              value={filters.class_id}
              onChange={(event) => setFilters((current) => ({ ...current, class_id: event.target.value }))}
            >
              <option value="">All classes</option>
              {classes.map((teacherClass) => (
                <option key={teacherClass.id} value={teacherClass.id}>
                  {teacherClass.name}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2">
            <span className="ta-label">Subject</span>
            <select
              className="ta-input"
              value={filters.subject_id}
              onChange={(event) =>
                setFilters((current) => ({ ...current, subject_id: event.target.value }))
              }
            >
              <option value="">All subjects</option>
              {subjects.map((subject) => (
                <option key={subject.id} value={subject.id}>
                  {subject.name}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2">
            <span className="ta-label">Status</span>
            <select
              className="ta-input"
              value={filters.status}
              onChange={(event) => setFilters((current) => ({ ...current, status: event.target.value }))}
            >
              <option value="">All statuses</option>
              {options?.assignment_statuses.map((status) => (
                <option key={status} value={status}>
                  {labelize(status)}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2">
            <span className="ta-label">Assignment type</span>
            <select
              className="ta-input"
              value={filters.assignment_type}
              onChange={(event) =>
                setFilters((current) => ({ ...current, assignment_type: event.target.value }))
              }
            >
              <option value="">All types</option>
              {options?.assignment_types.map((assignmentType) => (
                <option key={assignmentType} value={assignmentType}>
                  {labelize(assignmentType)}
                </option>
              ))}
            </select>
          </label>

          <label className="flex flex-col gap-2">
            <span className="ta-label">Search</span>
            <input
              className="ta-input"
              value={filters.q}
              onChange={(event) => setFilters((current) => ({ ...current, q: event.target.value }))}
              placeholder="Search assignment title or instructions"
            />
          </label>
        </div>
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
        <article className="ta-panel p-6">
          <div className="flex flex-col gap-2">
            <h2 className="text-xl font-semibold text-slate-900">Assignment workspace</h2>
            <p className="text-sm text-slate-600">
              Edit teacher-owned assignments, keep them in draft or ready state until reviewed, and
              move them through the later collection/review lifecycle manually.
            </p>
          </div>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("assignmentList")}
            onDismiss={() => clearSectionAlert("assignmentList")}
            className="mt-4"
          />

          {loading ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Loading assignments...
            </div>
          ) : assignments.length === 0 ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              No assignments match the current filters yet.
            </div>
          ) : (
            <div className="mt-5 space-y-3">
              {assignments.map((assignment) => (
                <div
                  key={assignment.id}
                  className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm shadow-slate-950/5"
                >
                  <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <h3 className="text-base font-semibold text-slate-900">{assignment.title}</h3>
                        <span className="rounded-full bg-sky-50 px-2.5 py-1 text-xs font-semibold text-sky-800">
                          {labelize(assignment.assignment_type)}
                        </span>
                        <span className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-semibold text-violet-800">
                          {labelize(assignment.status)}
                        </span>
                      </div>
                      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-600">
                        <span>School year: {schoolYearMap[assignment.school_year_id]?.title ?? "Unknown"}</span>
                        <span>Class: {classMap[assignment.class_id]?.name ?? "Unknown"}</span>
                        <span>Subject: {subjectMap[assignment.subject_id]?.name ?? "Unknown"}</span>
                        <span>Due: {formatDate(assignment.due_date)}</span>
                      </div>
                      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-500">
                        <span>Standards: {assignment.standard_ids.length}</span>
                        <span>Resources: {assignment.resource_ids.length}</span>
                        <span>
                          Grading period:{" "}
                          {assignment.grading_period_id
                            ? gradingPeriodMap[assignment.grading_period_id]?.title ?? "Linked"
                            : "None"}
                        </span>
                        <span>Updated: {formatDate(assignment.updated_at)}</span>
                        {assignment.source_plan_id ? <span>From plan starter</span> : null}
                      </div>
                    </div>

                    <div className="flex flex-col gap-2 xl:min-w-64">
                      <button
                        type="button"
                        onClick={() => handleEditAssignment(assignment)}
                        className="ta-button-primary"
                      >
                        Edit Assignment
                      </button>
                      <button
                        type="button"
                        onClick={() => handlePacketAssignment(assignment.id)}
                        className="ta-button-secondary"
                      >
                        Open Packet + Student Work Tools
                      </button>
                      <select
                        className="ta-input"
                        value={statusDrafts[assignment.id] ?? assignment.status}
                        onChange={(event) =>
                          setStatusDrafts((current) => ({
                            ...current,
                            [assignment.id]: event.target.value as Assignment["status"],
                          }))
                        }
                      >
                        {options?.assignment_statuses.map((status) => (
                          <option key={status} value={status}>
                            {labelize(status)}
                          </option>
                        ))}
                      </select>
                      <button
                        type="button"
                        onClick={() => {
                          void handleStatusUpdate(assignment.id);
                        }}
                        disabled={updatingStatusId === assignment.id}
                        className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {updatingStatusId === assignment.id ? "Saving..." : "Update Status"}
                      </button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">
                {editingAssignmentId ? "Edit assignment" : "Create assignment"}
              </h2>
              <p className="mt-1 text-sm text-slate-600">
                Standards can be attached here before you move the assignment out of draft.
              </p>
            </div>
            {editingAssignmentId ? (
              <button type="button" onClick={resetForm} className="ta-button-secondary">
                Clear
              </button>
            ) : null}
          </div>

          <TeacherAssistInlineAlert
            alert={getSectionAlert("assignmentForm")}
            onDismiss={() => clearSectionAlert("assignmentForm")}
            className="mt-4"
          />

          <div className="mt-5 space-y-4">
            <div className="grid gap-4 md:grid-cols-2">
              <label className="flex flex-col gap-2">
                <span className="ta-label">School year</span>
                <select
                  className="ta-input"
                  value={form.school_year_id}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      school_year_id: event.target.value,
                      grading_period_id:
                        current.grading_period_id &&
                        !gradingPeriods.some(
                          (period) =>
                            period.id === current.grading_period_id &&
                            period.school_year_id === event.target.value,
                        )
                          ? ""
                          : current.grading_period_id,
                    }))
                  }
                >
                  <option value="">Select school year</option>
                  {schoolYears.map((schoolYear) => (
                    <option key={schoolYear.id} value={schoolYear.id}>
                      {schoolYear.title}
                    </option>
                  ))}
                </select>
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Grading period</span>
                <select
                  className="ta-input"
                  value={form.grading_period_id}
                  onChange={(event) =>
                    setForm((current) => ({ ...current, grading_period_id: event.target.value }))
                  }
                >
                  <option value="">Optional</option>
                  {filteredGradingPeriods.map((gradingPeriod) => (
                    <option key={gradingPeriod.id} value={gradingPeriod.id}>
                      {gradingPeriod.title}
                    </option>
                  ))}
                </select>
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Class</span>
                <select
                  className="ta-input"
                  value={form.class_id}
                  onChange={(event) =>
                    setForm((current) => {
                      const nextClass = classes.find(
                        (teacherClass) => teacherClass.id === event.target.value,
                      );
                      const subjectStillValid =
                        !current.subject_id ||
                        !nextClass ||
                        nextClass.subject_ids.length === 0 ||
                        nextClass.subject_ids.includes(current.subject_id);
                      return {
                        ...current,
                        class_id: event.target.value,
                        subject_id: subjectStillValid ? current.subject_id : "",
                      };
                    })
                  }
                >
                  <option value="">Select class</option>
                  {classes.map((teacherClass) => (
                    <option key={teacherClass.id} value={teacherClass.id}>
                      {teacherClass.name}
                    </option>
                  ))}
                </select>
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Subject</span>
                <select
                  className="ta-input"
                  value={form.subject_id}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      subject_id: event.target.value,
                      standard_ids: [],
                    }))
                  }
                >
                  <option value="">Select subject</option>
                  {availableSubjects.map((subject) => (
                    <option key={subject.id} value={subject.id}>
                      {subject.name}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <label className="flex flex-col gap-2">
                <span className="ta-label">Assignment title</span>
                <input
                  className="ta-input"
                  value={form.title}
                  onChange={(event) => setForm((current) => ({ ...current, title: event.target.value }))}
                  placeholder="Short teacher-facing assignment title"
                />
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Assignment type</span>
                <select
                  className="ta-input"
                  value={form.assignment_type}
                  onChange={(event) =>
                    setForm((current) => ({
                      ...current,
                      assignment_type: event.target.value as Assignment["assignment_type"],
                    }))
                  }
                >
                  {options?.assignment_types.map((assignmentType) => (
                    <option key={assignmentType} value={assignmentType}>
                      {labelize(assignmentType)}
                    </option>
                  ))}
                </select>
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Due date</span>
                <input
                  className="ta-input"
                  type="date"
                  value={form.due_date}
                  onChange={(event) => setForm((current) => ({ ...current, due_date: event.target.value }))}
                />
              </label>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Status</span>
                <select
                  className="ta-input"
                  value={form.status}
                  onChange={(event) =>
                    setForm((current) => ({ ...current, status: event.target.value as Assignment["status"] }))
                  }
                >
                  {options?.assignment_statuses.map((status) => (
                    <option key={status} value={status}>
                      {labelize(status)}
                    </option>
                  ))}
                </select>
              </label>
            </div>

            <label className="flex flex-col gap-2">
              <span className="ta-label">Description</span>
              <textarea
                className="ta-input min-h-24"
                value={form.description}
                onChange={(event) => setForm((current) => ({ ...current, description: event.target.value }))}
                placeholder="Optional description for the teacher workspace"
              />
            </label>

            <label className="flex flex-col gap-2">
              <span className="ta-label">Instructions</span>
              <textarea
                className="ta-input min-h-28"
                value={form.instructions}
                onChange={(event) => setForm((current) => ({ ...current, instructions: event.target.value }))}
                placeholder="Teacher-facing instructions. Keep student references anonymous."
              />
            </label>

            <div>
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="ta-label">Standards attachment</p>
                  <p className="mt-1 text-sm text-slate-600">
                    Select the standards that belong to this assignment&apos;s subject context.
                  </p>
                </div>
                <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                  {form.standard_ids.length} selected
                </span>
              </div>

              <div className="mt-4 max-h-64 space-y-2 overflow-auto rounded-2xl border border-slate-200 bg-slate-50 p-3">
                {availableStandards.length === 0 ? (
                  <p className="text-sm text-slate-500">
                    Add subject-matching standards first, then they will appear here.
                  </p>
                ) : (
                  availableStandards.map((standard) => (
                    <label
                      key={standard.id}
                      className="flex cursor-pointer items-start gap-3 rounded-2xl border border-transparent bg-white px-3 py-3 text-sm text-slate-700 shadow-sm shadow-slate-950/5"
                    >
                      <input
                        type="checkbox"
                        checked={form.standard_ids.includes(standard.id)}
                        onChange={() => handleToggleStandard(standard.id)}
                        className="mt-1 h-4 w-4 rounded border-slate-300 text-sky-600 focus:ring-sky-500"
                      />
                      <span>
                        <span className="font-semibold text-slate-900">{standard.code}</span>
                        <span className="mt-1 block text-slate-600">{standard.description}</span>
                      </span>
                    </label>
                  ))
                )}
              </div>
            </div>

            <button
              type="button"
              onClick={() => {
                void handleSaveAssignment();
              }}
              disabled={saving}
              className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
            >
              {saving ? "Saving..." : editingAssignmentId ? "Save Assignment" : "Create Assignment"}
            </button>
          </div>
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Printable QR packets</h2>
              <p className="mt-1 text-sm text-slate-600">
                Generate per-student packets with QR-linked pages using anonymous STUDENT numbers only.
              </p>
            </div>
            {packetAssignment ? (
              <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-800">
                {packetAssignment.title}
              </span>
            ) : null}
          </div>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("printPacket")}
            onDismiss={() => clearSectionAlert("printPacket")}
            className="mt-4"
          />

          {!packetAssignment ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Choose “Generate Printable QR Packet” from an assignment row to open packet tools.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-700">
                <div className="flex flex-wrap gap-x-4 gap-y-2">
                  <span>Class: {classMap[packetAssignment.class_id]?.name ?? "Unknown"}</span>
                  <span>Subject: {subjectMap[packetAssignment.subject_id]?.name ?? "Unknown"}</span>
                  <span>Students: {classMap[packetAssignment.class_id]?.student_count ?? "Unknown"}</span>
                  <span>Assignment type: {labelize(packetAssignment.assignment_type)}</span>
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-3">
                <label className="flex flex-col gap-2">
                  <span className="ta-label">Pages per student</span>
                  <input
                    className="ta-input"
                    type="number"
                    min={1}
                    value={packetForm.pages_per_student}
                    onChange={(event) =>
                      setPacketForm((current) => ({
                        ...current,
                        pages_per_student: Math.max(1, Number(event.target.value) || 1),
                      }))
                    }
                  />
                </label>

                <label className="flex flex-col gap-2">
                  <span className="ta-label">Template type</span>
                  <select
                    className="ta-input"
                    value={packetForm.template_type}
                    onChange={(event) =>
                      setPacketForm((current) => ({
                        ...current,
                        template_type: event.target.value as AssignmentPrintPacket["template_type"],
                      }))
                    }
                  >
                    {(options?.assignment_print_template_types ?? [
                      "blank_writing_page",
                      "lined_writing_page",
                      "short_answer_page",
                    ]).map((templateType) => (
                      <option key={templateType} value={templateType}>
                        {labelize(templateType)}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="flex flex-col gap-2">
                  <span className="ta-label">Output format</span>
                  <select
                    className="ta-input"
                    value={packetForm.output_format}
                    onChange={(event) =>
                      setPacketForm((current) => ({
                        ...current,
                        output_format: event.target.value as AssignmentPrintPacket["output_format"],
                      }))
                    }
                  >
                    {(options?.assignment_print_output_formats ?? ["html"]).map((outputFormat) => (
                      <option key={outputFormat} value={outputFormat}>
                        {labelize(outputFormat)}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <button
                type="button"
                onClick={() => {
                  void handleGeneratePacket();
                }}
                disabled={generatingPacket}
                className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
              >
                {generatingPacket ? "Generating..." : "Generate Printable QR Packet"}
              </button>

              <div>
                <h3 className="text-base font-semibold text-slate-900">Packet summary</h3>
                {packetsLoading ? (
                  <div className="mt-3 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    Loading packet history...
                  </div>
                ) : packets.length === 0 ? (
                  <div className="mt-3 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    No printable packets generated for this assignment yet.
                  </div>
                ) : (
                  <div className="mt-3 space-y-3">
                    {packets.map((packet) => (
                      <PacketPreviewCard
                        key={packet.id}
                        packet={packet}
                        isSelected={packet.id === selectedPacketId}
                        onSelect={setSelectedPacketId}
                      />
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Packet preview</h2>
              <p className="mt-1 text-sm text-slate-600">
                Review page counts and a non-sensitive QR payload sample before printing.
              </p>
            </div>
            {selectedPacket ? (
              <Link
                href={`/teacher-assist/assignments/print-packets?id=${selectedPacket.id}`}
                className="ta-button-secondary"
              >
                Open Printable View
              </Link>
            ) : null}
          </div>

          {!selectedPacket ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Generate or select a packet to preview QR-linked pages.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
              <div className="grid gap-4 sm:grid-cols-3">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-semibold text-slate-500">Packet status</p>
                  <p className="mt-2 text-lg font-semibold text-slate-900">
                    {labelize(selectedPacket.packet_status)}
                  </p>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-semibold text-slate-500">Total pages</p>
                  <p className="mt-2 text-lg font-semibold text-slate-900">
                    {selectedPacket.total_page_count}
                  </p>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-semibold text-slate-500">Generated</p>
                  <p className="mt-2 text-lg font-semibold text-slate-900">
                    {formatDateTime(selectedPacket.created_at)}
                  </p>
                </div>
              </div>

              {packetPagesLoading ? (
                <div className="rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                  Loading packet pages...
                </div>
              ) : (
                <>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                    <p className="text-sm font-semibold text-slate-900">QR payload preview</p>
                    <p className="mt-1 text-sm text-slate-600">
                      The payload uses assignment ids, teacher ids, tenant ids, anonymous student numbers,
                      and packet/page metadata only.
                    </p>
                    <pre className="mt-4 overflow-auto rounded-2xl bg-slate-900 p-4 text-xs text-slate-100">
                      {JSON.stringify(selectedPacketPages[0]?.qr_payload_json ?? {}, null, 2)}
                    </pre>
                  </div>

                  <div>
                    <p className="text-sm font-semibold text-slate-900">First pages</p>
                    <div className="mt-3 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
                      {selectedPacketPages.slice(0, 3).map((page) => (
                        <div key={page.id} className="rounded-3xl border border-slate-200 bg-white p-4">
                          <div className="flex items-start justify-between gap-3">
                            <div>
                              <p className="text-sm font-semibold text-slate-900">
                                STUDENT #{page.student_number}
                              </p>
                              <p className="mt-1 text-xs text-slate-500">Page {page.page_number}</p>
                            </div>
                            <Image
                              src={page.qr_svg_data_uri}
                              alt={`QR code for student ${page.student_number} page ${page.page_number}`}
                              width={96}
                              height={96}
                              unoptimized
                              className="h-24 w-24 rounded-xl border border-slate-200 bg-white p-1"
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                </>
              )}
            </div>
          )}
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
        <article className="ta-panel p-6">
          <div className="flex items-start justify-between gap-3">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Student Work</h2>
              <p className="mt-1 text-sm text-slate-600">
                Upload student work by anonymous STUDENT # and keep review status separate from any
                extraction, grading review, or later AI workflow.
              </p>
            </div>
            {packetAssignment ? (
              <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-800">
                {packetAssignment.title}
              </span>
            ) : null}
          </div>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("studentWork")}
            onDismiss={() => clearSectionAlert("studentWork")}
            className="mt-4"
          />

          {!packetAssignment ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Choose “Open Packet + Student Work Tools” from an assignment row to upload and review
              anonymous student work.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
              {gradingPrepSummary ? (
                <div className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-900">
                  <p className="font-semibold">Grading prep readiness</p>
                  <p className="mt-1">
                    {gradingPrepLoading
                      ? "Refreshing grading prep summary..."
                      : `${gradingPrepSummary.ready_for_grading_prep_count} of ${gradingPrepSummary.total_submissions} submissions are ready for guarded AI grading suggestions.`}
                  </p>
                </div>
              ) : null}

              <div className="grid gap-4 md:grid-cols-2">
                <label className="flex flex-col gap-2">
                  <span className="ta-label">Student number</span>
                  <input
                    className="ta-input"
                    type="number"
                    min={1}
                    max={classMap[packetAssignment.class_id]?.student_count ?? undefined}
                    value={studentWorkForm.student_number}
                    onChange={(event) =>
                      setStudentWorkForm((current) => ({
                        ...current,
                        student_number: Math.max(1, Number(event.target.value) || 1),
                      }))
                    }
                  />
                </label>

                <label className="flex flex-col gap-2">
                  <span className="ta-label">Optional packet context</span>
                  <select
                    className="ta-input"
                    value={studentWorkForm.assignment_print_packet_id}
                    onChange={(event) =>
                      setStudentWorkForm((current) => ({
                        ...current,
                        assignment_print_packet_id: event.target.value,
                      }))
                    }
                  >
                    <option value="">No packet link yet</option>
                    {packets.map((packet) => (
                      <option key={packet.id} value={packet.id}>
                        {labelize(packet.template_type)} · {packet.created_at.slice(0, 10)}
                      </option>
                    ))}
                  </select>
                </label>
              </div>

              <label className="flex flex-col gap-2">
                <span className="ta-label">Upload file</span>
                <input
                  className={fieldErrorInputClass(Boolean(studentWorkFileError))}
                  type="file"
                  onChange={(event) => {
                    setSelectedSubmissionFile(event.target.files?.[0] ?? null);
                    setStudentWorkFileError(null);
                    setStudentWorkUploadError(null);
                  }}
                />
                <TeacherAssistFieldError message={studentWorkFileError} />
              </label>

              {studentWorkUploadError ? (
                <TeacherAssistAlert
                  variant="error"
                  title="Upload failed"
                  description={
                    selectedSubmissionFile
                      ? `${selectedSubmissionFile.name}: ${studentWorkUploadError}`
                      : studentWorkUploadError
                  }
                  actionLabel="Retry upload"
                  onAction={() => {
                    void handleUploadStudentWork();
                  }}
                />
              ) : null}

              <div className="flex flex-wrap items-center gap-3">
                <button
                  type="button"
                  onClick={() => {
                    void handleUploadStudentWork();
                  }}
                  disabled={uploadingStudentWork}
                  className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {uploadingStudentWork ? "Uploading..." : "Upload Student Work"}
                </button>
                {uploadingStudentWork ? (
                  <span className="text-sm text-slate-500">{studentWorkUploadProgress}% uploaded</span>
                ) : null}
              </div>

              <div>
                <h3 className="text-base font-semibold text-slate-900">Submission list</h3>
                {submissionsLoading ? (
                  <div className="mt-3 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    Loading student work...
                  </div>
                ) : submissions.length === 0 ? (
                  <div className="mt-3 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    No student-work submissions uploaded for this assignment yet.
                  </div>
                ) : (
                  <div className="mt-3 space-y-3">
                    {submissions.map((submission) => {
                      const linkedReview = reviewBySubmissionId[submission.id];
                      const gradingPrepItem = gradingPrepBySubmissionId[submission.id];
                      return (
                        <div
                          key={submission.id}
                          className={`rounded-2xl border p-4 transition ${
                            submission.id === selectedSubmissionId
                              ? "border-sky-300 bg-sky-50"
                              : "border-slate-200 bg-white"
                          }`}
                        >
                          <button
                            type="button"
                            onClick={() => setSelectedSubmissionId(submission.id)}
                            className="w-full text-left"
                          >
                            <div className="flex flex-wrap items-center gap-2">
                              <span className="text-sm font-semibold text-slate-900">
                                STUDENT #{submission.student_number}
                              </span>
                              <span className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-semibold text-violet-800">
                                {labelize(submission.processing_status)}
                              </span>
                              <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                                {labelize(submission.upload_status)}
                              </span>
                              <span
                                className={`rounded-full px-2.5 py-1 text-xs font-semibold ${extractionStatusClasses(
                                  submission.latest_extraction_job?.status,
                                )}`}
                              >
                                Extraction: {labelize(submission.latest_extraction_job?.status ?? "not_started")}
                              </span>
                              {linkedReview ? (
                                <span className="rounded-full bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-700">
                                  Review: {labelize(linkedReview.status)}
                                </span>
                              ) : null}
                              {gradingPrepItem?.ready_for_grading_prep ? (
                                <span className="rounded-full bg-teal-50 px-2.5 py-1 text-xs font-semibold text-teal-800">
                                  Ready for grading prep
                                </span>
                              ) : null}
                            </div>
                            <div className="mt-2 flex flex-wrap gap-x-4 gap-y-2 text-sm text-slate-600">
                              <span>{submission.original_filename}</span>
                              <span>{formatFileSize(submission.file_size)}</span>
                              <span>{submission.mime_type}</span>
                            </div>
                            <p className="mt-2 text-xs text-slate-500">
                              {submission.assignment_print_page_id
                                ? "Linked to a packet page"
                                : submission.assignment_print_packet_id
                                  ? "Linked to a packet"
                                  : "No packet/page link yet"}{" "}
                              · Updated {formatDateTime(submission.updated_at)}
                            </p>
                          </button>
                          <div className="mt-3 flex flex-wrap gap-3">
                            <button
                              type="button"
                              onClick={() => {
                                setSelectedSubmissionId(submission.id);
                                void handleStartGradingReview(submission);
                              }}
                              disabled={creatingGradingReview === submission.id}
                              className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              {creatingGradingReview === submission.id
                                ? "Starting..."
                                : linkedReview
                                  ? "Open Grading Review"
                                  : "Start Grading Review"}
                            </button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div className="flex flex-col gap-2">
            <h2 className="text-xl font-semibold text-slate-900">Submission + grading review detail</h2>
            <p className="mt-1 text-sm text-slate-600">
              Manage anonymous upload metadata first, then review work with teacher-confirmed score
              and feedback before any later automation exists.
            </p>
          </div>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("gradingReview")}
            onDismiss={() => clearSectionAlert("gradingReview")}
            className="mt-4"
          />

          {!selectedSubmission || !submissionContextForm ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Select a student-work submission to inspect its metadata, update packet context, or
              open a grading review.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-semibold text-slate-500">Anonymous student</p>
                  <p className="mt-2 text-lg font-semibold text-slate-900">
                    STUDENT #{selectedSubmission.student_number}
                  </p>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <p className="text-sm font-semibold text-slate-500">Uploaded</p>
                  <p className="mt-2 text-lg font-semibold text-slate-900">
                    {formatDateTime(selectedSubmission.created_at)}
                  </p>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <p className="text-sm font-semibold text-slate-900">Upload metadata</p>
                <div className="mt-3 grid gap-2 text-sm text-slate-600">
                  <p>Filename: {selectedSubmission.original_filename}</p>
                  <p>MIME type: {selectedSubmission.mime_type}</p>
                  <p>File size: {formatFileSize(selectedSubmission.file_size)}</p>
                  <p>Stored privately through the TeacherAssist backend.</p>
                </div>
                <button
                  type="button"
                  onClick={() => void handleDownloadSubmission()}
                  disabled={downloadingSubmissionId === selectedSubmission.id}
                  className="mt-4 ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {downloadingSubmissionId === selectedSubmission.id
                    ? "Preparing download..."
                    : "Download Uploaded File"}
                </button>
              </div>

              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="text-sm font-semibold text-slate-900">Extraction</p>
                  <span
                    className={`rounded-full px-2.5 py-1 text-xs font-semibold ${extractionStatusClasses(
                      selectedSubmission.latest_extraction_job?.status,
                    )}`}
                  >
                    {labelize(selectedSubmission.latest_extraction_job?.status ?? "not_started")}
                  </span>
                  {gradingPrepContext?.ready_for_grading_prep ? (
                    <span className="rounded-full bg-teal-50 px-2.5 py-1 text-xs font-semibold text-teal-800">
                      Ready for grading prep
                    </span>
                  ) : null}
                </div>
                <p className="mt-2 text-sm text-slate-600">
                  Extraction reads the uploaded file through private TeacherAssist storage. Approved text
                  unlocks draft AI grading suggestions in the grading review panel below.
                </p>
                {gradingPrepContext?.ready_for_grading_prep ? (
                  <p className="mt-2 rounded-xl bg-teal-50 px-3 py-2 text-sm text-teal-900">
                    {gradingPrepContext.message}
                  </p>
                ) : gradingPrepContext && !gradingPrepContext.ready_for_grading_prep ? (
                  <p className="mt-2 rounded-xl bg-amber-50 px-3 py-2 text-sm text-amber-900">
                    {gradingPrepContext.message}
                  </p>
                ) : null}
                {selectedSubmission.latest_extraction_job?.error_message ? (
                  <p className="mt-3 rounded-xl bg-rose-50 px-3 py-2 text-sm text-rose-700">
                    {selectedSubmission.latest_extraction_job.error_message}
                  </p>
                ) : null}
                {selectedSubmission.latest_extracted_text ? (
                  <div className="mt-3 rounded-xl bg-white px-3 py-3 text-sm text-slate-700">
                    <p className="font-semibold text-slate-900">Extracted text preview</p>
                    <p className="mt-2 leading-6">{selectedSubmission.latest_extracted_text.preview_text}</p>
                    <p className="mt-2 text-xs text-slate-500">
                      Review {labelize(selectedSubmission.latest_extracted_text.review_status)} · Confidence{" "}
                      {labelize(selectedSubmission.latest_extracted_text.confidence_level)}
                    </p>
                    {selectedSubmission.latest_extracted_text.redaction_applied ? (
                      <p className="mt-2 text-xs text-amber-700">
                        Preview was redacted because TeacherAssist flagged potential PII-like content.
                      </p>
                    ) : null}
                  </div>
                ) : null}
                <div className="mt-4 flex flex-wrap gap-3">
                  {selectedSubmission.latest_extracted_text ? (
                    <Link
                      href={`/teacher-assist/extractions?id=${selectedSubmission.latest_extracted_text.id}`}
                      className="ta-button-secondary"
                    >
                      Open extraction review
                    </Link>
                  ) : null}
                  <button
                    type="button"
                    onClick={() => {
                      void handleStartExtraction();
                    }}
                    disabled={
                      startingExtractionId === selectedSubmission.id ||
                      selectedSubmission.latest_extraction_job?.status === "queued" ||
                      selectedSubmission.latest_extraction_job?.status === "running"
                    }
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {startingExtractionId === selectedSubmission.id
                      ? "Queueing..."
                      : selectedSubmission.latest_extraction_job?.status === "queued" ||
                          selectedSubmission.latest_extraction_job?.status === "running"
                        ? "Extraction in progress"
                        : "Start extraction"}
                  </button>
                  {selectedSubmission.latest_extraction_job?.status === "queued" ||
                  selectedSubmission.latest_extraction_job?.status === "running" ? (
                    <button
                      type="button"
                      onClick={() => {
                        void handleCancelExtraction();
                      }}
                      disabled={cancellingExtractionId === selectedSubmission.latest_extraction_job?.id}
                      className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {cancellingExtractionId === selectedSubmission.latest_extraction_job?.id
                        ? "Cancelling..."
                        : "Cancel extraction"}
                    </button>
                  ) : null}
                  <button type="button" disabled className="ta-button-secondary opacity-60">
                    Gradebook sync coming later
                  </button>
                </div>
              </div>

              <div className="grid gap-4 md:grid-cols-[1fr_auto] md:items-end">
                <label className="flex flex-col gap-2">
                  <span className="ta-label">Processing status</span>
                  <select
                    className="ta-input"
                    value={submissionContextForm.processing_status}
                    onChange={(event) =>
                      setSubmissionContextForm((current) =>
                        current
                          ? {
                              ...current,
                              processing_status:
                                event.target.value as AssignmentStudentWorkSubmission["processing_status"],
                            }
                          : current,
                      )
                    }
                  >
                    {(options?.assignment_student_work_processing_statuses ?? [
                      "pending_review",
                      "ready_for_processing",
                      "processing_deferred",
                      "archived",
                    ]).map((status) => (
                      <option key={status} value={status}>
                        {labelize(status)}
                      </option>
                    ))}
                  </select>
                </label>
                <button
                  type="button"
                  onClick={() => {
                    void handleUpdateSubmissionStatus();
                  }}
                  disabled={savingSubmissionStatus}
                  className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {savingSubmissionStatus ? "Saving..." : "Update Student Work Status"}
                </button>
              </div>

              <div className="rounded-2xl border border-slate-200 p-4">
                <p className="text-sm font-semibold text-slate-900">Packet and page context</p>
                <div className="mt-4 grid gap-4 md:grid-cols-2">
                  <label className="flex flex-col gap-2">
                    <span className="ta-label">Print packet</span>
                    <select
                      className="ta-input"
                      value={submissionContextForm.assignment_print_packet_id}
                      onChange={(event) => {
                        const nextPacketId = event.target.value;
                        setSubmissionContextForm((current) =>
                          current
                            ? {
                                ...current,
                                assignment_print_packet_id: nextPacketId,
                                assignment_print_page_id: "",
                              }
                            : current,
                        );
                      }}
                    >
                      <option value="">No packet link</option>
                      {packets.map((packet) => (
                        <option key={packet.id} value={packet.id}>
                          {labelize(packet.template_type)} · {packet.created_at.slice(0, 10)}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="flex flex-col gap-2">
                    <span className="ta-label">Print page</span>
                    <select
                      className="ta-input"
                      value={submissionContextForm.assignment_print_page_id}
                      onChange={(event) =>
                        setSubmissionContextForm((current) =>
                          current
                            ? {
                                ...current,
                                assignment_print_page_id: event.target.value,
                              }
                            : current,
                        )
                      }
                      disabled={!submissionContextForm.assignment_print_packet_id}
                    >
                      <option value="">No page link</option>
                      {selectedSubmissionPages.map((page) => (
                        <option key={page.id} value={page.id}>
                          STUDENT #{page.student_number} · Page {page.page_number}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
                <div className="mt-4 flex flex-wrap items-center gap-3">
                  <button
                    type="button"
                    onClick={() => {
                      void handleUpdateSubmissionContext();
                    }}
                    disabled={savingSubmissionContext}
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {savingSubmissionContext ? "Saving..." : "Save Packet/Page Context"}
                  </button>
                  <p className="text-sm text-slate-500">
                    OCR remains disabled; this only stores anonymous upload metadata and packet/page
                    linkage.
                  </p>
                </div>
              </div>

              <div className="rounded-2xl border border-slate-200 p-4">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                  <div>
                    <p className="text-sm font-semibold text-slate-900">Grading reviews</p>
                    <p className="mt-1 text-sm text-slate-500">
                      Draft AI suggestions use teacher-approved extraction text only. Teachers must review,
                      edit, and manually confirm. No gradebook commit, mastery update, or parent messaging.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => {
                      void handleStartGradingReview(selectedSubmission);
                    }}
                    disabled={creatingGradingReview === selectedSubmission.id}
                    className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {creatingGradingReview === selectedSubmission.id
                      ? "Starting..."
                      : reviewBySubmissionId[selectedSubmission.id]
                        ? "Open Review"
                        : "Start Grading Review"}
                  </button>
                </div>

                {gradingReviewsLoading ? (
                  <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    Loading grading reviews...
                  </div>
                ) : gradingReviews.length === 0 ? (
                  <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    No grading reviews created for this assignment yet.
                  </div>
                ) : (
                  <div className="mt-4 space-y-3">
                    {gradingReviews.map((review) => (
                      <button
                        key={review.id}
                        type="button"
                        onClick={() => setSelectedGradingReviewId(review.id)}
                        className={`w-full rounded-2xl border p-4 text-left transition ${
                          review.id === selectedGradingReviewId
                            ? "border-sky-300 bg-sky-50"
                            : "border-slate-200 bg-white hover:border-sky-200 hover:bg-sky-50/40"
                        }`}
                      >
                        <div className="flex flex-wrap items-center gap-2">
                          <span className="text-sm font-semibold text-slate-900">
                            STUDENT #{review.student_number}
                          </span>
                          <span className="rounded-full bg-emerald-50 px-2.5 py-1 text-xs font-semibold text-emerald-700">
                            {labelize(review.status)}
                          </span>
                          <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                            {labelize(review.review_source)}
                          </span>
                        </div>
                        <p className="mt-2 text-sm text-slate-600">
                          Submission:{" "}
                          {submissions.find((submission) => submission.id === review.student_work_submission_id)
                            ?.original_filename ?? "Unknown"}
                        </p>
                        <p className="mt-1 text-xs text-slate-500">
                          Provider: {review.provider_name ?? "None"} · Model: {review.provider_model ?? "None"} ·
                          Cost: $0.00
                        </p>
                      </button>
                    ))}
                  </div>
                )}

                {!selectedGradingReview ? (
                  <div className="mt-4 rounded-2xl border border-dashed border-slate-200 px-4 py-4 text-sm text-slate-500">
                    Select or create a grading review to edit score, feedback, strengths, and teacher
                    confirmation fields.
                  </div>
                ) : (
                  <div className="mt-5 space-y-4">
                    {gradingPrepContext?.ready_for_grading_prep ? (
                      <div className="rounded-2xl border border-sky-200 bg-sky-50 p-4">
                        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                          <div>
                            <p className="text-sm font-semibold text-sky-950">Guarded AI grading assist</p>
                            <p className="mt-1 text-sm text-sky-900">
                              Generate a draft score and feedback suggestion from teacher-approved extraction
                              text. You must review and confirm manually.
                            </p>
                            {aiSuggestionMeta?.teacher_review_required ||
                            selectedGradingReview.status === "ai_suggested" ? (
                              <p className="mt-2 rounded-xl bg-amber-50 px-3 py-2 text-sm text-amber-900">
                                Teacher review required — this suggestion is a draft only.
                              </p>
                            ) : null}
                          </div>
                          <button
                            type="button"
                            onClick={() => {
                              void handleGenerateAISuggestion();
                            }}
                            disabled={
                              generatingAISuggestion || selectedGradingReview.status === "teacher_confirmed"
                            }
                            className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {generatingAISuggestion ? "Generating..." : "Generate AI Suggestion"}
                          </button>
                        </div>
                        {aiSuggestionMeta ? (
                          <p className="mt-3 text-sm text-sky-900">
                            {aiSuggestionMeta.message} Confidence: {labelize(aiSuggestionMeta.confidence_level)}.
                            {aiSuggestionMeta.text_source
                              ? ` Source: ${labelize(aiSuggestionMeta.text_source)}.`
                              : ""}
                          </p>
                        ) : null}
                      </div>
                    ) : (
                      <div className="rounded-2xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
                        <p className="font-semibold">AI suggestion blocked</p>
                        <p className="mt-1">
                          {gradingPrepContext?.message ??
                            "Approve or correct extracted text before requesting AI grading suggestions."}
                        </p>
                      </div>
                    )}

                    <div className="grid gap-4 md:grid-cols-2">
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Review status</span>
                        <select
                          className="ta-input"
                          value={gradingReviewForm.status}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              status: event.target.value as AssignmentGradingReview["status"],
                            }))
                          }
                        >
                          {(options?.assignment_grading_review_statuses ?? [
                            "draft",
                            "ai_suggested",
                            "teacher_reviewing",
                            "teacher_confirmed",
                            "returned_for_revision",
                            "archived",
                          ]).map((status) => (
                            <option key={status} value={status}>
                              {labelize(status)}
                            </option>
                          ))}
                        </select>
                      </label>
                      <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
                        <p>Provider: {selectedGradingReview.provider_name ?? "None"}</p>
                        <p>Model: {selectedGradingReview.provider_model ?? "None"}</p>
                        <p>Prompt version: {selectedGradingReview.prompt_version ?? "None"}</p>
                        <p>Usage event: {selectedGradingReview.ai_usage_event_id ?? "None"}</p>
                      </div>
                    </div>

                    <div className="grid gap-4 md:grid-cols-2">
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Score suggestion</span>
                        <input
                          className="ta-input"
                          value={gradingReviewForm.score_suggestion}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              score_suggestion: event.target.value,
                            }))
                          }
                          placeholder="Optional manual score suggestion"
                        />
                      </label>
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Max score</span>
                        <input
                          className="ta-input"
                          value={gradingReviewForm.max_score}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              max_score: event.target.value,
                            }))
                          }
                          placeholder="Optional max points"
                        />
                      </label>
                    </div>

                    <label className="flex flex-col gap-2">
                      <span className="ta-label">Feedback summary</span>
                      <textarea
                        className="ta-input min-h-24"
                        value={gradingReviewForm.feedback_summary}
                        onChange={(event) =>
                          setGradingReviewForm((current) => ({
                            ...current,
                            feedback_summary: event.target.value,
                          }))
                        }
                        placeholder="Teacher-facing summary without student names or emails"
                      />
                    </label>

                    <div className="grid gap-4 md:grid-cols-2">
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Strengths</span>
                        <textarea
                          className="ta-input min-h-28"
                          value={gradingReviewForm.strengths}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              strengths: event.target.value,
                            }))
                          }
                          placeholder="One strength per line"
                        />
                      </label>
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Improvement areas</span>
                        <textarea
                          className="ta-input min-h-28"
                          value={gradingReviewForm.improvement_areas}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              improvement_areas: event.target.value,
                            }))
                          }
                          placeholder="One improvement area per line"
                        />
                      </label>
                    </div>

                    <label className="flex flex-col gap-2">
                      <span className="ta-label">Teacher notes</span>
                      <textarea
                        className="ta-input min-h-24"
                        value={gradingReviewForm.teacher_notes}
                        onChange={(event) =>
                          setGradingReviewForm((current) => ({
                            ...current,
                            teacher_notes: event.target.value,
                          }))
                        }
                        placeholder="Private teacher review notes"
                      />
                    </label>

                    <div className="grid gap-4 md:grid-cols-2">
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Teacher confirmed score</span>
                        <input
                          className="ta-input"
                          value={gradingReviewForm.teacher_confirmed_score}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              teacher_confirmed_score: event.target.value,
                            }))
                          }
                          placeholder="Required before teacher_confirmed if no confirmed feedback"
                        />
                      </label>
                      <label className="flex flex-col gap-2">
                        <span className="ta-label">Teacher confirmed feedback</span>
                        <textarea
                          className="ta-input min-h-24"
                          value={gradingReviewForm.teacher_confirmed_feedback}
                          onChange={(event) =>
                            setGradingReviewForm((current) => ({
                              ...current,
                              teacher_confirmed_feedback: event.target.value,
                            }))
                          }
                          placeholder="Required before teacher_confirmed if no confirmed score"
                        />
                      </label>
                    </div>

                    <div className="flex flex-wrap gap-3">
                      <button
                        type="button"
                        onClick={() => {
                          void handleSaveGradingReview();
                        }}
                        disabled={savingGradingReview}
                        className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {savingGradingReview ? "Saving..." : "Save Review"}
                      </button>
                      <button
                        type="button"
                        onClick={() => {
                          void handleUpdateGradingReviewStatus();
                        }}
                        disabled={savingGradingReviewStatus}
                        className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        {savingGradingReviewStatus ? "Updating..." : "Update Review Status"}
                      </button>
                      {selectedGradingReview.status === "teacher_confirmed" ? (
                        activeGradeRecordByReviewId[selectedGradingReview.id] ? (
                          <Link href="/teacher-assist/gradebook" className="ta-button-secondary">
                            View Gradebook Commit
                          </Link>
                        ) : (
                          <button
                            type="button"
                            onClick={() => {
                              void handleCommitToGradebook();
                            }}
                            disabled={committingGradebook || gradeRecordsLoading}
                            className="ta-button-secondary disabled:cursor-not-allowed disabled:opacity-60"
                          >
                            {committingGradebook ? "Committing..." : "Commit to Gradebook"}
                          </button>
                        )
                      ) : null}
                    </div>
                    {selectedGradingReview.status === "teacher_confirmed" &&
                    activeGradeRecordByReviewId[selectedGradingReview.id] ? (
                      <p className="rounded-xl bg-emerald-50 px-3 py-2 text-sm text-emerald-900">
                        Grade committed for STUDENT #{selectedGradingReview.student_number}. Open Gradebook for
                        corrections, reversals, and export views.
                      </p>
                    ) : selectedGradingReview.status === "teacher_confirmed" ? (
                      <p className="rounded-xl bg-sky-50 px-3 py-2 text-sm text-sky-900">
                        Review is teacher-confirmed. Commit to gradebook manually when ready — no automatic
                        grade commits occur.
                      </p>
                    ) : null}
                  </div>
                )}
              </div>
            </div>
          )}
        </article>
      </section>

      <section className="ta-panel p-6">
        <h2 className="text-xl font-semibold text-slate-900">Coming later</h2>
        <p className="mt-1 text-sm text-slate-600">
          These downstream workflows still remain intentionally disabled in this phase.
        </p>
        <div className="mt-5 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {PLACEHOLDER_ACTIONS.map((label) => (
            <div key={label} className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <p className="text-sm font-semibold text-slate-900">{label}</p>
              <p className="mt-2 text-sm text-slate-500">Coming later</p>
              <button
                type="button"
                disabled
                className="mt-4 inline-flex h-11 items-center rounded-2xl border border-slate-200 bg-white px-4 text-sm font-semibold text-slate-400"
              >
                Disabled
              </button>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
