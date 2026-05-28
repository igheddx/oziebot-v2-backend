"use client";

import Image from "next/image";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  createAssignment,
  createAssignmentPrintPacket,
  fetchAssignmentStudentWork,
  fetchAssignmentPrintPacketPages,
  fetchAssignmentPrintPackets,
  fetchAssignments,
  fetchClasses,
  fetchGradingPeriods,
  fetchSchoolYears,
  fetchStandards,
  fetchSubjects,
  fetchTeacherAssistOptions,
  updateAssignmentStudentWorkPacketContext,
  updateAssignmentStudentWorkStatus,
  updateAssignment,
  updateAssignmentStatus,
  uploadAssignmentStudentWork,
} from "@/lib/teacher-assist-api";
import type {
  Assignment,
  AssignmentInput,
  AssignmentPrintPacket,
  AssignmentPrintPage,
  AssignmentStudentWorkSubmission,
  GradingPeriod,
  SchoolYear,
  Standard,
  Subject,
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

const PLACEHOLDER_ACTIONS = [
  "Start Grading Review",
  "Update Mastery Matrix",
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

function labelize(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
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
  const [studentWorkForm, setStudentWorkForm] = useState<StudentWorkUploadForm>(emptyStudentWorkUploadForm);
  const [selectedSubmissionId, setSelectedSubmissionId] = useState<string | null>(null);
  const [selectedSubmissionFile, setSelectedSubmissionFile] = useState<File | null>(null);
  const [submissionContextForm, setSubmissionContextForm] = useState<SubmissionContextForm | null>(null);
  const [loading, setLoading] = useState(true);
  const [packetsLoading, setPacketsLoading] = useState(false);
  const [packetPagesLoading, setPacketPagesLoading] = useState(false);
  const [submissionsLoading, setSubmissionsLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [generatingPacket, setGeneratingPacket] = useState(false);
  const [uploadingStudentWork, setUploadingStudentWork] = useState(false);
  const [studentWorkUploadProgress, setStudentWorkUploadProgress] = useState(0);
  const [savingSubmissionStatus, setSavingSubmissionStatus] = useState(false);
  const [savingSubmissionContext, setSavingSubmissionContext] = useState(false);
  const [updatingStatusId, setUpdatingStatusId] = useState<string | null>(null);
  const [statusDrafts, setStatusDrafts] = useState<Record<string, Assignment["status"]>>({});
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const load = useCallback(async (currentFilters: Filters) => {
    setLoading(true);
    setError(null);
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
      }
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load assignments.");
    } finally {
      setLoading(false);
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
      setError(nextError instanceof Error ? nextError.message : "Could not load printable packets.");
      setPackets([]);
      setSelectedPacketId(null);
    } finally {
      setPacketsLoading(false);
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
      setError(nextError instanceof Error ? nextError.message : "Could not load student work.");
      setSubmissions([]);
      setSelectedSubmissionId(null);
    } finally {
      setSubmissionsLoading(false);
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
      return;
    }
    void loadPackets(packetAssignmentId);
    void loadStudentWork(packetAssignmentId);
  }, [loadPackets, loadStudentWork, packetAssignmentId]);

  useEffect(() => {
    if (!selectedPacketId) {
      setSelectedPacketPages([]);
      return;
    }
    setPacketPagesLoading(true);
    setError(null);
    void fetchAssignmentPrintPacketPages(selectedPacketId)
      .then((pages) => {
        setSelectedPacketPages(pages);
      })
      .catch((nextError) => {
        setError(nextError instanceof Error ? nextError.message : "Could not load packet pages.");
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
    setNotice(null);
    setError(null);
  }, []);

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
    setNotice(null);
    setError(null);
  }, []);

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
    setError(null);
    setNotice(null);
    try {
      const payload = toAssignmentInput(form);
      const saved = editingAssignmentId
        ? await updateAssignment(editingAssignmentId, payload)
        : await createAssignment(payload);
      await load(filters);
      setEditingAssignmentId(saved.id);
      setForm(formFromAssignment(saved));
      setNotice(editingAssignmentId ? "Assignment updated." : "Assignment created.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not save assignment.");
    } finally {
      setSaving(false);
    }
  }, [editingAssignmentId, filters, form, load]);

  const handleStatusUpdate = useCallback(
    async (assignmentId: string) => {
      const nextStatus = statusDrafts[assignmentId];
      if (!nextStatus) return;
      setUpdatingStatusId(assignmentId);
      setError(null);
      setNotice(null);
      try {
        await updateAssignmentStatus(assignmentId, nextStatus);
        await load(filters);
        setNotice("Assignment status updated.");
      } catch (nextError) {
        setError(nextError instanceof Error ? nextError.message : "Could not update assignment status.");
      } finally {
        setUpdatingStatusId(null);
      }
    },
    [filters, load, statusDrafts],
  );

  const handleGeneratePacket = useCallback(async () => {
    if (!packetAssignmentId) return;
    setGeneratingPacket(true);
    setError(null);
    setNotice(null);
    try {
      const created = await createAssignmentPrintPacket(packetAssignmentId, packetForm);
      await loadPackets(packetAssignmentId);
      setSelectedPacketId(created.id);
      setNotice("Printable QR packet generated.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not generate printable packet.");
    } finally {
      setGeneratingPacket(false);
    }
  }, [loadPackets, packetAssignmentId, packetForm]);

  const handleUploadStudentWork = useCallback(async () => {
    if (!packetAssignmentId || !selectedSubmissionFile) {
      setError("Choose a file before uploading student work.");
      return;
    }
    setUploadingStudentWork(true);
    setStudentWorkUploadProgress(0);
    setError(null);
    setNotice(null);
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
      setNotice("Student work uploaded.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not upload student work.");
    } finally {
      setUploadingStudentWork(false);
      setStudentWorkUploadProgress(0);
    }
  }, [loadStudentWork, packetAssignmentId, selectedSubmissionFile, studentWorkForm]);

  const handleUpdateSubmissionStatus = useCallback(async () => {
    if (!selectedSubmission || !submissionContextForm) return;
    setSavingSubmissionStatus(true);
    setError(null);
    setNotice(null);
    try {
      await updateAssignmentStudentWorkStatus(selectedSubmission.id, submissionContextForm.processing_status);
      if (packetAssignmentId) {
        await loadStudentWork(packetAssignmentId);
      }
      setNotice("Student work status updated.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not update student work status.");
    } finally {
      setSavingSubmissionStatus(false);
    }
  }, [loadStudentWork, packetAssignmentId, selectedSubmission, submissionContextForm]);

  const handleUpdateSubmissionContext = useCallback(async () => {
    if (!selectedSubmission || !submissionContextForm) return;
    setSavingSubmissionContext(true);
    setError(null);
    setNotice(null);
    try {
      await updateAssignmentStudentWorkPacketContext(selectedSubmission.id, {
        assignment_print_packet_id: submissionContextForm.assignment_print_packet_id || null,
        assignment_print_page_id: submissionContextForm.assignment_print_page_id || null,
      });
      if (packetAssignmentId) {
        await loadStudentWork(packetAssignmentId);
      }
      setNotice("Student work packet context updated.");
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not update packet context.");
    } finally {
      setSavingSubmissionContext(false);
    }
  }, [loadStudentWork, packetAssignmentId, selectedSubmission, submissionContextForm]);

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

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-4xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Assignments
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Assignment packets and student-work intake foundation
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Create, edit, organize, and move assignments through a teacher-review lifecycle with
            class, subject, standards, printable QR packet context, and anonymous student-work
            uploads. OCR, grading review, and mastery updates remain intentionally deferred.
          </p>
        </div>
      </section>

      <section className="ta-alert ta-alert-info">
        Assignment packets and student-work intake are software-only in this phase. No provider
        call, OCR, grading automation, mastery update, or trading behavior is involved here.
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {notice ? <section className="ta-alert ta-alert-success">{notice}</section> : null}

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
                future OCR or grading workflow.
              </p>
            </div>
            {packetAssignment ? (
              <span className="rounded-full bg-sky-50 px-3 py-1 text-xs font-semibold text-sky-800">
                {packetAssignment.title}
              </span>
            ) : null}
          </div>

          {!packetAssignment ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Choose “Open Packet + Student Work Tools” from an assignment row to upload and review
              anonymous student work.
            </div>
          ) : (
            <div className="mt-5 space-y-5">
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
                  className="ta-input"
                  type="file"
                  onChange={(event) => setSelectedSubmissionFile(event.target.files?.[0] ?? null)}
                />
              </label>

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
                    {submissions.map((submission) => (
                      <button
                        key={submission.id}
                        type="button"
                        onClick={() => setSelectedSubmissionId(submission.id)}
                        className={`w-full rounded-2xl border p-4 text-left transition ${
                          submission.id === selectedSubmissionId
                            ? "border-sky-300 bg-sky-50"
                            : "border-slate-200 bg-white hover:border-sky-200 hover:bg-sky-50/40"
                        }`}
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
                    ))}
                  </div>
                )}
              </div>
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <div className="flex flex-col gap-2">
            <h2 className="text-xl font-semibold text-slate-900">Submission detail</h2>
            <p className="mt-1 text-sm text-slate-600">
              Keep uploads anonymous, set review state, and optionally attach packet/page context
              when printable packet metadata is available.
            </p>
          </div>

          {!selectedSubmission || !submissionContextForm ? (
            <div className="mt-5 rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
              Select a student-work submission to inspect its metadata and update status or packet
              context.
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
                  <p>Storage key: {selectedSubmission.storage_key}</p>
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
                    OCR and grading remain disabled; this only stores anonymous upload metadata and
                    packet/page linkage.
                  </p>
                </div>
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
