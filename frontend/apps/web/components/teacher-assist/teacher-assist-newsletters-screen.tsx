"use client";

import { useRouter, useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { fetchPeriodLaunchContext } from "@/lib/pacing-guide-api";

import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  createNewsletter,
  createNewsletterExport,
  createNewsletterVersion,
  fetchClasses,
  fetchNewsletter,
  fetchNewsletterExportDownload,
  fetchNewsletterVersions,
  fetchNewsletters,
  fetchSchoolYears,
  fetchSubjects,
  fetchTeacherAssistOptions,
  generateNewsletterAIDraft,
  regenerateNewsletterSection,
  updateNewsletter,
} from "@/lib/teacher-assist-api";
import type {
  Newsletter,
  NewsletterContent,
  NewsletterExportFormat,
  NewsletterRegeneratableSection,
  NewsletterVersion,
  SchoolYear,
  Subject,
  TeacherAssistOptions,
  TeacherClass,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function ContentSection({ title, items }: { title: string; items: string[] | undefined }) {
  if (!items || items.length === 0) return null;
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-4">
      <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-slate-700">
        {items.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </section>
  );
}

const REGEN_SECTIONS: Array<{ key: NewsletterRegeneratableSection; label: string }> = [
  { key: "overview", label: "Overview" },
  { key: "upcoming_learning", label: "Upcoming learning" },
  { key: "teacher_message", label: "Teacher message" },
  { key: "reminders", label: "Reminders" },
];

export function TeacherAssistNewslettersScreen() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const requestedId = searchParams.get("id");
  const pacingPeriodId = searchParams.get("pacing_period_id");
  const pacingPrefillRef = useRef<string | null>(null);
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();

  const [options, setOptions] = useState<TeacherAssistOptions | null>(null);
  const [schoolYears, setSchoolYears] = useState<SchoolYear[]>([]);
  const [classes, setClasses] = useState<TeacherClass[]>([]);
  const [subjects, setSubjects] = useState<Subject[]>([]);
  const [newsletters, setNewsletters] = useState<Newsletter[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(requestedId);
  const [newsletter, setNewsletter] = useState<Newsletter | null>(null);
  const [versions, setVersions] = useState<NewsletterVersion[]>([]);
  const [editContent, setEditContent] = useState<NewsletterContent | null>(null);
  const [teacherInstructions, setTeacherInstructions] = useState("");
  const [teacherNotes, setTeacherNotes] = useState("");
  const [changeReason, setChangeReason] = useState("");
  const [createForm, setCreateForm] = useState({
    school_year_id: "",
    class_id: "",
    subject_id: "",
    title: "",
  });
  const [pageError, setPageError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const selectedVersion = useMemo(() => versions.at(-1) ?? null, [versions]);

  const loadLists = useCallback(async () => {
    const [nextOptions, nextSchoolYears, nextClasses, nextSubjects, nextNewsletters] = await Promise.all([
      fetchTeacherAssistOptions(),
      fetchSchoolYears(),
      fetchClasses(),
      fetchSubjects(),
      fetchNewsletters(),
    ]);
    setOptions(nextOptions);
    setSchoolYears(nextSchoolYears);
    setClasses(nextClasses);
    setSubjects(nextSubjects);
    setNewsletters(nextNewsletters);
    if (!selectedId && nextNewsletters.length > 0) {
      setSelectedId(nextNewsletters[0].id);
    }
  }, [selectedId]);

  const loadDetail = useCallback(async (newsletterId: string) => {
    const [row, versionRows] = await Promise.all([
      fetchNewsletter(newsletterId),
      fetchNewsletterVersions(newsletterId),
    ]);
    setNewsletter(row);
    setVersions(versionRows);
    const current = versionRows.find((item) => item.id === row.current_version_id) ?? versionRows.at(-1) ?? null;
    setEditContent(current?.content_json ?? null);
    setTeacherNotes(row.teacher_notes ?? "");
  }, []);

  useEffect(() => {
    loadLists().catch((error: Error) => setPageError(error.message));
  }, [loadLists]);

  useEffect(() => {
    if (!pacingPeriodId || pacingPrefillRef.current === pacingPeriodId) {
      return;
    }
    pacingPrefillRef.current = pacingPeriodId;
    void fetchPeriodLaunchContext(pacingPeriodId)
      .then((context) => {
        const newsletter = (context.newsletter ?? {}) as Record<string, unknown>;
        setCreateForm((current) => ({
          ...current,
          school_year_id: String(newsletter.school_year_id ?? current.school_year_id),
          subject_id: String(newsletter.subject_id ?? current.subject_id),
          title: String(newsletter.title ?? current.title),
        }));
        if (typeof newsletter.notes === "string" && newsletter.notes.trim()) {
          setTeacherNotes(newsletter.notes);
        }
        setSectionAlert(
          "createDraft",
          sectionSuccess(
            `Pre-filled from pacing week${context.period_title ? `: ${context.period_title}` : "."}`,
            "Newsletter pre-filled",
          ),
        );
      })
      .catch((error: Error) => {
        pacingPrefillRef.current = null;
        setSectionAlert(
          "createDraft",
          sectionError(error.message || "Could not pre-fill newsletter from pacing week.", "Unable to pre-fill newsletter"),
        );
      });
  }, [pacingPeriodId, setSectionAlert]);

  useEffect(() => {
    if (requestedId) setSelectedId(requestedId);
  }, [requestedId]);

  useEffect(() => {
    if (!selectedId) {
      setNewsletter(null);
      setVersions([]);
      setEditContent(null);
      return;
    }
    loadDetail(selectedId).catch((error: Error) => setPageError(error.message));
  }, [loadDetail, selectedId]);

  async function handleCreate() {
    if (!createForm.school_year_id || !createForm.class_id || !createForm.subject_id) {
      setSectionAlert("createDraft", {
        type: "error",
        title: "Unable to create newsletter",
        description: "School year, class, and subject are required.",
      });
      return;
    }
    setBusy(true);
    clearSectionAlert("createDraft");
    try {
      const created = await createNewsletter({
        school_year_id: createForm.school_year_id,
        class_id: createForm.class_id,
        subject_id: createForm.subject_id,
        title: createForm.title.trim() || undefined,
      });
      await loadLists();
      setSelectedId(created.id);
      router.replace(`/teacher-assist/newsletters?id=${created.id}`);
      setSectionAlert(
        "createDraft",
        sectionSuccess("Newsletter draft created.", "Draft created"),
      );
    } catch (error) {
      setSectionAlert(
        "createDraft",
        sectionError(error instanceof Error ? error.message : "Could not create newsletter.", "Unable to create newsletter"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleGenerateDraft() {
    if (!newsletter) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      const payload = await generateNewsletterAIDraft(newsletter.id, {
        provider_mode: "mock",
        teacher_instructions: teacherInstructions.trim() || undefined,
      });
      setSectionAlert("newsletterEditor", sectionSuccess(payload.message, "AI draft generated"));
      await loadDetail(newsletter.id);
      await loadLists();
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not generate AI draft.", "AI draft failed"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleRegenerateSection(section: NewsletterRegeneratableSection) {
    if (!newsletter) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      const payload = await regenerateNewsletterSection(newsletter.id, {
        section,
        provider_mode: "mock",
        teacher_instructions: teacherInstructions.trim() || undefined,
      });
      setSectionAlert("newsletterEditor", sectionSuccess(payload.message, "Section regenerated"));
      await loadDetail(newsletter.id);
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not regenerate section.", "Regeneration failed"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveTeacherVersion() {
    if (!newsletter || !editContent) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      await createNewsletterVersion(newsletter.id, {
        content_json: { ...editContent, teacher_review_required: true },
        change_reason: changeReason.trim() || "Teacher reviewed newsletter draft.",
      });
      setSectionAlert(
        "newsletterEditor",
        sectionSuccess("Teacher-reviewed version saved. TeacherAssist does not send messages.", "Version saved"),
      );
      await loadDetail(newsletter.id);
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not save teacher version.", "Unable to save"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleApprove() {
    if (!newsletter) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      await updateNewsletter(newsletter.id, { status: "approved" });
      setSectionAlert(
        "newsletterEditor",
        sectionSuccess("Newsletter marked approved for your export/send workflow.", "Newsletter approved"),
      );
      await loadDetail(newsletter.id);
      await loadLists();
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not approve newsletter.", "Unable to approve"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleExport(format: NewsletterExportFormat) {
    if (!newsletter) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      const exportRow = await createNewsletterExport(newsletter.id, format);
      const download = await fetchNewsletterExportDownload(newsletter.id, exportRow.id);
      window.open(download.download_url, "_blank", "noopener,noreferrer");
      setSectionAlert(
        "newsletterEditor",
        sectionSuccess(`Exported ${format.toUpperCase()} for teacher-controlled distribution.`, "Export ready"),
      );
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not export newsletter.", "Export failed"),
      );
    } finally {
      setBusy(false);
    }
  }

  async function handleSaveNotes() {
    if (!newsletter) return;
    setBusy(true);
    clearSectionAlert("newsletterEditor");
    try {
      await updateNewsletter(newsletter.id, { teacher_notes: teacherNotes });
      setSectionAlert("newsletterEditor", sectionSuccess("Teacher notes saved.", "Notes saved"));
      await loadDetail(newsletter.id);
    } catch (error) {
      setSectionAlert(
        "newsletterEditor",
        sectionError(error instanceof Error ? error.message : "Could not save notes.", "Unable to save notes"),
      );
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="space-y-8">
      <header className="space-y-2">
        <p className="text-sm font-semibold uppercase tracking-wide text-sky-700">TeacherAssist · Communication</p>
        <h1 className="text-3xl font-semibold text-slate-900">Weekly Newsletters</h1>
        <p className="max-w-3xl text-sm text-slate-600">
          Generate parent newsletter drafts from lesson plans and instructional activity. You review and export —
          TeacherAssist never sends messages automatically. No student names, grades, or behavior comments in AI context.
        </p>
      </header>

      <TeacherAssistFormErrorSummary title="Unable to load newsletters" message={pageError} />

      <section className="grid gap-6 xl:grid-cols-[18rem_minmax(0,1fr)]">
        <aside className="space-y-4">
          <article className="ta-panel p-4">
            <h2 className="text-sm font-semibold text-slate-900">Create draft</h2>
            <TeacherAssistInlineAlert
              alert={getSectionAlert("createDraft")}
              onDismiss={() => clearSectionAlert("createDraft")}
              className="mt-3"
            />
            <div className="mt-3 space-y-2">
              <select
                className="ta-input"
                value={createForm.school_year_id}
                onChange={(event) =>
                  setCreateForm((current) => ({ ...current, school_year_id: event.target.value }))
                }
              >
                <option value="">School year</option>
                {schoolYears.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.title}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={createForm.class_id}
                onChange={(event) => setCreateForm((current) => ({ ...current, class_id: event.target.value }))}
              >
                <option value="">Class</option>
                {classes.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <select
                className="ta-input"
                value={createForm.subject_id}
                onChange={(event) => setCreateForm((current) => ({ ...current, subject_id: event.target.value }))}
              >
                <option value="">Subject</option>
                {subjects.map((row) => (
                  <option key={row.id} value={row.id}>
                    {row.name}
                  </option>
                ))}
              </select>
              <input
                className="ta-input"
                placeholder="Optional title"
                value={createForm.title}
                onChange={(event) => setCreateForm((current) => ({ ...current, title: event.target.value }))}
              />
              <button type="button" className="ta-button w-full" disabled={busy} onClick={handleCreate}>
                Create newsletter
              </button>
            </div>
          </article>

          <article className="ta-panel p-4">
            <h2 className="text-sm font-semibold text-slate-900">Your newsletters</h2>
            {newsletters.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">No newsletters yet.</p>
            ) : (
              <ul className="mt-3 space-y-2">
                {newsletters.map((row) => (
                  <li key={row.id}>
                    <button
                      type="button"
                      onClick={() => {
                        setSelectedId(row.id);
                        router.replace(`/teacher-assist/newsletters?id=${row.id}`);
                      }}
                      className={`w-full rounded-xl border px-3 py-2 text-left text-sm ${
                        row.id === selectedId
                          ? "border-sky-300 bg-sky-50"
                          : "border-slate-200 bg-white hover:bg-slate-50"
                      }`}
                    >
                      <p className="font-semibold text-slate-900">{row.title}</p>
                      <p className="mt-1 text-xs text-slate-500">{labelize(row.status)}</p>
                    </button>
                  </li>
                ))}
              </ul>
            )}
          </article>
        </aside>

        <div className="space-y-6">
          {!newsletter ? (
            <article className="ta-panel p-6 text-sm text-slate-500">Select or create a newsletter to begin.</article>
          ) : (
            <>
              <article className="ta-panel p-6">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-900">{newsletter.title}</h2>
                    <p className="mt-1 text-sm text-slate-600">
                      {newsletter.class_name ?? "Class"} · {newsletter.subject_name ?? "Subject"}
                    </p>
                  </div>
                  <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                    {labelize(newsletter.status)}
                  </span>
                </div>
                <TeacherAssistInlineAlert
                  alert={getSectionAlert("newsletterEditor")}
                  onDismiss={() => clearSectionAlert("newsletterEditor")}
                  className="mt-4"
                />
                <div className="mt-4 flex flex-wrap gap-2">
                  {newsletter.status !== "approved" && newsletter.status !== "archived" ? (
                    <button type="button" className="ta-button-secondary" disabled={busy} onClick={handleApprove}>
                      Mark approved
                    </button>
                  ) : null}
                  {(options?.newsletter_export_formats ?? ["html", "pdf", "docx"]).map((format) => (
                    <button
                      key={format}
                      type="button"
                      className="ta-button-secondary"
                      disabled={busy || !editContent}
                      onClick={() => handleExport(format as NewsletterExportFormat)}
                    >
                      Export {format.toUpperCase()}
                    </button>
                  ))}
                </div>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-lg font-semibold text-slate-900">Teacher notes (input context)</h2>
                <textarea
                  className="ta-input mt-3 min-h-20"
                  value={teacherNotes}
                  onChange={(event) => setTeacherNotes(event.target.value)}
                />
                <button type="button" className="ta-button-secondary mt-3" disabled={busy} onClick={handleSaveNotes}>
                  Save notes
                </button>
              </article>

              <article className="ta-panel p-6">
                <h2 className="text-lg font-semibold text-slate-900">Generate AI draft</h2>
                <textarea
                  className="ta-input mt-3 min-h-20"
                  placeholder="Optional instructions for the draft"
                  value={teacherInstructions}
                  onChange={(event) => setTeacherInstructions(event.target.value)}
                />
                <button
                  type="button"
                  className="ta-button mt-3"
                  disabled={busy || newsletter.status === "archived"}
                  onClick={handleGenerateDraft}
                >
                  Generate draft
                </button>
                <div className="mt-4 flex flex-wrap gap-2">
                  {REGEN_SECTIONS.map((section) => (
                    <button
                      key={section.key}
                      type="button"
                      className="ta-button-secondary"
                      disabled={busy || newsletter.status === "archived"}
                      onClick={() => handleRegenerateSection(section.key)}
                    >
                      Regenerate {section.label}
                    </button>
                  ))}
                </div>
              </article>

              {selectedVersion && editContent ? (
                <>
                  <p className="text-sm text-slate-500">
                    Version v{selectedVersion.version_number} · {labelize(selectedVersion.version_source)}
                  </p>
                  {editContent.overview ? (
                    <article className="ta-panel p-4">
                      <h3 className="text-sm font-semibold text-slate-900">Overview</h3>
                      <p className="mt-2 text-sm text-slate-700">{editContent.overview}</p>
                    </article>
                  ) : null}
                  <div className="grid gap-4 md:grid-cols-2">
                    <ContentSection title="What we learned" items={editContent.what_we_learned} />
                    <ContentSection title="Standards covered" items={editContent.standards_covered} />
                    <ContentSection title="Upcoming topics" items={editContent.upcoming_topics} />
                    <ContentSection title="Reminders" items={editContent.reminders} />
                    <ContentSection title="Celebration highlights" items={editContent.celebration_highlights} />
                  </div>
                  {editContent.teacher_message ? (
                    <article className="ta-panel p-4">
                      <h3 className="text-sm font-semibold text-slate-900">Teacher message</h3>
                      <p className="mt-2 text-sm text-slate-700">{editContent.teacher_message}</p>
                    </article>
                  ) : null}
                  <article className="ta-panel p-6">
                    <h2 className="text-lg font-semibold text-slate-900">Save teacher-reviewed version</h2>
                    <textarea
                      className="ta-input mt-3 min-h-20"
                      placeholder="Change reason (optional)"
                      value={changeReason}
                      onChange={(event) => setChangeReason(event.target.value)}
                    />
                    <button
                      type="button"
                      className="ta-button mt-3"
                      disabled={busy || newsletter.status === "archived"}
                      onClick={handleSaveTeacherVersion}
                    >
                      Save teacher version
                    </button>
                  </article>
                </>
              ) : (
                <article className="ta-panel p-6 text-sm text-slate-500">
                  Generate an AI draft to review newsletter sections.
                </article>
              )}
            </>
          )}
        </div>
      </section>
    </div>
  );
}
