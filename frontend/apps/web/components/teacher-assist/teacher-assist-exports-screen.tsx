"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import { TeacherAssistFormErrorSummary } from "@/components/teacher-assist/teacher-assist-form-error-summary";
import {
  TeacherAssistInlineAlert,
  sectionError,
  sectionSuccess,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  fetchExportArtifactDetail,
  fetchExportArtifactDownload,
  fetchExportArtifacts,
} from "@/lib/teacher-assist-api";
import type {
  TeacherAssistExportArtifact,
  TeacherAssistExportArtifactDetail,
} from "@/lib/teacher-assist-types";

function labelize(value: string) {
  return value
    .replaceAll("_", " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatDateTime(value: string | null | undefined) {
  if (!value) return "Unknown";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function statusClasses(status: string | null | undefined) {
  switch (status) {
    case "ready":
      return "bg-emerald-50 text-emerald-800";
    case "failed":
      return "bg-rose-50 text-rose-800";
    case "generating":
      return "bg-sky-50 text-sky-800";
    case "queued":
      return "bg-amber-50 text-amber-800";
    default:
      return "bg-slate-100 text-slate-700";
  }
}

export function TeacherAssistExportsScreen() {
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [exportsList, setExportsList] = useState<TeacherAssistExportArtifact[]>([]);
  const [selectedExportId, setSelectedExportId] = useState<string | null>(null);
  const [detail, setDetail] = useState<TeacherAssistExportArtifactDetail | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [downloadingId, setDownloadingId] = useState<string | null>(null);
  const [pageError, setPageError] = useState<string | null>(null);

  const loadExports = useCallback(async () => {
    setLoading(true);
    try {
      const rows = await fetchExportArtifacts({ limit: 100 });
      setExportsList(rows);
      setSelectedExportId((current) => {
        if (current && rows.some((row) => row.id === current)) return current;
        return rows[0]?.id ?? null;
      });
      setPageError(null);
    } catch (nextError) {
      setPageError(nextError instanceof Error ? nextError.message : "Could not load exports.");
      setExportsList([]);
      setSelectedExportId(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadExports();
  }, [loadExports]);

  useEffect(() => {
    if (!selectedExportId) {
      setDetail(null);
      return;
    }
    let cancelled = false;
    setDetailLoading(true);
    void fetchExportArtifactDetail(selectedExportId)
      .then((payload) => {
        if (!cancelled) setDetail(payload);
      })
      .catch((nextError) => {
        if (!cancelled) {
          setSectionAlert(
            "exportDetail",
            sectionError(
              nextError instanceof Error ? nextError.message : "Could not load export detail.",
              "Unable to load export detail",
            ),
          );
          setDetail(null);
        }
      })
      .finally(() => {
        if (!cancelled) setDetailLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedExportId, setSectionAlert]);

  const summaryCards = useMemo(
    () => [
      { label: "Ready", value: exportsList.filter((item) => item.artifact_status === "ready").length },
      { label: "Queued", value: exportsList.filter((item) => item.artifact_status === "queued").length },
      {
        label: "Generating",
        value: exportsList.filter((item) => item.artifact_status === "generating").length,
      },
      { label: "Failed", value: exportsList.filter((item) => item.artifact_status === "failed").length },
    ],
    [exportsList],
  );

  const handleDownload = useCallback(async (exportId: string) => {
    setDownloadingId(exportId);
    clearSectionAlert("exportDetail");
    try {
      const payload = await fetchExportArtifactDownload(exportId);
      window.open(payload.download_url, "_blank", "noopener,noreferrer");
      setSectionAlert(
        "exportDetail",
        sectionSuccess(
          `Download started for ${payload.filename}. Open in PowerPoint or import into Google Slides manually.`,
          "Download started",
        ),
      );
    } catch (nextError) {
      setSectionAlert(
        "exportDetail",
        sectionError(
          nextError instanceof Error ? nextError.message : "Could not prepare export download.",
          "Download failed",
        ),
      );
    } finally {
      setDownloadingId(null);
    }
  }, [clearSectionAlert, setSectionAlert]);

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">Export Workspace</p>
            <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900">Instructional exports</h1>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-600">
              Review async slide and quiz exports generated from weekly plans. Downloads stay private and
              teacher-controlled — no Google OAuth, auto publishing, or LMS sync in this phase.
            </p>
          </div>
          <div className="flex flex-wrap gap-3">
            <button type="button" className="ta-button-secondary" onClick={() => void loadExports()}>
              Refresh
            </button>
            <Link href="/teacher-assist/weekly-planning/plans" className="ta-button-primary">
              Open plans
            </Link>
          </div>
        </div>
      </section>

      <TeacherAssistFormErrorSummary title="Unable to load exports" message={pageError} />

      <section className="grid gap-4 md:grid-cols-4">
        {summaryCards.map((card) => (
          <article key={card.label} className="ta-panel p-4">
            <p className="text-sm font-semibold text-slate-500">{card.label}</p>
            <p className="mt-2 text-2xl font-semibold text-slate-900">{card.value}</p>
          </article>
        ))}
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Export history</h2>
          {loading ? (
            <p className="mt-4 text-sm text-slate-600">Loading exports...</p>
          ) : exportsList.length === 0 ? (
            <p className="mt-4 text-sm text-slate-600">
              No exports yet. Generate slides or quizzes from a weekly plan to populate this workspace.
            </p>
          ) : (
            <div className="mt-4 space-y-3">
              {exportsList.map((item) => (
                <button
                  key={item.id}
                  type="button"
                  onClick={() => setSelectedExportId(item.id)}
                  className={`w-full rounded-2xl border p-4 text-left transition ${
                    item.id === selectedExportId
                      ? "border-sky-300 bg-sky-50"
                      : "border-slate-200 bg-white hover:border-slate-300"
                  }`}
                >
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-semibold text-slate-900">{item.title}</span>
                    <span className={`rounded-full px-2.5 py-1 text-xs font-semibold ${statusClasses(item.artifact_status)}`}>
                      {labelize(item.artifact_status)}
                    </span>
                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-semibold text-slate-700">
                      {labelize(item.artifact_type)}
                    </span>
                    <span className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-semibold text-violet-800">
                      {item.export_format.toUpperCase()}
                    </span>
                  </div>
                  <p className="mt-2 text-xs text-slate-500">Updated {formatDateTime(item.updated_at)}</p>
                </button>
              ))}
            </div>
          )}
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Export detail</h2>
          <TeacherAssistInlineAlert
            alert={getSectionAlert("exportDetail")}
            onDismiss={() => clearSectionAlert("exportDetail")}
            className="mt-4"
          />
          {detailLoading ? (
            <p className="mt-4 text-sm text-slate-600">Loading export detail...</p>
          ) : !detail ? (
            <p className="mt-4 text-sm text-slate-600">Select an export to review workflow status and preview metadata.</p>
          ) : (
            <div className="mt-4 space-y-4 text-sm text-slate-700">
              <div className="flex flex-wrap gap-2">
                <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(detail.artifact.artifact_status)}`}>
                  Artifact {labelize(detail.artifact.artifact_status)}
                </span>
                {detail.workflow_status ? (
                  <span className={`rounded-full px-3 py-1 text-xs font-semibold ${statusClasses(detail.workflow_status)}`}>
                    Workflow {labelize(detail.workflow_status)}
                  </span>
                ) : null}
              </div>
              <p>
                Provider {detail.artifact.provider_name ?? "mock"} · Model {detail.artifact.provider_model ?? "Unknown"}
              </p>
              {detail.workflow_error_message ? (
                <p className="rounded-2xl border border-rose-200 bg-rose-50 p-3 text-rose-800">
                  {detail.workflow_error_message}
                </p>
              ) : null}
              {detail.artifact.preview_json?.slides ? (
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                  <p className="font-semibold text-slate-900">Slide preview</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {(detail.artifact.preview_json.slides as Array<{ title?: string }>).length} slides generated
                  </p>
                </div>
              ) : null}
              {Array.isArray(detail.artifact.preview_json?.questions) ? (
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                  <p className="font-semibold text-slate-900">Quiz preview</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {(detail.artifact.preview_json.questions as unknown[]).length} questions generated
                  </p>
                </div>
              ) : null}
              <div className="flex flex-wrap gap-3">
                {detail.artifact.artifact_status === "ready" ? (
                  <button
                    type="button"
                    className="ta-button-primary disabled:cursor-not-allowed disabled:opacity-60"
                    disabled={downloadingId === detail.artifact.id}
                    onClick={() => void handleDownload(detail.artifact.id)}
                  >
                    {downloadingId === detail.artifact.id ? "Preparing download..." : "Download export"}
                  </button>
                ) : detail.artifact.artifact_status === "failed" ? (
                  <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                    Retry placeholder — re-queue from plan viewer
                  </span>
                ) : (
                  <span className="text-xs text-slate-500">Export is still processing asynchronously.</span>
                )}
              </div>
            </div>
          )}
        </article>
      </section>
    </div>
  );
}
