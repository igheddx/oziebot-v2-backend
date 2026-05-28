"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import {
  createLinkResource,
  fetchResources,
  uploadResourceFile,
} from "@/lib/teacher-assist-api";
import type { ResourceLibraryItem } from "@/lib/teacher-assist-types";

type UploadEntry = {
  id: string;
  name: string;
  progress: number;
  status: "uploading" | "done" | "error";
  message?: string;
};

type LinkForm = {
  title: string;
  description: string;
  external_url: string;
};

function formatDate(value: string) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString();
}

export function TeacherAssistResourcesScreen() {
  const [resources, setResources] = useState<ResourceLibraryItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [uploads, setUploads] = useState<UploadEntry[]>([]);
  const [linkForm, setLinkForm] = useState<LinkForm>({
    title: "",
    description: "",
    external_url: "",
  });
  const [dragActive, setDragActive] = useState(false);
  const [savingLink, setSavingLink] = useState(false);
  const fileInputRef = useRef<HTMLInputElement | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      setResources(await fetchResources());
    } catch (nextError) {
      setError(nextError instanceof Error ? nextError.message : "Could not load resources.");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const totalLinkedCount = useMemo(
    () =>
      resources.reduce(
        (sum, resource) =>
          sum + resource.linked_pacing_items_count + resource.linked_planning_drafts_count,
        0,
      ),
    [resources],
  );

  const handleFiles = useCallback(
    async (files: File[]) => {
      setError(null);
      setNotice(null);
      for (const file of files) {
        const entryId = `${file.name}-${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
        setUploads((current) => [
          { id: entryId, name: file.name, progress: 0, status: "uploading" },
          ...current,
        ]);
        try {
          await uploadResourceFile(file, {}, (progress) => {
            setUploads((current) =>
              current.map((entry) =>
                entry.id === entryId ? { ...entry, progress, status: "uploading" } : entry,
              ),
            );
          });
          setUploads((current) =>
            current.map((entry) =>
              entry.id === entryId ? { ...entry, progress: 100, status: "done" } : entry,
            ),
          );
          await load();
          setNotice(`Uploaded ${file.name}.`);
        } catch (nextError) {
          setUploads((current) =>
            current.map((entry) =>
              entry.id === entryId
                ? {
                    ...entry,
                    status: "error",
                    message: nextError instanceof Error ? nextError.message : "Upload failed.",
                  }
                : entry,
            ),
          );
          setError(nextError instanceof Error ? nextError.message : "Upload failed.");
        }
      }
    },
    [load],
  );

  return (
    <div className="space-y-6">
      <section className="ta-panel p-6 sm:p-8">
        <div className="max-w-3xl">
          <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-700">
            TeacherAssist Resources
          </p>
          <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-900 sm:text-4xl">
            Resource library foundation
          </h1>
          <p className="mt-3 text-base leading-7 text-slate-600">
            Save curriculum files and links as reusable metadata-first assets. TeacherAssist stores
            file references and metadata only in this phase—no OCR, extraction, embeddings, or AI
            generation yet.
          </p>
        </div>
      </section>

      {error ? <section className="ta-alert ta-alert-error">{error}</section> : null}
      {notice ? <section className="ta-alert ta-alert-success">{notice}</section> : null}

      <section className="grid gap-4 lg:grid-cols-3">
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Saved resources</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading ? "..." : resources.length}
          </p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Files vs links</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">
            {loading ? "..." : `${resources.filter((row) => row.external_url).length} links`}
          </p>
        </article>
        <article className="ta-panel p-5">
          <p className="text-sm font-semibold text-slate-500">Current link usage</p>
          <p className="mt-3 text-3xl font-semibold text-slate-900">{loading ? "..." : totalLinkedCount}</p>
        </article>
      </section>

      <section className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
        <article className="ta-panel p-6">
          <div className="flex flex-col gap-2">
            <h2 className="text-xl font-semibold text-slate-900">Upload curriculum files</h2>
            <p className="text-sm text-slate-600">
              Drag files here or browse to upload PDFs, presentations, docs, spreadsheets, images,
              and other supporting materials. Metadata and local storage references are saved now.
            </p>
          </div>

          <button
            type="button"
            onClick={() => fileInputRef.current?.click()}
            onDragEnter={(event) => {
              event.preventDefault();
              setDragActive(true);
            }}
            onDragOver={(event) => {
              event.preventDefault();
              setDragActive(true);
            }}
            onDragLeave={(event) => {
              event.preventDefault();
              setDragActive(false);
            }}
            onDrop={(event) => {
              event.preventDefault();
              setDragActive(false);
              const files = Array.from(event.dataTransfer.files);
              if (files.length > 0) {
                void handleFiles(files);
              }
            }}
            className={`mt-5 flex min-h-48 w-full flex-col items-center justify-center rounded-3xl border border-dashed px-6 py-10 text-center transition ${
              dragActive
                ? "border-sky-400 bg-sky-50"
                : "border-slate-300 bg-slate-50 hover:border-sky-300 hover:bg-sky-50/50"
            }`}
          >
            <span className="text-base font-semibold text-slate-900">
              Drag and drop curriculum files here
            </span>
            <span className="mt-2 text-sm leading-6 text-slate-600">
              Or click to browse. Uploaded files become reusable resource-library items for pacing
              guides and planning drafts.
            </span>
          </button>
          <input
            ref={fileInputRef}
            type="file"
            multiple
            className="hidden"
            onChange={(event) => {
              const files = event.target.files ? Array.from(event.target.files) : [];
              if (files.length > 0) {
                void handleFiles(files);
              }
              event.target.value = "";
            }}
          />

          {uploads.length > 0 ? (
            <div className="mt-5 space-y-3">
              {uploads.map((upload) => (
                <div key={upload.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <p className="text-sm font-semibold text-slate-900">{upload.name}</p>
                      <p className="mt-1 text-xs text-slate-500">
                        {upload.status === "uploading"
                          ? `Uploading... ${upload.progress}%`
                          : upload.status === "done"
                            ? "Upload complete"
                            : upload.message ?? "Upload failed"}
                      </p>
                    </div>
                    <span
                      className={`rounded-full px-3 py-1 text-xs font-semibold ${
                        upload.status === "done"
                          ? "bg-emerald-100 text-emerald-700"
                          : upload.status === "error"
                            ? "bg-rose-100 text-rose-700"
                            : "bg-sky-100 text-sky-700"
                      }`}
                    >
                      {upload.status}
                    </span>
                  </div>
                  <div className="mt-3 h-2 rounded-full bg-slate-100">
                    <div
                      className={`h-2 rounded-full ${
                        upload.status === "error" ? "bg-rose-400" : "bg-sky-500"
                      }`}
                      style={{ width: `${upload.status === "error" ? 100 : upload.progress}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          ) : null}
        </article>

        <article className="ta-panel p-6">
          <h2 className="text-xl font-semibold text-slate-900">Save a URL resource</h2>
          <p className="mt-1 text-sm text-slate-600">
            Capture district links, curriculum portals, or online references without uploading files.
          </p>
          <form
            className="mt-5 space-y-4"
            onSubmit={(event) => {
              event.preventDefault();
              setSavingLink(true);
              setError(null);
              setNotice(null);
              void createLinkResource(linkForm)
                .then(async () => {
                  setLinkForm({ title: "", description: "", external_url: "" });
                  setNotice("Link resource saved.");
                  await load();
                })
                .catch((nextError) => {
                  setError(nextError instanceof Error ? nextError.message : "Could not save link.");
                })
                .finally(() => {
                  setSavingLink(false);
                });
            }}
          >
            <label className="space-y-2">
              <span className="ta-label">Title</span>
              <input
                value={linkForm.title}
                onChange={(event) => setLinkForm((current) => ({ ...current, title: event.target.value }))}
                className="ta-input"
                placeholder="District pacing portal"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">URL</span>
              <input
                value={linkForm.external_url}
                onChange={(event) =>
                  setLinkForm((current) => ({ ...current, external_url: event.target.value }))
                }
                className="ta-input"
                placeholder="https://example.com/resource"
              />
            </label>
            <label className="space-y-2">
              <span className="ta-label">Description</span>
              <textarea
                value={linkForm.description}
                onChange={(event) =>
                  setLinkForm((current) => ({ ...current, description: event.target.value }))
                }
                className="ta-input min-h-28"
                placeholder="How should this resource be used later?"
              />
            </label>
            <button type="submit" disabled={savingLink} className="ta-button-primary">
              {savingLink ? "Saving..." : "Save link resource"}
            </button>
          </form>
        </article>
      </section>

      <section className="ta-panel p-6">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-slate-900">Resource library</h2>
            <p className="mt-1 text-sm text-slate-600">
              Files and links are reusable assets. They are stored independently and linked into
              pacing items or planning drafts as needed.
            </p>
          </div>
        </div>

        {loading ? (
          <p className="mt-5 text-sm text-slate-600">Loading resources...</p>
        ) : resources.length === 0 ? (
          <div className="mt-5 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-4 text-sm text-sky-900">
            Upload curriculum resources or save curriculum URLs first. No extraction or AI processing
            runs yet in this phase.
          </div>
        ) : (
          <div className="mt-5 grid gap-3 lg:grid-cols-2">
            {resources.map((resource) => (
              <article key={resource.id} className="rounded-2xl border border-slate-200 bg-white p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <p className="text-base font-semibold text-slate-900">{resource.title}</p>
                    <p className="mt-2 text-sm leading-6 text-slate-600">
                      {resource.description || "No description saved yet."}
                    </p>
                  </div>
                  <span className="rounded-full bg-slate-100 px-3 py-1 text-xs font-semibold text-slate-700">
                    {resource.resource_type}
                  </span>
                </div>
                <dl className="mt-4 grid gap-2 text-sm text-slate-600 sm:grid-cols-2">
                  <div>
                    <dt className="font-semibold text-slate-900">Filename</dt>
                    <dd>{resource.original_filename ?? "External link"}</dd>
                  </div>
                  <div>
                    <dt className="font-semibold text-slate-900">Uploaded</dt>
                    <dd>{formatDate(resource.uploaded_at)}</dd>
                  </div>
                  <div>
                    <dt className="font-semibold text-slate-900">Linked pacing items</dt>
                    <dd>{resource.linked_pacing_items_count}</dd>
                  </div>
                  <div>
                    <dt className="font-semibold text-slate-900">Linked planning drafts</dt>
                    <dd>{resource.linked_planning_drafts_count}</dd>
                  </div>
                </dl>
                {resource.external_url ? (
                  <a
                    href={resource.external_url}
                    target="_blank"
                    rel="noreferrer"
                    className="mt-4 inline-flex text-sm font-semibold text-sky-700 hover:text-sky-600"
                  >
                    Open saved link
                  </a>
                ) : null}
              </article>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
