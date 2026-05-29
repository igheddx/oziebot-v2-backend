"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useCallback, useEffect, useMemo, useState } from "react";

import {
  TeacherAssistInlineAlert,
  sectionError,
  useTeacherAssistSectionAlerts,
} from "@/components/teacher-assist/teacher-assist-inline-alert";
import {
  createCopilotSession,
  fetchCopilotContext,
  fetchCopilotMessages,
  fetchCopilotSuggestedQuestions,
  sendCopilotMessage,
} from "@/lib/copilot-api";

type MessageRow = {
  id?: string;
  role?: string;
  content?: string;
  analysis?: {
    why?: string;
    evidence?: Array<{ type?: string; payload?: Record<string, unknown> }>;
    recommendations?: Array<{ label?: string; navigation_href?: string }>;
    draft_groups?: Array<{ title?: string; suggested_activities?: string[] }>;
    confidence?: string;
  };
};

export function TeacherAssistCopilotScreen() {
  const searchParams = useSearchParams();
  const initialPrompt = searchParams.get("prompt");
  const { setSectionAlert, clearSectionAlert, getSectionAlert } = useTeacherAssistSectionAlerts();
  const [loading, setLoading] = useState(true);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<MessageRow[]>([]);
  const [questions, setQuestions] = useState<string[]>([]);
  const [contextKeys, setContextKeys] = useState<string[]>([]);
  const [draft, setDraft] = useState(initialPrompt ?? "");
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    setLoading(true);
    clearSectionAlert("copilot");
    try {
      const [questionPayload, contextPayload, sessionPayload] = await Promise.all([
        fetchCopilotSuggestedQuestions(),
        fetchCopilotContext(),
        createCopilotSession("Teacher Copilot"),
      ]);
      setQuestions(questionPayload.questions ?? []);
      setContextKeys(Object.keys((contextPayload.context_packets as Record<string, unknown>) ?? {}));
      setSessionId(String(sessionPayload.id));
      setMessages([]);
    } catch (nextError) {
      setSectionAlert(
        "copilot",
        sectionError(nextError instanceof Error ? nextError.message : "Could not load copilot.", "Load failed"),
      );
    } finally {
      setLoading(false);
    }
  }, [clearSectionAlert, setSectionAlert]);

  useEffect(() => {
    void refresh();
  }, [refresh]);

  const submitQuestion = useCallback(
    async (question: string) => {
      if (!sessionId || !question.trim()) return;
      setBusy(true);
      clearSectionAlert("copilot");
      try {
        await sendCopilotMessage(sessionId, { question: question.trim(), provider_mode: "mock" });
        const rows = await fetchCopilotMessages(sessionId);
        setMessages(rows as MessageRow[]);
        setDraft("");
      } catch (nextError) {
        setSectionAlert(
          "copilot",
          sectionError(nextError instanceof Error ? nextError.message : "Could not send message.", "Send failed"),
        );
      } finally {
        setBusy(false);
      }
    },
    [clearSectionAlert, sessionId, setSectionAlert],
  );

  useEffect(() => {
    if (!loading && initialPrompt && sessionId && messages.length === 0) {
      void submitQuestion(initialPrompt);
    }
  }, [initialPrompt, loading, messages.length, sessionId, submitQuestion]);

  const latestAnalysis = useMemo(() => {
    const assistant = [...messages].reverse().find((row) => row.role === "assistant");
    return assistant?.analysis;
  }, [messages]);

  return (
    <div className="space-y-4">
      <header className="ta-panel p-4">
        <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Teacher Copilot</p>
        <h1 className="mt-1 text-2xl font-semibold text-slate-900">Instructional coach</h1>
        <p className="mt-1 text-sm text-slate-600">
          Ask questions grounded in your week, objectives, mastery, and reteach data. Recommendations only — you stay in control.
        </p>
        <div className="mt-3 flex flex-wrap gap-1.5">
          {contextKeys.map((key) => (
            <span key={key} className="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-semibold text-slate-600">
              {key.replaceAll("_", " ")}
            </span>
          ))}
        </div>
      </header>

      <TeacherAssistInlineAlert alert={getSectionAlert("copilot")} onDismiss={() => clearSectionAlert("copilot")} />

      {loading ? (
        <p className="text-sm text-slate-600">Loading copilot...</p>
      ) : (
        <section className="grid gap-4 xl:grid-cols-[1.1fr_0.9fr]">
          <article className="ta-panel flex min-h-[420px] flex-col p-4">
            <h2 className="text-base font-semibold text-slate-900">Conversation</h2>
            <div className="mt-3 flex-1 space-y-3 overflow-y-auto">
              {messages.length === 0 ? (
                <p className="text-sm text-slate-500">Ask a question or pick a starter prompt below.</p>
              ) : (
                messages.map((row, index) => (
                  <div
                    key={row.id ?? `${row.role}-${index}`}
                    className={`rounded-xl border px-3 py-2.5 text-sm ${
                      row.role === "assistant" ? "border-sky-100 bg-sky-50" : "border-slate-200 bg-white"
                    }`}
                  >
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">{row.role}</p>
                    <p className="mt-1 whitespace-pre-wrap text-slate-800">{row.content}</p>
                  </div>
                ))
              )}
            </div>
            <form
              className="mt-3 flex gap-2"
              onSubmit={(event) => {
                event.preventDefault();
                void submitQuestion(draft);
              }}
            >
              <input
                className="ta-input flex-1"
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                placeholder="Ask about objectives, students, or this week..."
              />
              <button type="submit" className="ta-button-primary" disabled={busy || !draft.trim()}>
                {busy ? "Thinking..." : "Ask"}
              </button>
            </form>
            <div className="mt-3 flex flex-wrap gap-2">
              {questions.slice(0, 8).map((question) => (
                <button
                  key={question}
                  type="button"
                  className="ta-button-secondary text-xs"
                  disabled={busy}
                  onClick={() => void submitQuestion(question)}
                >
                  {question}
                </button>
              ))}
            </div>
          </article>

          <article className="ta-panel space-y-3 p-4">
            <h2 className="text-base font-semibold text-slate-900">Evidence & recommendations</h2>
            {!latestAnalysis ? (
              <p className="text-sm text-slate-500">Analysis evidence appears after your first question.</p>
            ) : (
              <>
                <div className="rounded-xl border border-slate-200 px-3 py-2.5 text-sm">
                  <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Why</p>
                  <p className="mt-1 text-slate-700">{latestAnalysis.why}</p>
                  <p className="mt-2 text-xs text-slate-500">Confidence: {latestAnalysis.confidence ?? "—"}</p>
                </div>
                {(latestAnalysis.evidence ?? []).length > 0 ? (
                  <div className="space-y-2">
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Evidence</p>
                    {(latestAnalysis.evidence ?? []).slice(0, 5).map((row, index) => (
                      <div key={`${row.type}-${index}`} className="rounded-lg border border-slate-100 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                        {row.type}: {JSON.stringify(row.payload).slice(0, 160)}
                      </div>
                    ))}
                  </div>
                ) : null}
                {(latestAnalysis.recommendations ?? []).length > 0 ? (
                  <div className="flex flex-wrap gap-2">
                    {(latestAnalysis.recommendations ?? []).map((row, index) =>
                      row.navigation_href ? (
                        <Link key={`${row.label}-${index}`} href={row.navigation_href} className="ta-button-secondary text-xs">
                          {row.label}
                        </Link>
                      ) : null,
                    )}
                  </div>
                ) : null}
                {(latestAnalysis.draft_groups ?? []).length > 0 ? (
                  <div className="space-y-2">
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Draft groups (confirm before saving)</p>
                    {(latestAnalysis.draft_groups ?? []).map((row, index) => (
                      <div key={`${row.title}-${index}`} className="rounded-lg border border-amber-100 bg-amber-50 px-3 py-2 text-sm">
                        <p className="font-semibold text-amber-900">{row.title}</p>
                        <p className="text-xs text-amber-800">{(row.suggested_activities ?? []).join(" · ")}</p>
                      </div>
                    ))}
                  </div>
                ) : null}
              </>
            )}
            <Link href="/teacher-assist/reteach" className="ta-button-secondary inline-flex text-xs">
              Open reteach workspace
            </Link>
          </article>
        </section>
      )}
    </div>
  );
}
