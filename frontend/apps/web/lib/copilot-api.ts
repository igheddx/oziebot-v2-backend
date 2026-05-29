import { readJson } from "@/lib/api-client";

export async function fetchCopilotSuggestedQuestions() {
  return readJson<{ questions: string[] }>("/v1/teacher-assist/copilot/suggested-questions");
}

export async function fetchCopilotContext(instructionalWeekId?: string) {
  const query = instructionalWeekId ? `?instructional_week_id=${instructionalWeekId}` : "";
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/copilot/context${query}`);
}

export async function createCopilotSession(title?: string) {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/copilot/sessions", {
    method: "POST",
    body: JSON.stringify({ title: title ?? null }),
  });
}

export async function fetchCopilotSessions() {
  return readJson<Array<Record<string, unknown>>>("/v1/teacher-assist/copilot/sessions");
}

export async function fetchCopilotMessages(sessionId: string) {
  return readJson<Array<Record<string, unknown>>>(`/v1/teacher-assist/copilot/sessions/${sessionId}/messages`);
}

export async function sendCopilotMessage(
  sessionId: string,
  body: { question: string; provider_mode?: string; instructional_week_id?: string },
) {
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/copilot/sessions/${sessionId}/messages`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}
