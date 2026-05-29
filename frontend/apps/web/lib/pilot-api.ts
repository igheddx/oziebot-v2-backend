import { readJson } from "@/lib/api-client";

export async function fetchPilotCompletionReview() {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/pilot/completion-review");
}

export async function fetchPilotFeedback(mineOnly = true) {
  const query = mineOnly ? "?mine_only=true" : "?mine_only=false";
  return readJson<Array<Record<string, unknown>>>(`/v1/teacher-assist/pilot/feedback${query}`);
}

export async function createPilotFeedback(body: {
  category: string;
  severity: string;
  feature_area: string;
  description: string;
  requested_improvement?: string;
}) {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/pilot/feedback", {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function fetchPilotUsageMetrics(days = 30) {
  return readJson<Record<string, unknown>>(`/v1/teacher-assist/pilot/usage-metrics?days=${days}`);
}

export async function recordPilotLoginMetric() {
  return readJson<void>("/v1/teacher-assist/pilot/usage-metrics/login", { method: "POST" });
}

export async function fetchSystemHealth() {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/pilot/system-health");
}

export async function fetchSeedValidation() {
  return readJson<Record<string, unknown>>("/v1/teacher-assist/pilot/seed-validation");
}
