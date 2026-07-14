"""Shared JSON completion helper for TeacherAssist (OpenAI and Gemini OpenAI-compatible)."""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any

import httpx

from oziebot_api.config import Settings
from oziebot_api.services.teacher_assist.ai_provider import TeacherAssistAIProviderResult
from oziebot_api.services.teacher_assist.openai_pricing import estimate_openai_cost_cents

# Retry delays (seconds) for rate-limit (429) responses.
_RATE_LIMIT_RETRY_DELAYS = (5.0, 15.0, 30.0)


class _TokenBucket:
    """Thread-safe token bucket that enforces a maximum requests-per-minute rate."""

    def __init__(self, rate_per_minute: float) -> None:
        self._rate = rate_per_minute / 60.0  # tokens per second
        self._capacity = rate_per_minute      # max burst = 1 full minute of quota
        self._tokens = float(rate_per_minute) # start full so first requests aren't delayed
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block the calling thread until a request token is available."""
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity,
                    self._tokens + (now - self._last) * self._rate,
                )
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            time.sleep(min(wait, 1.0))


# Proactive rate limiter shared across all threads.
# Default 500 RPM — well below Gemini paid-tier limit (4000 RPM) but high enough
# that it never throttles natural throughput (5 workers × 15s/call ≈ 20 RPM).
# Acts purely as a safety net against runaway bursts.
# Set TEACHER_ASSIST_AI_RATE_LIMIT_RPM=0 in .env to disable entirely.
_rpm = float(os.environ.get("TEACHER_ASSIST_AI_RATE_LIMIT_RPM", "500"))
_proactive_limiter: _TokenBucket | None = _TokenBucket(_rpm) if _rpm > 0 else None


def execute_openai_json_completion(
    settings: Settings,
    *,
    model_name: str,
    instruction: str,
    prompt_payload: dict[str, Any],
    required_output_schema: dict[str, Any],
    system_prompt: str | None = None,
    timeout_seconds: float = 90.0,
    _api_key: str | None = None,
    _base_url: str | None = None,
    _provider: str = "openai",
) -> TeacherAssistAIProviderResult:
    """Call an OpenAI-compatible chat completions endpoint and return structured JSON.

    Pass _api_key / _base_url / _provider to use Gemini or any other
    OpenAI-compatible provider instead of the default OpenAI settings.
    """
    api_key = (_api_key or "").strip() or (settings.teacher_assist_openai_api_key or "").strip()
    base_url = (_base_url or "").strip() or settings.teacher_assist_openai_base_url
    if not api_key:
        raise RuntimeError(f"TeacherAssist {_provider} API key is not configured")

    request_body = {
        "model": model_name,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system",
                "content": system_prompt
                or (
                    "You generate teacher-ready instructional content as structured JSON only. "
                    "Your response MUST be a single JSON object {{ }} — never a JSON array [ ]. "
                    "Never include markdown, prose outside JSON, or personally identifying information."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "instruction": instruction,
                        "prompt_payload": prompt_payload,
                        "required_output_schema": required_output_schema,
                    },
                    default=str,
                ),
            },
        ],
        "temperature": 0.2,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    url = f"{base_url.rstrip('/')}/chat/completions"

    last_error: Exception | None = None
    # Up to 3 connection retries, each with its own 429 retry loop.
    for attempt in range(3):
        try:
            with httpx.Client(timeout=timeout_seconds) as client:
                # Retry on 429 rate-limit with back-off before giving up.
                for rate_delay in (*_RATE_LIMIT_RETRY_DELAYS, None):
                    if _proactive_limiter is not None:
                        _proactive_limiter.acquire()
                    response = client.post(url, headers=headers, json=request_body)
                    if response.status_code in (429, 503) and rate_delay is not None:
                        # Fast-fail for permanent billing errors — retrying will never work.
                        _body = response.text.lower()
                        if "credits are depleted" in _body or "prepayment credits" in _body or "billing" in _body:
                            response.raise_for_status()
                        time.sleep(rate_delay)
                        continue
                    response.raise_for_status()
                    break
                payload = response.json()
            break
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError) as exc:
            last_error = exc
            if attempt == 2:
                raise ValueError(
                    f"TeacherAssist could not reach the {_provider} API. "
                    "Check the configured model, base URL, API key, and outbound network access."
                ) from exc
            time.sleep(1.0 + attempt)
        except httpx.HTTPStatusError as exc:
            detail = exc.response.text.strip()
            raise ValueError(
                f"TeacherAssist {_provider} request failed ({exc.response.status_code}). "
                f"{detail or 'Check the provider configuration and try again.'}"
            ) from exc
    else:
        if last_error is not None:
            raise last_error

    choices = payload.get("choices") or []
    if not choices:
        raise RuntimeError(f"TeacherAssist {_provider} provider returned no choices")
    message = choices[0].get("message") or {}
    raw_content = message.get("content")
    if isinstance(raw_content, list):
        raw_content = "".join(entry.get("text", "") for entry in raw_content if isinstance(entry, dict))
    if not isinstance(raw_content, str) or not raw_content.strip():
        raise ValueError(f"TeacherAssist {_provider} provider returned empty JSON content")
    try:
        content_json = json.loads(raw_content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"TeacherAssist {_provider} provider returned malformed JSON") from exc

    usage = payload.get("usage") or {}
    input_tokens = int(usage.get("prompt_tokens") or 0)
    output_tokens = int(usage.get("completion_tokens") or 0)
    estimated_cost_cents = estimate_openai_cost_cents(
        model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
    )
    return TeacherAssistAIProviderResult(
        content_json=content_json,
        provider=_provider,
        model=model_name,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        estimated_cost_cents=estimated_cost_cents,
        metadata_json={
            "is_mock": False,
            "provider_mode": "real",
        },
    )
