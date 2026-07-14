from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Comma-separated origins browsers may use when calling this API.
# Override with env CORS_ORIGINS. Includes production app host so deploys missing
# env still allow https://app.oziebot.com; add more origins via CORS_ORIGINS.
CORS_ORIGINS_DEFAULT = "http://localhost:3000,https://app.oziebot.com"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_env: str = "development"
    database_url: str | None = None
    api_secret: str = Field(
        default="dev-insecure", description="Legacy internal signing; prefer jwt_secret"
    )
    jwt_secret: str = Field(
        default="change-me-use-openssl-rand-hex-32",
        description="HS256 signing key; set env JWT_SECRET in production",
    )
    jwt_access_exp_minutes: int = 15
    jwt_refresh_exp_days: int = 7
    cors_origins: str = CORS_ORIGINS_DEFAULT
    stripe_secret_key: str | None = None
    stripe_webhook_secret: str | None = None
    stripe_checkout_success_url: str = (
        "http://localhost:3000/billing/success?session_id={CHECKOUT_SESSION_ID}"
    )
    stripe_checkout_cancel_url: str = "http://localhost:3000/billing/cancel"
    exchange_credentials_encryption_key: str | None = Field(
        default=None,
        description="Fernet key (urlsafe base64) for exchange API secrets at rest",
    )
    coinbase_api_base_url: str = "https://api.coinbase.com"
    coinbase_force_ipv4: bool = Field(
        default=False,
        description="Force Coinbase API calls to use IPv4 egress only",
    )
    api_slow_request_ms: int = Field(
        default=1000,
        description="Log API requests at warning level when duration exceeds this threshold.",
    )
    api_slow_query_ms: int = Field(
        default=250,
        description="Log SQL queries at warning level when duration exceeds this threshold.",
    )
    api_slo_sample_window: int = Field(
        default=200,
        description="Rolling sample window size for API SLO calculations.",
    )
    api_slo_min_samples: int = Field(
        default=20,
        description="Minimum requests before an API SLO window is considered meaningful.",
    )
    api_slo_breach_rate_warn_pct: float = Field(
        default=10.0,
        description="Warn when rolling API SLO breach rate exceeds this percentage.",
    )
    ai_diagnostic_provider_api_key: str | None = None
    ai_diagnostic_provider_base_url: str | None = None
    ai_diagnostic_model_name: str = "gpt-4.1-mini"
    ai_diagnostic_prompt_version: str = "ai-diagnostics-v1"
    teacher_assist_storage_root: str = "/tmp/oziebot-teacher-assist"
    teacher_assist_storage_backend: str = Field(
        default="local",
        description="TeacherAssist storage backend: local or s3.",
    )
    teacher_assist_s3_bucket: str | None = Field(
        default=None,
        description="Private S3 bucket for TeacherAssist stored objects.",
    )
    teacher_assist_s3_region: str | None = Field(
        default=None,
        description="AWS region for the TeacherAssist S3 bucket.",
    )
    teacher_assist_s3_prefix: str = Field(
        default="teacher-assist",
        description="Key prefix used for TeacherAssist stored objects.",
    )
    teacher_assist_s3_endpoint: str | None = Field(
        default=None,
        description="Optional S3-compatible endpoint override for TeacherAssist storage.",
    )
    teacher_assist_s3_presign_expiration_seconds: int = Field(
        default=900,
        description="Expiration for TeacherAssist presigned download URLs in seconds.",
    )
    teacher_assist_upload_max_bytes: int = Field(
        default=25 * 1024 * 1024,
        description="Maximum size in bytes for TeacherAssist resource uploads.",
    )
    teacher_assist_ocr_provider: str = Field(
        default="mock",
        description="Backend-only TeacherAssist OCR provider selection.",
    )
    teacher_assist_ocr_mock_text_limit: int = Field(
        default=4000,
        description="Soft text-length cap for TeacherAssist mock OCR placeholder output.",
    )
    teacher_assist_real_ocr_enabled: bool = Field(
        default=False,
        description="Primary safety switch for any non-mock TeacherAssist OCR provider.",
    )
    teacher_assist_ocr_allowed_providers: str = Field(
        default="mock,textract,openai_vision",
        description="Comma-separated allowlist of TeacherAssist OCR providers.",
    )
    teacher_assist_ocr_allowed_models: str = Field(
        default="mock-ocr,textract-detect-document-text,gpt-4o-mini",
        description="Comma-separated allowlist of TeacherAssist OCR models.",
    )
    teacher_assist_ocr_max_file_bytes: int = Field(
        default=25 * 1024 * 1024,
        description="Maximum artifact size in bytes for TeacherAssist OCR execution.",
    )
    teacher_assist_ocr_max_pages: int = Field(
        default=15,
        description="Maximum page count allowed for TeacherAssist OCR execution.",
    )
    teacher_assist_ocr_provider_timeout_seconds: int = Field(
        default=120,
        description="Timeout for a single TeacherAssist OCR provider request.",
    )
    teacher_assist_ocr_daily_cost_limit_cents: int = Field(
        default=0,
        description="Daily TeacherAssist OCR cost limit in cents. Zero keeps real OCR disabled by policy.",
    )
    teacher_assist_ocr_openai_vision_model: str = Field(
        default="gpt-4o-mini",
        description="OpenAI vision model used when teacher_assist_ocr_provider=openai_vision.",
    )
    teacher_assist_ocr_aws_region: str | None = Field(
        default=None,
        description="AWS region for TeacherAssist Textract OCR. Defaults to TeacherAssist S3 region.",
    )
    teacher_assist_extraction_timeout_seconds: int = Field(
        default=180,
        description="Max runtime for a TeacherAssist extraction attempt before timing out.",
    )
    teacher_assist_ai_provider: str = Field(
        default="mock",
        description="Backend-only TeacherAssist AI provider selection.",
    )
    teacher_assist_real_provider_enabled: bool = Field(
        default=False,
        description="Primary safety switch for any future real TeacherAssist AI provider.",
    )
    teacher_assist_ai_enable_real_provider: bool = Field(
        default=False,
        description="Deprecated alias for teacher_assist_real_provider_enabled.",
    )
    teacher_assist_real_provider_model: str | None = Field(
        default=None,
        description="Configured model name for TeacherAssist OpenAI provider execution.",
    )
    teacher_assist_openai_default_model: str | None = Field(
        default=None,
        description="Default OpenAI model alias (TEACHER_ASSIST_OPENAI_DEFAULT_MODEL).",
    )
    teacher_assist_openai_api_key: str | None = Field(
        default=None,
        description="API key for guarded TeacherAssist OpenAI provider execution.",
    )
    teacher_assist_openai_base_url: str = Field(
        default="https://api.openai.com/v1",
        description="Base URL for the guarded TeacherAssist OpenAI-compatible provider.",
    )
    teacher_assist_gemini_api_key: str | None = Field(
        default=None,
        description="API key for Gemini (Google AI Studio) provider execution.",
    )
    teacher_assist_gemini_base_url: str = Field(
        default="https://generativelanguage.googleapis.com/v1beta/openai",
        description="Base URL for Gemini's OpenAI-compatible endpoint.",
    )
    teacher_assist_ai_fixture_mode: str = Field(
        default="off",
        description="TeacherAssist AI fixture mode: off, record, or replay.",
    )
    teacher_assist_ai_fixtures_root: str = Field(
        default="fixtures/teacher_assist",
        description="Directory used for optional TeacherAssist AI fixture replay/record support.",
    )
    teacher_assist_ai_max_input_tokens: int = Field(
        default=16000,
        description="Soft max input tokens allowed for TeacherAssist provider requests.",
    )
    teacher_assist_ai_max_output_tokens: int = Field(
        default=4000,
        description="Soft max output tokens allowed for TeacherAssist provider responses.",
    )
    teacher_assist_ai_daily_cost_limit_cents: int = Field(
        default=0,
        description="Daily TeacherAssist AI cost limit in cents. Zero keeps real-provider usage disabled by policy.",
    )
    teacher_assist_worker_enabled: bool = Field(
        default=True,
        description="Enable the dedicated TeacherAssist worker loop.",
    )
    teacher_assist_worker_poll_interval_seconds: float = Field(
        default=1.0,
        description="Idle poll interval for the TeacherAssist worker.",
    )
    teacher_assist_worker_lease_seconds: int = Field(
        default=30,
        description="Lease length for a claimed TeacherAssist workflow.",
    )
    teacher_assist_worker_max_retries: int = Field(
        default=3,
        description="Maximum retries for a failed TeacherAssist workflow.",
    )
    teacher_assist_workflow_timeout_seconds: int = Field(
        default=300,
        description="Max runtime for a TeacherAssist workflow attempt before timing out.",
    )
    teacher_assist_allowed_models: str = Field(
        default="mock",
        description="Comma-separated allowlist of TeacherAssist AI models.",
    )
    teacher_assist_reteach_mastery_healthy_threshold: float = Field(
        default=0.80,
        description="Mastery percentage threshold for healthy reteach operational status.",
    )
    teacher_assist_reteach_mastery_monitor_threshold: float = Field(
        default=0.50,
        description="Mastery percentage threshold for monitor reteach operational status.",
    )
    teacher_assist_reteach_mastery_recommended_threshold: float = Field(
        default=0.25,
        description="Mastery percentage threshold for reteach_recommended operational status.",
    )
    teacher_assist_assignment_effectiveness_healthy_threshold: float = Field(
        default=0.70,
        description="Mastery percentage threshold for effective assignment status.",
    )
    teacher_assist_assignment_effectiveness_mixed_threshold: float = Field(
        default=0.40,
        description="Mastery percentage threshold for mixed_results assignment status.",
    )
    teacher_assist_mastery_analytics_recent_days: int = Field(
        default=14,
        description="Recent window in days for mastery analytics counts and trends.",
    )
    teacher_assist_google_oauth_client_id: str | None = Field(
        default=None,
        description="Google OAuth client ID for TeacherAssist Forms integration.",
    )
    teacher_assist_google_oauth_client_secret: str | None = Field(
        default=None,
        description="Google OAuth client secret (server-only).",
    )
    teacher_assist_google_oauth_redirect_uri: str = Field(
        default="http://localhost:8000/v1/teacher-assist-v2/teacher/google/oauth/callback",
        description="OAuth redirect URI registered in Google Cloud Console.",
    )
    teacher_assist_google_oauth_frontend_redirect: str = Field(
        default="http://localhost:3000/teacher-assist-v2/settings/google",
        description="Frontend URL after successful Google OAuth callback.",
    )
    teacher_assist_pixabay_api_key: str | None = Field(
        default=None,
        description="Pixabay API key for fetching royalty-free educational images for slide decks. "
        "Get a free key at https://pixabay.com/api/docs/",
    )


def get_settings() -> Settings:
    return Settings()
