"""Constants for TeacherAssist v2 Google Forms integration."""

from __future__ import annotations

GOOGLE_OAUTH_SCOPES = (
    "https://www.googleapis.com/auth/forms.body",
    "https://www.googleapis.com/auth/forms.responses.readonly",
)

GOOGLE_FORM_SYNC_STATUSES = frozenset({"CREATED", "IMPORTED", "ERROR"})

GOOGLE_FORM_IMPORT_MATCH_METHOD = "GOOGLE_FORM"
