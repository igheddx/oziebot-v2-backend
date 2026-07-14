"""Google OAuth for TeacherAssist v2 (server-side only)."""

from __future__ import annotations

import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlencode

import httpx
from jose import jwt
from sqlalchemy import select
from sqlalchemy.orm import Session

from oziebot_api.config import Settings
from oziebot_api.models.teacher_assist_v2_teacher_google_connection import (
    TeacherAssistV2TeacherGoogleConnection,
)
from oziebot_api.services.credential_crypto import CredentialCrypto
from oziebot_api.services.teacher_assist_v2.google_integration_constants import GOOGLE_OAUTH_SCOPES

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"
STATE_PURPOSE = "teacher_assist_v2_google_oauth"


def _now() -> datetime:
    return datetime.now(UTC)


def _crypto(settings: Settings) -> CredentialCrypto:
    return CredentialCrypto(settings.exchange_credentials_encryption_key)


def google_oauth_configured(settings: Settings) -> bool:
    return bool(
        settings.teacher_assist_google_oauth_client_id
        and settings.teacher_assist_google_oauth_client_secret
        and settings.exchange_credentials_encryption_key
    )


def build_google_integration_status(settings: Settings) -> dict[str, Any]:
    configured = google_oauth_configured(settings)
    return {
        "oauth_client_configured": bool(settings.teacher_assist_google_oauth_client_id),
        "oauth_client_secret_configured": bool(settings.teacher_assist_google_oauth_client_secret),
        "token_encryption_configured": bool(settings.exchange_credentials_encryption_key),
        "integration_ready": configured,
        "redirect_uri": settings.teacher_assist_google_oauth_redirect_uri,
        "required_scopes": list(GOOGLE_OAUTH_SCOPES),
        "setup_instructions": [
            "Create a Google Cloud project and enable the Google Forms API.",
            "Configure an OAuth 2.0 Web client with the redirect URI shown above.",
            "Set TEACHER_ASSIST_GOOGLE_OAUTH_CLIENT_ID and TEACHER_ASSIST_GOOGLE_OAUTH_CLIENT_SECRET on the API server.",
            "Set EXCHANGE_CREDENTIALS_ENCRYPTION_KEY (Fernet) to encrypt stored teacher tokens.",
            "Teachers connect Google from the quiz card before creating a form.",
        ],
        "notes": [
            "Direct Google Classroom publishing is not enabled in this phase.",
            "Client secrets are never exposed to the browser.",
        ],
    }


def get_teacher_google_connection(
    db: Session, *, teacher_user_id: uuid.UUID
) -> TeacherAssistV2TeacherGoogleConnection | None:
    return db.scalars(
        select(TeacherAssistV2TeacherGoogleConnection).where(
            TeacherAssistV2TeacherGoogleConnection.teacher_user_id == teacher_user_id
        )
    ).one_or_none()


def serialize_teacher_google_connection(
    row: TeacherAssistV2TeacherGoogleConnection | None,
    *,
    settings: Settings,
) -> dict[str, Any]:
    if row is None:
        return {
            "connected": False,
            "google_email": None,
            "connected_at": None,
            "scopes": list(GOOGLE_OAUTH_SCOPES),
            "integration_ready": google_oauth_configured(settings),
        }
    return {
        "connected": True,
        "google_email": row.google_email,
        "connected_at": row.connected_at.isoformat(),
        "scopes": list(row.scopes_json or GOOGLE_OAUTH_SCOPES),
        "integration_ready": google_oauth_configured(settings),
    }


def build_oauth_authorization_url(
    settings: Settings,
    *,
    teacher_user_id: uuid.UUID,
) -> dict[str, str]:
    if not google_oauth_configured(settings):
        raise ValueError("Google OAuth is not configured on the server.")
    state = jwt.encode(
        {
            "sub": str(teacher_user_id),
            "purpose": STATE_PURPOSE,
            "nonce": secrets.token_urlsafe(16),
            "exp": int((_now() + timedelta(minutes=15)).timestamp()),
        },
        settings.jwt_secret,
        algorithm="HS256",
    )
    params = {
        "client_id": settings.teacher_assist_google_oauth_client_id,
        "redirect_uri": settings.teacher_assist_google_oauth_redirect_uri,
        "response_type": "code",
        "scope": " ".join(GOOGLE_OAUTH_SCOPES),
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    return {"authorization_url": f"{GOOGLE_AUTH_URL}?{urlencode(params)}", "state": state}


def _decode_oauth_state(settings: Settings, state: str) -> uuid.UUID:
    payload = jwt.decode(state, settings.jwt_secret, algorithms=["HS256"])
    if payload.get("purpose") != STATE_PURPOSE:
        raise ValueError("Invalid OAuth state.")
    return uuid.UUID(str(payload["sub"]))


def complete_oauth_callback(
    db: Session,
    *,
    settings: Settings,
    code: str,
    state: str,
) -> TeacherAssistV2TeacherGoogleConnection:
    if not google_oauth_configured(settings):
        raise ValueError("Google OAuth is not configured on the server.")
    teacher_user_id = _decode_oauth_state(settings, state)
    crypto = _crypto(settings)
    if not crypto.configured:
        raise ValueError("Token encryption is not configured on the server.")

    token_resp = httpx.post(
        GOOGLE_TOKEN_URL,
        data={
            "code": code,
            "client_id": settings.teacher_assist_google_oauth_client_id,
            "client_secret": settings.teacher_assist_google_oauth_client_secret,
            "redirect_uri": settings.teacher_assist_google_oauth_redirect_uri,
            "grant_type": "authorization_code",
        },
        timeout=30.0,
    )
    if token_resp.status_code >= 400:
        raise ValueError(f"Google token exchange failed: {token_resp.text}")
    token_data = token_resp.json()
    access_token = token_data.get("access_token")
    if not access_token:
        raise ValueError("Google did not return an access token.")

    refresh_token = token_data.get("refresh_token")
    expires_in = int(token_data.get("expires_in") or 3600)
    expires_at = _now() + timedelta(seconds=expires_in)

    google_email = None
    userinfo = httpx.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=15.0,
    )
    if userinfo.status_code < 400:
        google_email = userinfo.json().get("email")

    now = _now()
    row = get_teacher_google_connection(db, teacher_user_id=teacher_user_id)
    if row is None:
        row = TeacherAssistV2TeacherGoogleConnection(
            id=uuid.uuid4(),
            teacher_user_id=teacher_user_id,
            google_email=google_email,
            encrypted_access_token=crypto.encrypt(access_token.encode("utf-8")),
            encrypted_refresh_token=crypto.encrypt(refresh_token.encode("utf-8"))
            if refresh_token
            else None,
            token_expires_at=expires_at,
            scopes_json=list(GOOGLE_OAUTH_SCOPES),
            connected_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.google_email = google_email or row.google_email
        row.encrypted_access_token = crypto.encrypt(access_token.encode("utf-8"))
        if refresh_token:
            row.encrypted_refresh_token = crypto.encrypt(refresh_token.encode("utf-8"))
        row.token_expires_at = expires_at
        row.scopes_json = list(GOOGLE_OAUTH_SCOPES)
        row.updated_at = now
    db.flush()
    return row


def disconnect_teacher_google(db: Session, *, teacher_user_id: uuid.UUID) -> None:
    row = get_teacher_google_connection(db, teacher_user_id=teacher_user_id)
    if row is not None:
        db.delete(row)
        db.flush()


def get_valid_access_token(
    db: Session,
    *,
    settings: Settings,
    teacher_user_id: uuid.UUID,
) -> str:
    row = get_teacher_google_connection(db, teacher_user_id=teacher_user_id)
    if row is None:
        raise ValueError("Connect your Google account before creating a Google Form.")
    crypto = _crypto(settings)
    access_token = crypto.decrypt(row.encrypted_access_token).decode("utf-8")
    if row.token_expires_at and row.token_expires_at > _now() + timedelta(seconds=60):
        return access_token
    if not row.encrypted_refresh_token:
        raise ValueError("Google session expired. Connect Google again.")
    refresh_token = crypto.decrypt(row.encrypted_refresh_token).decode("utf-8")
    token_resp = httpx.post(
        GOOGLE_TOKEN_URL,
        data={
            "client_id": settings.teacher_assist_google_oauth_client_id,
            "client_secret": settings.teacher_assist_google_oauth_client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=30.0,
    )
    if token_resp.status_code >= 400:
        raise ValueError("Google session expired. Connect Google again.")
    token_data = token_resp.json()
    new_access = token_data.get("access_token")
    if not new_access:
        raise ValueError("Google token refresh failed.")
    expires_in = int(token_data.get("expires_in") or 3600)
    row.encrypted_access_token = crypto.encrypt(new_access.encode("utf-8"))
    row.token_expires_at = _now() + timedelta(seconds=expires_in)
    row.updated_at = _now()
    db.flush()
    return new_access
