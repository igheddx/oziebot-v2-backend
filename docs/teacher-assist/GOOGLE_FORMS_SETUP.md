# TeacherAssist v2 — Google Forms integration setup

## Google Cloud Console

1. Create or select a project.
2. Enable **Google Forms API**.
3. Configure **OAuth consent screen** (Internal or External testing).
4. Create **OAuth 2.0 Client ID** (Web application).
5. Add authorized redirect URI (must match API env):
   - Local: `http://localhost:8000/v1/teacher-assist-v2/teacher/google/oauth/callback`
   - Production: your API host + same path

## API environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TEACHER_ASSIST_GOOGLE_OAUTH_CLIENT_ID` | Yes | OAuth client ID |
| `TEACHER_ASSIST_GOOGLE_OAUTH_CLIENT_SECRET` | Yes | OAuth client secret (server only) |
| `TEACHER_ASSIST_GOOGLE_OAUTH_REDIRECT_URI` | Yes | Callback URL registered in Google Cloud |
| `TEACHER_ASSIST_GOOGLE_OAUTH_FRONTEND_REDIRECT` | Yes | Where teachers land after OAuth (e.g. `http://localhost:3000/teacher-assist-v2/settings/google`) |
| `EXCHANGE_CREDENTIALS_ENCRYPTION_KEY` | Yes | Fernet key for encrypting stored OAuth tokens |

## OAuth scopes (minimum)

- `https://www.googleapis.com/auth/forms.body`
- `https://www.googleapis.com/auth/forms.responses.readonly`

## Teacher workflow

1. Root admin confirms integration status at `/teacher-assist-v2/admin/google-settings`.
2. Teacher connects Google at `/teacher-assist-v2/settings/google` or from the quiz card.
3. On a package quiz, teacher clicks **Create Google Form** (one form per assignment).
4. Teacher opens the form, assigns via Google Classroom manually.
5. After students respond, teacher **Import Results** (API) or **Import Results CSV** (fallback).
6. Imported scores are **DRAFT** grades for this assignment only; teacher confirmation still required before gradebook/mastery sync.

## Limitations

- No Google Classroom publish API in this phase.
- Short-answer / paragraph questions are not auto-graded in Google Forms unless supported by API.
- No district-wide Google admin console in TeacherAssist.
