from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, Query

from oziebot_api.config import Settings
from oziebot_api.deps import DbSession, settings_dep
from oziebot_api.deps.auth import RootAdminUser
from oziebot_api.schemas.platform_admin import (
    AiDiagnosticFindingResponse,
    AiDiagnosticFindingStatusPatch,
    AiDiagnosticReviewCreate,
    AiDiagnosticReviewCreateResponse,
    AiDiagnosticReviewDetailResponse,
    AiDiagnosticReviewListResponse,
    DiagnosticSnapshotListResponse,
)
from oziebot_api.services.admin_ai_diagnostics import (
    AiDiagnosticReviewRequest,
    AiDiagnosticReviewService,
)

router = APIRouter(prefix="/admin/ai-diagnostics", tags=["admin-ai-diagnostics"])


@router.get("/snapshots", response_model=DiagnosticSnapshotListResponse)
def list_ai_diagnostic_snapshots(
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict:
    service = AiDiagnosticReviewService(db, app_settings)
    return {"snapshots": service.list_snapshots(limit=limit)}


@router.post("/reviews", response_model=AiDiagnosticReviewCreateResponse)
def create_ai_diagnostic_review(
    admin: RootAdminUser,
    body: AiDiagnosticReviewCreate,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
) -> dict:
    service = AiDiagnosticReviewService(db, app_settings)
    snapshot_id = uuid.UUID(body.snapshot_id) if body.snapshot_id else None
    return service.create_review(
        admin=admin,
        request=AiDiagnosticReviewRequest(
            snapshot_id=snapshot_id,
            trading_mode=None if body.trading_mode == "all" else body.trading_mode,
            strategy=None if body.strategy == "all" else body.strategy,
            token=body.token,
            days=body.days,
        ),
    )


@router.get("/reviews", response_model=AiDiagnosticReviewListResponse)
def list_ai_diagnostic_reviews(
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
    limit: int = Query(default=50, ge=1, le=100),
) -> dict:
    service = AiDiagnosticReviewService(db, app_settings)
    return {"reviews": service.list_reviews(limit=limit)}


@router.get("/reviews/{review_id}", response_model=AiDiagnosticReviewDetailResponse)
def read_ai_diagnostic_review(
    review_id: uuid.UUID,
    _admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
) -> dict:
    service = AiDiagnosticReviewService(db, app_settings)
    review = service.get_review(review_id)
    if review is None:
        raise HTTPException(status_code=404, detail="Review not found")
    return review


@router.patch("/findings/{finding_id}", response_model=AiDiagnosticFindingResponse)
def patch_ai_diagnostic_finding_status(
    finding_id: uuid.UUID,
    body: AiDiagnosticFindingStatusPatch,
    admin: RootAdminUser,
    db: DbSession,
    app_settings: Settings = Depends(settings_dep),
) -> dict:
    service = AiDiagnosticReviewService(db, app_settings)
    try:
        finding = service.update_finding_status(
            admin=admin,
            finding_id=finding_id,
            status=body.status,
            note=body.note,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if finding is None:
        raise HTTPException(status_code=404, detail="Finding not found")
    return finding
