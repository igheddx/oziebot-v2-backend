from __future__ import annotations

import csv
import io
import json
from uuid import UUID

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from oziebot_api.deps import DbSession
from oziebot_api.deps.auth import CurrentUser
from oziebot_api.models.backtesting import (
    BacktestPerformanceSnapshot,
    BacktestRun,
    StrategyAnalyticsArtifactRecord,
)
from oziebot_api.schemas.backtesting import (
    BacktestRunCreate,
    BacktestRunDetailOut,
    BacktestRunListOut,
    HistoricalPerformanceOut,
    StrategyAnalyticsListOut,
)
from oziebot_api.services.backtesting import run_and_store_backtest
from oziebot_api.services.tenant_scope import primary_tenant_id
from oziebot_domain.backtesting import BacktestCandle, BacktestConfig, BacktestRunRequest
from oziebot_domain.trading_mode import TradingMode

router = APIRouter(prefix="/me/backtests", tags=["backtests"])


@router.post("/run", response_model=BacktestRunDetailOut)
def run_backtest(
    body: BacktestRunCreate,
    user: CurrentUser,
    db: DbSession,
) -> BacktestRunDetailOut:
    tenant_id = primary_tenant_id(db, user)
    if tenant_id is None:
        raise HTTPException(status_code=400, detail="No tenant membership")

    req = BacktestRunRequest(
        strategy_id=body.strategy_id,
        trading_mode=TradingMode(body.trading_mode),
        dataset_name=body.dataset_name,
        timeframe=body.timeframe,
        candles=[BacktestCandle(**c.model_dump()) for c in body.candles],
        config=BacktestConfig(**body.config.model_dump()),
    )
    run = run_and_store_backtest(
        db,
        user_id=user.id,
        tenant_id=tenant_id,
        request=req,
    )

    row = (
        db.query(BacktestRun)
        .filter(BacktestRun.id == run.id, BacktestRun.user_id == user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Backtest run not found")
    return BacktestRunDetailOut.model_validate(row)


@router.get("/history", response_model=BacktestRunListOut)
def list_backtest_history(
    user: CurrentUser,
    db: DbSession,
    strategy_id: str | None = None,
    trading_mode: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> BacktestRunListOut:
    q = db.query(BacktestRun).filter(BacktestRun.user_id == user.id)
    if strategy_id:
        q = q.filter(BacktestRun.strategy_id == strategy_id)
    if trading_mode:
        q = q.filter(BacktestRun.trading_mode == trading_mode)

    rows = q.order_by(BacktestRun.started_at.desc()).offset(offset).limit(limit).all()
    return BacktestRunListOut(total=len(rows), runs=rows)


@router.get("/{run_id}", response_model=BacktestRunDetailOut)
def get_backtest_run(run_id: UUID, user: CurrentUser, db: DbSession) -> BacktestRunDetailOut:
    row = (
        db.query(BacktestRun)
        .filter(BacktestRun.id == run_id, BacktestRun.user_id == user.id)
        .first()
    )
    if row is None:
        raise HTTPException(status_code=404, detail="Backtest run not found")
    return BacktestRunDetailOut.model_validate(row)


@router.get("/performance/history", response_model=HistoricalPerformanceOut)
def list_performance_history(
    user: CurrentUser,
    db: DbSession,
    strategy_id: str | None = None,
    token_symbol: str | None = None,
    trading_mode: str | None = None,
    scope: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> HistoricalPerformanceOut:
    q = db.query(BacktestPerformanceSnapshot).filter(BacktestPerformanceSnapshot.user_id == user.id)
    if strategy_id:
        q = q.filter(BacktestPerformanceSnapshot.strategy_id == strategy_id)
    if token_symbol:
        q = q.filter(BacktestPerformanceSnapshot.token_symbol == token_symbol)
    if trading_mode:
        q = q.filter(BacktestPerformanceSnapshot.trading_mode == trading_mode)
    if scope:
        q = q.filter(BacktestPerformanceSnapshot.scope == scope)

    rows = q.order_by(BacktestPerformanceSnapshot.created_at.desc()).offset(offset).limit(limit).all()
    return HistoricalPerformanceOut(total=len(rows), snapshots=rows)


@router.get("/performance/history.csv")
def export_performance_history_csv(
    user: CurrentUser,
    db: DbSession,
    strategy_id: str | None = None,
    token_symbol: str | None = None,
    trading_mode: str | None = None,
    scope: str | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> StreamingResponse:
    q = db.query(BacktestPerformanceSnapshot).filter(BacktestPerformanceSnapshot.user_id == user.id)
    if strategy_id:
        q = q.filter(BacktestPerformanceSnapshot.strategy_id == strategy_id)
    if token_symbol:
        q = q.filter(BacktestPerformanceSnapshot.token_symbol == token_symbol)
    if trading_mode:
        q = q.filter(BacktestPerformanceSnapshot.trading_mode == trading_mode)
    if scope:
        q = q.filter(BacktestPerformanceSnapshot.scope == scope)

    rows = q.order_by(BacktestPerformanceSnapshot.created_at.desc()).offset(offset).limit(limit).all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "created_at",
            "scope",
            "scope_key",
            "strategy_id",
            "trading_mode",
            "token_symbol",
            "total_trades",
            "win_rate",
            "avg_return_bps",
            "max_drawdown",
            "sharpe_like",
            "avg_slippage_bps",
            "fee_impact_cents",
            "avg_holding_seconds",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.created_at.isoformat(),
                row.scope,
                row.scope_key,
                row.strategy_id,
                row.trading_mode,
                row.token_symbol or "",
                row.total_trades,
                row.win_rate,
                row.avg_return_bps,
                row.max_drawdown,
                row.sharpe_like,
                row.avg_slippage_bps,
                row.fee_impact_cents,
                row.avg_holding_seconds,
            ]
        )
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="backtest_performance.csv"'},
    )


@router.get("/analytics/history", response_model=StrategyAnalyticsListOut)
def list_analytics_history(
    user: CurrentUser,
    db: DbSession,
    strategy_id: str | None = None,
    token_symbol: str | None = None,
    trading_mode: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> StrategyAnalyticsListOut:
    q = db.query(StrategyAnalyticsArtifactRecord).filter(StrategyAnalyticsArtifactRecord.user_id == user.id)
    if strategy_id:
        q = q.filter(StrategyAnalyticsArtifactRecord.strategy_id == strategy_id)
    if token_symbol:
        q = q.filter(StrategyAnalyticsArtifactRecord.token_symbol == token_symbol)
    if trading_mode:
        q = q.filter(StrategyAnalyticsArtifactRecord.trading_mode == trading_mode)

    rows = q.order_by(StrategyAnalyticsArtifactRecord.created_at.desc()).offset(offset).limit(limit).all()
    return StrategyAnalyticsListOut(total=len(rows), artifacts=rows)


@router.get("/analytics/history.csv")
def export_analytics_history_csv(
    user: CurrentUser,
    db: DbSession,
    strategy_id: str | None = None,
    token_symbol: str | None = None,
    trading_mode: str | None = None,
    limit: int = 1000,
    offset: int = 0,
) -> StreamingResponse:
    q = db.query(StrategyAnalyticsArtifactRecord).filter(StrategyAnalyticsArtifactRecord.user_id == user.id)
    if strategy_id:
        q = q.filter(StrategyAnalyticsArtifactRecord.strategy_id == strategy_id)
    if token_symbol:
        q = q.filter(StrategyAnalyticsArtifactRecord.token_symbol == token_symbol)
    if trading_mode:
        q = q.filter(StrategyAnalyticsArtifactRecord.trading_mode == trading_mode)

    rows = q.order_by(StrategyAnalyticsArtifactRecord.created_at.desc()).offset(offset).limit(limit).all()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "created_at",
            "strategy_id",
            "trading_mode",
            "token_symbol",
            "feature_vector_json",
            "labels_json",
            "metadata_json",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.created_at.isoformat(),
                row.strategy_id,
                row.trading_mode,
                row.token_symbol or "",
                json.dumps(row.feature_vector, separators=(",", ":")),
                json.dumps(row.labels, separators=(",", ":")),
                json.dumps(row.metadata_json, separators=(",", ":")),
            ]
        )
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": 'attachment; filename="strategy_analytics.csv"'},
    )
