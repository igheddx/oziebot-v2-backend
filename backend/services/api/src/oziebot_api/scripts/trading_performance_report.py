"""Generate trading performance JSON + table summary from trade_outcome_features.

Usage (from repo root, with API venv):
  cd backend/services/api && PYTHONPATH=src python -m oziebot_api.scripts.trading_performance_report
  python -m oziebot_api.scripts.trading_performance_report --format csv > trades.csv
  python -m oziebot_api.scripts.trading_performance_report --format json --limit 200

Requires DATABASE_URL (see oziebot_api.config).
"""

from __future__ import annotations

import argparse
import csv
import io
import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from oziebot_common.trade_intelligence import DecisionAuditDecision
from oziebot_api.config import get_settings
from oziebot_api.db.session import make_session_factory
from oziebot_api.models.execution import ExecutionTradeRecord
from oziebot_api.models.strategy_allocation import (
    StrategyAllocationItem,
    StrategyAllocationPlan,
    StrategyCapitalBucket,
    StrategyCapitalLedger,
)
from oziebot_api.models.trade_intelligence import (
    StrategyDecisionAudit,
    StrategySignalSnapshot,
    TradeOutcomeFeature,
)

TRADE_LIMIT = 100

_TRADE_CSV_FIELDS = [
    "trade_id",
    "strategy",
    "token",
    "trading_mode",
    "entry_price",
    "exit_price",
    "entry_time",
    "exit_time",
    "size_usd",
    "pnl_pct",
    "pnl_usd",
    "fees",
    "exit_reason",
    "max_favorable_excursion_pct",
    "max_adverse_excursion_pct",
]


def _d(v: Any) -> Decimal:
    if v is None:
        return Decimal("0")
    return Decimal(str(v))


def _f(v: Decimal | float | int | None) -> float:
    if v is None:
        return 0.0
    return float(v)


def _fmt_ts(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def _max_drawdown_pnls(pnls: list[float]) -> float:
    """Max peak-to-trough drop on cumulative PnL curve (same units as pnls)."""
    peak = 0.0
    cum = 0.0
    max_dd = 0.0
    for p in pnls:
        cum += p
        peak = max(peak, cum)
        max_dd = min(max_dd, cum - peak)
    return round(abs(max_dd), 4)


def run(
    limit: int = TRADE_LIMIT,
    user_id: Any | None = None,
    trading_mode: str | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    if not settings.database_url:
        raise SystemExit("DATABASE_URL is required")
    factory = make_session_factory(settings)
    if factory is None:
        raise SystemExit("Could not create session factory")
    session: Session = factory()
    try:
        return _build_report(session, limit, user_id=user_id, trading_mode=trading_mode)
    finally:
        session.close()


def build_report(
    session: Session,
    limit: int,
    *,
    user_id: Any | None = None,
    trading_mode: str | None = None,
) -> dict[str, Any]:
    """Build report using an existing DB session (e.g. FastAPI request scope)."""
    return _build_report(session, limit, user_id=user_id, trading_mode=trading_mode)


def trades_to_csv_string(trades: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=_TRADE_CSV_FIELDS,
        extrasaction="ignore",
        lineterminator="\n",
    )
    writer.writeheader()
    for t in trades:
        writer.writerow({k: _csv_cell(t.get(k)) for k in _TRADE_CSV_FIELDS})
    return buf.getvalue()


def _build_report(
    session: Session,
    limit: int,
    *,
    user_id: Any | None = None,
    trading_mode: str | None = None,
) -> dict[str, Any]:
    count_q = (
        select(func.count())
        .select_from(TradeOutcomeFeature)
        .join(
            ExecutionTradeRecord,
            TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
        )
    )
    if user_id is not None:
        count_q = count_q.where(ExecutionTradeRecord.user_id == user_id)
    if trading_mode is not None:
        count_q = count_q.where(TradeOutcomeFeature.trading_mode == trading_mode)
    total_outcome_rows = int(session.scalar(count_q) or 0)

    id_base = select(TradeOutcomeFeature.id).join(
        ExecutionTradeRecord,
        TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
    )
    if user_id is not None:
        id_base = id_base.where(ExecutionTradeRecord.user_id == user_id)
    if trading_mode is not None:
        id_base = id_base.where(TradeOutcomeFeature.trading_mode == trading_mode)
    latest_ids = list(
        session.scalars(id_base.order_by(TradeOutcomeFeature.created_at.desc()).limit(limit)).all()
    )
    if not latest_ids:
        joined_rows: list[Any] = []
    else:
        row_stmt = (
            select(TradeOutcomeFeature, ExecutionTradeRecord)
            .join(
                ExecutionTradeRecord,
                TradeOutcomeFeature.trade_id == ExecutionTradeRecord.id,
            )
            .where(TradeOutcomeFeature.id.in_(latest_ids))
            .order_by(TradeOutcomeFeature.created_at.desc())
        )
        if user_id is not None:
            row_stmt = row_stmt.where(ExecutionTradeRecord.user_id == user_id)
        joined_rows = session.execute(row_stmt).all()

    trades_out: list[dict[str, Any]] = []
    user_ids: set[Any] = set()
    trading_modes: set[str] = set()
    exit_times: list[datetime] = []
    pnls_ordered: list[float] = []

    for feat, ex in joined_rows:
        user_ids.add(ex.user_id)
        trading_modes.add(feat.trading_mode)
        exit_at = feat.created_at
        exit_times.append(exit_at)
        hold = feat.hold_seconds
        entry_at = (
            exit_at - timedelta(seconds=int(hold)) if hold is not None and hold >= 0 else None
        )
        basis = _d(feat.entry_price) * _d(feat.filled_size)
        size_usd = round(_f(basis), 4)
        pnl_usd = round(_f(feat.realized_pnl), 4)
        ret_frac = feat.realized_return_pct
        pnl_pct = round(_f(ret_frac) * 100, 4) if ret_frac is not None else None

        trades_out.append(
            {
                "trade_id": str(feat.trade_id),
                "strategy": feat.strategy_name,
                "token": feat.token_symbol,
                "trading_mode": feat.trading_mode,
                "entry_price": _f(feat.entry_price),
                "exit_price": _f(feat.exit_price),
                "entry_time": _fmt_ts(entry_at),
                "exit_time": _fmt_ts(exit_at),
                "size_usd": size_usd,
                "pnl_pct": pnl_pct,
                "pnl_usd": pnl_usd,
                "fees": round(_f(feat.fee_paid), 4),
                "exit_reason": feat.exit_reason,
                "max_favorable_excursion_pct": (
                    round(_f(feat.max_favorable_excursion_pct) * 100, 4)
                    if feat.max_favorable_excursion_pct is not None
                    else None
                ),
                "max_adverse_excursion_pct": (
                    round(_f(feat.max_adverse_excursion_pct) * 100, 4)
                    if feat.max_adverse_excursion_pct is not None
                    else None
                ),
            }
        )

    # Chronological series for drawdown (oldest exit first among sample)
    chronological = sorted(
        zip(exit_times, (t["pnl_usd"] for t in trades_out)),
        key=lambda x: x[0],
    )
    pnls_ordered = [float(p or 0) for _, p in chronological]

    # Strategy summary
    by_strat: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades_out:
        by_strat[t["strategy"]].append(t)

    strategy_summary: dict[str, Any] = {}
    for strat, ts in by_strat.items():
        n = len(ts)
        wins = [x for x in ts if (x["pnl_usd"] or 0) > 0]
        losses = [x for x in ts if (x["pnl_usd"] or 0) < 0]
        total_pnl = round(sum(x["pnl_usd"] or 0 for x in ts), 4)
        max_dd = _max_drawdown_pnls(
            [x["pnl_usd"] or 0 for x in sorted(ts, key=lambda z: z["exit_time"] or "")]
        )
        strategy_summary[strat] = {
            "total_trades": n,
            "win_rate": round(len(wins) / n, 4) if n else 0.0,
            "win_rate_pct": round(len(wins) / n * 100, 2) if n else 0.0,
            "avg_win_pct": (
                round(sum((x["pnl_pct"] or 0) for x in wins) / len(wins), 4) if wins else 0.0
            ),
            "avg_loss_pct": (
                round(sum((x["pnl_pct"] or 0) for x in losses) / len(losses), 4) if losses else 0.0
            ),
            "total_pnl_usd": total_pnl,
            "total_pnl": total_pnl,
            "max_drawdown_usd": max_dd,
            "max_drawdown": max_dd,
        }

    # Token performance
    by_tok: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in trades_out:
        by_tok[t["token"]].append(t)
    token_summary: dict[str, Any] = {}
    for tok, ts in by_tok.items():
        n = len(ts)
        wins = sum(1 for x in ts if (x["pnl_usd"] or 0) > 0)
        total_pnl = sum(x["pnl_usd"] or 0 for x in ts)
        rets = [x["pnl_pct"] for x in ts if x["pnl_pct"] is not None]
        token_summary[tok] = {
            "trades": n,
            "win_rate": round(wins / n, 4) if n else 0.0,
            "win_rate_pct": round(wins / n * 100, 2) if n else 0.0,
            "total_pnl_usd": round(total_pnl, 4),
            "avg_return_pct": round(sum(rets) / len(rets), 4) if rets else 0.0,
            "avg_return": round(sum(rets) / len(rets), 4) if rets else 0.0,
        }

    # Signal funnel: time window spanned by sampled trades
    funnel: dict[str, Any] = {
        "window": {"start": None, "end": None},
        "evaluated": 0,
        "emitted": 0,
        "rejected": 0,
        "reduced": 0,
        "executed": 0,
        "decision_counts": {},
        "rejection_reason_counts": {},
    }
    if exit_times and user_ids:
        t0 = min(exit_times)
        t1 = max(exit_times)
        funnel["window"] = {"start": _fmt_ts(t0), "end": _fmt_ts(t1)}
        uid_list = list(user_ids)
        mode_list = list(trading_modes) or ["paper", "live"]

        snap_count = session.scalar(
            select(func.count())
            .select_from(StrategySignalSnapshot)
            .where(
                StrategySignalSnapshot.user_id.in_(uid_list),
                StrategySignalSnapshot.trading_mode.in_(mode_list),
                StrategySignalSnapshot.timestamp >= t0,
                StrategySignalSnapshot.timestamp <= t1,
            )
        )
        funnel["evaluated"] = int(snap_count or 0)

        audits = session.scalars(
            select(StrategyDecisionAudit).where(
                StrategyDecisionAudit.created_at >= t0,
                StrategyDecisionAudit.created_at <= t1,
            )
        ).all()

        # restrict to audits linked to snapshots owned by our users (when snapshot exists)
        snap_ids = set(
            session.scalars(
                select(StrategySignalSnapshot.id).where(
                    StrategySignalSnapshot.user_id.in_(uid_list),
                    StrategySignalSnapshot.timestamp >= t0,
                    StrategySignalSnapshot.timestamp <= t1,
                )
            ).all()
        )
        filtered = [
            a for a in audits if a.signal_snapshot_id is None or a.signal_snapshot_id in snap_ids
        ]

        dec_counts = Counter(a.decision for a in filtered)
        funnel["decision_counts"] = dict(dec_counts)
        funnel["emitted"] = int(dec_counts.get(DecisionAuditDecision.EMITTED.value, 0))
        funnel["rejected"] = int(dec_counts.get(DecisionAuditDecision.REJECTED.value, 0))
        funnel["reduced"] = int(dec_counts.get(DecisionAuditDecision.REDUCED.value, 0))
        funnel["executed"] = int(dec_counts.get(DecisionAuditDecision.EXECUTED.value, 0))

        rej = Counter()
        for a in filtered:
            if a.decision == DecisionAuditDecision.REJECTED.value:
                key = a.reason_code or "unspecified"
                rej[key] += 1
        funnel["rejection_reason_counts"] = dict(rej)

    # Capital (plans + current buckets + ledger peak in window)
    capital: dict[str, Any] = {
        "total_capital_usd_by_user": {},
        "capital_per_strategy_usd": [],
        "buckets_current_usd": [],
        "avg_deployed_capital_usd": None,
        "peak_deployed_capital_usd": None,
        "capital_notes": [
            "avg/peak deployed from current strategy_capital_buckets (locked+reserved) per bucket; avg is mean across buckets. For ledger-based window stats see ledger_* keys."
        ],
        "notes": [],
    }
    if user_ids:
        plans = session.scalars(
            select(StrategyAllocationPlan).where(StrategyAllocationPlan.user_id.in_(user_ids))
        ).all()
        if plans:
            capital["total_capital_usd"] = round(sum(p.total_capital_cents for p in plans) / 100, 2)
        for p in plans:
            capital["total_capital_usd_by_user"][str(p.user_id)] = round(
                p.total_capital_cents / 100, 2
            )

        items = (
            session.scalars(
                select(StrategyAllocationItem).where(
                    StrategyAllocationItem.plan_id.in_([x.id for x in plans])
                )
            ).all()
            if plans
            else []
        )
        for it in items:
            capital["capital_per_strategy_usd"].append(
                {
                    "strategy_id": it.strategy_id,
                    "assigned_capital_usd": round(it.assigned_capital_cents / 100, 2),
                    "allocation_bps": it.allocation_bps,
                }
            )

        buckets = session.scalars(
            select(StrategyCapitalBucket).where(StrategyCapitalBucket.user_id.in_(user_ids))
        ).all()
        deployed_vals: list[float] = []
        for b in buckets:
            dep = (b.locked_capital_cents + b.reserved_cash_cents) / 100
            deployed_vals.append(dep)
            capital["buckets_current_usd"].append(
                {
                    "strategy_id": b.strategy_id,
                    "trading_mode": b.trading_mode,
                    "locked_capital_usd": round(b.locked_capital_cents / 100, 2),
                    "reserved_cash_usd": round(b.reserved_cash_cents / 100, 2),
                    "deployed_proxy_usd": round(dep, 2),
                }
            )
        if deployed_vals:
            capital["total_deployed_proxy_usd"] = round(sum(deployed_vals), 2)
            capital["avg_deployed_capital_usd"] = round(sum(deployed_vals) / len(deployed_vals), 2)
            capital["peak_deployed_capital_usd"] = round(max(deployed_vals), 2)

        if exit_times:
            t0, t1 = min(exit_times), max(exit_times)
            deployed_col = (
                StrategyCapitalLedger.after_locked_capital_cents
                + StrategyCapitalLedger.after_reserved_cash_cents
            )
            stmt = select(deployed_col).where(
                StrategyCapitalLedger.user_id.in_(user_ids),
                StrategyCapitalLedger.created_at >= t0,
                StrategyCapitalLedger.created_at <= t1,
            )
            rows_ldg = session.execute(stmt).all()
            if rows_ldg:
                peaks = [r[0] / 100 for r in rows_ldg]
                capital["ledger_peak_deployed_usd_in_window"] = round(max(peaks), 2)
                capital["ledger_avg_deployed_usd_in_window"] = round(sum(peaks) / len(peaks), 2)
            else:
                capital["notes"].append(
                    "No strategy_capital_ledger rows in trade window; peak/avg from ledger omitted."
                )

    # Portfolio-level stats from sample
    n_all = len(trades_out)
    all_wins = [t for t in trades_out if (t["pnl_usd"] or 0) > 0]
    all_losses = [t for t in trades_out if (t["pnl_usd"] or 0) < 0]
    port_dd = _max_drawdown_pnls(pnls_ordered)
    port_pnl = round(sum(t["pnl_usd"] or 0 for t in trades_out), 4)
    portfolio = {
        "total_trades": n_all,
        "win_rate": round(len(all_wins) / n_all, 4) if n_all else 0.0,
        "win_rate_pct": round(len(all_wins) / n_all * 100, 2) if n_all else 0.0,
        "avg_win_pct": (
            round(sum((t["pnl_pct"] or 0) for t in all_wins) / len(all_wins), 4)
            if all_wins
            else 0.0
        ),
        "avg_loss_pct": (
            round(sum((t["pnl_pct"] or 0) for t in all_losses) / len(all_losses), 4)
            if all_losses
            else 0.0
        ),
        "total_pnl_usd": port_pnl,
        "total_pnl": port_pnl,
        "max_drawdown_usd": port_dd,
        "max_drawdown": port_dd,
        "sample_size": n_all,
        "avg_trade_notional_usd": (
            round(sum(t["size_usd"] or 0 for t in trades_out) / n_all, 4) if n_all else 0.0
        ),
        "max_trade_notional_usd": (
            round(max((t["size_usd"] or 0 for t in trades_out)), 4) if n_all else 0.0
        ),
    }

    meta = {
        "generated_at": _fmt_ts(datetime.now(UTC)),
        "trade_source": "trade_outcome_features joined execution_trades",
        "limit": limit,
        "mfe_mae_units": "percent (stored fractions in DB, reported as % points)",
        "pnl_pct_units": "percent points (return fraction * 100)",
        "diagnostics": {
            "trade_outcome_features_rows_in_db": total_outcome_rows,
            "feature_ids_fetched_for_sample": len(latest_ids),
            "rows_after_inner_join_execution_trades": len(joined_rows),
            "scoped_user_id": str(user_id) if user_id is not None else None,
            "trading_mode_filter": trading_mode,
            "hint_empty_trades": (
                "No rows in trade_outcome_features, or inner join dropped all sample rows "
                "(missing execution_trades for those trade_ids), or this is a fresh/empty DB volume."
            ),
        },
    }

    return {
        "meta": meta,
        "trades": trades_out,
        "portfolio_summary": portfolio,
        "strategy_summary": strategy_summary,
        "token_performance": token_summary,
        "signal_funnel": funnel,
        "capital_usage": capital,
    }


def _ascii_table(headers: list[str], rows: list[list[str]]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    sep = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sep]
    hdr = "|" + "|".join(f" {h.ljust(widths[i])} " for i, h in enumerate(headers)) + "|"
    out.append(hdr)
    out.append(sep)
    for row in rows:
        out.append(
            "|" + "|".join(f" {str(row[i]).ljust(widths[i])} " for i in range(len(headers))) + "|"
        )
    out.append(sep)
    return "\n".join(out)


def _csv_cell(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def _print_trades_csv(trades: list[dict[str, Any]]) -> None:
    print(trades_to_csv_string(trades), end="")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Trading performance report from trade_outcome_features."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=TRADE_LIMIT,
        help=f"Max trades to include (default: {TRADE_LIMIT})",
    )
    parser.add_argument(
        "--format",
        choices=("full", "json", "csv"),
        default="full",
        help="full: JSON + ASCII tables; json: JSON only; csv: trades table only (stdout)",
    )
    args = parser.parse_args()
    lim = max(1, args.limit)
    data = run(limit=lim)

    if args.format == "csv":
        _print_trades_csv(data["trades"])
        return
    if args.format == "json":
        print(json.dumps(data, indent=2, default=str))
        return

    print(json.dumps(data, indent=2, default=str))
    print("\n--- TABLE: Recent trades (up to {}) ---\n".format(lim))
    headers = [
        "strategy",
        "token",
        "exit_time",
        "size_usd",
        "pnl_usd",
        "pnl_%",
        "fees",
        "exit_reason",
    ]
    rows: list[list[str]] = []
    for t in data["trades"]:
        rows.append(
            [
                str(t["strategy"])[:14],
                str(t["token"])[:10],
                (t["exit_time"] or "")[:19],
                str(t["size_usd"]),
                str(t["pnl_usd"]),
                str(t["pnl_pct"]),
                str(t["fees"]),
                str(t["exit_reason"] or "")[:20],
            ]
        )
    print(_ascii_table(headers, rows))

    print("\n--- TABLE: Strategy summary ---\n")
    sh = ["strategy", "trades", "win%", "avg_win_%", "avg_loss_%", "total_pnl", "max_dd"]
    sr: list[list[str]] = []
    for k, v in sorted(data["strategy_summary"].items()):
        sr.append(
            [
                k[:20],
                str(v["total_trades"]),
                str(v["win_rate_pct"]),
                str(v["avg_win_pct"]),
                str(v["avg_loss_pct"]),
                str(v["total_pnl_usd"]),
                str(v["max_drawdown_usd"]),
            ]
        )
    print(_ascii_table(sh, sr))

    print("\n--- TABLE: Token performance ---\n")
    th = ["token", "trades", "win%", "total_pnl", "avg_return_%"]
    tr: list[list[str]] = []
    for k, v in sorted(data["token_performance"].items()):
        tr.append(
            [
                k[:12],
                str(v["trades"]),
                str(v["win_rate_pct"]),
                str(v["total_pnl_usd"]),
                str(v["avg_return_pct"]),
            ]
        )
    print(_ascii_table(th, tr))

    print("\n--- TABLE: Signal funnel (trade sample time window) ---\n")
    fun = data["signal_funnel"]
    print(
        _ascii_table(
            ["metric", "value"],
            [
                ["window_start", str(fun["window"].get("start") or "")],
                ["window_end", str(fun["window"].get("end") or "")],
                ["evaluated (snapshots)", str(fun.get("evaluated", 0))],
                ["emitted (audits)", str(fun.get("emitted", 0))],
                ["rejected (audits)", str(fun.get("rejected", 0))],
            ],
        )
    )
    cap = data["capital_usage"]
    print("\n--- TABLE: Capital snapshot ---\n")
    print(
        _ascii_table(
            ["field", "value"],
            [
                ["total_capital_usd", str(cap.get("total_capital_usd", ""))],
                ["total_deployed_proxy_usd", str(cap.get("total_deployed_proxy_usd", ""))],
                ["avg_deployed_capital_usd", str(cap.get("avg_deployed_capital_usd", ""))],
                ["peak_deployed_capital_usd", str(cap.get("peak_deployed_capital_usd", ""))],
            ],
        )
    )


if __name__ == "__main__":
    main()
