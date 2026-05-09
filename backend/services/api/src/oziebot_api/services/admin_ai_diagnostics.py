from __future__ import annotations

import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from oziebot_api.config import Settings
from oziebot_api.models.ai_diagnostics import (
    AiDiagnosticFinding,
    AiDiagnosticRecommendationAudit,
    AiDiagnosticReview,
    DiagnosticSnapshot,
)
from oziebot_api.models.user import User
from oziebot_api.services.admin_trading_diagnostics import (
    TradingDiagnosticsFilters,
    build_trading_diagnostics_report,
)
from oziebot_api.services.tenant_scope import primary_tenant_id

REVIEW_STATUSES = {"queued", "running", "completed", "failed"}
FINDING_STATUSES = {"new", "acknowledged", "dismissed", "resolved"}
SEVERITY_ORDER = {"critical": 3, "warning": 2, "info": 1}
PROMPT_VERSION = "ai-diagnostics-v1"
RECENT_EXECUTION_FINDING_HOURS = 24


@dataclass(slots=True)
class AiDiagnosticReviewRequest:
    snapshot_id: uuid.UUID | None = None
    trading_mode: str | None = None
    strategy: str | None = None
    token: str | None = None
    days: int = 7

    @property
    def filters(self) -> TradingDiagnosticsFilters:
        normalized_strategy = (self.strategy or "").strip().lower()
        return TradingDiagnosticsFilters(
            days=max(1, min(self.days, 365)),
            token=(self.token or "").strip().upper() or None,
            strategy=None if normalized_strategy in {"", "all"} else normalized_strategy,
            trading_mode=(self.trading_mode or "").strip().lower() or None,
            limit=100,
        )


class DiagnosticAiProvider(Protocol):
    name: str

    def review_diagnostics(
        self, snapshot: DiagnosticSnapshot, *, context: dict[str, Any]
    ) -> dict[str, Any] | None: ...


class OpenAICompatibleDiagnosticProvider:
    name = "openai-compatible-placeholder"

    def __init__(self, settings: Settings):
        self._settings = settings

    def review_diagnostics(
        self, snapshot: DiagnosticSnapshot, *, context: dict[str, Any]
    ) -> dict[str, Any] | None:
        if not self._settings.ai_diagnostic_provider_api_key:
            return None
        return {
            "model_name": self._settings.ai_diagnostic_model_name,
            "prompt_version": self._settings.ai_diagnostic_prompt_version or PROMPT_VERSION,
            "summary_suffix": "External AI provider hook is configured but Phase 1 still stores only deterministic rule-based findings.",
            "findings": [],
        }


class RuleBasedDiagnosticProvider:
    name = "rule-based"

    def review_diagnostics(
        self, snapshot: DiagnosticSnapshot, *, context: dict[str, Any]
    ) -> dict[str, Any]:
        report = snapshot.raw_json or {}
        report_generated_at = _parse_dt(snapshot.generated_at) or _parse_dt(
            report.get("generated_at")
        )
        window_start = (
            report_generated_at - timedelta(days=max(1, snapshot.days_filter))
            if report_generated_at is not None
            else None
        )
        findings: list[dict[str, Any]] = []
        findings.extend(self._check_dca_over_execution(report))
        findings.extend(
            self._check_zero_value_executions(report, report_generated_at=report_generated_at)
        )
        findings.extend(self._check_strategy_execution_gap(report))
        findings.extend(self._check_closed_trade_gap(report))
        findings.extend(self._check_generic_rejections(report))
        findings.extend(
            self._check_token_policy_conflicts(report, report_generated_at=report_generated_at)
        )
        findings.extend(
            self._check_position_reconciliation(
                report,
                report_generated_at=report_generated_at,
                window_start=window_start,
            )
        )
        findings.extend(self._check_capital_utilization(report))

        severity_counts = Counter(finding["severity"] for finding in findings)
        overall_health = (
            "critical"
            if severity_counts.get("critical")
            else "warning"
            if severity_counts.get("warning")
            else "healthy"
        )
        unavailable_metrics = len(report.get("signal_funnel", {}).get("unavailable_metrics", []))
        confidence_score = round(max(0.55, 0.92 - (unavailable_metrics * 0.04)), 2)
        summary_parts = [
            f"{len(findings)} findings",
            f"{severity_counts.get('critical', 0)} critical",
            f"{severity_counts.get('warning', 0)} warnings",
            f"{severity_counts.get('info', 0)} info",
        ]
        if findings:
            hottest = max(findings, key=lambda item: SEVERITY_ORDER.get(item["severity"], 0))
            summary_parts.append(f"highest priority: {hottest['finding_title'].lower()}")

        return {
            "overall_health": overall_health,
            "confidence_score": confidence_score,
            "summary": ", ".join(summary_parts),
            "model_name": self.name,
            "prompt_version": PROMPT_VERSION,
            "findings": findings,
        }

    def _check_dca_over_execution(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        dca_config = ((report.get("active_strategy_config") or {}).get("dca_config") or {}).get(
            "strategy_params", {}
        )
        buy_interval_hours = _safe_float(dca_config.get("buy_interval_hours"))
        if not buy_interval_hours or buy_interval_hours <= 0:
            return []

        dca_buys = sorted(
            [
                row
                for row in (report.get("execution_activity") or {}).get("execution_details", [])
                if row.get("strategy") == "dca"
                and row.get("side") == "buy"
                and row.get("executed_at")
            ],
            key=lambda row: row["executed_at"],
        )
        violations: list[dict[str, Any]] = []
        previous_by_scope: dict[tuple[str, str], str] = {}
        for row in dca_buys:
            scope = (row.get("token") or "", row.get("trading_mode") or "")
            previous = previous_by_scope.get(scope)
            previous_by_scope[scope] = row["executed_at"]
            if previous is None:
                continue
            gap_hours = _hours_between(previous, row["executed_at"])
            if gap_hours is None or gap_hours + 1e-9 >= buy_interval_hours:
                continue
            violations.append(
                {
                    "token": row.get("token"),
                    "trading_mode": row.get("trading_mode"),
                    "previous_executed_at": previous,
                    "executed_at": row["executed_at"],
                    "hours_since_previous": round(gap_hours, 4),
                }
            )
        if not violations:
            return []
        report_generated_at = _parse_dt(report.get("generated_at"))
        latest_violation_at = max(
            (_parse_dt(item.get("executed_at")) for item in violations),
            default=None,
        )
        latest_violation_age_hours = (
            (report_generated_at - latest_violation_at).total_seconds() / 3600
            if report_generated_at is not None and latest_violation_at is not None
            else None
        )
        violation_is_active = (
            latest_violation_age_hours is None or latest_violation_age_hours < buy_interval_hours
        )
        severity = "critical" if violation_is_active else "warning"
        title = (
            "DCA interval is being violated"
            if violation_is_active
            else "Historical DCA interval violations detected"
        )
        detail = (
            (
                f"DCA is configured for every {buy_interval_hours:g} hours, but "
                f"{len(violations)} buy executions landed sooner than that interval."
            )
            if violation_is_active
            else (
                f"DCA is configured for every {buy_interval_hours:g} hours, and "
                f"{len(violations)} earlier buys in the selected review window landed sooner than that interval. "
                "No recent violation appears in the latest interval window."
            )
        )
        recommendation = (
            "Inspect the scheduler and worker dedupe path, then compare emitted DCA buy timestamps "
            "against the last successful execution before allowing the next order."
            if violation_is_active
            else (
                "Confirm the fix by reviewing only fresh post-deploy diagnostics data or a shorter days window, "
                "then keep the scheduler and worker dedupe checks in place."
            )
        )
        return [
            _finding(
                severity=severity,
                category="dca_interval",
                strategy="dca",
                token=violations[0].get("token"),
                title=title,
                detail=detail,
                recommendation=recommendation,
                risk_if_ignored=(
                    "Capital can be deployed much faster than intended and the diagnostics will no longer "
                    "reflect the configured DCA policy."
                ),
                confidence_score=0.95,
                automation_eligibility="future_human_approval_required",
                evidence={
                    "buy_interval_hours": buy_interval_hours,
                    "violation_count": len(violations),
                    "violation_is_active": violation_is_active,
                    "latest_violation_at": _iso(latest_violation_at),
                    "latest_violation_age_hours": round(latest_violation_age_hours, 4)
                    if latest_violation_age_hours is not None
                    else None,
                    "violations": violations[:20],
                },
                affected_strategy="dca",
                risk_level=severity,
            )
        ]

    def _check_zero_value_executions(
        self,
        report: dict[str, Any],
        *,
        report_generated_at: datetime | None,
    ) -> list[dict[str, Any]]:
        offenders = [
            row
            for row in (report.get("execution_activity") or {}).get("execution_details", [])
            if _safe_float(row.get("quantity")) is not None
            and _safe_float(row.get("quantity")) <= 0
            or _safe_float(row.get("notional_usd")) is not None
            and _safe_float(row.get("notional_usd")) <= 0
        ]
        if not offenders:
            return []
        latest_offender_at = max(
            (_parse_dt(row.get("executed_at")) for row in offenders),
            default=None,
        )
        offender_is_recent = _is_recent_issue(
            latest_event_at=latest_offender_at,
            report_generated_at=report_generated_at,
            threshold_hours=RECENT_EXECUTION_FINDING_HOURS,
        )
        severity = "critical" if offender_is_recent else "warning"
        return [
            _finding(
                severity=severity,
                category="execution_accounting",
                strategy=offenders[0].get("strategy"),
                token=offenders[0].get("token"),
                title=(
                    "Executions contain zero quantity or notional"
                    if offender_is_recent
                    else "Historical executions contained zero quantity or notional"
                ),
                detail=(
                    f"{len(offenders)} execution records show a non-positive quantity or notional, "
                    + (
                        "which indicates accounting drift or execution validation leakage."
                        if offender_is_recent
                        else "but no recent offender appears in the latest review freshness window."
                    )
                ),
                recommendation=(
                    "Trace the affected orders from signal sizing through execution reservation and "
                    "position mutation, and block any request whose quantity or notional is not strictly positive."
                    if offender_is_recent
                    else (
                        "Confirm the fix with a fresh post-deploy review window and keep strict pre-execution "
                        "validation enabled."
                    )
                ),
                risk_if_ignored=(
                    "PnL, balances, and trade history can diverge even when positions continue to accumulate."
                ),
                confidence_score=0.98,
                automation_eligibility="not_eligible",
                evidence={
                    "execution_count": len(offenders),
                    "offender_is_recent": offender_is_recent,
                    "latest_offender_at": _iso(latest_offender_at),
                    "executions": offenders[:20],
                },
                affected_strategy=offenders[0].get("strategy"),
                affected_token=offenders[0].get("token"),
                risk_level=severity,
            )
        ]

    def _check_strategy_execution_gap(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        active_config = report.get("active_strategy_config") or {}
        matrix = active_config.get("token_strategy_policy_matrix") or {}
        execution_summary = {
            row.get("strategy"): int(row.get("total_executions") or 0)
            for row in (report.get("execution_activity") or {}).get("strategy_summary", [])
        }
        findings: list[dict[str, Any]] = []
        for strategy in sorted((active_config.get("signal_rules") or {}).keys()):
            mapped_tokens = [
                token
                for token, strategy_map in matrix.items()
                if (strategy_map.get(strategy) or {}).get("effective_recommendation_status")
                != "blocked"
            ]
            if not mapped_tokens or execution_summary.get(strategy, 0) > 0:
                continue
            findings.append(
                _finding(
                    severity="warning",
                    category="signal_funnel",
                    strategy=strategy,
                    token=None,
                    title=f"{strategy.replace('_', ' ').title()} has no executions",
                    detail=(
                        f"{strategy.replace('_', ' ').title()} is mapped to {len(mapped_tokens)} non-blocked "
                        "tokens in the active policy matrix, but the diagnostics window shows zero executions."
                    ),
                    recommendation=(
                        "Compare emitted signals, risk rejects, and lifecycle failures for this strategy to find "
                        "the stage where it stops converting into orders."
                    ),
                    risk_if_ignored=(
                        "Capital allocation and strategy health can look normal while a strategy is effectively inert."
                    ),
                    confidence_score=0.82,
                    automation_eligibility="future_human_approval_required",
                    evidence={
                        "mapped_token_count": len(mapped_tokens),
                        "mapped_tokens": mapped_tokens[:20],
                        "total_executions": 0,
                    },
                    affected_strategy=strategy,
                    risk_level="warning",
                )
            )
        return findings

    def _check_closed_trade_gap(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        signal_funnel = report.get("signal_funnel") or {}
        signals_emitted = int(signal_funnel.get("signals_emitted") or 0)
        trade_count = int(report.get("trade_count") or 0)
        if signals_emitted < 5 or trade_count > 1:
            return []
        return [
            _finding(
                severity="warning",
                category="strategy_lifecycle",
                strategy=None,
                token=None,
                title="Signals are not converting into closed trades",
                detail=(
                    f"The window shows {signals_emitted} emitted signals but only {trade_count} closed trade "
                    "records, which suggests strategies are failing before exit completion."
                ),
                recommendation=(
                    "Inspect lifecycle traces for execution failures, missing exits, and positions that remain open "
                    "without a closing trade."
                ),
                risk_if_ignored=(
                    "Trade diagnostics will understate real strategy activity and hide where positions get stuck."
                ),
                confidence_score=0.84,
                automation_eligibility="future_human_approval_required",
                evidence={"signals_emitted": signals_emitted, "closed_trade_count": trade_count},
                risk_level="warning",
            )
        ]

    def _check_generic_rejections(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        signal_funnel = report.get("signal_funnel") or {}
        other_count = int(((signal_funnel.get("rejection_reasons") or {}).get("other")) or 0)
        rejected_count = int(signal_funnel.get("signals_rejected") or 0)
        if rejected_count <= 0 or other_count <= 0 or other_count < max(3, rejected_count * 0.25):
            return []
        return [
            _finding(
                severity="warning",
                category="signal_funnel",
                strategy=None,
                token=None,
                title="Too many signal rejections are still classified as other",
                detail=(
                    f"{other_count} of {rejected_count} rejected signals fell into the generic 'other' bucket, "
                    "which makes the signal funnel hard to debug deterministically."
                ),
                recommendation=(
                    "Replace the remaining generic reason mapping with stage-specific reason codes and surface "
                    "those counts in diagnostics."
                ),
                risk_if_ignored=(
                    "Strategies will continue to appear inactive without a clear explanation of why they were blocked."
                ),
                confidence_score=0.88,
                automation_eligibility="not_eligible",
                evidence={"other_rejections": other_count, "signals_rejected": rejected_count},
                risk_level="warning",
            )
        ]

    def _check_token_policy_conflicts(
        self,
        report: dict[str, Any],
        *,
        report_generated_at: datetime | None,
    ) -> list[dict[str, Any]]:
        matrix = (
            (report.get("active_strategy_config") or {}).get("token_strategy_policy_matrix")
        ) or {}
        conflicts: list[dict[str, Any]] = []
        for row in (report.get("execution_activity") or {}).get("execution_details", []):
            token = row.get("token")
            strategy = row.get("strategy")
            if not token or not strategy:
                continue
            policy = ((matrix.get(token) or {}).get(strategy)) or {}
            if policy.get("effective_recommendation_status") != "blocked":
                continue
            conflicts.append(
                {
                    "token": token,
                    "strategy": strategy,
                    "trading_mode": row.get("trading_mode"),
                    "executed_at": row.get("executed_at"),
                    "policy": policy,
                }
            )
        if not conflicts:
            return []
        first = conflicts[0]
        latest_conflict_at = max(
            (_parse_dt(item.get("executed_at")) for item in conflicts),
            default=None,
        )
        conflict_is_recent = _is_recent_issue(
            latest_event_at=latest_conflict_at,
            report_generated_at=report_generated_at,
            threshold_hours=RECENT_EXECUTION_FINDING_HOURS,
        )
        severity = "critical" if conflict_is_recent else "warning"
        return [
            _finding(
                severity=severity,
                category="token_policy",
                strategy=first.get("strategy"),
                token=first.get("token"),
                title=(
                    "Trades executed against a blocked token policy"
                    if conflict_is_recent
                    else "Historical trades conflict with the current blocked token policy"
                ),
                detail=(
                    f"{len(conflicts)} executions were recorded for token/strategy pairs whose effective policy "
                    + (
                        "status is currently blocked."
                        if conflict_is_recent
                        else "status is currently blocked, but the latest conflicting execution is not recent."
                    )
                ),
                recommendation=(
                    "Verify policy enforcement in the strategy and risk pipeline, and compare execution timestamps "
                    "with the active token policy state used during evaluation."
                    if conflict_is_recent
                    else (
                        "Review policy snapshots around the historical executions and rerun the review on fresh data "
                        "to confirm the current pipeline is no longer bypassing blocked pairs."
                    )
                ),
                risk_if_ignored=(
                    "Trades can bypass explicit platform controls and undermine trust in policy enforcement."
                ),
                confidence_score=0.93,
                automation_eligibility="future_human_approval_required",
                evidence={
                    "conflict_count": len(conflicts),
                    "conflict_is_recent": conflict_is_recent,
                    "latest_conflict_at": _iso(latest_conflict_at),
                    "conflicts": conflicts[:20],
                },
                affected_strategy=first.get("strategy"),
                affected_token=first.get("token"),
                risk_level=severity,
            )
        ]

    def _check_position_reconciliation(
        self,
        report: dict[str, Any],
        *,
        report_generated_at: datetime | None,
        window_start: datetime | None,
    ) -> list[dict[str, Any]]:
        executions = defaultdict(list)
        for trade in (report.get("execution_activity") or {}).get("execution_details", []):
            key = (trade.get("strategy"), trade.get("token"), trade.get("trading_mode"))
            executions[key].append(trade)
        mismatches: list[dict[str, Any]] = []
        for position in (report.get("open_positions") or {}).get("positions", []):
            key = (position.get("strategy"), position.get("token"), position.get("trading_mode"))
            scoped = sorted(
                executions.get(key, []),
                key=lambda row: row.get("executed_at") or "",
            )
            if not scoped:
                last_trade_at = _parse_dt(position.get("last_trade_at")) or _parse_dt(
                    position.get("opened_at")
                )
                if (
                    window_start is not None
                    and last_trade_at is not None
                    and last_trade_at < window_start
                ):
                    mismatches.append(
                        {
                            "type": "history_outside_review_window",
                            "position": position,
                        }
                    )
                    continue
                mismatches.append(
                    {
                        "type": "missing_execution_history",
                        "position": position,
                    }
                )
                continue
            latest = scoped[-1]
            latest_qty = _safe_float(latest.get("position_quantity_after"))
            position_qty = _safe_float(position.get("quantity"))
            if latest_qty is None or position_qty is None:
                continue
            if abs(latest_qty - position_qty) > 1e-8:
                mismatches.append(
                    {
                        "type": "quantity_mismatch",
                        "position": position,
                        "latest_execution": latest,
                    }
                )
        if not mismatches:
            return []
        active_mismatches = [
            item for item in mismatches if item["type"] != "history_outside_review_window"
        ]
        severity = (
            "critical"
            if any(item["type"] == "missing_execution_history" for item in active_mismatches)
            else "warning"
        )
        return [
            _finding(
                severity=severity,
                category="position_reconciliation",
                strategy=None,
                token=None,
                title="Open positions do not fully reconcile with execution history",
                detail=(
                    f"{len(mismatches)} open positions are missing matching execution history or differ from the "
                    + (
                        "latest recorded post-trade quantity."
                        if active_mismatches
                        else "latest recorded post-trade quantity, but the gaps appear to be outside the selected review window."
                    )
                ),
                recommendation=(
                    "Run an execution-to-position reconciliation pass and log the exact position, execution, and "
                    "PnL rows that disagree."
                    if active_mismatches
                    else (
                        "Use a broader review window or inspect the dedicated reconciliation report before treating "
                        "these older positions as active accounting drift."
                    )
                ),
                risk_if_ignored=(
                    "Portfolio state can drift away from the trade ledger and distort both exposure and P&L."
                ),
                confidence_score=0.9,
                automation_eligibility="not_eligible",
                evidence={
                    "mismatch_count": len(mismatches),
                    "active_mismatch_count": len(active_mismatches),
                    "report_generated_at": _iso(report_generated_at),
                    "window_start": _iso(window_start),
                    "mismatches": mismatches[:20],
                },
                risk_level=severity,
            )
        ]

    def _check_capital_utilization(self, report: dict[str, Any]) -> list[dict[str, Any]]:
        capital = report.get("capital_utilization") or {}
        avg_deployed = _safe_float(capital.get("avg_capital_deployed_pct"))
        peak_deployed = _safe_float(capital.get("peak_capital_deployed_pct"))
        avg_idle = _safe_float(capital.get("avg_cash_idle_pct"))
        if avg_deployed is None or peak_deployed is None or avg_idle is None:
            return []
        anomaly = (
            peak_deployed + 1e-9 < avg_deployed
            or avg_deployed > 100
            or peak_deployed > 100
            or avg_idle < 0
            or avg_idle > 100
            or abs((avg_deployed + avg_idle) - 100) > 35
        )
        if not anomaly:
            return []
        return [
            _finding(
                severity="warning",
                category="capital_utilization",
                strategy=None,
                token=None,
                title="Capital utilization metrics look inconsistent",
                detail=(
                    "Average deployed capital, peak deployed capital, and idle cash percentages do not line up "
                    "cleanly, which suggests a reporting or reconciliation issue in the capital buckets."
                ),
                recommendation=(
                    "Compare bucket balances and ledger-derived utilization values for the selected window, then "
                    "recompute the summary from the same normalized source."
                ),
                risk_if_ignored=(
                    "Allocation decisions can be made from misleading capital availability and utilization data."
                ),
                confidence_score=0.76,
                automation_eligibility="not_eligible",
                evidence={
                    "avg_capital_deployed_pct": avg_deployed,
                    "peak_capital_deployed_pct": peak_deployed,
                    "avg_cash_idle_pct": avg_idle,
                },
                risk_level="warning",
            )
        ]


class AiDiagnosticReviewService:
    def __init__(self, db: Session, settings: Settings):
        self._db = db
        self._settings = settings
        self._rule_provider = RuleBasedDiagnosticProvider()
        self._ai_provider = OpenAICompatibleDiagnosticProvider(settings)

    def list_snapshots(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._db.scalars(
            select(DiagnosticSnapshot)
            .order_by(DiagnosticSnapshot.generated_at.desc())
            .limit(max(1, min(limit, 100)))
        ).all()
        return [self._snapshot_out(row) for row in rows]

    def create_review(
        self,
        *,
        admin: User,
        request: AiDiagnosticReviewRequest,
    ) -> dict[str, Any]:
        snapshot = (
            self._db.get(DiagnosticSnapshot, request.snapshot_id)
            if request.snapshot_id is not None
            else None
        )
        if snapshot is None:
            snapshot = self._create_snapshot(admin=admin, filters=request.filters)
        now = datetime.now(UTC)
        review = AiDiagnosticReview(
            id=uuid.uuid4(),
            tenant_id=snapshot.tenant_id,
            snapshot_id=snapshot.id,
            status="running",
            model_name=self._rule_provider.name,
            prompt_version=self._settings.ai_diagnostic_prompt_version or PROMPT_VERSION,
            created_by_admin_id=admin.id,
            started_at=now,
            created_at=now,
            updated_at=now,
        )
        self._db.add(review)
        self._db.flush()

        try:
            rule_result = self._rule_provider.review_diagnostics(
                snapshot,
                context={"admin_id": str(admin.id)},
            )
            ai_result = self._ai_provider.review_diagnostics(
                snapshot,
                context={"admin_id": str(admin.id), "review_id": str(review.id)},
            )
            if ai_result and ai_result.get("summary_suffix"):
                rule_result["summary"] = f"{rule_result['summary']}. {ai_result['summary_suffix']}"

            created_at = datetime.now(UTC)
            for finding in rule_result.get("findings", []):
                self._db.add(
                    AiDiagnosticFinding(
                        id=uuid.uuid4(),
                        review_id=review.id,
                        severity=finding["severity"],
                        category=finding["category"],
                        strategy=finding.get("strategy"),
                        token=finding.get("token"),
                        finding_title=finding["finding_title"],
                        finding_detail=finding["finding_detail"],
                        evidence_json=finding.get("evidence_json") or {},
                        recommendation=finding["recommendation"],
                        risk_if_ignored=finding.get("risk_if_ignored"),
                        confidence_score=finding.get("confidence_score"),
                        automation_eligibility=finding.get(
                            "automation_eligibility", "not_eligible"
                        ),
                        status="new",
                        future_config_change_candidate=bool(
                            finding.get("future_config_change_candidate", False)
                        ),
                        proposed_config_change_json=finding.get("proposed_config_change_json"),
                        approval_required=bool(finding.get("approval_required", False)),
                        eligible_for_auto_tune=bool(finding.get("eligible_for_auto_tune", False)),
                        rollback_plan=finding.get("rollback_plan"),
                        expected_impact=finding.get("expected_impact"),
                        risk_level=finding.get("risk_level"),
                        affected_strategy=finding.get("affected_strategy"),
                        affected_token=finding.get("affected_token"),
                        parameter_name=finding.get("parameter_name"),
                        current_value_json=finding.get("current_value_json"),
                        proposed_value_json=finding.get("proposed_value_json"),
                        created_at=created_at,
                        updated_at=created_at,
                    )
                )
            review.status = "completed"
            review.overall_health = rule_result["overall_health"]
            review.confidence_score = rule_result["confidence_score"]
            review.summary = rule_result["summary"]
            review.model_name = (
                ai_result.get("model_name")
                if ai_result and ai_result.get("model_name")
                else rule_result["model_name"]
            )
            review.prompt_version = rule_result["prompt_version"]
            review.completed_at = created_at
            review.updated_at = created_at
            self._db.flush()
        except Exception as exc:
            review.status = "failed"
            review.error_message = str(exc)
            review.completed_at = datetime.now(UTC)
            review.updated_at = review.completed_at
            self._db.flush()

        return {"review_id": str(review.id), "status": review.status}

    def list_reviews(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._db.scalars(
            select(AiDiagnosticReview)
            .options(
                selectinload(AiDiagnosticReview.findings), selectinload(AiDiagnosticReview.snapshot)
            )
            .order_by(AiDiagnosticReview.created_at.desc())
            .limit(max(1, min(limit, 100)))
        ).all()
        return [self._review_summary_out(row) for row in rows]

    def get_review(self, review_id: uuid.UUID) -> dict[str, Any] | None:
        review = self._db.scalar(
            select(AiDiagnosticReview)
            .options(
                selectinload(AiDiagnosticReview.findings), selectinload(AiDiagnosticReview.snapshot)
            )
            .where(AiDiagnosticReview.id == review_id)
        )
        if review is None:
            return None
        return self._review_detail_out(review)

    def update_finding_status(
        self,
        *,
        admin: User,
        finding_id: uuid.UUID,
        status: str,
        note: str | None = None,
    ) -> dict[str, Any] | None:
        normalized = status.strip().lower()
        if normalized not in FINDING_STATUSES:
            raise ValueError("invalid status")
        finding = self._db.get(AiDiagnosticFinding, finding_id)
        if finding is None:
            return None
        previous_status = finding.status
        now = datetime.now(UTC)
        finding.status = normalized
        finding.updated_at = now
        self._db.add(
            AiDiagnosticRecommendationAudit(
                id=uuid.uuid4(),
                finding_id=finding.id,
                action="status_update",
                previous_status=previous_status,
                new_status=normalized,
                admin_id=admin.id,
                note=note,
                created_at=now,
            )
        )
        self._db.flush()
        return self._finding_out(finding)

    def _create_snapshot(
        self, *, admin: User, filters: TradingDiagnosticsFilters
    ) -> DiagnosticSnapshot:
        report = build_trading_diagnostics_report(self._db, self._settings, filters=filters)
        now = datetime.now(UTC)
        snapshot = DiagnosticSnapshot(
            id=uuid.uuid4(),
            tenant_id=primary_tenant_id(self._db, admin),
            generated_at=_parse_dt(report.get("generated_at")) or now,
            trading_mode=filters.normalized_mode,
            strategy_filter=filters.normalized_strategy,
            token_filter=filters.normalized_token,
            days_filter=filters.days,
            raw_json=report,
            created_at=now,
        )
        self._db.add(snapshot)
        self._db.flush()
        return snapshot

    @staticmethod
    def _snapshot_out(snapshot: DiagnosticSnapshot) -> dict[str, Any]:
        return {
            "id": str(snapshot.id),
            "generated_at": snapshot.generated_at.isoformat(),
            "trading_mode": snapshot.trading_mode or "all",
            "strategy_filter": snapshot.strategy_filter or "all",
            "token_filter": snapshot.token_filter,
            "days_filter": snapshot.days_filter,
            "created_at": snapshot.created_at.isoformat(),
        }

    def _review_summary_out(self, review: AiDiagnosticReview) -> dict[str, Any]:
        severity_counts = Counter(finding.severity for finding in review.findings)
        status_counts = Counter(finding.status for finding in review.findings)
        return {
            "id": str(review.id),
            "snapshot_id": str(review.snapshot_id),
            "status": review.status,
            "overall_health": review.overall_health,
            "confidence_score": review.confidence_score,
            "summary": review.summary,
            "model_name": review.model_name,
            "prompt_version": review.prompt_version,
            "started_at": _iso(review.started_at),
            "completed_at": _iso(review.completed_at),
            "error_message": review.error_message,
            "created_at": review.created_at.isoformat(),
            "updated_at": review.updated_at.isoformat(),
            "generated_at": _iso(review.snapshot.generated_at if review.snapshot else None),
            "finding_count": len(review.findings),
            "critical_count": severity_counts.get("critical", 0),
            "warning_count": severity_counts.get("warning", 0),
            "info_count": severity_counts.get("info", 0),
            "status_counts": dict(status_counts),
        }

    def _review_detail_out(self, review: AiDiagnosticReview) -> dict[str, Any]:
        summary = self._review_summary_out(review)
        summary["snapshot"] = self._snapshot_out(review.snapshot)
        summary["snapshot_raw_json"] = review.snapshot.raw_json
        summary["findings"] = [
            self._finding_out(finding)
            for finding in sorted(
                review.findings,
                key=lambda item: (
                    -SEVERITY_ORDER.get(item.severity, 0),
                    item.created_at,
                ),
            )
        ]
        return summary

    @staticmethod
    def _finding_out(finding: AiDiagnosticFinding) -> dict[str, Any]:
        return {
            "id": str(finding.id),
            "review_id": str(finding.review_id),
            "severity": finding.severity,
            "category": finding.category,
            "strategy": finding.strategy,
            "token": finding.token,
            "finding_title": finding.finding_title,
            "finding_detail": finding.finding_detail,
            "evidence_json": finding.evidence_json,
            "recommendation": finding.recommendation,
            "risk_if_ignored": finding.risk_if_ignored,
            "confidence_score": finding.confidence_score,
            "automation_eligibility": finding.automation_eligibility,
            "status": finding.status,
            "future_config_change_candidate": finding.future_config_change_candidate,
            "proposed_config_change_json": finding.proposed_config_change_json,
            "approval_required": finding.approval_required,
            "eligible_for_auto_tune": finding.eligible_for_auto_tune,
            "rollback_plan": finding.rollback_plan,
            "expected_impact": finding.expected_impact,
            "risk_level": finding.risk_level,
            "affected_strategy": finding.affected_strategy,
            "affected_token": finding.affected_token,
            "parameter_name": finding.parameter_name,
            "current_value_json": finding.current_value_json,
            "proposed_value_json": finding.proposed_value_json,
            "created_at": finding.created_at.isoformat(),
            "updated_at": finding.updated_at.isoformat(),
        }


def _finding(
    *,
    severity: str,
    category: str,
    strategy: str | None,
    token: str | None,
    title: str,
    detail: str,
    recommendation: str,
    risk_if_ignored: str,
    confidence_score: float,
    automation_eligibility: str,
    evidence: dict[str, Any],
    risk_level: str,
    affected_strategy: str | None = None,
    affected_token: str | None = None,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "category": category,
        "strategy": strategy,
        "token": token,
        "finding_title": title,
        "finding_detail": detail,
        "evidence_json": evidence,
        "recommendation": recommendation,
        "risk_if_ignored": risk_if_ignored,
        "confidence_score": confidence_score,
        "automation_eligibility": automation_eligibility,
        "future_config_change_candidate": False,
        "proposed_config_change_json": None,
        "approval_required": automation_eligibility != "not_eligible",
        "eligible_for_auto_tune": False,
        "rollback_plan": None,
        "expected_impact": recommendation,
        "risk_level": risk_level,
        "affected_strategy": affected_strategy or strategy,
        "affected_token": affected_token or token,
        "parameter_name": None,
        "current_value_json": None,
        "proposed_value_json": None,
    }


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed
    except ValueError:
        return None


def _hours_between(previous: str, current: str) -> float | None:
    previous_dt = _parse_dt(previous)
    current_dt = _parse_dt(current)
    if previous_dt is None or current_dt is None:
        return None
    return (current_dt - previous_dt).total_seconds() / 3600


def _is_recent_issue(
    *,
    latest_event_at: datetime | None,
    report_generated_at: datetime | None,
    threshold_hours: float,
) -> bool:
    if latest_event_at is None or report_generated_at is None:
        return True
    return (report_generated_at - latest_event_at).total_seconds() / 3600 < threshold_hours


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None
