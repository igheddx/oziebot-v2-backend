from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy.orm import Session

from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.strategy_allocation import (
    StrategyAllocationItem,
    StrategyAllocationPlan,
    StrategyCapitalBucket,
    StrategyCapitalLedger,
)
from oziebot_api.models.user import User
from oziebot_api.models.user_strategy import UserStrategy
from oziebot_api.services.entitlements import has_strategy_entitlement
from oziebot_api.services.strategy_catalog import ensure_platform_strategy_catalog
from oziebot_api.services.tenant_scope import primary_tenant_id

BPS_TOTAL = 10_000

PRESET_WEIGHTS: dict[str, dict[str, int]] = {
    "conservative": {"dca": 4000, "momentum": 3500, "day_trading": 1500, "reversion": 1000},
    "balanced": {"momentum": 4500, "day_trading": 2000, "reversion": 1000, "dca": 2500},
    "aggressive": {"momentum": 5000, "day_trading": 2500, "dca": 2000, "reversion": 500},
}


@dataclass
class AllocationInput:
    strategy_id: str
    allocation_bps: int


class StrategyAllocationError(ValueError):
    pass


class InsufficientBuyingPowerError(StrategyAllocationError):
    pass


def _validate_mode(trading_mode: str) -> None:
    if trading_mode not in {"paper", "live"}:
        raise StrategyAllocationError("trading_mode must be 'paper' or 'live'")


def _recompute_buying_power(bucket: StrategyCapitalBucket) -> None:
    # No leverage for now: buying power equals available cash.
    bucket.available_buying_power_cents = max(0, bucket.available_cash_cents)


def _write_ledger(
    db: Session,
    *,
    bucket: StrategyCapitalBucket,
    event_type: str,
    amount_cents: int,
    before: dict[str, int],
    reference_id: str | None,
    metadata: dict[str, Any] | None,
) -> None:
    row = StrategyCapitalLedger(
        user_id=bucket.user_id,
        strategy_id=bucket.strategy_id,
        trading_mode=bucket.trading_mode,
        event_type=event_type,
        amount_cents=amount_cents,
        before_available_cash_cents=before["available_cash_cents"],
        after_available_cash_cents=bucket.available_cash_cents,
        before_reserved_cash_cents=before["reserved_cash_cents"],
        after_reserved_cash_cents=bucket.reserved_cash_cents,
        before_locked_capital_cents=before["locked_capital_cents"],
        after_locked_capital_cents=bucket.locked_capital_cents,
        before_realized_pnl_cents=before["realized_pnl_cents"],
        after_realized_pnl_cents=bucket.realized_pnl_cents,
        before_unrealized_pnl_cents=before["unrealized_pnl_cents"],
        after_unrealized_pnl_cents=bucket.unrealized_pnl_cents,
        reference_id=reference_id,
        metadata_json=metadata,
        created_at=datetime.now(UTC),
    )
    db.add(row)


def _snapshot(bucket: StrategyCapitalBucket) -> dict[str, int]:
    return {
        "available_cash_cents": bucket.available_cash_cents,
        "reserved_cash_cents": bucket.reserved_cash_cents,
        "locked_capital_cents": bucket.locked_capital_cents,
        "realized_pnl_cents": bucket.realized_pnl_cents,
        "unrealized_pnl_cents": bucket.unrealized_pnl_cents,
    }


class StrategyAllocationService:
    @staticmethod
    def enabled_strategy_ids(db: Session, *, user_id: UUID) -> list[str]:
        rows = (
            db.query(UserStrategy)
            .filter(UserStrategy.user_id == user_id, UserStrategy.is_enabled == True)  # noqa: E712
            .order_by(UserStrategy.strategy_id)
            .all()
        )
        return [row.strategy_id for row in rows]

    @staticmethod
    def _allowed_strategy_ids(db: Session, *, user_id: UUID) -> set[str]:
        ensure_platform_strategy_catalog(db)
        configured_enabled = {
            row.strategy_id
            for row in db.query(UserStrategy)
            .filter(UserStrategy.user_id == user_id, UserStrategy.is_enabled == True)  # noqa: E712
            .all()
        }

        user = db.get(User, user_id)
        if user is None:
            return configured_enabled

        allowed = set(configured_enabled)
        all_catalog = db.query(PlatformStrategy).order_by(PlatformStrategy.slug).all()
        tenant_id = primary_tenant_id(db, user)
        if tenant_id is None and user.is_root_admin:
            allowed.update(row.slug for row in all_catalog)
            return allowed

        if tenant_id is None:
            return allowed

        for row in all_catalog:
            if has_strategy_entitlement(db, tenant_id, row.slug):
                allowed.add(row.slug)
        return allowed

    @staticmethod
    def derive_live_allocations(db: Session, *, user_id: UUID) -> list[AllocationInput]:
        enabled = StrategyAllocationService.enabled_strategy_ids(db, user_id=user_id)
        if not enabled:
            raise StrategyAllocationError("No enabled strategies configured")

        plan = StrategyAllocationService.get_plan(db, user_id=user_id, trading_mode="live")
        if plan is None or not plan.items:
            equal_bps = BPS_TOTAL // len(enabled)
            remainder = BPS_TOTAL - (equal_bps * len(enabled))
            allocations: list[AllocationInput] = []
            for strategy_id in enabled:
                allocation_bps = equal_bps + (1 if remainder > 0 else 0)
                if remainder > 0:
                    remainder -= 1
                allocations.append(
                    AllocationInput(strategy_id=strategy_id, allocation_bps=allocation_bps)
                )
            return allocations

        current_bps = {
            item.strategy_id: max(0, item.allocation_bps)
            for item in plan.items
            if item.strategy_id in enabled
        }
        total_bps = sum(current_bps.values())
        if total_bps <= 0:
            return StrategyAllocationService.derive_live_allocations_without_plan(enabled)

        normalized = {
            strategy_id: (current_bps.get(strategy_id, 0) * BPS_TOTAL) // total_bps
            for strategy_id in enabled
        }
        assigned = sum(normalized.values())
        remainder = BPS_TOTAL - assigned
        order = sorted(
            enabled, key=lambda strategy_id: current_bps.get(strategy_id, 0), reverse=True
        )
        idx = 0
        while remainder > 0 and order:
            normalized[order[idx % len(order)]] += 1
            remainder -= 1
            idx += 1

        return [
            AllocationInput(strategy_id=strategy_id, allocation_bps=normalized[strategy_id])
            for strategy_id in enabled
        ]

    @staticmethod
    def derive_live_allocations_without_plan(strategy_ids: list[str]) -> list[AllocationInput]:
        equal_bps = BPS_TOTAL // len(strategy_ids)
        remainder = BPS_TOTAL - (equal_bps * len(strategy_ids))
        allocations: list[AllocationInput] = []
        for strategy_id in strategy_ids:
            allocation_bps = equal_bps + (1 if remainder > 0 else 0)
            if remainder > 0:
                remainder -= 1
            allocations.append(
                AllocationInput(strategy_id=strategy_id, allocation_bps=allocation_bps)
            )
        return allocations

    @staticmethod
    def guided_preset_allocations(
        preset_name: str,
        strategy_ids: list[str],
    ) -> list[AllocationInput]:
        preset = PRESET_WEIGHTS.get(preset_name)
        if preset is None:
            raise StrategyAllocationError("Unknown preset")

        available = [s for s in strategy_ids if s in preset]
        if not available:
            raise StrategyAllocationError("No strategies available for selected preset")

        total = sum(preset[s] for s in available)
        raw = [(s, (preset[s] * BPS_TOTAL) // total) for s in available]
        assigned = sum(v for _, v in raw)

        # Allocate rounding remainder to highest-weight strategies first.
        remainder = BPS_TOTAL - assigned
        order = sorted(available, key=lambda s: preset[s], reverse=True)
        out = {s: bps for s, bps in raw}
        idx = 0
        while remainder > 0:
            out[order[idx % len(order)]] += 1
            remainder -= 1
            idx += 1

        return [AllocationInput(strategy_id=s, allocation_bps=out[s]) for s in sorted(out.keys())]

    @staticmethod
    def apply_allocations(
        db: Session,
        *,
        user_id: UUID,
        trading_mode: str,
        total_capital_cents: int,
        allocation_mode: str,
        allocations: list[AllocationInput],
        preset_name: str | None,
    ) -> StrategyAllocationPlan:
        _validate_mode(trading_mode)
        if allocation_mode not in {"manual", "guided"}:
            raise StrategyAllocationError("allocation_mode must be 'manual' or 'guided'")
        if total_capital_cents < 0:
            raise StrategyAllocationError("total_capital_cents must be >= 0")
        if not allocations:
            raise StrategyAllocationError("At least one allocation is required")

        configured = StrategyAllocationService._allowed_strategy_ids(db, user_id=user_id)
        for alloc in allocations:
            if alloc.strategy_id not in configured:
                raise StrategyAllocationError(
                    f"Strategy '{alloc.strategy_id}' is not assigned to user"
                )

        bps_sum = sum(a.allocation_bps for a in allocations)
        if bps_sum != BPS_TOTAL:
            raise StrategyAllocationError("Allocation percentages must sum to 10000 bps")

        now = datetime.now(UTC)
        plan = (
            db.query(StrategyAllocationPlan)
            .filter(
                StrategyAllocationPlan.user_id == user_id,
                StrategyAllocationPlan.trading_mode == trading_mode,
            )
            .first()
        )
        if plan is None:
            plan = StrategyAllocationPlan(
                user_id=user_id,
                trading_mode=trading_mode,
                allocation_mode=allocation_mode,
                preset_name=preset_name,
                total_capital_cents=total_capital_cents,
                created_at=now,
                updated_at=now,
            )
            db.add(plan)
            db.flush()
        else:
            plan.allocation_mode = allocation_mode
            plan.preset_name = preset_name
            plan.total_capital_cents = total_capital_cents
            plan.updated_at = now

        existing_items = {item.strategy_id: item for item in plan.items}

        # integer split with deterministic remainder handling.
        raw_values = {
            a.strategy_id: (total_capital_cents * a.allocation_bps) // BPS_TOTAL
            for a in allocations
        }
        assigned = sum(raw_values.values())
        remainder = total_capital_cents - assigned
        for alloc in sorted(allocations, key=lambda x: x.allocation_bps, reverse=True):
            if remainder <= 0:
                break
            raw_values[alloc.strategy_id] += 1
            remainder -= 1

        seen = set()
        for alloc in allocations:
            seen.add(alloc.strategy_id)
            item = existing_items.get(alloc.strategy_id)
            assigned_capital_cents = raw_values[alloc.strategy_id]
            if item is None:
                item = StrategyAllocationItem(
                    plan_id=plan.id,
                    strategy_id=alloc.strategy_id,
                    allocation_bps=alloc.allocation_bps,
                    assigned_capital_cents=assigned_capital_cents,
                    created_at=now,
                    updated_at=now,
                )
                db.add(item)
            else:
                item.allocation_bps = alloc.allocation_bps
                item.assigned_capital_cents = assigned_capital_cents
                item.updated_at = now

            bucket = (
                db.query(StrategyCapitalBucket)
                .filter(
                    StrategyCapitalBucket.user_id == user_id,
                    StrategyCapitalBucket.strategy_id == alloc.strategy_id,
                    StrategyCapitalBucket.trading_mode == trading_mode,
                )
                .first()
            )

            if bucket is None:
                bucket = StrategyCapitalBucket(
                    user_id=user_id,
                    strategy_id=alloc.strategy_id,
                    trading_mode=trading_mode,
                    assigned_capital_cents=assigned_capital_cents,
                    available_cash_cents=assigned_capital_cents,
                    reserved_cash_cents=0,
                    locked_capital_cents=0,
                    realized_pnl_cents=0,
                    unrealized_pnl_cents=0,
                    available_buying_power_cents=assigned_capital_cents,
                    version=1,
                    created_at=now,
                    updated_at=now,
                )
                db.add(bucket)
            else:
                bucket.assigned_capital_cents = assigned_capital_cents
                # available = assigned + realized - reserved - locked
                bucket.available_cash_cents = (
                    bucket.assigned_capital_cents
                    + bucket.realized_pnl_cents
                    - bucket.reserved_cash_cents
                    - bucket.locked_capital_cents
                )
                if bucket.available_cash_cents < 0:
                    bucket.available_cash_cents = 0
                _recompute_buying_power(bucket)
                bucket.version += 1
                bucket.updated_at = now

        for strategy_id, item in existing_items.items():
            if strategy_id not in seen:
                db.delete(item)

        db.flush()
        return plan

    @staticmethod
    def reserve_capital(
        db: Session,
        *,
        user_id: UUID,
        strategy_id: str,
        trading_mode: str,
        amount_cents: int,
        reference_id: str,
    ) -> StrategyCapitalBucket:
        _validate_mode(trading_mode)
        if amount_cents <= 0:
            raise StrategyAllocationError("amount_cents must be > 0")

        bucket = (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.strategy_id == strategy_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .first()
        )
        if bucket is None:
            raise StrategyAllocationError("Capital bucket not found")

        if (
            amount_cents > bucket.available_cash_cents
            or amount_cents > bucket.available_buying_power_cents
        ):
            raise InsufficientBuyingPowerError("Insufficient buying power for strategy bucket")

        before = _snapshot(bucket)
        bucket.available_cash_cents -= amount_cents
        bucket.reserved_cash_cents += amount_cents
        _recompute_buying_power(bucket)
        bucket.version += 1
        bucket.updated_at = datetime.now(UTC)

        _write_ledger(
            db,
            bucket=bucket,
            event_type="reserve",
            amount_cents=amount_cents,
            before=before,
            reference_id=reference_id,
            metadata=None,
        )
        db.flush()
        return bucket

    @staticmethod
    def release_reserved_capital(
        db: Session,
        *,
        user_id: UUID,
        strategy_id: str,
        trading_mode: str,
        amount_cents: int,
        reference_id: str,
    ) -> StrategyCapitalBucket:
        _validate_mode(trading_mode)
        if amount_cents <= 0:
            raise StrategyAllocationError("amount_cents must be > 0")

        bucket = (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.strategy_id == strategy_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .first()
        )
        if bucket is None:
            raise StrategyAllocationError("Capital bucket not found")
        if amount_cents > bucket.reserved_cash_cents:
            raise StrategyAllocationError("Cannot release more than reserved cash")

        before = _snapshot(bucket)
        bucket.reserved_cash_cents -= amount_cents
        bucket.available_cash_cents += amount_cents
        _recompute_buying_power(bucket)
        bucket.version += 1
        bucket.updated_at = datetime.now(UTC)

        _write_ledger(
            db,
            bucket=bucket,
            event_type="release",
            amount_cents=amount_cents,
            before=before,
            reference_id=reference_id,
            metadata=None,
        )
        db.flush()
        return bucket

    @staticmethod
    def lock_reserved_capital(
        db: Session,
        *,
        user_id: UUID,
        strategy_id: str,
        trading_mode: str,
        amount_cents: int,
        reference_id: str,
    ) -> StrategyCapitalBucket:
        _validate_mode(trading_mode)
        if amount_cents <= 0:
            raise StrategyAllocationError("amount_cents must be > 0")

        bucket = (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.strategy_id == strategy_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .first()
        )
        if bucket is None:
            raise StrategyAllocationError("Capital bucket not found")
        if amount_cents > bucket.reserved_cash_cents:
            raise StrategyAllocationError("Cannot lock more than reserved cash")

        before = _snapshot(bucket)
        bucket.reserved_cash_cents -= amount_cents
        bucket.locked_capital_cents += amount_cents
        _recompute_buying_power(bucket)
        bucket.version += 1
        bucket.updated_at = datetime.now(UTC)

        _write_ledger(
            db,
            bucket=bucket,
            event_type="lock",
            amount_cents=amount_cents,
            before=before,
            reference_id=reference_id,
            metadata=None,
        )
        db.flush()
        return bucket

    @staticmethod
    def settle_position(
        db: Session,
        *,
        user_id: UUID,
        strategy_id: str,
        trading_mode: str,
        released_locked_cents: int,
        realized_pnl_delta_cents: int,
        reference_id: str,
    ) -> StrategyCapitalBucket:
        _validate_mode(trading_mode)
        if released_locked_cents < 0:
            raise StrategyAllocationError("released_locked_cents must be >= 0")

        bucket = (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.strategy_id == strategy_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .first()
        )
        if bucket is None:
            raise StrategyAllocationError("Capital bucket not found")
        if released_locked_cents > bucket.locked_capital_cents:
            raise StrategyAllocationError("Cannot settle more than locked capital")

        before = _snapshot(bucket)
        bucket.locked_capital_cents -= released_locked_cents
        bucket.realized_pnl_cents += realized_pnl_delta_cents
        bucket.available_cash_cents += released_locked_cents + realized_pnl_delta_cents
        if bucket.available_cash_cents < 0:
            bucket.available_cash_cents = 0
        _recompute_buying_power(bucket)
        bucket.version += 1
        bucket.updated_at = datetime.now(UTC)

        _write_ledger(
            db,
            bucket=bucket,
            event_type="settle",
            amount_cents=released_locked_cents,
            before=before,
            reference_id=reference_id,
            metadata={"realized_pnl_delta_cents": realized_pnl_delta_cents},
        )
        db.flush()
        return bucket

    @staticmethod
    def mark_unrealized_pnl(
        db: Session,
        *,
        user_id: UUID,
        strategy_id: str,
        trading_mode: str,
        unrealized_pnl_cents: int,
        reference_id: str,
    ) -> StrategyCapitalBucket:
        _validate_mode(trading_mode)

        bucket = (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.strategy_id == strategy_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .first()
        )
        if bucket is None:
            raise StrategyAllocationError("Capital bucket not found")

        before = _snapshot(bucket)
        bucket.unrealized_pnl_cents = unrealized_pnl_cents
        bucket.version += 1
        bucket.updated_at = datetime.now(UTC)

        _write_ledger(
            db,
            bucket=bucket,
            event_type="mark_unrealized",
            amount_cents=unrealized_pnl_cents,
            before=before,
            reference_id=reference_id,
            metadata=None,
        )
        db.flush()
        return bucket

    @staticmethod
    def list_buckets(
        db: Session, *, user_id: UUID, trading_mode: str
    ) -> list[StrategyCapitalBucket]:
        _validate_mode(trading_mode)
        return (
            db.query(StrategyCapitalBucket)
            .filter(
                StrategyCapitalBucket.user_id == user_id,
                StrategyCapitalBucket.trading_mode == trading_mode,
            )
            .order_by(StrategyCapitalBucket.strategy_id)
            .all()
        )

    @staticmethod
    def get_plan(db: Session, *, user_id: UUID, trading_mode: str) -> StrategyAllocationPlan | None:
        _validate_mode(trading_mode)
        return (
            db.query(StrategyAllocationPlan)
            .filter(
                StrategyAllocationPlan.user_id == user_id,
                StrategyAllocationPlan.trading_mode == trading_mode,
            )
            .first()
        )
