"""Strategy entitlements and trading access (trial, subscription, trading_mode)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import delete, or_, select, update
from sqlalchemy.orm import Session

from oziebot_api.models.platform_setting import PlatformSetting
from oziebot_api.models.platform_strategy import PlatformStrategy
from oziebot_api.models.stripe_subscription import StripeSubscription
from oziebot_api.models.tenant import Tenant
from oziebot_api.models.tenant_entitlement import TenantEntitlement
from oziebot_api.services.coinbase import coinbase_valid_for_live_trading
from oziebot_domain.trading_mode import TradingMode

SETTING_ALLOW_PAPER_WITHOUT_SUBSCRIPTION = "billing.allow_paper_without_subscription"


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt


def allow_paper_without_subscription(db: Session) -> bool:
    row = db.get(PlatformSetting, SETTING_ALLOW_PAPER_WITHOUT_SUBSCRIPTION)
    if row is None:
        return True
    v = row.value
    if isinstance(v, dict):
        return bool(v.get("enabled", True))
    return True


def is_trial_active(db: Session, tenant_id: uuid.UUID) -> bool:
    t = db.get(Tenant, tenant_id)
    if t is None or t.trial_ends_at is None or t.trial_started_at is None:
        return False
    now = datetime.now(UTC)
    start = _as_utc(t.trial_started_at)
    end = _as_utc(t.trial_ends_at)
    return start <= now < end


def has_active_subscription(db: Session, tenant_id: uuid.UUID) -> bool:
    row = db.scalars(
        select(StripeSubscription)
        .where(
            StripeSubscription.tenant_id == tenant_id,
            StripeSubscription.status.in_(("active", "trialing")),
        )
        .limit(1)
    ).first()
    return row is not None


def sync_trial_entitlement_rows(db: Session, tenant_id: uuid.UUID) -> None:
    """Disable trial entitlement rows when trial window has ended."""
    t = db.get(Tenant, tenant_id)
    if t is None or t.trial_ends_at is None:
        return
    now = datetime.now(UTC)
    if now < _as_utc(t.trial_ends_at):
        return
    db.execute(
        update(TenantEntitlement)
        .where(
            TenantEntitlement.tenant_id == tenant_id,
            TenantEntitlement.source == "trial",
        )
        .values(is_active=False, updated_at=now)
    )


def has_live_trading_billing(db: Session, tenant_id: uuid.UUID) -> bool:
    sync_trial_entitlement_rows(db, tenant_id)
    return is_trial_active(db, tenant_id) or has_active_subscription(db, tenant_id)


def has_strategy_entitlement(db: Session, tenant_id: uuid.UUID, strategy_slug: str) -> bool:
    sync_trial_entitlement_rows(db, tenant_id)
    now = datetime.now(UTC)
    strat = db.scalar(
        select(PlatformStrategy).where(PlatformStrategy.slug == strategy_slug.strip().lower())
    )
    if strat is None:
        return False
    q = select(TenantEntitlement.id).where(
        TenantEntitlement.tenant_id == tenant_id,
        TenantEntitlement.is_active.is_(True),
        TenantEntitlement.valid_from <= now,
        or_(
            TenantEntitlement.valid_until.is_(None),
            TenantEntitlement.valid_until > now,
        ),
        or_(
            TenantEntitlement.platform_strategy_id.is_(None),
            TenantEntitlement.platform_strategy_id == strat.id,
        ),
    )
    return db.scalar(q.limit(1)) is not None


def can_use_trading_for_mode(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    trading_mode: TradingMode,
) -> tuple[bool, str | None]:
    """Paper works without exchange credentials. Live requires billing + valid Coinbase connection."""
    if trading_mode == TradingMode.PAPER:
        if allow_paper_without_subscription(db):
            return True, None
        if has_live_trading_billing(db, tenant_id):
            return True, None
        return False, "Paper trading requires an active trial or subscription"
    if not has_live_trading_billing(db, tenant_id):
        return False, "Active subscription or trial required for live trading"
    if not coinbase_valid_for_live_trading(db, tenant_id):
        return (
            False,
            "Valid Coinbase connection with trading and balance permissions is required for live trading",
        )
    return True, None


def revoke_subscription_entitlements(db: Session, stripe_subscription_row_id: uuid.UUID) -> None:
    db.execute(
        delete(TenantEntitlement).where(
            TenantEntitlement.stripe_subscription_id == stripe_subscription_row_id,
        )
    )
