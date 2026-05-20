from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP

MAX_COINBASE_DECIMAL_PLACES = 8
COINBASE_MIN_NOTIONAL_USD = Decimal("1.00")
COINBASE_QUANTITY_STEP = Decimal("0.00000001")


def money_to_cents(value: Decimal) -> int:
    return int((value * Decimal("100")).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def normalize_coinbase_quantity(quantity: Decimal) -> Decimal:
    return quantity.quantize(COINBASE_QUANTITY_STEP, rounding=ROUND_DOWN)


def decimal_places(value: Decimal) -> int:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return 0
    return -normalized.as_tuple().exponent


@dataclass(frozen=True)
class CoinbaseOrderValidationResult:
    normalized_quantity: Decimal
    quantity_adjusted: bool
    requested_notional: Decimal | None
    reserve_cents: int | None
    failure_code: str | None = None
    failure_detail: str | None = None


def validate_coinbase_order(
    *,
    quantity: Decimal,
    side: str,
    price_hint: Decimal | None,
) -> CoinbaseOrderValidationResult:
    if not quantity.is_finite():
        return CoinbaseOrderValidationResult(
            normalized_quantity=quantity,
            quantity_adjusted=False,
            requested_notional=None,
            reserve_cents=None,
            failure_code="non_finite_quantity",
            failure_detail="Execution rejected: quantity must be finite",
        )
    if quantity <= 0:
        return CoinbaseOrderValidationResult(
            normalized_quantity=quantity,
            quantity_adjusted=False,
            requested_notional=None,
            reserve_cents=None,
            failure_code="invalid_quantity",
            failure_detail="Execution rejected: quantity must be greater than zero",
        )

    normalized_quantity = normalize_coinbase_quantity(quantity)
    quantity_adjusted = normalized_quantity != quantity
    if normalized_quantity <= 0:
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=None,
            reserve_cents=None,
            failure_code="quantity_rounded_to_zero",
            failure_detail=(
                "Execution rejected: requested quantity rounded to zero after Coinbase "
                f"{MAX_COINBASE_DECIMAL_PLACES}-decimal precision checks"
            ),
        )

    if side != "buy":
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=None,
            reserve_cents=None,
        )

    if price_hint is None:
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=None,
            reserve_cents=None,
            failure_code="missing_price_hint",
            failure_detail="Execution rejected: missing price hint for buy execution",
        )
    if not price_hint.is_finite():
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=None,
            reserve_cents=None,
            failure_code="non_finite_price_hint",
            failure_detail="Execution rejected: price hint must be finite",
        )
    if price_hint <= 0:
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=None,
            reserve_cents=None,
            failure_code="invalid_price_hint",
            failure_detail="Execution rejected: price hint must be greater than zero",
        )

    requested_notional = normalized_quantity * price_hint
    if not requested_notional.is_finite():
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=requested_notional,
            reserve_cents=None,
            failure_code="non_finite_notional",
            failure_detail="Execution rejected: requested notional must be finite",
        )
    if requested_notional <= 0:
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=requested_notional,
            reserve_cents=None,
            failure_code="invalid_notional",
            failure_detail="Execution rejected: requested notional must be greater than zero",
        )

    reserve_cents = money_to_cents(requested_notional)
    if reserve_cents <= 0:
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=requested_notional,
            reserve_cents=reserve_cents,
            failure_code="notional_rounded_to_zero",
            failure_detail=(
                "Execution rejected: requested notional rounded to zero cents after "
                "precision checks"
            ),
        )
    if reserve_cents < money_to_cents(COINBASE_MIN_NOTIONAL_USD):
        return CoinbaseOrderValidationResult(
            normalized_quantity=normalized_quantity,
            quantity_adjusted=quantity_adjusted,
            requested_notional=requested_notional,
            reserve_cents=reserve_cents,
            failure_code="below_minimum_notional",
            failure_detail=(
                "Execution rejected: requested notional is below Coinbase minimum "
                f"order size of ${COINBASE_MIN_NOTIONAL_USD}"
            ),
        )

    return CoinbaseOrderValidationResult(
        normalized_quantity=normalized_quantity,
        quantity_adjusted=quantity_adjusted,
        requested_notional=requested_notional,
        reserve_cents=reserve_cents,
    )
