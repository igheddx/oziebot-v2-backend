from decimal import Decimal

from oziebot_common.dynamic_sizing import (
    DynamicSizingInput,
    calculate_dynamic_trade_size,
)


def _input(**overrides) -> DynamicSizingInput:
    base = DynamicSizingInput(
        confidence=Decimal("0.8"),
        total_capital_usd=Decimal("5000"),
        assigned_capital_usd=Decimal("2000"),
        available_buying_power_usd=Decimal("2000"),
        reserved_capital_usd=Decimal("0"),
        locked_capital_usd=Decimal("0"),
        current_position_usd=Decimal("0"),
        position_size_fraction=Decimal("0.25"),
        buy_amount_usd=Decimal("0"),
        min_trade_usd=Decimal("50"),
        max_trade_usd=Decimal("1000"),
        max_position_usd=Decimal("1000"),
        target_bucket_utilization_pct=Decimal("0.60"),
        dynamic_sizing_enabled=True,
        drawdown_size_reduction_enabled=True,
        drawdown_reduction_multiplier=Decimal("0.75"),
        realized_drawdown_pct=Decimal("0"),
        daily_loss_pct=Decimal("0"),
    )
    return DynamicSizingInput(**{**base.__dict__, **overrides})


def test_dynamic_sizing_increases_with_larger_bucket():
    smaller = calculate_dynamic_trade_size(
        _input(assigned_capital_usd=Decimal("500"), total_capital_usd=Decimal("1000"))
    )
    larger = calculate_dynamic_trade_size(_input())

    assert larger.final_trade_usd > smaller.final_trade_usd


def test_dynamic_sizing_caps_to_max_position_usd():
    result = calculate_dynamic_trade_size(
        _input(max_position_usd=Decimal("300"), max_trade_usd=Decimal("1000"))
    )

    assert result.final_trade_usd == Decimal("300.00")
    assert "max_position_usd_cap" in result.reduction_reasons


def test_dynamic_sizing_respects_bucket_buying_power():
    result = calculate_dynamic_trade_size(
        _input(available_buying_power_usd=Decimal("120"), max_trade_usd=Decimal("500"))
    )

    assert result.final_trade_usd == Decimal("120.00")
    assert "bucket_buying_power_cap" in result.reduction_reasons


def test_dynamic_sizing_respects_token_position_override():
    result = calculate_dynamic_trade_size(
        _input(
            max_trade_usd=Decimal("500"),
            max_position_usd=Decimal("500"),
            token_policy_max_position_pct_override=Decimal("0.05"),
        )
    )

    assert result.final_trade_usd == Decimal("250.00")
    assert "token_policy_position_cap" in result.reduction_reasons


def test_dynamic_sizing_reduces_size_during_drawdown():
    normal = calculate_dynamic_trade_size(_input(max_trade_usd=Decimal("1000")))
    reduced = calculate_dynamic_trade_size(
        _input(
            max_trade_usd=Decimal("1000"),
            realized_drawdown_pct=Decimal("0.06"),
        )
    )

    assert reduced.final_trade_usd < normal.final_trade_usd
    assert reduced.drawdown_state == "elevated"
    assert reduced.drawdown_multiplier_applied == Decimal("0.7500")


def test_dca_dynamic_sizing_scales_to_bucket_gap():
    result = calculate_dynamic_trade_size(
        _input(
            confidence=Decimal("0.9"),
            assigned_capital_usd=Decimal("1250"),
            buy_amount_usd=Decimal("100"),
            position_size_fraction=Decimal("0"),
            max_trade_usd=Decimal("150"),
            max_position_usd=Decimal("0"),
            target_bucket_utilization_pct=Decimal("0.50"),
        )
    )

    assert result.final_trade_usd == Decimal("150.00")
