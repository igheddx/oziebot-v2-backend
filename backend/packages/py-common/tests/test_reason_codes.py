from oziebot_common.reason_codes import (
    normalize_reason_code,
    summarize_rejection_reason,
    top_reason_rows,
)


def test_normalize_reason_code_maps_known_aliases() -> None:
    assert normalize_reason_code("token_strategy_policy") == "policy_blocked"
    assert normalize_reason_code("below min_confidence") == "insufficient_confidence"
    assert normalize_reason_code("max_position_usd exceeded") == "max_exposure_reached"
    assert (
        normalize_reason_code("outside liquid-hours window")
        == "liquidity_window_closed"
    )
    assert (
        normalize_reason_code("quantity_precision_exceeded")
        == "execution_validation_failed"
    )


def test_normalize_reason_code_uses_reason_detail_fallback() -> None:
    assert (
        normalize_reason_code(
            None,
            reason_detail="Execution rejected: insufficient available cash or buying power",
        )
        == "insufficient_buying_power"
    )


def test_summarize_rejection_reason_and_top_rows() -> None:
    assert summarize_rejection_reason("policy_blocked") == "token_strategy_policy"
    assert summarize_rejection_reason("cooldown_active") == "cooldown"
    assert top_reason_rows(
        {
            "policy_blocked": 1,
            "insufficient_confidence": 3,
            "cooldown_active": 2,
        }
    ) == [
        {"reason": "insufficient_confidence", "count": 3},
        {"reason": "cooldown_active", "count": 2},
        {"reason": "policy_blocked", "count": 1},
    ]
