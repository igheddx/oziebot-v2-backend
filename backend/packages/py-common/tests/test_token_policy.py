from __future__ import annotations

from oziebot_common.token_policy import resolve_effective_token_policy


def test_paper_mode_softens_computed_block_to_discouraged() -> None:
    resolved = resolve_effective_token_policy(
        {
            "admin_enabled": True,
            "recommendation_status": "blocked",
            "recommendation_reason": "Favors trend strength, liquidity, and relative volume",
            "recommendation_status_override": None,
        },
        trading_mode="paper",
    )

    assert resolved["computed_recommendation_status"] == "blocked"
    assert resolved["effective_recommendation_status"] == "discouraged"
    assert str(resolved["size_multiplier"]) == "0.60"


def test_paper_mode_preserves_explicit_admin_block_override() -> None:
    resolved = resolve_effective_token_policy(
        {
            "admin_enabled": True,
            "recommendation_status": "allowed",
            "recommendation_reason": "computed allowed",
            "recommendation_status_override": "blocked",
            "recommendation_reason_override": "admin blocked",
        },
        trading_mode="paper",
    )

    assert resolved["effective_recommendation_status"] == "blocked"
    assert str(resolved["size_multiplier"]) == "0"
