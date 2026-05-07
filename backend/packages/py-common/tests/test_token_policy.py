from __future__ import annotations

from oziebot_common.token_policy import resolve_effective_token_policy


def test_missing_policy_defaults_to_allowed() -> None:
    resolved = resolve_effective_token_policy(None)

    assert resolved["effective_recommendation_status"] == "allowed"
    assert str(resolved["size_multiplier"]) == "1"


def test_missing_policy_can_default_to_blocked() -> None:
    resolved = resolve_effective_token_policy(None, missing_policy_behavior="blocked")

    assert resolved["effective_recommendation_status"] == "blocked"
    assert (
        resolved["effective_recommendation_reason"]
        == "No token-strategy policy configured"
    )
    assert str(resolved["size_multiplier"]) == "0"


def test_blocked_policy_stays_blocked_in_paper_mode() -> None:
    resolved = resolve_effective_token_policy(
        {
            "admin_enabled": True,
            "recommendation_status": "blocked",
            "recommendation_reason": "blocked by admin mapping",
        },
        trading_mode="paper",
    )

    assert resolved["effective_recommendation_status"] == "blocked"
    assert str(resolved["size_multiplier"]) == "0"


def test_discouraged_policy_defaults_to_half_size() -> None:
    resolved = resolve_effective_token_policy(
        {
            "admin_enabled": True,
            "recommendation_status": "discouraged",
            "recommendation_reason": "thin liquidity",
        }
    )

    assert resolved["effective_recommendation_status"] == "discouraged"
    assert str(resolved["size_multiplier"]) == "0.50"
