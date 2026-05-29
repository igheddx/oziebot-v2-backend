from __future__ import annotations

from typing import Any


def build_workspace_mastery_insights(dashboard_payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "matrix_count": dashboard_payload.get("matrix_count", 0),
        "active_evaluation_count": dashboard_payload.get("active_evaluation_count", 0),
        "reteach_recommended_count": len(dashboard_payload.get("reteach_recommended_standards", [])),
        "low_mastery_alert_count": len(dashboard_payload.get("low_mastery_alerts", [])),
        "unassessed_standard_count": len(dashboard_payload.get("unassessed_standards", [])),
        "improving_standard_count": len(dashboard_payload.get("improving_standards", [])),
        "declining_standard_count": len(dashboard_payload.get("declining_standards", [])),
        "reteach_recommended_standards": dashboard_payload.get("reteach_recommended_standards", [])[:8],
        "standards_needing_attention": dashboard_payload.get("standards_needing_attention", [])[:8],
        "low_mastery_alerts": dashboard_payload.get("low_mastery_alerts", [])[:8],
        "improving_standards": dashboard_payload.get("improving_standards", [])[:8],
        "declining_standards": dashboard_payload.get("declining_standards", [])[:8],
        "unassessed_standards": dashboard_payload.get("unassessed_standards", [])[:8],
        "class_snapshots": dashboard_payload.get("matrix_snapshots", [])[:8],
    }
