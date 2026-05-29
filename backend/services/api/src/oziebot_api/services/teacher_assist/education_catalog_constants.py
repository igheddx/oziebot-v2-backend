"""Education catalog validation constants."""

from __future__ import annotations

from typing import Literal

ObjectiveTypeLiteral = Literal["TEKS", "CommonCore", "DistrictObjective", "Custom"]
CoverageTypeLiteral = Literal["required", "optional", "enrichment"]
CatalogResourceTypeLiteral = Literal["curriculum", "textbook", "reference"]

OBJECTIVE_TYPES = frozenset({"TEKS", "CommonCore", "DistrictObjective", "Custom"})
COVERAGE_TYPES = frozenset({"required", "optional", "enrichment"})
CATALOG_RESOURCE_TYPES = frozenset({"curriculum", "textbook", "reference"})


def validate_objective_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in OBJECTIVE_TYPES:
        raise ValueError(f"Invalid objective type: {value}")
    return normalized


def validate_coverage_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in COVERAGE_TYPES:
        raise ValueError(f"Invalid coverage type: {value}")
    return normalized


def validate_catalog_resource_type(value: str) -> str:
    normalized = value.strip()
    if normalized not in CATALOG_RESOURCE_TYPES:
        raise ValueError(f"Invalid resource type: {value}")
    return normalized
