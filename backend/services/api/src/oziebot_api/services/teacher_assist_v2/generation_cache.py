"""Build-system utilities for incremental and cached package generation.

There are three cache layers:

  Layer 1 — curriculum (tenant-wide, cross-package):
    Stores curriculum_sequence_plan + instructional_resource_bank.
    Key = hash(pacing_guide_ids, doc_content_hashes, curriculum_prompt_version)
    If the pacing guide and curriculum documents have not changed, any package
    in this tenant can skip Phase 0a entirely and load from cache.

  Layer 2 — planning (package-specific, but reusable when inputs match):
    Stores instructional_design_plan + strand_learning_journeys.
    Key = hash(curriculum_key, teacher_notes_hash, delivery_profile_hash,
                lost_days, planning_prompt_version)
    If teacher notes and delivery profile haven't changed, the design plan
    and strand journeys are loaded from cache (skip Phase 0c/d/f).

  Layer 3 — artifact (per-artifact dirty check):
    Stored inline in artifact.metadata_json["generation_provenance"].
    Checked in _populate_instructional_package via is_artifact_stale().

All layer 1+2 cache entries live in teacher_assist_v2_generation_cache and
are invalidated only when their cache_key changes.


Each artifact stores a generation_provenance dict that records:
  - prompt_version: version of the prompt template that produced it
  - dependency_hash: hash of the Tier-1 planning outputs it depends on
  - generation_source: "ai" | "deterministic" | "cached"
  - model, token counts, cost

A generation_manifest is stored in package.metadata_json at completion,
summarising what was regenerated vs cached and the total AI cost.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any

PIPELINE_SCHEMA_VERSION = "v1"


def _stable_hash(data: Any) -> str:
    """16-char prefix of SHA-256 of a stable JSON representation."""
    serialized = json.dumps(data, sort_keys=True, ensure_ascii=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


# ── Input hash helpers ────────────────────────────────────────────────────────


def compute_curriculum_hash(context: dict[str, Any]) -> str:
    """Hash of curriculum inputs. Changes when pacing guides, subjects, or document
    content changes; invalidates all Tier-1 planning stages downstream."""
    key = {
        "pacing_guide_ids": sorted(context.get("pacing_guide_ids") or []),
        "subject_ids": sorted(
            str(row.get("subject_id", "")) for row in (context.get("subjects") or [])
        ),
        "week_range": [context.get("week_start"), context.get("week_end")],
        "district_doc_hash": _stable_hash(context.get("district_document_context")),
        "teacher_doc_hash": _stable_hash(context.get("teacher_document_context")),
    }
    return _stable_hash(key)


def compute_planning_hash(
    *,
    curriculum_sequence_plan: dict[str, Any],
    instructional_design_plan: dict[str, Any],
    strand_learning_journeys: dict[str, Any],
) -> str:
    """Hash of all Tier-1 planning stage outputs.
    Changes when any planning-stage AI output changes; invalidates all per-artifact
    Tier-2 slots downstream."""
    return _stable_hash({
        "csp": curriculum_sequence_plan,
        "idp": instructional_design_plan,
        "slj": strand_learning_journeys,
    })


def compute_delivery_hash(delivery_profile: dict[str, Any] | None) -> str:
    """Hash of the instructional delivery profile (affects student-facing artifacts)."""
    return _stable_hash(delivery_profile or {})


# ── Dirty-checking ────────────────────────────────────────────────────────────


def is_artifact_stale(
    artifact_metadata: dict[str, Any],
    *,
    current_prompt_version: str,
    current_planning_hash: str,
) -> bool:
    """Return True if the artifact must be regenerated.

    Rules:
    - No provenance → always stale (pre-incremental artifact)
    - generation_source="deterministic" → never stale (no AI dependency)
    - prompt version mismatch → stale (developer changed the prompt)
    - dependency_hash mismatch → stale (planning inputs changed)
    """
    provenance = artifact_metadata.get("generation_provenance") or {}
    if not provenance:
        return True
    if provenance.get("generation_source") == "deterministic":
        return False
    if provenance.get("prompt_version") != current_prompt_version:
        return True
    if provenance.get("dependency_hash") != current_planning_hash:
        return True
    return False


# ── Provenance builder ────────────────────────────────────────────────────────


def build_artifact_provenance(
    *,
    prompt_version: str,
    model: str | None,
    generation_source: str,
    dependency_hash: str,
    estimated_cost_cents: int = 0,
    input_tokens: int = 0,
    output_tokens: int = 0,
) -> dict[str, Any]:
    """Return a generation_provenance dict to embed in artifact metadata_json."""
    return {
        "prompt_version": prompt_version,
        "model": model,
        "generation_source": generation_source,
        "dependency_hash": dependency_hash,
        "estimated_cost_cents": estimated_cost_cents,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "generated_at": datetime.now(UTC).isoformat(),
    }


# ── Manifest builder ──────────────────────────────────────────────────────────


# ── Layer 1 + 2 cache keys ────────────────────────────────────────────────────


def compute_curriculum_cache_key(
    context: dict[str, Any],
    *,
    curriculum_prompt_version: str,
) -> str:
    """Stable cache key for Layer 1 (curriculum).

    Changes when:
    - pacing guide IDs change
    - curriculum document content changes (content hash, not just filename)
    - the curriculum sequence plan prompt version is bumped
    """
    key = {
        "layer": "curriculum",
        "pacing_guide_ids": sorted(context.get("pacing_guide_ids") or []),
        "subject_ids": sorted(
            str(row.get("subject_id", "")) for row in (context.get("subjects") or [])
        ),
        "week_range": [context.get("week_start"), context.get("week_end")],
        "district_doc_hash": _stable_hash(context.get("district_document_context")),
        "teacher_doc_hash": _stable_hash(context.get("teacher_document_context")),
        "prompt_version": curriculum_prompt_version,
    }
    return _stable_hash(key)


def compute_planning_cache_key(
    curriculum_cache_key: str,
    context: dict[str, Any],
    delivery_profile: dict[str, Any] | None,
    lost_instructional_days: int,
    *,
    planning_prompt_version: str,
) -> str:
    """Stable cache key for Layer 2 (planning).

    Changes when:
    - curriculum cache key changes (upstream changed)
    - teacher supplemental materials / notes change
    - instructional delivery profile changes
    - number of lost instructional days changes
    - the instructional design plan prompt version is bumped
    """
    # Hash supplemental materials that teachers add (notes, reference links, etc.)
    _supp_hash = _stable_hash(context.get("ai_readiness_summary"))
    _link_hash = _stable_hash(
        sorted(
            str(link.get("external_url", "")) + str(link.get("title", ""))
            for link in (
                (context.get("district_link_context") or {}).get("used_links") or []
            ) + (
                (context.get("teacher_link_context") or {}).get("used_links") or []
            )
        )
    )
    key = {
        "layer": "planning",
        "curriculum_key": curriculum_cache_key,
        "supplemental_hash": _supp_hash,
        "link_hash": _link_hash,
        "delivery_profile_hash": _stable_hash(delivery_profile or {}),
        "lost_days": int(lost_instructional_days),
        "prompt_version": planning_prompt_version,
    }
    return _stable_hash(key)


# ── DB helpers ────────────────────────────────────────────────────────────────


def lookup_generation_cache(
    db: Any,
    *,
    tenant_id: Any,
    cache_layer: str,
    cache_key: str,
) -> dict[str, Any] | None:
    """Return cached content_json for (tenant, layer, key), or None on miss."""
    import logging
    from sqlalchemy import text as _text

    try:
        # Use a SAVEPOINT so a missing table or any DB error doesn't abort the outer transaction.
        with db.begin_nested():
            row = db.execute(
                _text(
                    "SELECT id, content_json FROM teacher_assist_v2_generation_cache "
                    "WHERE tenant_id = :tid AND cache_layer = :layer AND cache_key = :key "
                    "LIMIT 1"
                ),
                {"tid": str(tenant_id), "layer": cache_layer, "key": cache_key},
            ).fetchone()
        if row is None:
            return None
        _cache_id, content = row
        # Fire-and-forget hit count increment (best effort; ignore failure)
        try:
            with db.begin_nested():
                db.execute(
                    _text(
                        "UPDATE teacher_assist_v2_generation_cache "
                        "SET hit_count = hit_count + 1 "
                        "WHERE id = :id"
                    ),
                    {"id": str(_cache_id)},
                )
        except Exception:
            pass
        return content if isinstance(content, dict) else {}
    except Exception:
        logging.getLogger(__name__).debug(
            "generation_cache lookup failed — layer=%s key=%s", cache_layer, cache_key
        )
        return None


def store_generation_cache(
    db: Any,
    *,
    tenant_id: Any,
    cache_layer: str,
    cache_key: str,
    prompt_version: str,
    model: str | None,
    content: dict[str, Any],
    total_cost_cents: int = 0,
) -> None:
    """Upsert a cache entry.  On duplicate key (tenant, layer, key), does nothing
    — the first writer wins; stale entries are bypassed by key-change, not update.
    """
    import uuid as _uuid
    import logging
    from sqlalchemy import text as _text

    try:
        # Use a SAVEPOINT so a missing table or any DB error doesn't abort the outer transaction.
        with db.begin_nested():
            db.execute(
                _text(
                    "INSERT INTO teacher_assist_v2_generation_cache "
                    "(id, tenant_id, cache_layer, cache_key, prompt_version, model, "
                    " content_json, total_cost_cents, hit_count, created_at) "
                    "VALUES (:id, :tid, :layer, :key, :pv, :model, "
                    "        :content::jsonb, :cost, 0, NOW()) "
                    "ON CONFLICT (tenant_id, cache_layer, cache_key) DO NOTHING"
                ),
                {
                    "id": str(_uuid.uuid4()),
                    "tid": str(tenant_id),
                    "layer": cache_layer,
                    "key": cache_key,
                    "pv": prompt_version,
                    "model": model,
                    "content": __import__("json").dumps(content, default=str),
                    "cost": int(total_cost_cents),
                },
            )
    except Exception:
        logging.getLogger(__name__).debug(
            "generation_cache store failed — layer=%s key=%s", cache_layer, cache_key
        )


def build_generation_manifest(
    *,
    generation_mode: str,
    curriculum_hash: str,
    planning_hash: str,
    delivery_hash: str,
    prompt_versions: dict[str, str],
    cost_cents_by_feature: dict[str, int],
    artifact_source_counts: dict[str, dict[str, int]],
    cache_layer_hits: dict[str, bool] | None = None,
) -> dict[str, Any]:
    """Build the generation_manifest stored in package.metadata_json.

    artifact_source_counts: {artifact_type: {"ai": N, "deterministic": N, "cached": N}}
    cost_cents_by_feature: {feature_name: total_cents}
    cache_layer_hits: {"curriculum": True/False, "planning": True/False}
    """
    return {
        "pipeline_schema_version": PIPELINE_SCHEMA_VERSION,
        "generation_mode": generation_mode,
        "curriculum_hash": curriculum_hash,
        "planning_hash": planning_hash,
        "delivery_hash": delivery_hash,
        "prompt_versions_used": prompt_versions,
        "cost_cents_by_feature": cost_cents_by_feature,
        "total_generation_cost_cents": sum(cost_cents_by_feature.values()),
        "artifact_source_counts": artifact_source_counts,
        "cache_layer_hits": cache_layer_hits or {},
        "built_at": datetime.now(UTC).isoformat(),
    }
