"""Postgres outbox replacing Redis FIFO worker queues."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from oziebot_common.queues import BOUNDED_QUEUE_MAX_LENGTH, QueueNames


def bounded_queue_cap(queue_name: str) -> int | None:
    if queue_name == QueueNames.ops_alerts():
        return BOUNDED_QUEUE_MAX_LENGTH
    if queue_name.startswith("oziebot:queue:alerts:"):
        return BOUNDED_QUEUE_MAX_LENGTH
    if queue_name.startswith("oziebot:queue:alerts_retry:"):
        return BOUNDED_QUEUE_MAX_LENGTH
    return None


def _trim_bounded(conn: Connection, queue_name: str, cap: int) -> None:
    conn.execute(
        text(
            """
            DELETE FROM worker_message_outbox wo
            WHERE wo.id IN (
              SELECT id FROM (
                SELECT id,
                  ROW_NUMBER() OVER (
                    PARTITION BY queue_name ORDER BY created_at DESC
                  ) AS rn
                FROM worker_message_outbox
                WHERE queue_name = :queue_name AND status = 'pending'
              ) ranked
              WHERE ranked.rn > :cap
            )
            """
        ),
        {"queue_name": queue_name, "cap": cap},
    )


def enqueue_worker_payload(
    engine: Engine,
    queue_name: str,
    payload: dict[str, Any],
) -> None:
    """Insert a FIFO message; trims bounded queues to newest-first cap."""
    cap = bounded_queue_cap(queue_name)
    now = datetime.now(UTC)
    with engine.begin() as conn:
        if cap is not None:
            _trim_bounded(conn, queue_name, cap)
        conn.execute(
            text(
                """
                INSERT INTO worker_message_outbox (
                  id, queue_name, payload, status, attempt_count,
                  retry_after, lease_expires_at, created_at, updated_at
                ) VALUES (
                  CAST(:id AS uuid), :queue_name, CAST(:payload_json AS JSONB),
                  'pending', 0, NULL, NULL, :created_at, :updated_at
                )
                """
            ),
            {
                "id": str(uuid.uuid4()),
                "queue_name": queue_name,
                "payload_json": json.dumps(payload, default=str),
                "created_at": now,
                "updated_at": now,
            },
        )


def reclaim_stale_leases(engine: Engine) -> int:
    """Move expired leases back to pending for retry."""
    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                UPDATE worker_message_outbox
                SET status = 'pending', lease_expires_at = NULL, updated_at = :now
                WHERE status = 'leased' AND lease_expires_at IS NOT NULL
                  AND lease_expires_at < :now
                """
            ),
            {"now": datetime.now(UTC)},
        )
        return int(res.rowcount or 0)


def claim_next_worker_message(
    conn: Connection,
    queue_names: list[str],
    *,
    lease_seconds: int,
) -> tuple[str, uuid.UUID, dict[str, Any]] | None:
    """Within an open transaction, claim one pending row."""
    lease_until = datetime.now(UTC) + timedelta(seconds=max(lease_seconds, 5))
    row = conn.execute(
        text(
            """
            WITH picked AS (
              SELECT id FROM worker_message_outbox
              WHERE queue_name = ANY(:names)
                AND status = 'pending'
                AND (retry_after IS NULL OR retry_after <= :now)
              ORDER BY created_at ASC
              FOR UPDATE SKIP LOCKED
              LIMIT 1
            ), upd AS (
              UPDATE worker_message_outbox mo
              SET status = 'leased',
                  lease_expires_at = :expires,
                  attempt_count = attempt_count + 1,
                  updated_at = :now
              WHERE mo.id IN (SELECT id FROM picked)
              RETURNING mo.id, mo.queue_name, mo.payload::text AS payload
            )
            SELECT id, queue_name, payload FROM upd
            """
        ),
        {
            "names": queue_names,
            "now": datetime.now(UTC),
            "expires": lease_until,
        },
    ).fetchone()

    if row is None:
        return None
    pid, qn, payload_text = row[0], str(row[1]), str(row[2])
    return qn, uuid.UUID(str(pid)), json.loads(payload_text)


def finalize_worker_success(conn: Connection, message_id: uuid.UUID) -> None:
    conn.execute(
        text("DELETE FROM worker_message_outbox WHERE id = CAST(:id AS uuid)"),
        {"id": str(message_id)},
    )


def finalize_worker_retry(
    conn: Connection,
    message_id: uuid.UUID,
    *,
    retry_after_seconds: int,
) -> None:
    when = datetime.now(UTC) + timedelta(seconds=max(1, retry_after_seconds))
    conn.execute(
        text(
            """
            UPDATE worker_message_outbox
            SET status = 'pending', retry_after = :retry_after,
                lease_expires_at = NULL, updated_at = :now
            WHERE id = CAST(:id AS uuid)
            """
        ),
        {"id": str(message_id), "retry_after": when, "now": datetime.now(UTC)},
    )


LEASE_SECONDS_DEFAULT = 120
