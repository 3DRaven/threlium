"""CRDT recompute helpers for ``<response-state>`` / ``<task-state>`` MIME parts.

Shared by enrich, enrich_fast, response_observe, response_finalize.
"""
from __future__ import annotations

from dataclasses import dataclass

from threlium.enrich_context import trim_context_text
from threlium.mime_reform import EnrichContentId, EnrichPartId
from threlium.response.collect import collect_ops
from threlium.response.ops import ResponseOp
from threlium.response.state_summary import build_state_summary
from threlium.task import build_task_state_summary, collect_task_ops, reduce_task_ops
from threlium.task.ops import TaskOp
from threlium.types import NotmuchMessageIdInner, TaskLedger


@dataclass(frozen=True)
class CrdtLedgerState:
    """Reduced response ops + task ops/ledger at ``inner`` (current hop frame)."""

    response_ops: tuple[ResponseOp, ...]
    task_ops: tuple[TaskOp, ...]
    task_ledger: TaskLedger


def crdt_ledger_state(inner: NotmuchMessageIdInner) -> CrdtLedgerState:
    task_ops = tuple(collect_task_ops(inner))
    return CrdtLedgerState(
        response_ops=tuple(collect_ops(inner)),
        task_ops=task_ops,
        task_ledger=reduce_task_ops(task_ops),
    )


def trimmed_crdt_state_texts(inner: NotmuchMessageIdInner, *, limit: int) -> tuple[str, str]:
    """Trimmed ``<response-state>`` and ``<task-state>`` texts from CRDT at ``inner``."""
    state = crdt_ledger_state(inner)
    response = trim_context_text(build_state_summary(list(state.response_ops)), limit)
    task = trim_context_text(build_task_state_summary(state.task_ledger), limit)
    return response, task


def ledger_context_parts(
    inner: NotmuchMessageIdInner, limit: int
) -> list[tuple[EnrichContentId, str]]:
    """MIME part texts for ``<response-state>`` (enrich extra bucket)."""
    response, _ = trimmed_crdt_state_texts(inner, limit=limit)
    parts: list[tuple[EnrichContentId, str]] = []
    if response:
        parts.append(
            (EnrichContentId.from_part_id(EnrichPartId.RESPONSE_STATE), response)
        )
    return parts


__all__ = [
    "CrdtLedgerState",
    "crdt_ledger_state",
    "ledger_context_parts",
    "trimmed_crdt_state_texts",
]
