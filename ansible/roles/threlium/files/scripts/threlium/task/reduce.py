"""Редукция task-операций → :class:`~threlium.types.TaskLedger` (порядок не важен — решётка)."""
from __future__ import annotations

from collections.abc import Iterable

from threlium.logutil import logger
from threlium.types import (
    SubtaskStatus,
    TaskLedger,
    TaskSubtaskContentId,
    TaskSubtaskState,
    TaskSubtaskText,
)

from .ops import TaskInitOp, TaskOp, TasksUpsertOp

log = logger.bind(stage="task")


def _ensure_exists(
    states: dict[str, TaskSubtaskState],
    content_id: TaskSubtaskContentId,
    text: TaskSubtaskText,
    status: SubtaskStatus,
) -> None:
    """Add если отсутствует; иначе merge статуса (без сброса) — общая ensure-exists семантика."""
    key = content_id.value
    cur = states.get(key)
    if cur is None:
        states[key] = TaskSubtaskState(content_id=content_id, text=text, status=status)
        return
    states[key] = TaskSubtaskState(
        content_id=cur.content_id, text=cur.text, status=cur.status.merge(status)
    )


def _apply_update(
    states: dict[str, TaskSubtaskState],
    content_id: TaskSubtaskContentId,
    status: SubtaskStatus,
) -> None:
    key = content_id.value
    cur = states.get(key)
    if cur is None:
        log.warning("task_update_target_missing", content_id=key, known=sorted(states))
        return
    states[key] = TaskSubtaskState(
        content_id=cur.content_id, text=cur.text, status=cur.status.merge(status)
    )


def reduce_task_ops(ops: Iterable[TaskOp]) -> TaskLedger:
    """Свести ops в ledger: init/additions = ensure-exists, updates = merge статуса.

    Коммутативно/идемпотентно (``SubtaskStatus.merge`` = max ранга) → результат не зависит
    от порядка писем в IRT.
    """
    states: dict[str, TaskSubtaskState] = {}
    for op in ops:
        if isinstance(op, TaskInitOp):
            for d in op.subtasks:
                _ensure_exists(states, d.content_id, d.text, SubtaskStatus.PENDING)
        elif isinstance(op, TasksUpsertOp):
            for a in op.additions:
                _ensure_exists(states, a.content_id, a.text, a.status)
            for u in op.updates:
                _apply_update(states, u.content_id, u.status)

    in_progress = [s for s in states.values() if s.status is SubtaskStatus.IN_PROGRESS]
    if len(in_progress) > 1:
        log.warning(
            "task_ledger_multiple_in_progress",
            count=len(in_progress),
            content_ids=[s.content_id.value for s in in_progress],
        )
    return TaskLedger.from_states(states)
