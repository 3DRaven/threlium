"""Task-ledger CRDT (anti-drift): content-addressed подзадачи + монотонная решётка статусов.

Зеркало :mod:`threlium.response`, но identity подзадачи — ``content_id`` (хеш текста),
а не позиция. ``reduce`` не зависит от порядка писем в IRT (решётка статусов).

* :mod:`.ops` — операции ``TaskInitOp`` (ensure-exists от enrich) / ``TasksUpsertOp``
  (add новых subtasks + status существующих от стадии ``tasks_upsert``).
* :mod:`.reduce` — ``reduce_task_ops`` → :class:`~threlium.types.TaskLedger`.
* :mod:`.collect` — сбор ops по IRT (весь фрейм текущего субагента, изоляция по hop-стеку).
* :mod:`.gate` — ``ledger_has_open_work`` для жёсткого gate ``response_finalize``.
* :mod:`.state_summary` — рендер ``<task-state>`` для промпта reasoning / observe.
"""
from __future__ import annotations

from .collect import collect_task_ops
from .gate import build_task_incomplete_notice, ledger_has_open_work
from .ops import (
    NewSubtask,
    SubtaskStatusUpdate,
    TaskInitOp,
    TaskOp,
    TaskSubtaskDef,
    TasksUpsertOp,
    parse_task_init_op,
    parse_tasks_upsert_op,
    serialize_task_init,
)
from .reduce import reduce_task_ops
from .state_summary import build_task_state_summary

__all__ = [
    "NewSubtask",
    "SubtaskStatusUpdate",
    "TaskInitOp",
    "TaskOp",
    "TaskSubtaskDef",
    "TasksUpsertOp",
    "build_task_incomplete_notice",
    "build_task_state_summary",
    "collect_task_ops",
    "ledger_has_open_work",
    "parse_task_init_op",
    "parse_tasks_upsert_op",
    "reduce_task_ops",
    "serialize_task_init",
]
