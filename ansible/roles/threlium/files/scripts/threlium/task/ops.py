"""Операции task-ledger (Init / Upsert) + сериализация/парсинг на границе писем.

Стиль — ``@dataclass(frozen=True)``, как :mod:`threlium.response.ops`; поля — доменные VO
(``TaskSubtaskContentId`` / ``TaskSubtaskText`` / ``SubtaskStatus``), не примитивы.

Сборка ops из писем — фабрики ``from_*`` / ``parse_*`` (граница): примитивы jsonschema
→ VO. ``from_tool_args`` (эмит на стадии ``tasks_upsert``) валидирует ``content_id`` против
текущего ledger и бросает ``RuntimeError`` (Level 3); ``parse_*`` (collect) — толерантны.
"""
from __future__ import annotations

from dataclasses import dataclass

import msgspec

from threlium.logutil import logger
from threlium.types import (
    NotmuchMessageIdInner,
    SubtaskStatus,
    TaskBlockerText,
    TaskDiscoveryNoteText,
    TaskNextActionText,
    TaskSubtaskContentId,
    TaskSubtaskText,
    TasksUpsertToolArgs,
)

log = logger.bind(stage="task")


@dataclass(frozen=True)
class TaskSubtaskDef:
    """Стартовое определение подзадачи (от enrich ``TaskInitOp``)."""

    content_id: TaskSubtaskContentId
    text: TaskSubtaskText


@dataclass(frozen=True)
class TaskInitOp:
    """Стартовый набор подзадач из письма ``enrich → reasoning`` (MIME ``<task-init>``).

    Ensure-exists при reduce: добавить отсутствующие как ``pending``, существующие НЕ трогать.
    """

    subtasks: tuple[TaskSubtaskDef, ...]
    message_id_inner: NotmuchMessageIdInner


@dataclass(frozen=True)
class NewSubtask:
    """Новая подзадача из ``tasks_upsert`` (content_id из текста; статус по умолчанию pending)."""

    content_id: TaskSubtaskContentId
    text: TaskSubtaskText
    status: SubtaskStatus


@dataclass(frozen=True)
class SubtaskStatusUpdate:
    """Статус-изменение существующей подзадачи по ``content_id`` (видимому в ``<task-state>``)."""

    content_id: TaskSubtaskContentId
    status: SubtaskStatus


@dataclass(frozen=True)
class TasksUpsertOp:
    """Ядро CRDT: за один вызов add новых subtasks + status существующих.

    Инвариант: ``additions`` или ``updates`` непусто (минимум одно действие).
    """

    additions: tuple[NewSubtask, ...]
    updates: tuple[SubtaskStatusUpdate, ...]
    discovery_append: TaskDiscoveryNoteText | None
    next_action: TaskNextActionText | None
    blockers: TaskBlockerText | None
    allow_finalize_with_blocker: bool
    message_id_inner: NotmuchMessageIdInner

    @classmethod
    def from_tool_args(
        cls,
        args: TasksUpsertToolArgs,
        *,
        message_id_inner: NotmuchMessageIdInner,
        known_content_ids: frozenset[str],
    ) -> TasksUpsertOp:
        """Граница стадии ``tasks_upsert``: примитивы → VO + валидация против ledger.

        Неизвестный ``content_id`` в ``subtask_updates`` (нет в reduced-ledger) или
        пустой набор действий → ``RuntimeError`` (стадия ловит и шлёт ingress с ошибкой).
        """
        return _build_from_args(
            args, message_id_inner=message_id_inner, known_content_ids=known_content_ids
        )


def _build_from_args(
    args: TasksUpsertToolArgs,
    *,
    message_id_inner: NotmuchMessageIdInner,
    known_content_ids: frozenset[str] | None,
) -> TasksUpsertOp:
    additions: list[NewSubtask] = []
    for na in args.new_subtasks:
        text = TaskSubtaskText.require(name="tasks_upsert.new_subtasks[].text", raw=na.text)
        additions.append(
            NewSubtask(
                content_id=TaskSubtaskContentId.from_text(text),
                text=text,
                status=na.status,
            )
        )

    updates: list[SubtaskStatusUpdate] = []
    for su in args.subtask_updates:
        cid = TaskSubtaskContentId.require_value(su.content_id)
        if known_content_ids is not None and cid.value not in known_content_ids:
            raise RuntimeError(
                f"tasks_upsert: unknown content_id {cid.value!r} in subtask_updates "
                f"(not present in current task ledger); known={sorted(known_content_ids)}"
            )
        updates.append(SubtaskStatusUpdate(content_id=cid, status=su.status))

    if not additions and not updates:
        raise RuntimeError(
            "tasks_upsert: no actions (both new_subtasks and subtask_updates are empty)"
        )

    return TasksUpsertOp(
        additions=tuple(additions),
        updates=tuple(updates),
        discovery_append=TaskDiscoveryNoteText.parse_present_optional(args.discovery_append),
        next_action=TaskNextActionText.parse_present_optional(args.next_action),
        blockers=TaskBlockerText.parse_present_optional(args.blockers),
        allow_finalize_with_blocker=args.allow_finalize_with_blocker,
        message_id_inner=message_id_inner,
    )


TaskOp = TaskInitOp | TasksUpsertOp


# --- Сериализация <task-init> MIME-части (enrich пишет, collect читает) ---


class _TaskInitSubtaskWire(msgspec.Struct, frozen=True):
    content_id: str
    text: str


class _TaskInitWire(msgspec.Struct, frozen=True):
    subtasks: list[_TaskInitSubtaskWire] = []


def serialize_task_init(subtasks: tuple[TaskSubtaskDef, ...]) -> str:
    """``TaskSubtaskDef`` набор → JSON для MIME ``<task-init>``. Пустой набор → ``''``."""
    if not subtasks:
        return ""
    wire = _TaskInitWire(
        subtasks=[
            _TaskInitSubtaskWire(content_id=d.content_id.value, text=d.text.value)
            for d in subtasks
        ]
    )
    return msgspec.json.encode(wire).decode("utf-8")


def parse_task_init_op(
    body_text: str, *, message_id_inner: NotmuchMessageIdInner
) -> TaskInitOp | None:
    """JSON ``<task-init>`` → ``TaskInitOp`` (толерантно: мусор/пусто → ``None``)."""
    raw = body_text.strip()
    if not raw:
        return None
    try:
        wire = msgspec.json.decode(raw.encode("utf-8"), type=_TaskInitWire)
    except msgspec.DecodeError as exc:
        log.warning("task_init_parse_failed", error=str(exc))
        return None
    defs: list[TaskSubtaskDef] = []
    for sub in wire.subtasks:
        try:
            text = TaskSubtaskText.require(name="task_init.text", raw=sub.text)
            cid = TaskSubtaskContentId.require_value(sub.content_id)
        except ValueError:
            continue
        defs.append(TaskSubtaskDef(content_id=cid, text=text))
    if not defs:
        return None
    return TaskInitOp(subtasks=tuple(defs), message_id_inner=message_id_inner)


def parse_tasks_upsert_op(
    body_text: str, *, message_id_inner: NotmuchMessageIdInner
) -> TasksUpsertOp | None:
    """JSON tool-args ``tasks_upsert`` (durable письмо ``To: tasks_upsert``) → ``TasksUpsertOp``.

    Толерантно (collect): невалидный JSON / пустой набор действий → ``None`` (без валидации
    против ledger — :func:`reduce_task_ops` защитно пропускает неизвестные ``content_id``).
    """
    raw = body_text.strip()
    if not raw:
        return None
    try:
        args = msgspec.json.decode(raw.encode("utf-8"), type=TasksUpsertToolArgs)
        return _build_from_args(args, message_id_inner=message_id_inner, known_content_ids=None)
    except (msgspec.DecodeError, msgspec.ValidationError, ValueError, RuntimeError) as exc:
        log.warning("tasks_upsert_parse_failed", error=str(exc))
        return None
