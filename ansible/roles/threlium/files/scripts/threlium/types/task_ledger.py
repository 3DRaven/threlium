"""Value Objects для task-ledger (anti-drift): content-addressed подзадачи + решётка статусов.

DDD (``docs/TYPES.md``): атомарные строки несут смысл **именем класса**, нагрузка — ``.value``.

* :class:`TaskSubtaskText` — текст подзадачи на границе смысла (не голый ``str``).
* :class:`TaskSubtaskContentId` — **identity** подзадачи; фабрика :meth:`TaskSubtaskContentId.from_text`
  инкапсулирует нормализацию (``strip`` + схлопывание пробелов) и хеш (sha256 hex, усечённый),
  как ``IrtHashWire.from_irt_header_value`` (кодек только внутри VO, публичной ``str → hash`` нет).
* :class:`SubtaskStatus` — монотонная решётка статусов; семантика **методами на VO**
  (``rank`` / ``is_terminal`` / ``merge``), как у ``FsmStage`` / ``FormalReasonErrorKind``.
* :class:`TaskSubtaskState` / :class:`TaskLedger` — reduced-состояние (Level 3) для gate и кэша
  ``<task-state>``; иммутабельны (``frozen``), инвариант «уникальные ``content_id``» — в фабрике
  :meth:`TaskLedger.from_states`.

CRDT-операции (``TaskInitOp`` / ``TasksUpsertOp``) и их reduce/collect живут в пакете
``threlium.task`` (зеркало ``threlium.response``); здесь — только доменные VO.
"""
from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Self

import msgspec

from ._core import _OptionalStripEmpty, _RequiredNonEmpty

# Длина усечённого sha256-hex для content_id: достаточно против коллизий на масштабе
# подзадач одного треда, компактно в промпте ``<task-state>``.
_CONTENT_ID_HEX_LEN = 12


class TaskSubtaskText(_RequiredNonEmpty):
    """Текст одной подзадачи плана (обязателен, непуст после ``strip``)."""

    def normalized(self) -> str:
        """``strip`` + схлопывание любых пробелов в один — основа content-addressed identity."""
        return " ".join(self.value.split())


class TaskSubtaskContentId(_RequiredNonEmpty):
    """Identity подзадачи (content-addressed): усечённый sha256 hex нормализованного текста.

    Один и тот же текст всегда даёт один ``content_id`` → повторная запись подзадачи
    (другой цикл enrich, другой ход) сливается в одну. Прямое декодирование невозможно.
    """

    @classmethod
    def from_text(cls, text: TaskSubtaskText) -> Self:
        """Нормализованный текст подзадачи → усечённый sha256 hex content_id."""
        digest = hashlib.sha256(text.normalized().encode()).hexdigest()
        return cls(value=digest[:_CONTENT_ID_HEX_LEN])

    @classmethod
    def require_value(cls, raw: str | None) -> Self:
        """Сырой ``content_id`` (из ``<task-state>`` в промпте) → VO; пусто → ``ValueError``."""
        return cls.require(name="TaskSubtaskContentId", raw=raw)


_SUBTASK_STATUS_RANK: dict[str, int] = {
    "pending": 0,
    "in_progress": 1,
    "done": 2,
    "cancelled": 2,
}


class SubtaskStatus(StrEnum):
    """Монотонная решётка статусов подзадачи (CRDT): ``merge`` = max ранга.

    ``PENDING(0) → IN_PROGRESS(1) → DONE/CANCELLED(2)``. ``merge`` коммутативен и
    идемпотентен → reduce не зависит от порядка писем в IRT; терминальные (rank 2)
    «липкие» автоматически. При равном ранге 2 (``done`` vs ``cancelled``) выигрывает
    ``DONE`` — выполненная работа не «отменяется» гонкой статусов.
    """

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    DONE = "done"
    CANCELLED = "cancelled"

    @property
    def rank(self) -> int:
        return _SUBTASK_STATUS_RANK[self.value]

    @property
    def is_terminal(self) -> bool:
        """Терминальный статус (rank == 2): ``done`` / ``cancelled``."""
        return self.rank == 2

    def merge(self, other: SubtaskStatus) -> SubtaskStatus:
        """Решёточный join: статус максимального ранга; ничья ранга 2 → ``DONE`` > ``CANCELLED``."""
        if self.rank > other.rank:
            return self
        if other.rank > self.rank:
            return other
        if self is other:
            return self
        # равный ранг, разные значения: возможно только rank 2 (done/cancelled) → done.
        return SubtaskStatus.DONE


class TaskSubtaskState(msgspec.Struct, frozen=True):
    """Reduced-состояние одной подзадачи: identity + текст + текущий статус решётки."""

    content_id: TaskSubtaskContentId
    text: TaskSubtaskText
    status: SubtaskStatus


class TaskLedger(msgspec.Struct, frozen=True):
    """Reduced task-ledger: уникальные по ``content_id`` подзадачи (для gate и ``<task-state>``).

    Иммутабелен; строится фабрикой :meth:`from_states` (инвариант уникальности
    ``content_id``). Это детерминированный результат ``reduce_task_ops`` —
    порядок подзадач стабилен (по ``content_id``).
    """

    subtasks: tuple[TaskSubtaskState, ...] = ()

    @classmethod
    def empty(cls) -> Self:
        return cls(subtasks=())

    @classmethod
    def from_states(cls, states: dict[str, TaskSubtaskState]) -> Self:
        """``content_id.value → state`` → ledger со стабильным порядком (sort по content_id)."""
        ordered = tuple(states[k] for k in sorted(states))
        return cls(subtasks=ordered)

    @property
    def is_empty(self) -> bool:
        return not self.subtasks

    def open_subtasks(self) -> tuple[TaskSubtaskState, ...]:
        """Подзадачи нетерминального ранга (``pending`` / ``in_progress``) — блокируют finalize."""
        return tuple(s for s in self.subtasks if not s.status.is_terminal)

    def done_subtasks(self) -> tuple[TaskSubtaskState, ...]:
        return tuple(s for s in self.subtasks if s.status is SubtaskStatus.DONE)

    def cancelled_subtasks(self) -> tuple[TaskSubtaskState, ...]:
        return tuple(s for s in self.subtasks if s.status is SubtaskStatus.CANCELLED)

    def content_ids(self) -> frozenset[str]:
        return frozenset(s.content_id.value for s in self.subtasks)


class TaskDiscoveryNoteText(_OptionalStripEmpty):
    """Опц. заметка discovery из ``tasks_upsert`` (что узнали / куда смотрели); пусто → секция не рендерится."""


class TaskNextActionText(_OptionalStripEmpty):
    """Опц. следующий шаг из ``tasks_upsert``; пусто → секция не рендерится."""


class TaskBlockerText(_OptionalStripEmpty):
    """Опц. описание блокера из ``tasks_upsert`` (пара с ``allow_finalize_with_blocker``)."""


__all__ = [
    "SubtaskStatus",
    "TaskBlockerText",
    "TaskDiscoveryNoteText",
    "TaskLedger",
    "TaskNextActionText",
    "TaskSubtaskContentId",
    "TaskSubtaskState",
    "TaskSubtaskText",
]
