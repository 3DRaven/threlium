"""Типы операций буфера ответа (Append / Edit) и применение CRDT."""
from __future__ import annotations

from dataclasses import dataclass

import msgspec

from threlium.logutil import logger
from threlium.types import NotmuchMessageIdInner

log = logger.bind(stage="response")


class ResponseEditStagePayload(msgspec.Struct, frozen=True):
    """Wire-форма ``<system>`` для ``response_edit``: ``{position, new_content}``.

    TYPES (``docs/TYPES.md`` § CRDT msgspec boundary): edit-op читается строго через
    ``msgspec.json.decode(..., type=ResponseEditStagePayload)``, без ``json.loads`` + ручного
    ``dict``. Поле ``reasoning`` tool-арга в ``<system>`` НЕ релеится (нужно только LLM на
    tool-call, ``ResponseEditToolArgs``), поэтому это отдельная wire-структура, а не
    ``ResponseEditToolArgs``.
    """

    position: int
    new_content: str | None = None


def parse_response_edit_stage_payload(raw: str) -> ResponseEditStagePayload | None:
    """``<system>`` JSON → :class:`ResponseEditStagePayload`; невалидный payload → ``None``."""
    try:
        return msgspec.json.decode(raw.encode("utf-8"), type=ResponseEditStagePayload)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None


@dataclass(frozen=True)
class AppendOp:
    """Добавление чанка в буфер ответа.

    ``position`` — 0-based индекс чанка в итоговом буфере (назначается при
    ``collect_ops``); ``content`` парсится из тела письма только при reduce/observe.
    """

    position: int
    content: str
    message_id_inner: NotmuchMessageIdInner


@dataclass(frozen=True)
class EditOp:
    """Правка/удаление чанка по ``target_position``.

    ``new_content is None`` = удаление; ``str`` = замена.
    Порядок в массиве операций определяет приоритет применения.
    """

    target_position: int
    new_content: str | None
    message_id_inner: NotmuchMessageIdInner


ResponseOp = AppendOp | EditOp


def apply_response_ops(ops: list[ResponseOp]) -> dict[int, str | None]:
    """Линейное применение операций → позиция → контент (``None`` = удалён).

    LLM-галлюцинация ``EditOp`` на несуществующий chunk — ``log.warning``, не ``RuntimeError``.
    """
    chunks: dict[int, str | None] = {}
    for op in ops:
        if isinstance(op, AppendOp):
            chunks[op.position] = op.content
        elif isinstance(op, EditOp):
            if op.target_position not in chunks:
                log.warning(
                    "edit_op_target_missing",
                    target_position=op.target_position,
                    known_positions=sorted(chunks),
                )
                continue
            chunks[op.target_position] = op.new_content
    return chunks
