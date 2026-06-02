"""Сбор response-операций из IRT-цепочки (фрейм-локально, leaf → tag:route boundary)."""
from __future__ import annotations

from threlium.irt_chain import IrtAncestorSnapshot
from threlium.mime_reform import system_part_text_from_path
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.types import FsmStage, NotmuchMessageIdInner

from .ops import AppendOp, EditOp, ResponseOp, parse_response_edit_stage_payload

_APPEND_STAGES = frozenset({FsmStage.RESPONSE_APPEND})
_EDIT_STAGES = frozenset({FsmStage.RESPONSE_EDIT})
_ALL_RESPONSE_STAGES = _APPEND_STAGES | _EDIT_STAGES


def _is_response_stage(snap: IrtAncestorSnapshot) -> bool:
    return any(snap.is_sent_from_fsm_stage(s) for s in _ALL_RESPONSE_STAGES)


def _is_append_stage(snap: IrtAncestorSnapshot) -> bool:
    return any(snap.is_sent_from_fsm_stage(s) for s in _APPEND_STAGES)


def _is_edit_stage(snap: IrtAncestorSnapshot) -> bool:
    return any(snap.is_sent_from_fsm_stage(s) for s in _EDIT_STAGES)


def _parse_edit_body(snap: IrtAncestorSnapshot) -> tuple[int, str | None]:
    """JSON body EditOp из ``<system>``: ``{position: int, new_content: str | null}``.

    Через msgspec (TYPES § CRDT boundary). Письмо уже прошло валидацию ``response_edit.main``,
    поэтому невалидный payload здесь — нарушение инварианта (``RuntimeError``), не graceful.
    """
    raw = system_part_text_from_path(snap.path).strip()
    payload = parse_response_edit_stage_payload(raw)
    if payload is None:
        raise RuntimeError(
            f"collect_ops: response_edit <system> not a valid edit payload: {raw[:120]!r}"
        )
    return payload.position, payload.new_content


def _read_append_content(snap: IrtAncestorSnapshot) -> str:
    return system_part_text_from_path(snap.path).strip()


def collect_ops(start_inner: NotmuchMessageIdInner) -> list[ResponseOp]:
    """Собрать response-операции своего фрейма из IRT-цепочки до ``tag:route``.

    Фрейм-локальный обход (``stop_at_route=True``): операции вложенных субагентов
    в родительский буфер не утекают, а обход обрывается на корне текущего хода.
    Возвращает хронологический список (корень → лист).
    ``AppendOp.position`` — 0-based индекс среди append-операций.
    """
    relevant: list[IrtAncestorSnapshot] = []
    for snap in iter_irt_ancestors_filtered(start_inner, stop_at_route=True):
        if _is_response_stage(snap):
            relevant.append(snap)

    relevant.reverse()

    ops: list[ResponseOp] = []
    append_position = 0
    for snap in relevant:
        if _is_append_stage(snap):
            content = _read_append_content(snap)
            if content:
                ops.append(
                    AppendOp(
                        position=append_position,
                        content=content,
                        message_id_inner=snap.message_id_inner,
                    )
                )
                append_position += 1
        elif _is_edit_stage(snap):
            target_position, new_content = _parse_edit_body(snap)
            ops.append(
                EditOp(
                    target_position=target_position,
                    new_content=new_content,
                    message_id_inner=snap.message_id_inner,
                )
            )

    return ops
