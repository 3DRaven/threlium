"""Сбор task-операций из IRT-цепочки (весь фрейм текущего субагента, изоляция по hop-стеку).

IRT непрерывен (инвариант FSM) → идём от листа до начала текущего фрейма, собирая задачи
всех ходов пользователя = «общее направление треда». Изоляция субагента — по глубине
hop-стека: берём только снапшоты с глубиной == глубине текущего письма и останавливаемся
на письме ``subagent_intent`` той же глубины (граница фрейма). Вложенные субагенты (глубже)
пропускаются — у каждого уровня свой ledger.
"""
from __future__ import annotations

from threlium.irt_chain import (
    IrtAncestorSnapshot,
    iter_in_reply_to_ancestors_from_inner_id,
)
from threlium.mime_reform import (
    EnrichPartId,
    email_message_from_path,
    extract_part_by_content_id,
    extract_plain_body,
)
from threlium.types import FsmStage, HopBudgetLine, NotmuchMessageIdInner

from .ops import TaskOp, parse_task_init_op, parse_tasks_upsert_op


def _depth_of_line(line: HopBudgetLine) -> int:
    parts = line.value.split() if line.value else []
    return len(parts) if parts else 1


def _frame_snapshots(
    chain: list[IrtAncestorSnapshot], depth: int
) -> list[IrtAncestorSnapshot]:
    """Снапшоты текущего фрейма (leaf→root), до границы ``subagent_intent`` той же глубины."""
    frame: list[IrtAncestorSnapshot] = []
    for snap in chain:
        d = snap.hop_stack_depth()
        if d > depth:
            continue  # вложенный субагент — чужой фрейм
        if d < depth:
            break  # вышли ниже текущего фрейма (защитно)
        if snap.is_sent_from_fsm_stage(FsmStage.SUBAGENT_INTENT):
            break  # начало текущего фрейма субагента
        frame.append(snap)
    return frame


def collect_task_ops(
    start_inner: NotmuchMessageIdInner, current_hop_budget: HopBudgetLine
) -> list[TaskOp]:
    """Task-операции текущего фрейма в хронологическом порядке (корень → лист).

    ``current_hop_budget`` — ``X-Threlium-Hop-Budget`` входного письма стадии (его глубина
    задаёт фрейм). Источники op: письма ``enrich → reasoning`` (MIME ``<task-init>``) и
    durable письма ``→ tasks_upsert`` (JSON tool-args в теле).
    """
    chain = iter_in_reply_to_ancestors_from_inner_id(start_inner)
    depth = _depth_of_line(current_hop_budget)
    frame = _frame_snapshots(chain, depth)
    frame.reverse()

    ops: list[TaskOp] = []
    for snap in frame:
        if snap.is_sent_from_fsm_stage(FsmStage.ENRICH):
            e = email_message_from_path(snap.path)
            raw = extract_part_by_content_id(e, EnrichPartId.TASK_INIT)
            if raw:
                op = parse_task_init_op(raw, message_id_inner=snap.message_id_inner)
                if op is not None:
                    ops.append(op)
        elif snap.is_addressed_to_fsm_stage(FsmStage.TASKS_UPSERT):
            msg = email_message_from_path(snap.path)
            body = extract_plain_body(msg)
            op2 = parse_tasks_upsert_op(body, message_id_inner=snap.message_id_inner)
            if op2 is not None:
                ops.append(op2)

    return ops
