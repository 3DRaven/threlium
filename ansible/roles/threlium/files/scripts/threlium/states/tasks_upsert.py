"""tasks_upsert@localhost → enrich_fast@localhost | ingress@localhost.

Тело письма (от reasoning) — JSON ``TasksUpsertToolArgs``: за один вызов модель
**добавляет** новые подзадачи (``new_subtasks``) и **меняет статусы** существующих
(``subtask_updates`` по ``content_id`` из ``<task-state>``).

Стадия валидирует ``content_id`` обновлений против reduced-ledger предков (Level 3 граница):
неизвестный id / пустой набор действий → ingress с ошибкой; ok → enrich_fast (тот
пересобирает ``<task-state>`` из CRDT, включая это durable письмо ``To: tasks_upsert``).
"""
from __future__ import annotations

from email.message import EmailMessage

import msgspec

from threlium.fsm_emit import HDR_HOP_BUDGET, build_fsm_plain_to_stage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload
from threlium.logutil import logger
from threlium.mime_reform import extract_plain_body
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.task import build_task_state_summary, collect_task_ops, reduce_task_ops
from threlium.task.ops import TasksUpsertOp
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    HopBudgetLine,
    MailHeaderName,
    NotmuchMessageIdInner,
    PromptPath,
    RfcInReplyToWire,
    RfcMessageIdWire,
    TaskLedger,
    TasksUpsertToolArgs,
)

log = logger.bind(stage="tasks_upsert")

_HDR = MailHeaderName


def _ingress_error(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings, error: str, ledger: TaskLedger
) -> EmailMessage:
    body = render_prompt(
        PromptPath.INGRESS_TASKS_UPSERT_ERROR,
        error=error,
        task_state=build_task_state_summary(ledger),
    ).strip()
    return build_fsm_plain_to_stage(
        msg,
        to_addr=FsmStage.INGRESS,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(body),
        settings=config,
    )


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID.value)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if inner is None:
        raise RuntimeError("tasks_upsert: no Message-ID on incoming message")

    hop_line = HopBudgetLine.parse(msg.get(HDR_HOP_BUDGET))

    irt_w = RfcInReplyToWire.parse_present_from_email(msg, _HDR.IN_REPLY_TO.value)
    parent_inner = NotmuchMessageIdInner.from_optional_raw(irt_w.value if irt_w else None)
    prior_ops = collect_task_ops(parent_inner, hop_line) if parent_inner is not None else []
    prior_ledger = reduce_task_ops(prior_ops)

    body_raw = extract_plain_body(msg).strip()
    try:
        args = msgspec.json.decode(body_raw.encode("utf-8"), type=TasksUpsertToolArgs)
        op = TasksUpsertOp.from_tool_args(
            args, message_id_inner=inner, known_content_ids=prior_ledger.content_ids()
        )
    except (msgspec.DecodeError, msgspec.ValidationError, ValueError, RuntimeError) as exc:
        log.error("invalid_tasks_upsert", error=str(exc), message_id=mid_w.value if mid_w else None)
        return _ingress_error(msg, stage, config=config, error=str(exc), ledger=prior_ledger)

    new_ledger = reduce_task_ops([*prior_ops, op])
    log.info(
        "tasks_upserted",
        additions=len(op.additions),
        updates=len(op.updates),
        subtasks_total=len(new_ledger.subtasks),
        open_count=len(new_ledger.open_subtasks()),
        message_id=mid_w.value if mid_w else None,
    )

    return emit_transition_simple_step_preserving_payload(
        msg, to_addr=FsmStage.ENRICH_FAST, from_stage=stage, settings=config,
    )
