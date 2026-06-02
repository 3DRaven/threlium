"""tasks_upsert@localhost → enrich_fast@localhost | ingress@localhost.

``<system>``-payload (от reasoning) — JSON ``TasksUpsertToolArgs``: за один вызов модель
**добавляет** новые подзадачи (``new_subtasks``) и **меняет статусы** существующих
(``subtask_updates`` по ``content_id`` из ``<task-state>``).

Стадия валидирует ``content_id`` обновлений против reduced-ledger предков (Level 3 граница):
неизвестный id / пустой набор действий → ingress с ошибкой; ok → enrich_fast (тот
пересобирает ``<task-state>`` из CRDT, включая это durable письмо ``To: tasks_upsert``).
"""
from __future__ import annotations

from email.message import EmailMessage

import msgspec

from threlium.fsm_emit_semantic import (
    emit_ingress_validation_error,
    emit_preserving_to_enrich_fast,
)
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.nm import require_fsm_message_id
from threlium.settings import ThreliumSettings
from threlium.task import build_task_state_summary, reduce_task_ops
from threlium.task.ops import TasksUpsertOp
from threlium.ledger_context_parts import crdt_ledger_state
from threlium.types import (
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
    PromptPath,
    RfcInReplyToWire,
    TaskLedger,
    TasksUpsertToolArgs,
)

log = logger.bind(stage="tasks_upsert")

_HDR = MailHeaderName


def _ingress_error(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings, error: str, ledger: TaskLedger
) -> EmailMessage:
    return emit_ingress_validation_error(
        msg,
        from_stage=stage,
        settings=config,
        prompt_path=PromptPath.INGRESS_TASKS_UPSERT_ERROR,
        error=error,
        task_state=build_task_state_summary(ledger),
    )


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "tasks_upsert")

    irt_w = RfcInReplyToWire.parse_present_from_email(msg, _HDR.IN_REPLY_TO.value)
    parent_inner = NotmuchMessageIdInner.from_optional_raw(irt_w.value if irt_w else None)
    if parent_inner is not None:
        crdt = crdt_ledger_state(parent_inner)
        prior_ops = list(crdt.task_ops)
        prior_ledger = crdt.task_ledger
    else:
        prior_ops = []
        prior_ledger = reduce_task_ops([])

    body_raw = system_part_text(msg).strip()
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

    return emit_preserving_to_enrich_fast(msg, stage, settings=config)
