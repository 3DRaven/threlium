"""response_finalize@localhost → egress_router@localhost | ingress@localhost.

Четыре режима:

* **Mode 1**: buffer пуст + content есть → быстрый ответ (только content) → egress_router
* **Mode 2**: buffer есть + content пуст → ответ из буфера → egress_router
* **Mode 3**: buffer есть + content есть → buffer + content → egress_router
* **Mode 4**: buffer пуст + content пуст → response_not_formed.j2 → ingress
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.fsm_emit import (
    build_fsm_plain_to_stage,
    build_fsm_step_to_stage,
    hop_budget_remaining,
)
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.nm import require_fsm_message_id
from threlium.prompts import render_prompt
from threlium.response.collect import collect_ops
from threlium.response.reduce import reduce_ops
from threlium.settings import ThreliumSettings
from threlium.ledger_context_parts import crdt_ledger_state
from threlium.task import build_task_incomplete_notice, ledger_has_open_work
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    MailHeaderName,
    PromptPath,
    RfcSubjectWire,
)

log = logger.bind(stage="response_finalize")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "response_finalize")

    inline_content = system_part_text(msg).strip() or None

    ops = collect_ops(inner)
    buffer_text = reduce_ops(ops).strip() or None

    has_buffer = buffer_text is not None
    has_content = inline_content is not None

    # Жёсткий gate (anti-drift, fail-closed): не отдаём ответ пользователю, пока в task-ledger
    # нет завершённой работы (проверка по IRT, независимо от текста LLM). Пустой ledger тоже
    # блокирует — даже trivial-ответ фиксирует одну подзадачу done через tasks_upsert.
    # См. docs/RESPONSE_TABLE.md (Task CRDT) и threlium.task.gate.
    #
    # Исключение — исчерпанный hop-budget (remaining == 0): reasoning форсирует
    # сюда последний ответ, и он возвращается пользователю жёстко, минуя gate
    # (иначе finalize↔ingress зациклится). См. states/reasoning.py (force_finalize).
    hop_line = HopBudgetLine.parse_from_email(msg)
    budget_exhausted = hop_budget_remaining(hop_line, config) < 1
    ledger = crdt_ledger_state(inner).task_ledger
    if (
        not budget_exhausted
        and (has_buffer or has_content)
        and ledger_has_open_work(ledger)
    ):
        log.warning(
            "finalize_blocked_open_tasks",
            open_subtasks=len(ledger.open_subtasks()),
            done=len(ledger.done_subtasks()),
            cancelled=len(ledger.cancelled_subtasks()),
            message_id=mid_w.value if mid_w else None,
        )
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(build_task_incomplete_notice(ledger)),
            settings=config,
        )

    subj_w = RfcSubjectWire.parse_present_from_email(msg, MailHeaderName.SUBJECT)
    subject_raw = (
        subj_w.value if subj_w is not None
        else render_prompt(PromptPath.RESPONSE_FINALIZE_FALLBACK_SUBJECT).strip()
    )

    if has_buffer or has_content:
        mode = 3 if (has_buffer and has_content) else (2 if has_buffer else 1)
        final_body = render_prompt(
            PromptPath.RESPONSE_FINALIZE_COMPOSE,
            buffer_text=buffer_text,
            inline_content=inline_content,
        ).strip()
    else:
        mode = 4
        log.warning(
            "empty_buffer_and_content",
            mode=4,
            budget_exhausted=budget_exhausted,
            message_id=mid_w.value if mid_w else None,
        )
        notice = render_prompt(PromptPath.INGRESS_RESPONSE_NOT_FORMED).strip()
        # При исчерпанном бюджете отбивка в ingress зациклит — отдаём notice пользователю.
        if budget_exhausted:
            # Финальная отбивка пользователю: <system> = тело для egress, <history> =
            # копия в долгую память (что отправили пользователю).
            return build_fsm_step_to_stage(
                msg,
                to_addr=FsmStage.EGRESS_ROUTER,
                from_stage=stage,
                history=notice,
                system=notice,
                subject_line=FsmTransitionPlainSubjectLine.parse(subject_raw),
                settings=config,
            )
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(notice),
            settings=config,
        )

    log.info(
        "finalized",
        mode=mode,
        has_buffer=has_buffer,
        has_content=has_content,
        budget_exhausted=budget_exhausted,
        body_chars=len(final_body),
        message_id=mid_w.value if mid_w else None,
    )

    # Итоговый ответ агента: <system> — тело для egress (внешнее письмо строится из него),
    # <history> — копия в conversation-history (что отправили пользователю), origin поставит
    # enrich_fast при сплайсе на следующем ходе.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.EGRESS_ROUTER,
        from_stage=stage,
        history=final_body,
        system=final_body,
        subject_line=FsmTransitionPlainSubjectLine.parse(subject_raw),
        settings=config,
    )
