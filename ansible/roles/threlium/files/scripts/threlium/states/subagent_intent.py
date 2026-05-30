#!/usr/bin/env python3
"""subagent_intent@localhost → ingress@localhost; IRT-непрерывный переход с изолированным hop/cap."""
from email.message import EmailMessage

from threlium.settings import ThreliumSettings
from threlium.fsm_emit import (
    HDR_HOP_BUDGET,
    build_fsm_plain_to_stage,
    emit_transition_preserving_payload,
    push_subagent_capability,
    push_subagent_hop_budget,
)
from threlium.fsm_emit_semantic import managed_patch_subagent_push_to_ingress
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    PromptPath,
)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    hb = push_subagent_hop_budget(HopBudgetLine.parse(msg.get(HDR_HOP_BUDGET)), config)
    if hb is None:
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(
                render_prompt(PromptPath.SUBAGENT_INTENT_BUDGET_EXHAUSTED)
            ),
            subject_line=FsmTransitionPlainSubjectLine.parse(
                render_prompt(PromptPath.SUBAGENT_INTENT_BUDGET_EXHAUSTED_SUBJECT).strip()
            ),
            settings=config,
        )
    cap = push_subagent_capability(HopBudgetLine.parse(msg.get(HDR_HOP_BUDGET)), config)
    # Запрос делегирования в долгую память родителя: фильтр фрейма
    # (iter_irt_ancestors_filtered) изолирует все внутренние письма субагента и
    # отдаёт родителю только граничные снимки subagent_intent/subagent_end по
    # message_has_history. subagent_end несёт <history>-результат субагента, а вот
    # задача жила только в <system> (reasoning шлёт payload без history) и терялась.
    # request_echo кладёт её как <hash@history> (origin=reasoning) — родитель помнит,
    # ЧТО делегировал, симметрично результату от subagent_end.
    return emit_transition_preserving_payload(
        msg,
        to_addr=FsmStage.INGRESS,
        from_stage=stage,
        managed_headers=managed_patch_subagent_push_to_ingress(
            msg,
            hop_budget=hb,
            capabilities=cap,
        ),
        request_echo=system_part_text(msg).strip(),
        settings=config,
    )
