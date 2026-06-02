#!/usr/bin/env python3
"""subagent_intent@localhost → ingress@localhost; IRT-непрерывный переход с изолированным hop."""
from email.message import EmailMessage

from threlium.settings import ThreliumSettings
from threlium.fsm_emit import (
    build_fsm_plain_to_stage,
    emit_transition_preserving_payload,
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
    hb = push_subagent_hop_budget(HopBudgetLine.parse_from_email(msg), config)
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
    return emit_transition_preserving_payload(
        msg,
        to_addr=FsmStage.INGRESS,
        from_stage=stage,
        managed_headers=managed_patch_subagent_push_to_ingress(
            msg,
            hop_budget=hb,
        ),
        request_echo=system_part_text(msg).strip(),
        settings=config,
    )
