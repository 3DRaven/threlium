"""response_edit@localhost → enrich_fast@localhost | ingress@localhost.

Парсит JSON body ``{position, new_content}``, валидирует position через
collect_ops. Невалидная позиция → ingress с ошибкой; ok → enrich_fast.
"""
from __future__ import annotations

import json

from email.message import EmailMessage

from threlium.fsm_emit import build_fsm_plain_to_stage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.response.collect import collect_ops
from threlium.response.ops import AppendOp
from threlium.response.state_summary import build_state_summary
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    MailHeaderName,
    NotmuchMessageIdInner,
    PromptPath,
    RfcMessageIdWire,
)

log = logger.bind(stage="response_edit")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID.value)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    if inner is None:
        raise RuntimeError("response_edit: no Message-ID on incoming message")

    body_raw = system_part_text(msg).strip()
    try:
        data = json.loads(body_raw)
        target_position = int(data["position"])
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        log.error("invalid_body_json", error=str(exc), message_id=mid_w.value if mid_w else None)
        error_body = render_prompt(
            PromptPath.RESPONSE_EDIT_ERROR_INVALID_BODY,
            exc=str(exc),
        ).strip()
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(error_body),
            settings=config,
        )

    ops = collect_ops(inner)
    valid_positions = {op.position for op in ops if isinstance(op, AppendOp)}

    if target_position not in valid_positions:
        log.error(
            "invalid_position",
            position=target_position,
            valid_positions=sorted(valid_positions),
            message_id=mid_w.value if mid_w else None,
        )
        error_body = render_prompt(
            PromptPath.RESPONSE_EDIT_ERROR_INVALID_POSITION,
            position=target_position,
            new_content=data.get("new_content"),
            valid_positions=sorted(valid_positions),
            buffer_summary=build_state_summary(ops),
        ).strip()
        return build_fsm_plain_to_stage(
            msg,
            to_addr=FsmStage.INGRESS,
            from_stage=stage,
            body=FsmTransitionPlainBody.parse(error_body),
            settings=config,
        )

    return emit_transition_simple_step_preserving_payload(
        msg, to_addr=FsmStage.ENRICH_FAST, from_stage=stage, settings=config,
    )
