"""response_edit@localhost → enrich_fast@localhost | enrich@localhost."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.enrich_user_query import require_enrich_user_query_for_reenrich
from threlium.fsm_emit_semantic import (
    emit_enrich_validation_error,
    emit_preserving_to_enrich_fast,
)
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.nm import require_fsm_message_id
from threlium.response.collect import collect_ops
from threlium.response.ops import AppendOp, parse_response_edit_stage_payload
from threlium.response.state_summary import build_state_summary
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage, PromptPath

log = logger.bind(stage="response_edit")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "response_edit")
    user_query = require_enrich_user_query_for_reenrich(msg, stage_label="response_edit")

    body_raw = system_part_text(msg).strip()
    payload = parse_response_edit_stage_payload(body_raw)
    if payload is None:
        log.error("invalid_body_json", message_id=mid_w.value if mid_w else None)
        return emit_enrich_validation_error(
            msg,
            from_stage=stage,
            settings=config,
            user_query=user_query,
            prompt_path=PromptPath.RESPONSE_EDIT_ERROR_INVALID_BODY,
            exc=f"not a valid edit payload: {body_raw[:120]!r}",
        )
    target_position = payload.position

    ops = collect_ops(inner)
    valid_positions = {op.position for op in ops if isinstance(op, AppendOp)}

    if target_position not in valid_positions:
        log.error(
            "invalid_position",
            position=target_position,
            valid_positions=sorted(valid_positions),
            message_id=mid_w.value if mid_w else None,
        )
        return emit_enrich_validation_error(
            msg,
            from_stage=stage,
            settings=config,
            user_query=user_query,
            prompt_path=PromptPath.RESPONSE_EDIT_ERROR_INVALID_POSITION,
            position=target_position,
            new_content=payload.new_content,
            valid_positions=sorted(valid_positions),
            buffer_summary=build_state_summary(ops),
        )

    return emit_preserving_to_enrich_fast(msg, stage, settings=config)
