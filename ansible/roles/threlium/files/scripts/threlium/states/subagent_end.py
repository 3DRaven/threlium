#!/usr/bin/env python3
"""subagent_end@localhost → enrich@localhost: маркер завершения субагента."""
from email.message import EmailMessage

from threlium.enrich_user_query import require_enrich_user_query_for_reenrich
from threlium.settings import ThreliumSettings
from threlium.fsm_emit import irt_wire_from_incoming_message_id
from threlium.fsm_emit_semantic import emit_to_enrich
from threlium.irt_subagent_classifier import (
    find_matching_subagent_intent_ancestor,
    hop_from_intent_parent,
)
from threlium.logutil import logger
from threlium.nm import require_fsm_message_id
from threlium.types import FsmStage, MailHeaderName

log = logger.bind(stage="subagent_end")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "subagent_end")

    ancestor = find_matching_subagent_intent_ancestor(inner)
    hop = hop_from_intent_parent(ancestor)

    irt = irt_wire_from_incoming_message_id(msg)
    patch: dict[MailHeaderName, object] = {MailHeaderName.HOP_BUDGET: hop}
    if irt is not None and irt.value.strip():
        patch[MailHeaderName.IN_REPLY_TO] = irt

    log.info("transition_to_enrich", hop=hop.value, message_id=mid_w.value if mid_w else None)

    user_query = require_enrich_user_query_for_reenrich(msg, stage_label="subagent_end")
    return emit_to_enrich(
        msg,
        stage,
        user_query=user_query,
        relay_history_from=msg,
        settings=config,
        managed_headers=patch,  # type: ignore[arg-type]
    )
