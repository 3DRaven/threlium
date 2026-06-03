#!/usr/bin/env python3
"""summarize_memory@localhost: стадия-хранитель итога суммаризации → enrich@."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.fsm_emit_semantic import emit_to_enrich
from threlium.mime_reform import system_part_text
from threlium.settings import ThreliumSettings
from threlium.types import EnrichUserQueryText, FsmStage


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    user_query = EnrichUserQueryText.require_value(
        name="summarize user_query", raw=system_part_text(msg)
    )
    return emit_to_enrich(
        msg,
        stage,
        user_query=user_query,
        relay_history_from=msg,
        settings=config,
    )
