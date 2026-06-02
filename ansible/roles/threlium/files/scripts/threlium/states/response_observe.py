"""response_observe@localhost → enrich_fast@localhost.

Собирает текущее состояние буфера ответа, вызывает LLM для
структурированной суммаризации и передаёт результат в enrich_fast
для возврата в reasoning.
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.fsm_emit_semantic import emit_to_enrich_fast
from threlium.litellm_correlation_headers import fsm_correlation_snap
from threlium.litellm_required_tool import build_site_call, invoke_required_tool
from threlium.litellm_tool_spec import load_tool_spec
from threlium.logutil import logger
from threlium.nm import require_fsm_message_id
from threlium.prompts import render_prompt
from threlium.ledger_context_parts import crdt_ledger_state
from threlium.response.state_summary import build_state_data
from threlium.settings import ThreliumSettings
from threlium.summarize_tool_bridge import parse_summarize_response_buffer_assistant
from threlium.types import (
    FsmStage,
    LiteLlmChatMessage,
    LitellmRoutingSite,
    PromptPath,
)

log = logger.bind(stage="response_observe")


def _llm_observe(data_kw: dict[str, object], config: ThreliumSettings) -> str:
    """LLM-суммаризация буфера ответа через tool ``summarize_response_buffer``."""
    system = render_prompt(PromptPath.RESPONSE_OBSERVE_SYSTEM).strip()
    user = render_prompt(PromptPath.RESPONSE_OBSERVE_USER, **data_kw).strip()
    call = build_site_call(
        config,
        LitellmRoutingSite.RESPONSE_OBSERVE,
        [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user),
        ],
    )
    tool_spec = load_tool_spec(PromptPath.RESPONSE_OBSERVE_TOOL_SPEC)
    correlation = fsm_correlation_snap(None, config)
    assistant = invoke_required_tool(
        settings=config,
        call=call,
        tool_spec=tool_spec,
        correlation_snap=correlation,
        context="summarize_response_buffer",
    )
    return parse_summarize_response_buffer_assistant(assistant).observation


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w, inner = require_fsm_message_id(msg, "response_observe")

    crdt = crdt_ledger_state(inner)
    data = build_state_data(list(crdt.response_ops))
    ledger = crdt.task_ledger

    data_kw: dict[str, object] = {
        "is_empty": data.is_empty,
        "live_count": data.live_count,
        "total_chars": data.total_chars,
        "chunks": [
            {"position": c.position, "content": c.content, "deleted": c.deleted}
            for c in data.chunks
        ],
        "deleted_positions": [c.position for c in data.chunks if c.deleted],
        "task_is_empty": ledger.is_empty,
        "subtasks": [
            {"content_id": s.content_id.value, "text": s.text.value, "status": s.status.value}
            for s in ledger.subtasks
        ],
        "task_open_count": len(ledger.open_subtasks()),
        "task_done_count": len(ledger.done_subtasks()),
        "task_cancelled_count": len(ledger.cancelled_subtasks()),
    }

    observation = _llm_observe(data_kw, config)
    log.info(
        "observed",
        ops_count=len(crdt.response_ops),
        subtasks=len(ledger.subtasks),
        open_subtasks=len(ledger.open_subtasks()),
        observation_chars=len(observation),
        message_id=mid_w.value if mid_w else None,
    )

    return emit_to_enrich_fast(
        msg,
        stage,
        history=observation,
        settings=config,
    )
