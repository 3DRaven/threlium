#!/usr/bin/env python3
"""summarize_context@localhost: LLM-суммаризация + тегирование оригиналов.

Получает JSON-задание от enrich с mids и телами писем, вызывает LLM score 0
для суммаризации, тегирует оригиналы tag:context_summarized и передаёт
plain text summary в summarize_memory (стадия-хранитель).
"""
from __future__ import annotations

import json
from email.message import EmailMessage

from threlium import nm as nmlib
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.litellm_client import litellm_site_completion_text
from threlium.litellm_correlation_headers import build_litellm_correlation_headers
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    LiteLlmChatMessage,
    LitellmCallSite,
    LitellmRoutingSite,
    NotmuchMessageIdInner,
    NotmuchTag,
    PromptPath,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader

log = logger.bind(stage="summarize_context")


def _parse_payload(text: str) -> tuple[list[str], list[str]] | None:
    """Parse ``{"summarize": {"mids": [...], "bodies": [...]}}``."""
    try:
        obj = json.loads(text.strip())
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None
    inner = obj.get("summarize")
    if not isinstance(inner, dict):
        return None
    mids = inner.get("mids")
    bodies = inner.get("bodies")
    if not isinstance(mids, list) or not isinstance(bodies, list):
        return None
    if not mids:
        return None
    return ([str(m) for m in mids], [str(b) for b in bodies])


def _e2e_litellm_correlation(
    msg: EmailMessage, config: ThreliumSettings
) -> dict[str, str] | None:
    """Снимок TLS FSM + ``X-Threlium-Call-Site: summarize_context`` для WireMock (E2E_ISOLATION)."""
    if not config.e2e.litellm_route_correlation:
        return None
    snap = get_litellm_http_correlation()
    if snap is not None:
        corr = dict(snap)
    else:
        corr = build_litellm_correlation_headers(
            msg, call_site=LitellmCallSite.SUMMARIZE_CONTEXT
        )
    corr[LitellmCorrelationHeader.CALL_SITE.value] = (
        LitellmCallSite.SUMMARIZE_CONTEXT.value
    )
    return corr


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    body_raw = system_part_text(msg).strip()
    parsed = _parse_payload(body_raw)
    if parsed is None:
        log.error("unparseable_payload", body_preview=body_raw[:200])
        return None

    mids, bodies = parsed
    log.info("summarizing", message_count=len(mids))

    system = render_prompt(PromptPath.SUMMARIZE_CONTEXT_SYSTEM).strip()
    user = render_prompt(
        PromptPath.SUMMARIZE_CONTEXT_USER,
        message_count=len(bodies),
        bodies=bodies,
    ).strip()

    summary = litellm_site_completion_text(
        config,
        LitellmRoutingSite.SUMMARIZE_CONTEXT,
        [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user),
        ],
        correlation_override=_e2e_litellm_correlation(msg, config),
    )
    if not summary:
        log.warning("empty_summary", message_count=len(mids))
        summary = "(summary unavailable)"

    nm_mids = [NotmuchMessageIdInner.parse(m) for m in mids]
    tagged = nmlib.batch_tag_add(nm_mids, NotmuchTag.CONTEXT_SUMMARIZED)
    log.info("tagged_originals", tagged=tagged, total=len(nm_mids))

    # Сводка едет <history>-частью: оригиналы помечены context_summarized (выпадают из
    # unified), поэтому именно эта history-копия заменяет их в контексте следующего enrich.
    # summarize_memory payload не потребляет (re-trigger по IRT), <system> не нужен.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.SUMMARIZE_MEMORY,
        from_stage=stage,
        history=summary,
        settings=config,
    )
