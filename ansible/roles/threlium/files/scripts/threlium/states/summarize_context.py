#!/usr/bin/env python3
"""summarize_context@localhost: LLM-суммаризация гранулярных ``<history>`` + тегирование.

Получает JSON-задание от enrich overflow (гранулярные ``SummarizeHistoryUnit`` + canonical
user_query). Строит ``history_by_mid`` (фаза A: split oversized, без trim), затем многораундовый
цикл LLM (фаза B: prior summary + новый блок ≤ token budget). Итог — rolling summary едет
``<history>``-частью; оригиналы (``source_mid``) тегируются ``context_summarized`` только после
валидной сводки; user_query релеится в ``<system>`` для re-trigger enrich (CONTEXT_CONTRACT §5).
"""
from __future__ import annotations

from email.message import EmailMessage

import msgspec

from threlium import nm as nmlib
from threlium.context_token_count import build_tokenizer, count_tokens, summarize_content_budget
from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.litellm_correlation_headers import fsm_correlation_snap
from threlium.litellm_required_tool import build_site_call, invoke_required_tool
from threlium.litellm_tool_spec import load_tool_spec
from threlium.logutil import logger
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.summarize_pack import (
    HistoryPart,
    build_history_by_mid,
    consume_fitted,
    history_by_mid_empty,
    pack_next_fitted,
    split_oversized_in_place,
)
from threlium.summarize_tool_bridge import parse_summarize_thread_context_assistant
from threlium.types import (
    EnrichUserQueryText,
    FsmStage,
    LiteLlmChatMessage,
    LitellmCallSite,
    LitellmRoutingSite,
    NotmuchMessageIdInner,
    NotmuchTag,
    PromptPath,
    SummarizeContextStagePayload,
    SummarizeHistoryUnit,
    SummarizeToolBridgeError,
    validated_user_query,
)

log = logger.bind(stage="summarize_context")


def _parse_payload(
    text: str,
) -> tuple[list[SummarizeHistoryUnit], EnrichUserQueryText] | None:
    """payload → ``(units, user_query)``. Невалидный/пустой batch → ``None``."""
    try:
        payload = msgspec.json.decode(
            text.strip().encode("utf-8"), type=SummarizeContextStagePayload
        )
    except (msgspec.DecodeError, msgspec.ValidationError):
        return None
    units = list(payload.summarize.units)
    if not units:
        return None
    try:
        user_query = validated_user_query(payload)
    except ValueError:
        return None
    return (units, user_query)


def _summarize_round(
    config: ThreliumSettings,
    msg: EmailMessage,
    *,
    prior_summary: str,
    parts: list[HistoryPart],
) -> str:
    """Один LLM-раунд: prior summary + новый блок history-частей → обновлённая сводка."""
    system = render_prompt(PromptPath.SUMMARIZE_CONTEXT_SYSTEM).strip()
    user = render_prompt(
        PromptPath.SUMMARIZE_CONTEXT_USER,
        message_count=len(parts),
        bodies=[p.text for p in parts],
        prior_summary=prior_summary or None,
    ).strip()
    call = build_site_call(
        config,
        LitellmRoutingSite.SUMMARIZE_CONTEXT,
        [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user),
        ],
    )
    tool_spec = load_tool_spec(PromptPath.SUMMARIZE_CONTEXT_TOOL_SPEC)
    assistant = invoke_required_tool(
        settings=config,
        call=call,
        tool_spec=tool_spec,
        correlation_snap=fsm_correlation_snap(
            msg, config, LitellmCallSite.SUMMARIZE_THREAD_CONTEXT
        ),
        context="summarize_thread_context",
    )
    return parse_summarize_thread_context_assistant(assistant).summary


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    body_raw = system_part_text(msg).strip()
    parsed = _parse_payload(body_raw)
    if parsed is None:
        log.error("unparseable_payload", body_preview=body_raw[:200])
        return None

    units, user_query = parsed
    source_mids = {u.source_mid for u in units}
    log.info("summarizing", unit_count=len(units), source_mids=len(source_mids))

    tokenizer = build_tokenizer(config)
    budget = summarize_content_budget(config)
    # chunk_token_limit < content_budget, чтобы единичная часть всегда влезала рядом с prior
    # summary; деление (не trim) гарантирует, что ни одна часть не превышает лимит.
    chunk_token_limit = max(1, budget // 2)

    history_by_mid = build_history_by_mid(units)
    split_oversized_in_place(history_by_mid, tokenizer, chunk_token_limit)

    # Fail-fast симметрично enrich overflow (CONTEXT_CONTRACT §5): при провале tool bridge
    # или пустой сводке оригиналы НЕ тегируются context_summarized — иначе они выпали бы из
    # unified без замены. Тег ставится только после валидной финальной сводки.
    rolling_summary = ""
    rounds = 0
    try:
        while not history_by_mid_empty(history_by_mid):
            prior_tokens = count_tokens(tokenizer, rolling_summary)
            content_budget = max(1, budget - prior_tokens)
            fitted = pack_next_fitted(history_by_mid, tokenizer, content_budget)
            if not fitted:
                break
            rolling_summary = _summarize_round(
                config, msg, prior_summary=rolling_summary, parts=fitted
            )
            consume_fitted(history_by_mid, fitted)
            rounds += 1
    except SummarizeToolBridgeError as exc:
        log.error("summarize_tool_bridge_failed", error=str(exc))
        raise RuntimeError(
            "summarize_context: tool bridge failed; originals left untagged"
        ) from exc

    if not rolling_summary.strip():
        log.error("empty_summary", unit_count=len(units), rounds=rounds)
        raise RuntimeError(
            "summarize_context: empty summary; originals left untagged"
        )

    nm_mids = [NotmuchMessageIdInner.parse(m) for m in sorted(source_mids)]
    tagged = nmlib.batch_tag_add(nm_mids, NotmuchTag.CONTEXT_SUMMARIZED)
    log.info("tagged_originals", tagged=tagged, total=len(nm_mids), rounds=rounds)

    # Сводка едет <history>-частью: оригиналы помечены context_summarized (выпадают из
    # unified), поэтому именно эта history-копия заменяет их в контексте следующего enrich.
    # user_query релеится в <system>: summarize_memory отдаст его enrich как <history>, чтобы
    # re-trigger повторил тот же ход пользователя (суммаризация его не меняет, CONTEXT §5).
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.SUMMARIZE_MEMORY,
        from_stage=stage,
        history=rolling_summary,
        system=user_query.value,
        settings=config,
    )
