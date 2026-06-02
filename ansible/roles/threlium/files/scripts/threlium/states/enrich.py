#!/usr/bin/env python3
"""enrich@localhost: notmuch-контекст + Jinja/LLM + LightRAG query_api → reasoning@localhost.

``docs/INDEX.md`` §7, ``docs/FSM.md`` §5.2, ADR 0001:

  * canonical входа: ``render_prompt(PromptPath.LIGHTRAG_ENRICH_INCOMING_USER_TEXT, incoming=msg)``;
  * план: ``render_prompt(PromptPath.LIGHTRAG_ENRICH_QUERY_PLAN)`` → один LLM →
    ``render_prompt(PromptPath.LIGHTRAG_ENRICH_AQUERY_USER)`` →
    ``run_rag_coroutine(rag.<query_api>(...), ...)`` (метод из ``settings.lightrag.query_api``);
  * envelope-dict собирается в Python, JSON — через ``json.dumps`` для ``<graph-answer>``;
  * ``build_enriched_multipart`` — ``multipart/mixed`` с гранулярными MIME-частями по ``Content-ID``.
"""
from __future__ import annotations

import asyncio
import copy
import json
import threading
from collections.abc import AsyncIterator
from dataclasses import dataclass, replace
from email.message import EmailMessage
from typing import Any

from lightrag import QueryParam

from threlium import nm as nmlib
from threlium.context_budget import (
    BucketConfig,
    BucketConfigTier,
    assign_tiers,
    estimate_unified_weight,
    normalize_weights,
    score_messages,
    solve_mckp,
)
from threlium.settings import ThreliumSettings
from threlium.enrich_tool_bridge import (
    parse_enrich_query_plan_assistant,
    parse_enrich_task_hypotheses_assistant,
    parse_enrich_task_plan_assistant,
)
from threlium.litellm_required_tool import (
    ainvoke_required_tool,
    build_site_call,
    invoke_required_tool,
)
from threlium.litellm_tool_spec import load_tool_spec
from threlium.litellm_route_context import e2e_route_wire_tail, get_litellm_http_correlation
from threlium.enrich_context import build_unified_email_messages, trim_context_text, UnifiedEmailContext
from threlium.fsm_emit import build_fsm_plain_to_stage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload
from threlium.logutil import logger
from threlium.mail import email_message_from_path
from threlium.mime_reform import (
    EnrichContentId,
    EnrichPartId,
    build_enriched_multipart,
    concat_history_parts_text,
)
from threlium.prompts import render_prompt
from threlium.response.collect import collect_ops
from threlium.response.state_summary import build_state_summary
from threlium.runners.lightrag import daemon_lightrag, run_rag_coroutine
from threlium.task import (
    build_task_state_summary,
    collect_task_ops,
    reduce_task_ops,
    serialize_task_init,
)
from threlium.task.ops import TaskInitOp, TaskOp, TaskSubtaskDef
from threlium.types import (
    EnrichGlobalMemoryText,
    EnrichGraphAnswerText,
    EnrichThreadMemoryText,
    EnrichUnifiedMailContextText,
    FsmTransitionPlainBody,
    LightragPromptLibraryKey,
    LitellmCallSite,
    LiteLlmChatMessage,
    FsmStage,
    LightragLiteLlmCompletionBody,
    LitellmRoutingSite,
    MailHeaderName,
    NotmuchMessageIdInner,
    PromptPath,
    RfcMessageIdWire,
    TaskLedger,
    TaskSubtaskContentId,
    TaskSubtaskText,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader

log = logger.bind(stage="enrich")

_HDR = MailHeaderName

_ALLOWED_QUERY_MODES = frozenset(
    {"local", "global", "hybrid", "naive", "mix", "bypass"}
)


def _query_param(cfg: ThreliumSettings) -> QueryParam:
    raw = (cfg.lightrag.query_mode or "hybrid").strip().lower()
    mode = raw if raw in _ALLOWED_QUERY_MODES else "hybrid"
    base = QueryParam()
    return replace(
        base,
        mode=mode,  # type: ignore[arg-type]
        top_k=cfg.lightrag.query_top_k,
        chunk_top_k=cfg.lightrag.query_chunk_top_k,
        max_total_tokens=cfg.lightrag.query_max_total_tokens,
        max_entity_tokens=cfg.lightrag.query_max_entity_tokens,
        max_relation_tokens=cfg.lightrag.query_max_relation_tokens,
        response_type=cfg.lightrag.query_response_type,
        enable_rerank=cfg.lightrag.enable_rerank,
    )


async def _enrich_llm_plan(cfg: ThreliumSettings, user_prompt: str) -> str:
    """Один tool-вызов ``enrich_query_plan`` для формулировки запроса к LightRAG."""
    call = build_site_call(
        cfg,
        LitellmRoutingSite.ENRICH_PLAN,
        [LiteLlmChatMessage(role="user", content=user_prompt)],
    )
    tool_spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_QUERY_PLAN_TOOL_SPEC)
    correlation = (
        get_litellm_http_correlation() if cfg.e2e.litellm_route_correlation else None
    )
    assistant = await ainvoke_required_tool(
        settings=cfg,
        call=call,
        tool_spec=tool_spec,
        correlation_snap=correlation,
        context="enrich_query_plan",
    )
    raw = parse_enrich_query_plan_assistant(assistant).formulated_query
    return LightragLiteLlmCompletionBody.parse(raw).value if raw else ""


def _build_lightrag_envelope(
    *,
    raw_result: dict[str, Any] | str | None,
    query_api: str,
    query_mode: str,
    formulated_query: str,
) -> dict[str, Any]:
    """Envelope dict for Jinja ``| tojson(indent=2)``; no ``json.dumps`` in Python."""
    envelope: dict[str, Any] = {
        "query_api": query_api,
        "query_mode": query_mode,
        "ok": True,
        "threlium": {"formulated_query": formulated_query},
        "lightrag": {"raw": None, "llm_text": None},
    }
    if raw_result is None:
        envelope["lightrag"]["llm_text"] = None
        return envelope

    if query_api == "aquery":
        # aquery → str
        envelope["lightrag"]["llm_text"] = raw_result if isinstance(raw_result, str) else str(raw_result)
        return envelope

    if not isinstance(raw_result, dict):
        envelope["ok"] = False
        envelope["error"] = f"expected dict from {query_api}, got {type(raw_result).__name__}"
        return envelope

    if query_api == "aquery_data":
        envelope["lightrag"]["raw"] = raw_result
        return envelope

    # aquery_llm: sanitize response_iterator (not JSON-serializable)
    sanitized = copy.copy(raw_result)
    llm_resp = sanitized.get("llm_response")
    if isinstance(llm_resp, dict):
        llm_resp = dict(llm_resp)
        if llm_resp.get("is_streaming"):
            log.warning("aquery_llm_streaming_ignored")
        llm_resp.pop("response_iterator", None)
        sanitized["llm_response"] = llm_resp
        envelope["lightrag"]["llm_text"] = llm_resp.get("content")
    envelope["lightrag"]["raw"] = sanitized
    return envelope


@dataclass(frozen=True)
class EnrichResult:
    """Гранулярные компоненты enriched-контекста (заменяет монолитный payload)."""

    graph_answer: EnrichGraphAnswerText | None
    unified_mail_context: EnrichUnifiedMailContextText | None
    thread_memory: EnrichThreadMemoryText | None
    global_memory: EnrichGlobalMemoryText | None


def _collect_extra_parts(
    inner: NotmuchMessageIdInner, limit: int
) -> list[tuple[EnrichContentId, str]]:
    """Пересчёт ``<response-state>`` из CRDT для полного enrich.

    Carry-over relay-частей из E_prev убран: после унификации полный enrich обновляет
    контекст через LightRAG + ``<unified-mail-context>`` (собранный из ``<history>``-частей
    треда по :func:`message_has_history`), а не переносом хвоста E_prev. ``<task-state>``
    enrich пересобирает детерминированно отдельно (как ``<response-state>``).
    """
    parts: list[tuple[EnrichContentId, str]] = []
    ops = collect_ops(inner)
    summary = build_state_summary(ops)
    trimmed = trim_context_text(summary, limit)
    if trimmed:
        parts.append((EnrichContentId.from_part_id(EnrichPartId.RESPONSE_STATE), trimmed))
    return parts


def _parse_subtask_defs(
    raw_subtasks: list[str], *, name: str, exclude_ids: frozenset[str]
) -> list[TaskSubtaskDef]:
    """Сырые тексты подзадач → дедуплицированные ``TaskSubtaskDef`` (VO-only, content-addressed).

    ``exclude_ids`` — content_id, уже присутствующие в ledger (для late-гипотез: seed
    этого hop + существующие подзадачи); внутри батча дубли отсекаются по content_id.
    """
    seen: set[str] = set()
    defs: list[TaskSubtaskDef] = []
    for text_raw in raw_subtasks:
        try:
            text = TaskSubtaskText.require(name=name, raw=text_raw)
        except ValueError:
            continue
        cid = TaskSubtaskContentId.from_text(text)
        if cid.value in seen or cid.value in exclude_ids:
            continue
        seen.add(cid.value)
        defs.append(TaskSubtaskDef(content_id=cid, text=text))
    return defs


def _build_task_seed_defs(
    *,
    config: ThreliumSettings,
    inner: NotmuchMessageIdInner,
    user_message_text: str,
) -> tuple[list[TaskSubtaskDef], list[TaskOp], TaskLedger]:
    """Early seed-набор подзадач (LLM ДО сбора контекста LightRAG).

    Возвращает seed-``defs``, существующие ops треда и ``ledger_after_seed`` (in-memory
    reduce existing+seed) — его тексты подмешиваются в графовый запрос. MIME-части НЕ
    пишутся здесь: финализация (один ``<task-init>``) откладывается до слияния с late-гипотезами.

    Fail-open: ошибка LLM / мусор → пустой seed (gate не блокирует пустой ledger).
    """
    existing_ops = collect_task_ops(inner)
    existing_ledger = reduce_task_ops(existing_ops)

    plan_prompt = render_prompt(
        PromptPath.LIGHTRAG_ENRICH_TASK_PLAN,
        incoming_user_message=user_message_text,
        existing_subtasks=[
            {"content_id": s.content_id.value, "text": s.text.value, "status": s.status.value}
            for s in existing_ledger.subtasks
        ],
    )
    call = build_site_call(
        config,
        LitellmRoutingSite.ENRICH_PLAN,
        [LiteLlmChatMessage(role="user", content=plan_prompt)],
    )
    tool_spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_TASK_PLAN_TOOL_SPEC)
    correlation = (
        get_litellm_http_correlation()
        if config.e2e.litellm_route_correlation
        else None
    )
    try:
        assistant = invoke_required_tool(
            settings=config,
            call=call,
            tool_spec=tool_spec,
            correlation_snap=correlation,
            context="enrich_task_plan",
        )
        subtasks = parse_enrich_task_plan_assistant(assistant).subtasks
    except Exception as exc:  # noqa: BLE001 — fail-open: seed опционален, ledger не обязателен
        log.warning("task_plan_llm_failed", error=str(exc))
        subtasks = []

    seed_defs = _parse_subtask_defs(
        subtasks, name="enrich_task_plan.subtask", exclude_ids=frozenset()
    )
    if seed_defs:
        seed_op = TaskInitOp(subtasks=tuple(seed_defs), message_id_inner=inner)
        ledger_after_seed = reduce_task_ops([*existing_ops, seed_op])
    else:
        ledger_after_seed = existing_ledger
    log.info(
        "task_seed",
        seeded=len(seed_defs),
        existing=len(existing_ledger.subtasks),
        total=len(ledger_after_seed.subtasks),
    )
    return seed_defs, existing_ops, ledger_after_seed


def _build_task_hypothesis_defs(
    *,
    config: ThreliumSettings,
    user_message_text: str,
    result: EnrichResult,
    ledger_after_seed: TaskLedger,
) -> list[TaskSubtaskDef]:
    """Late-проход (LLM ПОСЛЕ RAG): новые проверяемые гипотезы на полном контексте.

    Тот же каркас, что seed (другой site/prompt/tool). Гипотезы дедуплицируются против
    seed+существующих подзадач (``ledger_after_seed``). Fail-open: ошибка LLM → ``[]``.
    """
    prompt = trim_context_text(
        render_prompt(
            PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES,
            incoming_user_message=user_message_text,
            graph_answer=result.graph_answer.value if result.graph_answer else "",
            unified_mail_context=(
                result.unified_mail_context.value if result.unified_mail_context else ""
            ),
            thread_memory=result.thread_memory.value if result.thread_memory else "",
            global_memory=result.global_memory.value if result.global_memory else "",
            existing_subtasks=[
                {"content_id": s.content_id.value, "text": s.text.value, "status": s.status.value}
                for s in ledger_after_seed.subtasks
            ],
        ),
        config.enrich.context_max_chars,
    )
    call = build_site_call(
        config,
        LitellmRoutingSite.ENRICH_TASK_HYPOTHESES,
        [LiteLlmChatMessage(role="user", content=prompt)],
    )
    tool_spec = load_tool_spec(PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES_TOOL_SPEC)
    correlation = (
        get_litellm_http_correlation()
        if config.e2e.litellm_route_correlation
        else None
    )
    try:
        assistant = invoke_required_tool(
            settings=config,
            call=call,
            tool_spec=tool_spec,
            correlation_snap=correlation,
            context="enrich_task_hypotheses",
        )
        subtasks = parse_enrich_task_hypotheses_assistant(assistant).subtasks
    except Exception as exc:  # noqa: BLE001 — fail-open: гипотезы опциональны
        log.warning("task_hypotheses_llm_failed", error=str(exc))
        return []

    hyp_defs = _parse_subtask_defs(
        subtasks,
        name="enrich_task_hypotheses.subtask",
        exclude_ids=ledger_after_seed.content_ids(),
    )
    log.info(
        "task_hypotheses",
        added=len(hyp_defs),
        ledger=len(ledger_after_seed.subtasks),
    )
    return hyp_defs


def _finalize_task_mime_parts(
    *,
    seed_defs: list[TaskSubtaskDef],
    hyp_defs: list[TaskSubtaskDef],
    existing_ops: list[TaskOp],
    fallback_ledger: TaskLedger,
    inner: NotmuchMessageIdInner,
) -> tuple[list[tuple[EnrichContentId, str]], TaskLedger]:
    """Один ``<task-init>`` (seed + late-гипотезы) + детерминированный ``<task-state>``.

    Слияние seed+hyp в один ``TaskInitOp`` на письмо enrich→reasoning; один reduce итогового
    ledger. Если ничего нового — только ``<task-state>`` из ``fallback_ledger`` (== existing,
    т.к. пустой ``all_new`` означает пустой seed).
    """
    seen: set[str] = set()
    all_new: list[TaskSubtaskDef] = []
    for d in (*seed_defs, *hyp_defs):
        if d.content_id.value in seen:
            continue
        seen.add(d.content_id.value)
        all_new.append(d)

    parts: list[tuple[EnrichContentId, str]] = []
    if all_new:
        init_op = TaskInitOp(subtasks=tuple(all_new), message_id_inner=inner)
        combined = reduce_task_ops([*existing_ops, init_op])
        parts.append(
            (EnrichContentId.from_part_id(EnrichPartId.TASK_INIT), serialize_task_init(tuple(all_new)))
        )
    else:
        combined = fallback_ledger

    parts.append(
        (EnrichContentId.from_part_id(EnrichPartId.TASK_STATE), build_task_state_summary(combined))
    )
    log.info(
        "task_finalize",
        seeded=len(seed_defs),
        hypotheses=len(hyp_defs),
        new_total=len(all_new),
        total=len(combined.subtasks),
    )
    return parts, combined


def _render_mail_context(
    messages: list[EmailMessage],
    limit: int,
    *,
    tier_assignments: dict[int, int] | None = None,
    tier_assignments_types: dict[int, str] | None = None,
    preview_chars: int,
    total_messages: int,
) -> str:
    raw = render_prompt(
        PromptPath.LIGHTRAG_MAIL_CONTEXT,
        messages=messages,
        tier_assignments=tier_assignments or {},
        tier_assignments_types=tier_assignments_types or {},
        preview_chars=preview_chars,
        total_messages=total_messages,
    ).strip()
    return trim_context_text(raw, limit)


def _is_empty_rag_result(raw_result: dict[str, Any] | str | None, api: str) -> bool:
    """Check if RAG returned no useful context."""
    if raw_result is None:
        return True
    if isinstance(raw_result, str):
        stripped = raw_result.strip()
        return not stripped or stripped == "(no graph context)"
    if isinstance(raw_result, dict):
        if api == "aquery_llm":
            llm_resp = raw_result.get("llm_response")
            if isinstance(llm_resp, dict):
                content = llm_resp.get("content", "")
                if not content or content.strip() == "(no graph context)":
                    return True
        if api == "aquery_data":
            data = raw_result.get("data", {})
            if not data or (not data.get("entities") and not data.get("chunks")):
                return True
    return False


def _full_body_weight(msgs: list[EmailMessage], preview_chars: int) -> int:
    """Вес списка писем при full-body рендере — единый аппарат ``estimate_unified_weight``.

    Память (thread/global) рендерится без tier-демоушена, поэтому считаем все письма как
    tier1 (полное тело). Это тот же estimator, что и у unified-бакета, без отдельной
    модели веса с капом.
    """
    if not msgs:
        return 0
    return estimate_unified_weight(score_messages(msgs), len(msgs), 0, preview_chars)


def _truncate_at_paragraph(text: str) -> str:
    """Обрезка до последней границы абзаца перед серединой текста."""
    if not text:
        return ""
    mid = len(text) // 2
    boundary = text.rfind("\n\n", 0, mid)
    if boundary > 0:
        return text[:boundary]
    return text[:mid]


async def _enrich_async(
    *,
    cfg: ThreliumSettings,
    question: str,
    scope: str,
    ctx: UnifiedEmailContext,
    rag_correlation: dict[str, str] | None,
    mckp_capacity: int,
    mckp_priorities: dict[EnrichPartId, float],
    subtask_texts: list[str],
) -> EnrichResult:
    _plan_recent_n = cfg.enrich.plan_recent_n
    _recent_msgs = ctx.all_messages[-_plan_recent_n:] if ctx.all_messages else []
    _older_msgs = ctx.all_messages[:-_plan_recent_n] if len(ctx.all_messages) > _plan_recent_n else []
    _subject_skeleton = [
        {
            "date": m.get("Date", ""),
            "from": m.get("From", ""),
            "subject": m.get("Subject", ""),
        }
        for m in _older_msgs
    ]
    plan_prompt = render_prompt(
        PromptPath.LIGHTRAG_ENRICH_QUERY_PLAN,
        incoming_user_message=question,
        scope=scope,
        recent_messages=_recent_msgs,
        subject_skeleton=_subject_skeleton,
        subtasks=subtask_texts,
    )
    plan_prompt = trim_context_text(plan_prompt, mckp_capacity)
    formulated = (await _enrich_llm_plan(cfg, plan_prompt)).strip()
    if not formulated:
        formulated = question

    extra_instructions = cfg.lightrag.aquery_hints
    aquery_question = render_prompt(
        PromptPath.LIGHTRAG_ENRICH_AQUERY_USER,
        formulated_query=formulated,
        extra_instructions=extra_instructions,
        subtasks=subtask_texts,
    ).strip()
    if not aquery_question:
        raise RuntimeError("enrich: empty aquery question after template render")

    system_prompt = render_prompt(
        LightragPromptLibraryKey.RAG_RESPONSE.prompt_path(), scope=scope
    )
    rag = daemon_lightrag()
    if rag is None:
        raise RuntimeError("enrich: LightRAG daemon is not running (start_rag_loop_thread)")

    qparam = _query_param(cfg)
    api = cfg.lightrag.query_api

    async def _rag_query(q: str) -> dict[str, Any] | str | None:
        if api == "aquery":
            raw = await rag.aquery(
                q,
                param=qparam,
                system_prompt=system_prompt,
            )
            if isinstance(raw, AsyncIterator):
                raise RuntimeError("enrich: streaming LightRAG aquery is not supported")
            if raw is None:
                return None
            if not isinstance(raw, str):
                raise RuntimeError(
                    f"enrich: unexpected aquery return type {type(raw).__name__!r}"
                )
            return raw.strip() or None
        elif api == "aquery_data":
            return await rag.aquery_data(
                q,
                param=qparam,
            )
        elif api == "aquery_llm":
            return await rag.aquery_llm(
                q,
                param=qparam,
                system_prompt=system_prompt,
            )
        else:
            raise RuntimeError(f"enrich: unknown query_api {api!r}")

    raw_result = run_rag_coroutine(
        _rag_query(aquery_question), settings=cfg, correlation=rag_correlation
    )

    retried = False
    if _is_empty_rag_result(raw_result, api):
        log.info("enrich_rag_retry", reason="empty_first_attempt", formulated=formulated)
        retry_query = question if formulated != question else f"key facts about: {question}"
        raw_result = run_rag_coroutine(
            _rag_query(retry_query), settings=cfg, correlation=rag_correlation
        )
        retried = True
        if _is_empty_rag_result(raw_result, api):
            log.info("enrich_rag_retry_failed", formulated=retry_query)

    lightrag_envelope = _build_lightrag_envelope(
        raw_result=raw_result, query_api=api, query_mode=qparam.mode, formulated_query=formulated,
    )
    if retried:
        lightrag_envelope["retried"] = True

    log.info(
        "lightrag_envelope_meta",
        query_api=api,
        query_mode=qparam.mode,
        formulated_query=formulated,
        ok=lightrag_envelope.get("ok"),
    )

    llm_text = lightrag_envelope.get("lightrag", {}).get("llm_text")
    if llm_text and isinstance(llm_text, str) and llm_text.strip():
        graph_answer_raw = llm_text.strip()
    else:
        graph_answer_raw = ""

    _preview = cfg.enrich.tier_preview_chars
    _tier1 = cfg.enrich.tier1_full
    _tier2 = cfg.enrich.tier2_summary

    scored = score_messages(ctx.all_messages) if ctx.all_messages else ()

    # --- Phase 1: estimate weights for MCKP (no Jinja rendering) ---

    _med_ratio = cfg.enrich.tier1_medium_ratio
    _tier1_med = max(1, _tier1 // _med_ratio)
    _tier2_med = max(1, _tier2 // _med_ratio)

    unified_configs = [
        BucketConfig(bucket=EnrichPartId.UNIFIED_MAIL_CONTEXT, tier=BucketConfigTier.FULL,
                     weight=estimate_unified_weight(scored, _tier1, _tier2, _preview) if scored else 0,
                     value=1.0, tier1_count=_tier1, tier2_count=_tier2),
        BucketConfig(bucket=EnrichPartId.UNIFIED_MAIL_CONTEXT, tier=BucketConfigTier.MEDIUM,
                     weight=estimate_unified_weight(scored, _tier1_med, _tier2_med, _preview) if scored else 0,
                     value=0.6, tier1_count=_tier1_med, tier2_count=_tier2_med),
        BucketConfig(bucket=EnrichPartId.UNIFIED_MAIL_CONTEXT, tier=BucketConfigTier.COMPACT,
                     weight=estimate_unified_weight(scored, _tier1, 0, _preview) if scored else 0,
                     value=0.3, tier1_count=_tier1, tier2_count=0),
        BucketConfig(bucket=EnrichPartId.UNIFIED_MAIL_CONTEXT, tier=BucketConfigTier.EMPTY,
                     weight=0, value=0.0, tier1_count=0, tier2_count=0),
    ]

    graph_signal = 1.0 if graph_answer_raw.strip() else 0.0
    thread_signal = 1.0 if ctx.thread_memory_msgs else 0.0
    global_signal = 1.0 if ctx.global_memory_msgs else 0.0

    graph_full_weight = len(graph_answer_raw)
    graph_medium_weight = graph_full_weight // 2

    thread_medium_msgs = ctx.thread_memory_msgs[len(ctx.thread_memory_msgs) // 2:]
    global_medium_msgs = ctx.global_memory_msgs[len(ctx.global_memory_msgs) // 2:]

    def _make_bucket_configs(
        bucket: EnrichPartId, full_weight: int, medium_weight: int, signal: float,
        *, allow_empty: bool = True,
    ) -> list[BucketConfig]:
        configs = [
            BucketConfig(bucket=bucket, tier=BucketConfigTier.FULL,
                         weight=full_weight, value=signal,
                         tier1_count=0, tier2_count=0),
        ]
        if medium_weight > 0 and medium_weight < full_weight:
            configs.append(BucketConfig(bucket=bucket, tier=BucketConfigTier.MEDIUM,
                                        weight=medium_weight, value=0.5 * signal,
                                        tier1_count=0, tier2_count=0))
        if allow_empty:
            configs.append(BucketConfig(bucket=bucket, tier=BucketConfigTier.EMPTY,
                                        weight=0, value=0.0, tier1_count=0, tier2_count=0))
        return configs

    bucket_configs_map: dict[EnrichPartId, list[BucketConfig]] = {
        EnrichPartId.GRAPH_ANSWER: _make_bucket_configs(
            EnrichPartId.GRAPH_ANSWER, graph_full_weight, graph_medium_weight, graph_signal,
            allow_empty=not bool(graph_answer_raw.strip())),
        EnrichPartId.UNIFIED_MAIL_CONTEXT: unified_configs,
        EnrichPartId.THREAD_MEMORY: _make_bucket_configs(
            EnrichPartId.THREAD_MEMORY,
            _full_body_weight(ctx.thread_memory_msgs, _preview),
            _full_body_weight(thread_medium_msgs, _preview),
            thread_signal),
        EnrichPartId.GLOBAL_MEMORY: _make_bucket_configs(
            EnrichPartId.GLOBAL_MEMORY,
            _full_body_weight(ctx.global_memory_msgs, _preview),
            _full_body_weight(global_medium_msgs, _preview),
            global_signal),
    }

    # --- Phase 2: MCKP solve ---

    allocation = solve_mckp(bucket_configs_map, mckp_capacity, mckp_priorities)

    # --- Phase 3: lazy rendering — render only the chosen variant ---

    chosen_unified = allocation.get(EnrichPartId.UNIFIED_MAIL_CONTEXT)
    if chosen_unified and chosen_unified.tier != BucketConfigTier.EMPTY and ctx.all_messages:
        tiered = assign_tiers(scored, chosen_unified.tier1_count, chosen_unified.tier2_count)
        ta = {t.chronological_index: t.assigned_tier for t in tiered}
        ta_types = {t.chronological_index: t.origin for t in tiered}
        final_unified = _render_mail_context(
            ctx.all_messages, mckp_capacity,
            tier_assignments=ta, tier_assignments_types=ta_types,
            preview_chars=_preview, total_messages=len(ctx.all_messages),
        )
    else:
        final_unified = ""

    chosen_graph = allocation.get(EnrichPartId.GRAPH_ANSWER)
    if chosen_graph and chosen_graph.tier == BucketConfigTier.FULL:
        final_graph = graph_answer_raw
    elif chosen_graph and chosen_graph.tier == BucketConfigTier.MEDIUM:
        final_graph = _truncate_at_paragraph(graph_answer_raw)
    else:
        final_graph = ""

    chosen_thread = allocation.get(EnrichPartId.THREAD_MEMORY)
    if chosen_thread and chosen_thread.tier == BucketConfigTier.FULL:
        final_thread = _render_mail_context(
            ctx.thread_memory_msgs, mckp_capacity,
            tier_assignments=None, tier_assignments_types=None,
            preview_chars=_preview, total_messages=len(ctx.thread_memory_msgs),
        )
    elif chosen_thread and chosen_thread.tier == BucketConfigTier.MEDIUM:
        final_thread = _render_mail_context(
            thread_medium_msgs, mckp_capacity,
            tier_assignments=None, tier_assignments_types=None,
            preview_chars=_preview, total_messages=len(ctx.thread_memory_msgs),
        )
    else:
        final_thread = ""

    chosen_global = allocation.get(EnrichPartId.GLOBAL_MEMORY)
    if chosen_global and chosen_global.tier == BucketConfigTier.FULL:
        final_global = _render_mail_context(
            ctx.global_memory_msgs, mckp_capacity,
            tier_assignments=None, tier_assignments_types=None,
            preview_chars=_preview, total_messages=len(ctx.global_memory_msgs),
        )
    elif chosen_global and chosen_global.tier == BucketConfigTier.MEDIUM:
        final_global = _render_mail_context(
            global_medium_msgs, mckp_capacity,
            tier_assignments=None, tier_assignments_types=None,
            preview_chars=_preview, total_messages=len(ctx.global_memory_msgs),
        )
    else:
        final_global = ""

    graph_answer_vo = EnrichGraphAnswerText.parse(final_graph)

    log.info(
        "budget_allocation",
        capacity=mckp_capacity,
        graph_tier=chosen_graph.tier if chosen_graph else "none",
        unified_tier=chosen_unified.tier if chosen_unified else "none",
        thread_tier=chosen_thread.tier if chosen_thread else "none",
        global_tier=chosen_global.tier if chosen_global else "none",
        total_weight=sum(c.weight for c in allocation.values()),
    )

    return EnrichResult(
        graph_answer=graph_answer_vo if graph_answer_vo.value else None,
        unified_mail_context=EnrichUnifiedMailContextText.parse(final_unified) if final_unified else None,
        thread_memory=EnrichThreadMemoryText.parse(final_thread) if final_thread else None,
        global_memory=EnrichGlobalMemoryText.parse(final_global) if final_global else None,
    )


def _emit_summarize_overflow(
    msg: EmailMessage,
    stage: FsmStage,
    *,
    config: ThreliumSettings,
    ctx: UnifiedEmailContext,
    mckp_capacity: int,
) -> EmailMessage:
    """Build JSON payload for summarize_context when unified overflows budget."""
    batch_max = config.enrich.summarize_batch_max_messages
    candidates = ctx.all_messages[:batch_max]

    mids: list[str] = []
    bodies: list[str] = []
    for m in candidates:
        raw_mid = m.get(MailHeaderName.MESSAGE_ID)
        if not raw_mid:
            continue
        w = RfcMessageIdWire.parse_present_optional(str(raw_mid))
        if w is None:
            continue
        inner = NotmuchMessageIdInner.from_optional_wire(w)
        if inner is None:
            continue
        body_text = concat_history_parts_text(m)
        if not body_text.strip():
            log.warning(
                "summarize_overflow_skip_no_history",
                message_id=inner.value,
            )
            continue
        mids.append(inner.value)
        bodies.append(body_text)

    if not mids:
        log.error(
            "summarize_overflow_empty_batch",
            candidate_count=len(candidates),
            mckp_capacity=mckp_capacity,
        )
        raise RuntimeError(
            "overflow summarize: no messages with non-empty <history> in batch"
        )

    payload = json.dumps({"summarize": {"mids": mids, "bodies": bodies}}, ensure_ascii=False)
    log.info(
        "overflow_to_summarize",
        candidate_count=len(mids),
        mckp_capacity=mckp_capacity,
    )
    return build_fsm_plain_to_stage(
        msg,
        to_addr=FsmStage.SUMMARIZE_CONTEXT,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(payload),
        settings=config,
    )


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    mid_w = RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID)
    inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
    tid_vo = nmlib.thread_id_for_optional_message_id(inner)
    if tid_vo is None:
        raise RuntimeError("enrich: notmuch has no thread_id for this message (is it indexed yet?)")

    ctx = build_unified_email_messages(
        settings=config,
        leaf_inner=inner,
        thread_id=tid_vo.value,
    )

    limit = config.enrich.context_max_chars
    _all_priorities = {
        EnrichPartId.USER_MESSAGE: config.enrich.priority_user,
        EnrichPartId.GRAPH_ANSWER: config.enrich.priority_graph,
        EnrichPartId.UNIFIED_MAIL_CONTEXT: config.enrich.priority_unified,
        EnrichPartId.THREAD_MEMORY: config.enrich.priority_thread_mem,
        EnrichPartId.GLOBAL_MEMORY: config.enrich.priority_global_mem,
        EnrichPartId.RESPONSE_STATE: config.enrich.priority_extra,
    }
    _norm = normalize_weights(_all_priorities)
    budget_user = int(limit * _norm[EnrichPartId.USER_MESSAGE])
    budget_extra = int(limit * _norm[EnrichPartId.RESPONSE_STATE])
    user_message_text = trim_context_text(
        render_prompt(
            PromptPath.LIGHTRAG_ENRICH_INCOMING_USER_TEXT,
            incoming=msg,
        ).strip(),
        budget_user,
    )
    if not user_message_text:
        raise RuntimeError(
            f"enrich: empty user message after incoming template ({_HDR.SUBJECT} / body)"
        )

    scope = tid_vo.as_notmuch_thread_term()
    rag_correlation: dict[str, str] | None = None
    if config.e2e.litellm_route_correlation:
        snap = get_litellm_http_correlation()
        th = threading.current_thread()
        route_k = _HDR.ROUTE.value
        route_v = snap.get(route_k) if snap else None
        rt = route_v if isinstance(route_v, str) else None
        log.debug(
            "e2e_litellm_tls",
            thread_name=th.name,
            thread_ident=threading.get_ident(),
            snap_is_none=snap is None,
            snap_keys=sorted(snap.keys()) if snap else [],
            route_header_present=bool(snap and route_k in snap),
            route_tail=e2e_route_wire_tail(rt),
            message_id=mid_w.value if mid_w else None,
        )
        rag_correlation = dict(snap) if snap else None
        if rag_correlation is not None:
            rag_correlation[LitellmCorrelationHeader.CALL_SITE.value] = (
                LitellmCallSite.LIGHTRAG_QUERY.value
            )
        log.debug(
            "asyncio_run_starting",
            fsm_thread=th.name,
            ident=threading.get_ident(),
            message_id=mid_w.value if mid_w else None,
        )
    mckp_capacity = max(0, limit - budget_user - budget_extra)

    if config.enrich.summarize_enabled and ctx.all_messages:
        # Overflow: estimate_unified_weight (все <history> на письмо, как history_text) × те же
        # tier1/tier2/preview, что unified_configs[FULL] — без полного Jinja mail_context.j2.
        raw_weight = estimate_unified_weight(
            score_messages(ctx.all_messages),
            config.enrich.tier1_full,
            config.enrich.tier2_summary,
            config.enrich.tier_preview_chars,
        )
        excess = raw_weight - mckp_capacity
        if excess > config.enrich.summarize_trigger_min_excess_chars:
            return _emit_summarize_overflow(
                msg, stage, config=config, ctx=ctx,
                mckp_capacity=mckp_capacity,
            )

    mckp_priorities = {
        EnrichPartId.GRAPH_ANSWER: config.enrich.priority_graph,
        EnrichPartId.UNIFIED_MAIL_CONTEXT: config.enrich.priority_unified,
        EnrichPartId.THREAD_MEMORY: config.enrich.priority_thread_mem,
        EnrichPartId.GLOBAL_MEMORY: config.enrich.priority_global_mem,
    }

    # Early seed формируется ДО сбора контекста LightRAG: тексты подзадач ledger
    # подмешиваются в графовый запрос (помимо сформулированного LLM вопроса). Финализация
    # MIME (<task-init>/<task-state>) откладывается до слияния с late-гипотезами после RAG.
    task_parts: list[tuple[EnrichContentId, str]] = []
    subtask_texts: list[str] = []
    seed_defs: list[TaskSubtaskDef] = []
    existing_task_ops: list[TaskOp] = []
    ledger_after_seed = TaskLedger.empty()
    if inner is not None:
        seed_defs, existing_task_ops, ledger_after_seed = _build_task_seed_defs(
            config=config,
            inner=inner,
            user_message_text=user_message_text,
        )
        subtask_texts = [s.text.value for s in ledger_after_seed.subtasks]

    result = asyncio.run(
        _enrich_async(
            cfg=config,
            question=user_message_text,
            scope=scope,
            ctx=ctx,
            rag_correlation=rag_correlation,
            mckp_capacity=mckp_capacity,
            mckp_priorities=mckp_priorities,
            subtask_texts=subtask_texts,
        )
    )

    # Late-гипотезы: LLM на полном контексте после RAG, тот же ledger (один TaskInitOp).
    if inner is not None:
        hyp_defs = _build_task_hypothesis_defs(
            config=config,
            user_message_text=user_message_text,
            result=result,
            ledger_after_seed=ledger_after_seed,
        )
        task_parts, _ = _finalize_task_mime_parts(
            seed_defs=seed_defs,
            hyp_defs=hyp_defs,
            existing_ops=existing_task_ops,
            fallback_ledger=ledger_after_seed,
            inner=inner,
        )

    extra_parts = _collect_extra_parts(inner, budget_extra) if inner is not None else []
    extra_parts.extend(task_parts)

    enriched = build_enriched_multipart(
        msg,
        user_message_text=trim_context_text(user_message_text, budget_user),
        graph_answer=result.graph_answer,
        unified_mail_context=result.unified_mail_context,
        thread_memory=result.thread_memory,
        global_memory=result.global_memory,
        stage=stage.value,
        extra_parts=extra_parts or None,
    )
    return emit_transition_simple_step_preserving_payload(
        enriched, to_addr=FsmStage.REASONING, from_stage=stage, settings=config,
    )
