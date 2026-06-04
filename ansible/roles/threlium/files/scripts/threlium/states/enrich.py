#!/usr/bin/env python3
"""enrich@localhost: линейный пайплайн notmuch-контекст → LightRAG → reasoning@localhost.

11 шагов (plan ``simplified_enrich_pipeline``, ``docs/FSM.md`` §5.2):

  1. user query (``<user-query>`` CID → ``enrich_incoming_user_text.j2``);
  2. unified messages (``build_unified_email_messages``);
  3. task seed (``enrich_task_plan``) + ``lightrag_query.j2`` (user → seed subtasks → thread);
  4. cap строки запроса по токенам (``trim_from_end_tokens`` → ``lightrag_query_budget``);
  5. один ``run_lightrag_aquery`` (без отдельного plan-LLM);
  6. graph answer (``format_graph_answer_part``);
  7. late hypotheses (``enrich_task_hypotheses``, промпт тоже capped по токенам);
  8. полный набор MIME (``<task-init>``/``<task-state>``/``<response-state>``);
  9. token ledger: mandatory (FULL) + гранулярные ``<history>`` (reducible);
  10. overflow X = total − effective_budget → summarize_context самых старых history CID;
  11. ``build_context_backpack_multipart`` → reasoning@ (гранулярная история, не merged блоб).
"""
from __future__ import annotations

import asyncio
import copy
import threading
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any

import msgspec

from threlium import nm as nmlib
from threlium.context_token_count import (
    build_tokenizer,
    count_tokens,
    lightrag_query_budget,
    reasoning_effective_budget,
    trim_from_end_tokens,
)
from threlium.enrich_context import (
    build_unified_email_messages,
    trim_context_text,
    UnifiedEmailContext,
)
from threlium.graph_answer_view import format_graph_answer_part
from threlium.fsm_emit import build_fsm_plain_to_stage
from threlium.fsm_emit_semantic import emit_transition_simple_step_preserving_payload
from threlium.ledger_context_parts import ledger_context_parts
from threlium.litellm_route_context import e2e_route_wire_tail, get_litellm_http_correlation
from threlium.logutil import logger
from threlium.mime_reform import (
    EnrichContentId,
    EnrichPartId,
    build_context_backpack_multipart,
    history_part_text,
    iter_history_parts,
    require_enrich_user_query_text,
)
from threlium.nm import require_fsm_message_id
from threlium.prompts import render_prompt
from threlium.runners.lightrag.aquery import build_lightrag_query_param, run_lightrag_aquery
from threlium.settings import ThreliumSettings
from threlium.states.enrich_task_llm import (
    invoke_task_hypothesis_subtasks,
    invoke_task_plan_subtasks,
)
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
    EnrichTaskHypothesesPromptContext,
    EnrichThreadMemoryText,
    EnrichUnifiedMailContextText,
    ReasoningUserMessageText,
    FsmTransitionPlainBody,
    LightragPromptLibraryKey,
    LitellmCallSite,
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
    PromptPath,
    RfcMessageIdWire,
    SummarizeContextBatch,
    SummarizeContextStagePayload,
    SummarizeHistoryUnit,
    TaskLedger,
    TaskSubtaskContentId,
    TaskSubtaskText,
)
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader

log = logger.bind(stage="enrich")

_HDR = MailHeaderName


def _require_rendered_user_message(text: str) -> ReasoningUserMessageText:
    """Отрендеренный ``<user-message>`` для task-plan / hypotheses (не ``EnrichUserQueryText``)."""
    vo = ReasoningUserMessageText.parse_present_optional(text)
    if vo is None:
        raise RuntimeError("enrich: empty user message for task LLM prompt")
    return vo


def _message_inner(msg: EmailMessage) -> NotmuchMessageIdInner | None:
    """``Message-ID`` письма → notmuch inner mid (для summarize ``source_mid``)."""
    raw = msg.get(_HDR.MESSAGE_ID)
    if not raw:
        return None
    w = RfcMessageIdWire.parse_present_optional(str(raw))
    if w is None:
        return None
    return NotmuchMessageIdInner.from_optional_wire(w)


def _build_lightrag_envelope(
    *,
    raw_result: dict[str, Any] | str | None,
    query_api: str,
    query_mode: str,
    formulated_query: str,
) -> dict[str, Any]:
    """Envelope dict для ``format_graph_answer_part`` / strict parse ``lightrag.raw``."""
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
    """Гранулярные компоненты enriched-контекста (graph + полные тексты памяти/unified).

    ``unified_mail_context`` рендерится полностью только для промпта late-гипотез; в backpack
    unified едет гранулярными ``<history>`` leaf-частями, не этим блобом.
    """

    graph_answer: EnrichGraphAnswerText | None
    unified_mail_context: EnrichUnifiedMailContextText | None
    thread_memory: EnrichThreadMemoryText | None
    global_memory: EnrichGlobalMemoryText | None


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

    subtasks = invoke_task_plan_subtasks(
        config=config,
        incoming_user_message=_require_rendered_user_message(user_message_text),
        existing_ledger=existing_ledger,
    )
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

    Промпт ``enrich_task_hypotheses.j2`` рендерится в порядке graph → memory →
    existing_subtasks → unified и **token-capped** внутри ``invoke_task_hypothesis_subtasks``
    (хвост unified режется первым). Гипотезы дедуплицируются против seed+существующих
    подзадач (``ledger_after_seed``). Fail-open: ошибка LLM → ``[]``.
    """
    subtasks = invoke_task_hypothesis_subtasks(
        config=config,
        prompt_context=EnrichTaskHypothesesPromptContext(
            incoming_user_message=_require_rendered_user_message(user_message_text),
            graph_answer=result.graph_answer,
            unified_mail_context=result.unified_mail_context,
            thread_memory=result.thread_memory,
            global_memory=result.global_memory,
        ),
        ledger_after_seed=ledger_after_seed,
    )
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
    limit: int,
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

    # <task-state> усекается симметрично <response-state> (CONTEXT_CONTRACT §4/§6):
    # детерминированный recompute не должен переполнять бюджет extra-части.
    parts.append(
        (
            EnrichContentId.from_part_id(EnrichPartId.TASK_STATE),
            trim_context_text(build_task_state_summary(combined), limit),
        )
    )
    log.info(
        "task_finalize",
        seeded=len(seed_defs),
        hypotheses=len(hyp_defs),
        new_total=len(all_new),
        total=len(combined.subtasks),
    )
    return parts, combined


def _render_mail_context_full(messages: list[EmailMessage], config: ThreliumSettings) -> str:
    """Полный рендер списка писем (full body, без MCKP-tier): unified / thread / global."""
    if not messages:
        return ""
    return render_prompt(
        PromptPath.LIGHTRAG_MAIL_CONTEXT,
        messages=messages,
        tier_assignments={},
        tier_assignments_types={},
        preview_chars=config.enrich.tier_preview_chars,
        total_messages=len(messages),
    ).strip()


def _render_thread_context(messages: list[EmailMessage], config: ThreliumSettings) -> str:
    """Контекст треда для ``lightrag_query.j2`` — newest-first (хвост = старые письма)."""
    if not messages:
        return ""
    return _render_mail_context_full(list(reversed(messages)), config)


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
            if not data:
                return True
            if not data.get("entities") and not data.get("relationships") and not data.get(
                "chunks"
            ):
                return True
    return False


async def _enrich_lightrag_once(
    *,
    cfg: ThreliumSettings,
    question: str,
    scope: str,
    ctx: UnifiedEmailContext,
    rag_correlation: dict[str, str] | None,
    subtask_texts: list[str],
    tokenizer: Any,
) -> EnrichResult:
    """Шаги 3b–6: один ``lightrag_query.j2`` (+ token cap) → один ``aquery`` → graph answer."""
    thread_context = _render_thread_context(ctx.all_messages, cfg)
    budget = lightrag_query_budget(cfg)
    query_text = render_prompt(
        PromptPath.LIGHTRAG_QUERY,
        incoming_user_message=question,
        subtasks=subtask_texts,
        extra_instructions=cfg.lightrag.aquery_hints or None,
        thread_context=thread_context or None,
    ).strip()
    query_text = trim_from_end_tokens(tokenizer, query_text, budget)
    if not query_text.strip():
        raise RuntimeError("enrich: empty lightrag query after render/cap")

    system_prompt = render_prompt(
        LightragPromptLibraryKey.RAG_RESPONSE.prompt_path(), scope=scope
    )
    api = cfg.lightrag.query_api
    qparam = build_lightrag_query_param(cfg)

    raw_result = run_lightrag_aquery(
        query_text,
        settings=cfg,
        correlation=rag_correlation,
        system_prompt=system_prompt,
    )

    retried = False
    if _is_empty_rag_result(raw_result, api):
        log.info("enrich_rag_retry", reason="empty_first_attempt")
        retry_query = question if query_text != question else f"key facts about: {question}"
        retry_query = trim_from_end_tokens(tokenizer, retry_query, budget)
        raw_result = run_lightrag_aquery(
            retry_query,
            settings=cfg,
            correlation=rag_correlation,
            system_prompt=system_prompt,
        )
        retried = True
        if _is_empty_rag_result(raw_result, api):
            log.info("enrich_rag_retry_failed")

    lightrag_envelope = _build_lightrag_envelope(
        raw_result=raw_result, query_api=api, query_mode=qparam.mode, formulated_query=question,
    )
    if retried:
        lightrag_envelope["retried"] = True

    log.info(
        "lightrag_envelope_meta",
        query_api=api,
        query_mode=qparam.mode,
        ok=lightrag_envelope.get("ok"),
    )

    graph_prose = format_graph_answer_part(lightrag_envelope, cfg.enrich)

    unified_full = _render_mail_context_full(ctx.all_messages, cfg)
    thread_full = _render_mail_context_full(ctx.thread_memory_msgs, cfg)
    global_full = _render_mail_context_full(ctx.global_memory_msgs, cfg)

    return EnrichResult(
        graph_answer=EnrichGraphAnswerText.parse_present_optional(graph_prose),
        unified_mail_context=EnrichUnifiedMailContextText.parse_present_optional(unified_full),
        thread_memory=EnrichThreadMemoryText.parse_present_optional(thread_full),
        global_memory=EnrichGlobalMemoryText.parse_present_optional(global_full),
    )


@dataclass(frozen=True)
class _HistoryUnit:
    """Одна ``<history>`` leaf-часть unified: CID, тело, письмо-носитель, токены."""

    cid: EnrichContentId
    part: EmailMessage
    text: str
    source_inner: NotmuchMessageIdInner | None
    tokens: int


def _collect_history_units(
    ctx: UnifiedEmailContext, tokenizer: Any
) -> list[_HistoryUnit]:
    """Все ``<history>`` leaf-части unified, oldest→newest, с подсчётом токенов."""
    units: list[_HistoryUnit] = []
    for m in ctx.all_messages:
        m_inner = _message_inner(m)
        for cid, part in iter_history_parts(m):
            text = history_part_text(part).strip()
            if not text:
                continue
            units.append(
                _HistoryUnit(
                    cid=cid,
                    part=part,
                    text=text,
                    source_inner=m_inner,
                    tokens=count_tokens(tokenizer, text),
                )
            )
    return units


def _emit_summarize_overflow(
    msg: EmailMessage,
    stage: FsmStage,
    *,
    config: ThreliumSettings,
    history_units: list[_HistoryUnit],
    excess_tokens: int,
) -> EmailMessage:
    """Шаг 10: самые старые ``<history>`` CID под избыток X → JSON для summarize_context."""
    selected: list[SummarizeHistoryUnit] = []
    acc = 0
    for u in history_units:
        if u.source_inner is None:
            continue
        selected.append(
            SummarizeHistoryUnit(cid=u.cid.value, text=u.text, source_mid=u.source_inner.value)
        )
        acc += u.tokens
        if acc >= excess_tokens:
            break

    if not selected:
        log.error(
            "summarize_overflow_empty_batch",
            unit_count=len(history_units),
            excess_tokens=excess_tokens,
        )
        raise RuntimeError(
            "overflow summarize: no <history> units with resolvable source_mid in batch"
        )

    # Канонический ход = <user-query> CID текущего enrich-листа (не последняя <history>);
    # суммаризация его не меняет — тот же текст по enrich → summarize_context (<system>)
    # → summarize_memory → re-trigger enrich (CONTEXT_CONTRACT §5).
    user_query = require_enrich_user_query_text(msg).value
    payload = msgspec.json.encode(
        SummarizeContextStagePayload(
            summarize=SummarizeContextBatch(units=selected),
            user_query=user_query,
        )
    ).decode("utf-8")
    log.info(
        "overflow_to_summarize",
        selected=len(selected),
        selected_tokens=acc,
        excess_tokens=excess_tokens,
    )
    return build_fsm_plain_to_stage(
        msg,
        to_addr=FsmStage.SUMMARIZE_CONTEXT,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(payload),
        settings=config,
    )


def _build_rag_correlation(
    config: ThreliumSettings, mid_w: RfcMessageIdWire | None
) -> dict[str, str] | None:
    """e2e route correlation snapshot со стампом ``LIGHTRAG_QUERY`` call-site."""
    if not config.e2e.litellm_route_correlation:
        return None
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
    return rag_correlation


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    _mid_w, inner = require_fsm_message_id(msg, "enrich")
    tid_vo = nmlib.thread_id_for_optional_message_id(inner)
    if tid_vo is None:
        raise RuntimeError("enrich: notmuch has no thread_id for this message (is it indexed yet?)")

    ctx = build_unified_email_messages(
        settings=config,
        leaf_inner=inner,
        thread_id=tid_vo.value,
    )

    tokenizer = build_tokenizer(config)
    # Малые CRDT-части (<response-state>/<task-state>) усекаются по символам отдельно от
    # токенного overflow unified-истории (они mandatory и невелики).
    state_limit = config.enrich.context_max_chars

    user_message_text = render_prompt(
        PromptPath.LIGHTRAG_ENRICH_INCOMING_USER_TEXT,
        incoming=msg,
    ).strip()
    if not user_message_text:
        raise RuntimeError(
            f"enrich: empty user message after incoming template ({_HDR.SUBJECT} / body)"
        )

    scope = tid_vo.as_notmuch_thread_term()
    rag_correlation = _build_rag_correlation(config, _mid_w)

    # --- Шаг 3a: seed подзадачи (ДО RAG); их тексты идут в lightrag_query.j2 ---
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

    # --- Шаги 3b–6: один RAG → graph answer ---
    result = asyncio.run(
        _enrich_lightrag_once(
            cfg=config,
            question=user_message_text,
            scope=scope,
            ctx=ctx,
            rag_correlation=rag_correlation,
            subtask_texts=subtask_texts,
            tokenizer=tokenizer,
        )
    )

    # --- Шаги 7–8: late hypotheses (capped) + один <task-init>/<task-state> ---
    task_parts: list[tuple[EnrichContentId, str]] = []
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
            limit=state_limit,
        )

    extra_parts = ledger_context_parts(inner, state_limit) if inner is not None else []
    extra_parts.extend(task_parts)

    # --- Шаг 9: token ledger (mandatory FULL + гранулярная history) ---
    history_units = _collect_history_units(ctx, tokenizer)

    mandatory_texts: list[str] = [user_message_text]
    if result.graph_answer is not None:
        mandatory_texts.append(result.graph_answer.value)
    if result.thread_memory is not None:
        mandatory_texts.append(result.thread_memory.value)
    if result.global_memory is not None:
        mandatory_texts.append(result.global_memory.value)
    for _cid, text in extra_parts:
        mandatory_texts.append(text)

    mandatory_tokens = sum(count_tokens(tokenizer, t) for t in mandatory_texts)
    history_tokens = sum(u.tokens for u in history_units)
    effective_budget = reasoning_effective_budget(config)

    if mandatory_tokens > effective_budget:
        raise RuntimeError(
            f"enrich: mandatory context {mandatory_tokens} tok exceeds reasoning "
            f"effective_budget {effective_budget} tok (cannot summarize task/graph/memory)"
        )

    total_tokens = mandatory_tokens + history_tokens
    excess = total_tokens - effective_budget
    log.info(
        "token_ledger",
        mandatory_tokens=mandatory_tokens,
        history_tokens=history_tokens,
        total_tokens=total_tokens,
        effective_budget=effective_budget,
        excess=excess,
    )

    # --- Шаг 10: overflow X > 0 → summarize самых старых history CID ---
    if config.enrich.summarize_enabled and excess > 0:
        return _emit_summarize_overflow(
            msg, stage, config=config, history_units=history_units, excess_tokens=excess,
        )

    # --- Шаг 11: гранулярный backpack → reasoning ---
    backpack = build_context_backpack_multipart(
        msg,
        user_message_text=user_message_text,
        graph_answer=result.graph_answer,
        thread_memory=result.thread_memory,
        global_memory=result.global_memory,
        history_parts=[(u.cid, u.part) for u in history_units],
        stage=stage.value,
        extra_parts=extra_parts or None,
    )
    return emit_transition_simple_step_preserving_payload(
        backpack, to_addr=FsmStage.REASONING, from_stage=stage, settings=config,
    )
