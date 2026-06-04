#!/usr/bin/env python3
"""reasoning@localhost: LLM reducer → следующая стадия (ARCHITECTURE §6.1).

Маршрут — через OpenAI-compatible ``tool_calls``: для каждой целевой
:class:`~threlium.types.fsm_stage.FsmStage` из разрешённого набора загружается
отдельный tool из ``prompts/reasoning/<stage>/tool_spec.j2``. ``tool_choice`` = ``required``.
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.formal_reason_gate import formal_reason_gate_active
from threlium.litellm_required_tool import build_site_call, correlation_with_call_site
from threlium.litellm_route_context import get_litellm_http_correlation
from threlium.litellm_tool_completion import completion_required_tool_sync
from threlium.litellm_tool_response import require_tool_calls_response
from litellm.types.utils import Message

from threlium.fsm_emit import build_fsm_step_to_stage, hop_budget_remaining
from threlium.logutil import clip_log_text, logger
from threlium.mail import canonicalize_mime
from threlium.nm import require_fsm_message_id
from threlium.prompts import render_prompt
from threlium.states.reasoning_tool_spec import (
    load_tools_for_routes,
    route_decision_from_tool_call,
)
from threlium.context_token_count import (
    build_tokenizer,
    reasoning_effective_budget,
    trim_from_end_tokens,
)
from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    LiteLlmChatMessage,
    LitellmCallSite,
    LitellmRoutingSite,
    PromptPath,
    REASONING_TARGET_STAGES,
    ReasoningIncomingEnvelope,
    ReasoningEnrichContext,
    ReasoningRouteDecision,
    LiteLlmToolCallArgumentsWire,
    ReasoningToolFunctionName,
    RfcMessageIdWire,
    MailHeaderName,
    reasoning_assistant_message,
    reasoning_assistant_plain_text,
    reasoning_finish_reason,
    reasoning_first_tool_call,
)

_HDR = MailHeaderName

log = logger.bind(stage="reasoning")

_MAX_WRONG_TOOL_RETRIES = 12


class ReasoningStageError(Exception):
    """Ошибка стадии reasoning."""


def compute_allowed_routes(msg: EmailMessage, remaining: int) -> frozenset[FsmStage]:
    if remaining < 1:
        return frozenset({FsmStage.RESPONSE_FINALIZE})
    if formal_reason_gate_active(msg):
        return frozenset({FsmStage.FORMAL_REASON, FsmStage.MEMORY_QUERY})
    return REASONING_TARGET_STAGES


def _render_user_prompt(
    msg: EmailMessage, hop_budget: HopBudgetLine, max_chars: int,
) -> str:
    envelope = ReasoningIncomingEnvelope.from_email(msg, hop_budget=hop_budget)
    ctx = ReasoningEnrichContext.from_email(msg, max_chars=max_chars)
    return render_prompt(
        PromptPath.REASONING_USER,
        message_id=envelope.message_id.value if envelope.message_id is not None else None,
        in_reply_to=envelope.in_reply_to.value if envelope.in_reply_to is not None else None,
        references=envelope.references.value if envelope.references is not None else None,
        subject=envelope.subject.value if envelope.subject is not None else None,
        from_hdr=envelope.from_hdr.value if envelope.from_hdr is not None else None,
        hop_budget=envelope.hop_budget.value,
        user_text=ctx.user_message.value if ctx.user_message is not None else None,
        knowledge_graph=ctx.knowledge_graph.value if ctx.knowledge_graph is not None else None,
        mail_context=ctx.mail_context.value if ctx.mail_context is not None else None,
        thread_memory=ctx.thread_memory.value if ctx.thread_memory is not None else None,
        global_memory=ctx.global_memory.value if ctx.global_memory is not None else None,
        response_state=ctx.response_state.value if ctx.response_state is not None else None,
        task_state=ctx.task_state.value if ctx.task_state is not None else None,
        history=[{"origin": e.origin, "text": e.text} for e in ctx.history],
    )


def _route_from_assistant(
    assistant: Message,
    schemas: dict[FsmStage, dict[str, object]],
) -> ReasoningRouteDecision:
    tc = reasoning_first_tool_call(assistant)
    if tc is None:
        text = reasoning_assistant_plain_text(assistant)
        if not text.value:
            raise ReasoningStageError("LLM returned neither tool_call nor text")
        raise ReasoningStageError(
            "LLM returned plain text without tool_call (tool-only policy)"
        )
    tool_name = ReasoningToolFunctionName.parse_tool_call(tc)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    log.info(
        "tool_call_args",
        route=tool_name.value,
        args_len=len(wire.value),
        args_wire=clip_log_text(wire.value),
    )
    decision = route_decision_from_tool_call(tool_name, wire, schemas)
    log.info(
        "rendered_email",
        route=decision.target.value,
        subject_len=len(decision.subject.value),
        body_len=len(decision.body.value),
        body_stripped_len=len(decision.body.value.strip()),
    )
    if decision.target == FsmStage.RESPONSE_APPEND and not decision.body.value.strip():
        raise ReasoningStageError(
            "response_append: rendered body is empty after strip "
            "(LLM likely sent whitespace-only content)"
        )
    return decision


def _mode_notices(
    msg: EmailMessage,
    remaining: int,
    retry_count: int,
) -> list[str]:
    notices: list[str] = []
    if remaining < 1:
        notices.append(
            render_prompt(
                PromptPath.REASONING_BUDGET_EXHAUSTED, retry_count=retry_count
            ).strip()
        )
    elif formal_reason_gate_active(msg):
        notices.append(
            render_prompt(
                PromptPath.REASONING_FORMAL_REASON_GATE, retry_count=retry_count
            ).strip()
        )
    return notices


def _decide(
    msg: EmailMessage,
    hop_budget: HopBudgetLine,
    *,
    config: ThreliumSettings,
) -> ReasoningRouteDecision:
    ep = resolve_llm_endpoint(config.litellm, LitellmRoutingSite.REASONING)
    length_max_attempts = (
        ep.length_recovery_max_attempts
        if ep.length_recovery_max_attempts is not None
        else config.litellm.length_recovery_max_attempts
    )
    remaining = hop_budget_remaining(hop_budget, config)
    gate_active = formal_reason_gate_active(msg)
    log.info(
        "litellm_routing",
        site=LitellmRoutingSite.REASONING.value,
        score=ep.score,
        remaining_hops=remaining,
        formal_reason_gate_active=gate_active,
    )

    allowed = compute_allowed_routes(msg, remaining)
    routes = sorted(allowed, key=lambda s: s.value)
    tools, schemas = load_tools_for_routes(routes)
    restricted = len(allowed) < len(REASONING_TARGET_STAGES)
    call_site_wire = LitellmCallSite.REASONING.value

    system = render_prompt(PromptPath.REASONING_SYSTEM).strip()
    length_recovery_system = render_prompt(
        PromptPath.REASONING_LENGTH_RECOVERY_SYSTEM
    ).strip()
    # Один глобальный token-cap на собранный backpack (а не per-field char-cap): хвост =
    # старая <conversation_delta>/история режется первым, mandatory-секции у начала промпта
    # сохраняются. enrich-сторона уже гарантирует X<=0; это страховка для enrich_fast/субагентов.
    user_content = _render_user_prompt(msg, hop_budget, config.enrich.context_max_chars)
    user_content = trim_from_end_tokens(
        build_tokenizer(config), user_content, reasoning_effective_budget(config)
    )

    wrong_tool_retries = 0
    length_attempt = 0
    length_recovery_extra: list[LiteLlmChatMessage] = []

    while True:
        notices = _mode_notices(msg, remaining, wrong_tool_retries)
        messages: list[LiteLlmChatMessage] = [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user_content),
            *length_recovery_extra,
        ]
        for notice in notices:
            messages.append(LiteLlmChatMessage(role="system", content=notice))

        call = build_site_call(
            config,
            LitellmRoutingSite.REASONING,
            messages,
            endpoint=ep,
        )

        correlation = None
        if config.e2e.litellm_route_correlation:
            correlation = correlation_with_call_site(
                get_litellm_http_correlation(), call_site_wire
            )
        resp = completion_required_tool_sync(
            settings=config,
            call=call,
            tools=tools,
            correlation_override=correlation,
        )
        finish = reasoning_finish_reason(resp)
        if finish == "length":
            log.warning(
                "llm_finish_reason_length",
                attempt=length_attempt + 1,
                max_attempts=length_max_attempts,
            )
            length_attempt += 1
            if length_attempt >= length_max_attempts:
                raise ReasoningStageError(
                    "LLM completion truncated (finish_reason=length) after recovery retry"
                )
            length_recovery_extra.append(
                LiteLlmChatMessage(role="system", content=length_recovery_system)
            )
            continue

        if remaining >= 1:
            require_tool_calls_response(resp, context="reasoning")

        assistant = reasoning_assistant_message(resp)
        tc = reasoning_first_tool_call(assistant)
        if tc is None:
            if remaining < 1:
                wrong_tool_retries += 1
                if wrong_tool_retries > _MAX_WRONG_TOOL_RETRIES:
                    raise ReasoningStageError(
                        "hop budget exhausted: no tool_call after max retries"
                    )
                log.warning(
                    "restricted_mode_no_tool_call",
                    retry_count=wrong_tool_retries,
                    mode="force_finalize",
                )
                continue
            return _route_from_assistant(assistant, schemas)

        name = ReasoningToolFunctionName.parse_tool_call(tc)
        route = name.target_stage()
        if restricted and route not in allowed:
            wrong_tool_retries += 1
            if wrong_tool_retries > _MAX_WRONG_TOOL_RETRIES:
                raise ReasoningStageError(
                    f"wrong tool {name.value!r} after max retries "
                    f"(allowed={[s.value for s in allowed]})"
                )
            log.warning(
                "wrong_tool_rejected",
                got=name.value,
                retry_count=wrong_tool_retries,
                allowed=[s.value for s in allowed],
                formal_reason_gate_active=gate_active,
            )
            continue

        if remaining < 1 and route != FsmStage.RESPONSE_FINALIZE:
            wrong_tool_retries += 1
            if wrong_tool_retries > _MAX_WRONG_TOOL_RETRIES:
                raise ReasoningStageError(
                    f"hop budget exhausted: got {name.value!r} after max retries"
                )
            log.warning(
                "force_finalize_wrong_tool",
                retry_count=wrong_tool_retries,
                got=name.value,
            )
            continue

        if remaining < 1:
            log.info("force_finalize_resolved", retry_count=wrong_tool_retries)
        return _route_from_assistant(assistant, schemas)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    canonical = canonicalize_mime(msg)

    hop_line = HopBudgetLine.parse_from_email(canonical)
    remaining = hop_budget_remaining(hop_line, config)
    mid_w, _inner = require_fsm_message_id(canonical, "reasoning")
    log.info(
        "envelope",
        message_id=mid_w.value if mid_w else None,
        remaining_hops=remaining,
        formal_reason_gate_active=formal_reason_gate_active(canonical),
    )

    decision = _decide(canonical, hop_line, config=config)

    log.info(
        "decision",
        route=decision.target.value,
        target=decision.target.rfc822_mailbox,
    )
    decision_body = FsmTransitionPlainBody.parse(decision.body.value).value
    return build_fsm_step_to_stage(
        canonical,
        to_addr=decision.target,
        from_stage=stage,
        system=decision_body,
        subject_line=FsmTransitionPlainSubjectLine.parse(decision.subject.value),
        settings=config,
    )
