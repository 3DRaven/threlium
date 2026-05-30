#!/usr/bin/env python3
"""reasoning@localhost: LLM reducer → следующая стадия (ARCHITECTURE §6.1).

Маршрут — через OpenAI-compatible ``tool_calls``: для каждой целевой
:class:`~threlium.types.fsm_stage.FsmStage` из
:data:`~threlium.types.reasoning.REASONING_TARGET_STAGES` загружается отдельный
tool из ``prompts/reasoning/<stage>/tool_spec.j2``. ``tool_choice`` = ``required``.
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.litellm_client import litellm_completion_sync
from litellm.types.utils import Message

from threlium.fsm_emit import HDR_HOP_BUDGET, build_fsm_plain_to_stage, hop_budget_remaining
from threlium.logutil import clip_log_text, logger
from threlium.mime_reform import canonicalize_mime
from threlium.prompts import render_prompt
from threlium.states.reasoning_tool_spec import (
    load_tools_for_routes,
    route_decision_from_tool_call,
)
from threlium.litellm_wire import require_chat_model_response
from threlium.settings import ThreliumSettings, resolve_llm_endpoint
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    LiteLlmAcompletionKwargs,
    LiteLlmChatMessage,
    LitellmRoutingSite,
    PromptPath,
    REASONING_TARGET_STAGES,
    ReasoningIncomingEnvelope,
    ReasoningEnrichContext,
    ReasoningRouteDecision,
    ReasoningToolCallArgumentsWire,
    ReasoningToolFunctionName,
    RfcMessageIdWire,
    lite_llm_acompletion_to_dict,
    MailHeaderName,
    reasoning_assistant_message,
    reasoning_assistant_plain_text,
    reasoning_finish_reason,
    reasoning_first_tool_call,
)

_HDR = MailHeaderName

log = logger.bind(stage="reasoning")


class ReasoningStageError(Exception):
    """Ошибка стадии reasoning."""


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
        response_observations=list(ctx.response_observations),
        memory_notes=list(ctx.memory_notes),
        observation_notes=list(ctx.observation_notes),
        unified_deltas=list(ctx.unified_deltas),
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
    wire = ReasoningToolCallArgumentsWire.from_tool_call(tc)
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


def _decide(
    msg: EmailMessage,
    hop_budget: HopBudgetLine,
    *,
    config: ThreliumSettings,
) -> ReasoningRouteDecision:
    ep = resolve_llm_endpoint(config.litellm, LitellmRoutingSite.REASONING)
    mr = ep.max_retries if ep.max_retries is not None else config.litellm.max_retries
    length_max_attempts = (
        ep.length_recovery_max_attempts
        if ep.length_recovery_max_attempts is not None
        else config.litellm.length_recovery_max_attempts
    )
    log.info("litellm_routing", site=LitellmRoutingSite.REASONING.value, score=ep.score)

    routes = sorted(REASONING_TARGET_STAGES, key=lambda s: s.value)
    tools, schemas = load_tools_for_routes(routes)

    system = render_prompt(PromptPath.REASONING_SYSTEM).strip()
    length_recovery_system = render_prompt(
        PromptPath.REASONING_LENGTH_RECOVERY_SYSTEM
    ).strip()
    user_content = _render_user_prompt(msg, hop_budget, config.enrich.context_max_chars)
    messages: list[LiteLlmChatMessage] = [
        LiteLlmChatMessage(role="system", content=system),
        LiteLlmChatMessage(role="user", content=user_content),
    ]

    for attempt in range(length_max_attempts):
        call = LiteLlmAcompletionKwargs(
            model=ep.model,
            messages=messages,
            timeout=float(ep.timeout),
            max_retries=mr,
            api_key=ep.api_key,
            api_base=ep.api_base,
            max_tokens=ep.max_tokens,
            thinking_token_budget=ep.thinking_token_budget,
            tools=tools,
            tool_choice="required",
            chat_template_kwargs=ep.chat_template_kwargs or None,
        )
        kwargs = lite_llm_acompletion_to_dict(call)

        resp = require_chat_model_response(
            litellm_completion_sync(settings=config, **kwargs, stream=False)
        )
        finish = reasoning_finish_reason(resp)
        if finish == "length":
            log.warning(
                "llm_finish_reason_length",
                attempt=attempt + 1,
                max_attempts=length_max_attempts,
            )
            if attempt + 1 >= length_max_attempts:
                raise ReasoningStageError(
                    "LLM completion truncated (finish_reason=length) after recovery retry"
                )
            messages = [
                *messages,
                LiteLlmChatMessage(role="system", content=length_recovery_system),
            ]
            continue

        assistant = reasoning_assistant_message(resp)
        return _route_from_assistant(assistant, schemas)

    raise ReasoningStageError("reasoning LLM attempt loop exhausted")


def _decide_force_finalize(
    msg: EmailMessage,
    hop_budget: HopBudgetLine,
    *,
    config: ThreliumSettings,
) -> ReasoningRouteDecision:
    """Hop-budget исчерпан: LLM с промптом ``budget_exhausted.j2`` (``retry_count``)
    до ``response_finalize`` tool call; жёсткий egress — в ``response_finalize``."""
    ep = resolve_llm_endpoint(config.litellm, LitellmRoutingSite.REASONING)
    mr = ep.max_retries if ep.max_retries is not None else config.litellm.max_retries
    log.info(
        "litellm_routing_force_finalize",
        site=LitellmRoutingSite.REASONING.value,
        score=ep.score,
    )

    routes = sorted(REASONING_TARGET_STAGES, key=lambda s: s.value)
    tools, schemas = load_tools_for_routes(routes)
    system = render_prompt(PromptPath.REASONING_SYSTEM).strip()
    user_content = _render_user_prompt(msg, hop_budget, config.enrich.context_max_chars)

    retry_count = 0
    while True:
        budget_notice = render_prompt(
            PromptPath.REASONING_BUDGET_EXHAUSTED, retry_count=retry_count
        ).strip()
        messages: list[LiteLlmChatMessage] = [
            LiteLlmChatMessage(role="system", content=system),
            LiteLlmChatMessage(role="user", content=user_content),
            LiteLlmChatMessage(role="system", content=budget_notice),
        ]
        call = LiteLlmAcompletionKwargs(
            model=ep.model,
            messages=messages,
            timeout=float(ep.timeout),
            max_retries=mr,
            api_key=ep.api_key,
            api_base=ep.api_base,
            max_tokens=ep.max_tokens,
            thinking_token_budget=ep.thinking_token_budget,
            tools=tools,
            tool_choice="required",
            chat_template_kwargs=ep.chat_template_kwargs or None,
        )
        kwargs = lite_llm_acompletion_to_dict(call)
        resp = require_chat_model_response(
            litellm_completion_sync(settings=config, **kwargs, stream=False)
        )
        assistant = reasoning_assistant_message(resp)
        tc = reasoning_first_tool_call(assistant)
        if tc is not None:
            name = ReasoningToolFunctionName.parse_tool_call(tc)
            if name.target_stage() == FsmStage.RESPONSE_FINALIZE:
                log.info("force_finalize_resolved", retry_count=retry_count)
                return _route_from_assistant(assistant, schemas)
            log.warning(
                "force_finalize_wrong_tool",
                retry_count=retry_count,
                got=name.value,
            )
        else:
            log.warning("force_finalize_no_tool_call", retry_count=retry_count)
        retry_count += 1


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    canonical = canonicalize_mime(msg)

    hop_line = HopBudgetLine.parse(canonical.get(HDR_HOP_BUDGET))
    remaining = hop_budget_remaining(hop_line, config)
    mid_w = RfcMessageIdWire.parse_present_from_email(canonical, _HDR.MESSAGE_ID)
    log.info("envelope", message_id=mid_w.value if mid_w else None)

    if remaining < 1:
        log.warning(
            "hop_budget_exhausted_force_finalize",
            remaining=remaining,
            message_id=mid_w.value if mid_w else None,
        )
        decision = _decide_force_finalize(canonical, hop_line, config=config)
    else:
        decision = _decide(canonical, hop_line, config=config)

    log.info(
        "decision",
        route=decision.target.value,
        target=decision.target.rfc822_mailbox,
    )
    return build_fsm_plain_to_stage(
        canonical,
        to_addr=decision.target,
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(decision.body.value),
        subject_line=FsmTransitionPlainSubjectLine.parse(decision.subject.value),
        settings=config,
    )
