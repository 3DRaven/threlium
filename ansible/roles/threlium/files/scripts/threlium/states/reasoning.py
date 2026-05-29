#!/usr/bin/env python3
"""reasoning@localhost: LLM reducer → следующая стадия (ARCHITECTURE §6.1).

Маршрут — через OpenAI-compatible ``tool_calls``: для каждого ключа из
``ROUTE_TO_ADDRESS`` загружается отдельный tool из шаблона
``prompts/reasoning/<route>/tool_spec.j2`` (см.
:mod:`threlium.states.reasoning_tool_spec`). ``tool_choice`` = ``required`` —
провайдер обязан вернуть ``tool_calls`` (иначе Qwen/hermes при ``auto``
кладёт вызов в ``content`` как XML, и FSM не видит маршрут).
"""
from __future__ import annotations

from email.message import EmailMessage
from typing import Literal, get_args

from threlium.litellm_client import litellm_completion_sync
from litellm.types.utils import ChatCompletionMessageToolCall, Message, ModelResponse

from threlium.fsm_emit import (
    HDR_HOP_BUDGET,
    build_fsm_plain_to_stage,
    hop_budget_remaining,
)
from threlium.logutil import logger
from threlium.enrich_context import trim_context_text
from threlium.mime_reform import (
    EnrichPartId,
    canonicalize_mime,
    extract_part_by_content_id,
    group_relay_notes_by_family,
)
from threlium.prompts import render_prompt
from threlium.states.reasoning_tool_spec import (
    load_tools_for_routes,
    render_route_email,
    validate_tool_args,
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
    lite_llm_acompletion_to_dict,
    MailHeaderName,
    PromptPath,
    ReasoningAssistantMessageText,
    ReasoningToolRouteEmailBody,
    ReasoningToolRouteEmailSubject,
    RfcFromWire,
    RfcInReplyToWire,
    RfcMessageIdWire,
    RfcReferencesWire,
    RfcSubjectWire,
)

_HDR = MailHeaderName

log = logger.bind(stage="reasoning")


ROUTE_TO_ADDRESS: dict[RouteLiteral, FsmStage] = {
    "cli_intent": FsmStage.CLI_INTENT,
    "thread_memory": FsmStage.THREAD_MEMORY,
    "global_memory": FsmStage.GLOBAL_MEMORY,
    "subagent_intent": FsmStage.SUBAGENT_INTENT,
    "reflect": FsmStage.REFLECT,
    "response_append": FsmStage.RESPONSE_APPEND,
    "response_edit": FsmStage.RESPONSE_EDIT,
    "response_observe": FsmStage.RESPONSE_OBSERVE,
    "response_finalize": FsmStage.RESPONSE_FINALIZE,
    "logic_validate": FsmStage.LOGIC_VALIDATE,
    "memory_query": FsmStage.MEMORY_QUERY,
}

RouteLiteral = Literal[
    "cli_intent",
    "thread_memory",
    "global_memory",
    "subagent_intent",
    "reflect",
    "response_append",
    "response_edit",
    "response_observe",
    "response_finalize",
    "logic_validate",
    "memory_query",
]


_l_keys = set(get_args(RouteLiteral))
assert _l_keys == set(ROUTE_TO_ADDRESS.keys()), (_l_keys, set(ROUTE_TO_ADDRESS))


class ReasoningStageError(Exception):
    """Ошибка стадии reasoning."""


def _extract_context_part(
    msg: EmailMessage, part_id: EnrichPartId, max_chars: int,
) -> str | None:
    """Extract a single MIME part by Content-ID; trim and return None if empty."""
    raw = extract_part_by_content_id(msg, part_id)
    if raw is None:
        return None
    trimmed = trim_context_text(raw.strip(), max_chars)
    return trimmed or None


# Семейства relay-частей в порядке их появления в reasoning-промпте.
_RELAY_FAMILY_ORDER: tuple[EnrichPartId, ...] = (
    EnrichPartId.PLAN_STATE,
    EnrichPartId.MEMORY_NOTE,
    EnrichPartId.OBSERVATION_NOTE,
)


def _budget_relay_notes(notes: list[str], max_chars: int) -> list[str]:
    """Tail-keep новейшие записи семейства в пределах ``max_chars``.

    Старые хопы (в начале списка) отбрасываются первыми при переполнении бюджета;
    порядок оставшихся — oldest-first (как в MIME). ``max_chars <= 0`` — без обрезки.
    """
    if max_chars <= 0 or not notes:
        return notes
    kept: list[str] = []
    total = 0
    for note in reversed(notes):
        if kept and total + len(note) > max_chars:
            break
        kept.append(note)
        total += len(note)
    kept.reverse()
    return kept


def _collect_relay_notes(
    msg: EmailMessage, max_chars: int
) -> dict[EnrichPartId, list[str]]:
    """Сгруппировать relay-части по семейству (oldest-first), с бюджетом на семейство."""
    grouped = group_relay_notes_by_family(msg)
    return {
        fam: _budget_relay_notes(grouped.get(fam, []), max_chars)
        for fam in _RELAY_FAMILY_ORDER
    }


def _build_prompt(
    msg: EmailMessage, hop_budget: HopBudgetLine, max_chars: int,
) -> str:
    """``hop_budget`` — нормализованная строка ``X-Threlium-Hop-Budget`` (VO)."""

    mid_w = RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID)
    irt_w = RfcInReplyToWire.parse_present_from_email(msg, _HDR.IN_REPLY_TO)
    ref_w = RfcReferencesWire.parse_present_from_email(msg, _HDR.REFERENCES)
    sub_w = RfcSubjectWire.parse_present_from_email(msg, _HDR.SUBJECT)
    from_w = RfcFromWire.parse_present_from_email(msg, _HDR.FROM)

    user_text = _extract_context_part(msg, EnrichPartId.USER_MESSAGE, max_chars)
    knowledge_graph = _extract_context_part(msg, EnrichPartId.GRAPH_ANSWER, max_chars)
    mail_context = _extract_context_part(msg, EnrichPartId.UNIFIED_MAIL_CONTEXT, max_chars)
    thread_memory = _extract_context_part(msg, EnrichPartId.THREAD_MEMORY, max_chars)
    global_memory = _extract_context_part(msg, EnrichPartId.GLOBAL_MEMORY, max_chars)
    response_state = _extract_context_part(msg, EnrichPartId.RESPONSE_STATE, max_chars)

    relay = _collect_relay_notes(msg, max_chars)

    return render_prompt(
        PromptPath.REASONING_USER,
        message_id=mid_w.value if mid_w is not None else None,
        in_reply_to=irt_w.value if irt_w is not None else None,
        references=ref_w.value if ref_w is not None else None,
        subject=sub_w.value if sub_w is not None else None,
        from_hdr=from_w.value if from_w is not None else None,
        hop_budget=hop_budget.value,
        user_text=user_text,
        knowledge_graph=knowledge_graph,
        mail_context=mail_context,
        thread_memory=thread_memory,
        global_memory=global_memory,
        response_state=response_state,
        plan_states=relay[EnrichPartId.PLAN_STATE],
        memory_notes=relay[EnrichPartId.MEMORY_NOTE],
        observation_notes=relay[EnrichPartId.OBSERVATION_NOTE],
    )


def _extract_assistant_message(resp: ModelResponse) -> Message:
    choices = resp.choices or []
    if not choices:
        raise ValueError("empty choices")
    msg = choices[0].message
    if msg is None:
        raise ValueError("no message")
    return msg


def _finish_reason(resp: ModelResponse) -> str | None:
    choices = resp.choices or []
    if not choices:
        return None
    fr = choices[0].finish_reason
    if fr is None:
        return None
    return str(fr)


def _first_tool_call(msg: Message) -> ChatCompletionMessageToolCall | None:
    tcs = msg.tool_calls
    if not tcs:
        return None
    return tcs[0]


def _tool_call_name_and_args(tc: ChatCompletionMessageToolCall) -> tuple[str, str | bytes]:
    func = tc.function
    if func is None:
        raise ValueError("tool_call without function")
    if not func.name:
        raise ValueError("tool_call without function.name")
    return func.name, func.arguments


def _message_content(msg: Message) -> str:
    content = msg.content
    return ReasoningAssistantMessageText.parse(content).value if isinstance(content, str) else ""


def _route_from_tool_call(
    assistant: Message,
    schemas: dict[str, dict[str, object]],
) -> tuple[str, str, str]:
    tc = _first_tool_call(assistant)
    if tc is None:
        text = _message_content(assistant)
        if not text:
            raise ReasoningStageError("LLM returned neither tool_call nor text")
        raise ReasoningStageError(
            "LLM returned plain text without tool_call (tool-only policy)"
        )
    name, raw_args = _tool_call_name_and_args(tc)
    if name not in ROUTE_TO_ADDRESS:
        raise ReasoningStageError(
            f"LLM picked unknown route {name!r} (not in ROUTE_TO_ADDRESS)"
        )
    args = validate_tool_args(name, schemas[name], raw_args)
    log.info(
        "tool_call_args",
        route=name,
        args_len=len(raw_args) if isinstance(raw_args, (str, bytes)) else 0,
    )
    subject, body = render_route_email(name, args)
    log.info(
        "rendered_email",
        route=name,
        subject_len=len(subject),
        body_len=len(body),
        body_stripped_len=len(body.strip()),
    )
    if name == "response_append" and not body.strip():
        raise ReasoningStageError(
            "response_append: rendered body is empty after strip "
            "(LLM likely sent whitespace-only content)"
        )
    return (
        name,
        ReasoningToolRouteEmailSubject.parse(subject).value,
        ReasoningToolRouteEmailBody.parse(body).value,
    )


def _decide(
    msg: EmailMessage,
    hop_budget: HopBudgetLine | None,
    *,
    config: ThreliumSettings,
) -> tuple[str, str, str]:
    """Вернуть ``(route, subject, body)`` исходящего письма."""
    hb = hop_budget if hop_budget is not None else HopBudgetLine.parse(None)
    ep = resolve_llm_endpoint(config.litellm, LitellmRoutingSite.REASONING)
    mr = ep.max_retries if ep.max_retries is not None else config.litellm.max_retries
    length_max_attempts = (
        ep.length_recovery_max_attempts
        if ep.length_recovery_max_attempts is not None
        else config.litellm.length_recovery_max_attempts
    )
    log.info("litellm_routing", site=LitellmRoutingSite.REASONING.value, score=ep.score)

    tools, schemas = load_tools_for_routes(list(ROUTE_TO_ADDRESS.keys()))

    system = render_prompt(PromptPath.REASONING_SYSTEM).strip()
    length_recovery_system = render_prompt(
        PromptPath.REASONING_LENGTH_RECOVERY_SYSTEM
    ).strip()
    user_content = _build_prompt(msg, hb, config.enrich.context_max_chars)
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

        # stream=False → ModelResponse (см. ``threlium.litellm_wire``).
        resp = require_chat_model_response(
            litellm_completion_sync(settings=config, **kwargs, stream=False)
        )
        finish = _finish_reason(resp)
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

        assistant = _extract_assistant_message(resp)
        return _route_from_tool_call(assistant, schemas)

    raise ReasoningStageError("reasoning LLM attempt loop exhausted")


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    canonical = canonicalize_mime(msg)

    hop_line = HopBudgetLine.parse(canonical.get(HDR_HOP_BUDGET))
    remaining = hop_budget_remaining(hop_line, config)
    mid_w = RfcMessageIdWire.parse_present_from_email(canonical, _HDR.MESSAGE_ID)
    log.info("envelope", message_id=mid_w.value if mid_w else None)

    if remaining < 1:
        raise ReasoningStageError(
            f"FSM hop-budget exhausted (remaining={remaining})"
        )

    route, subject, body = _decide(canonical, hop_line, config=config)

    log.info("decision", route=route, target=ROUTE_TO_ADDRESS[route].rfc822_mailbox)
    return build_fsm_plain_to_stage(
        canonical,
        to_addr=ROUTE_TO_ADDRESS[route],
        from_stage=stage,
        body=FsmTransitionPlainBody.parse(body),
        subject_line=FsmTransitionPlainSubjectLine.parse(subject),
        settings=config,
    )
