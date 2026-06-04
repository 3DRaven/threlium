"""Shared LLM invocations for enrich task seed / late hypotheses (phase 2 dedup).

Вынесено из ``enrich.py``: один каркас tool-call (``build_site_call`` +
``invoke_required_tool`` + bridge parse), различаются только prompt/site/context.
"""
from __future__ import annotations

from collections.abc import Callable

from threlium.context_token_count import (
    build_tokenizer,
    hypotheses_prompt_budget,
    trim_from_end_tokens,
)
from threlium.enrich_tool_bridge import (
    parse_enrich_task_hypotheses_assistant,
    parse_enrich_task_plan_assistant,
)
from threlium.litellm_correlation_headers import fsm_correlation_snap
from threlium.litellm_required_tool import build_site_call, invoke_required_tool
from threlium.litellm_tool_spec import load_tool_spec
from threlium.logutil import logger
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    EnrichTaskHypothesesPromptContext,
    LiteLlmChatMessage,
    LitellmRoutingSite,
    PromptPath,
    ReasoningUserMessageText,
    TaskLedger,
)

log = logger.bind(component="enrich_task_llm")


def _invoke_enrich_task_subtasks_llm(
    *,
    config: ThreliumSettings,
    site: LitellmRoutingSite,
    prompt_path: PromptPath,
    tool_spec_path: PromptPath,
    context: str,
    prompt_kwargs: dict[str, object],
    parse_assistant: Callable[..., object],
    token_budget: int | None = None,
) -> list[str]:
    """Один required-tool вызов → список сырых текстов подзадач. Fail-open: ``[]``."""
    prompt = render_prompt(prompt_path, **prompt_kwargs).strip()
    if token_budget is not None:
        prompt = trim_from_end_tokens(build_tokenizer(config), prompt, token_budget)
        if not prompt.strip():
            log.warning(f"{context}_prompt_empty_after_cap", token_budget=token_budget)
            return []
    call = build_site_call(
        config,
        site,
        [LiteLlmChatMessage(role="user", content=prompt)],
    )
    tool_spec = load_tool_spec(tool_spec_path)
    correlation = fsm_correlation_snap(None, config)
    try:
        assistant = invoke_required_tool(
            settings=config,
            call=call,
            tool_spec=tool_spec,
            correlation_snap=correlation,
            context=context,
        )
        parsed = parse_assistant(assistant)
        return list(parsed.subtasks)
    except Exception as exc:  # noqa: BLE001 — fail-open: seed/гипотезы опциональны
        log.warning(f"{context}_llm_failed", error=str(exc))
        return []


def _existing_subtasks_kw(ledger: TaskLedger) -> list[dict[str, str]]:
    return [
        {"content_id": s.content_id.value, "text": s.text.value, "status": s.status.value}
        for s in ledger.subtasks
    ]


def invoke_task_plan_subtasks(
    *,
    config: ThreliumSettings,
    incoming_user_message: ReasoningUserMessageText,
    existing_ledger: TaskLedger,
) -> list[str]:
    """Early seed (LLM до LightRAG): ``enrich_task_plan`` tool."""
    return _invoke_enrich_task_subtasks_llm(
        config=config,
        site=LitellmRoutingSite.ENRICH_PLAN,
        prompt_path=PromptPath.LIGHTRAG_ENRICH_TASK_PLAN,
        tool_spec_path=PromptPath.LIGHTRAG_ENRICH_TASK_PLAN_TOOL_SPEC,
        context="enrich_task_plan",
        prompt_kwargs={
            "incoming_user_message": incoming_user_message.value,
            "existing_subtasks": _existing_subtasks_kw(existing_ledger),
        },
        parse_assistant=parse_enrich_task_plan_assistant,
    )


def invoke_task_hypothesis_subtasks(
    *,
    config: ThreliumSettings,
    prompt_context: EnrichTaskHypothesesPromptContext,
    ledger_after_seed: TaskLedger,
) -> list[str]:
    """Late hypotheses (LLM после RAG): ``enrich_task_hypotheses`` tool."""
    return _invoke_enrich_task_subtasks_llm(
        config=config,
        site=LitellmRoutingSite.ENRICH_TASK_HYPOTHESES,
        prompt_path=PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES,
        tool_spec_path=PromptPath.LIGHTRAG_ENRICH_TASK_HYPOTHESES_TOOL_SPEC,
        context="enrich_task_hypotheses",
        prompt_kwargs={
            **prompt_context.for_jinja(),
            "existing_subtasks": _existing_subtasks_kw(ledger_after_seed),
        },
        parse_assistant=parse_enrich_task_hypotheses_assistant,
        token_budget=hypotheses_prompt_budget(config),
    )


__all__ = [
    "invoke_task_hypothesis_subtasks",
    "invoke_task_plan_subtasks",
]
