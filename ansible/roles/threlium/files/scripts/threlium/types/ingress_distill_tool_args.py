"""Аргументы tool ``ingress_distill`` (ingress → enrich distill).

После :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire`
и ``validate_tool_args_json`` — см. ``ingress_distill_tool_bridge``.
"""
from __future__ import annotations

import msgspec


class IngressDistillToolArgs(msgspec.Struct, frozen=True):
    """Структурированный ответ LLM (tool_choice=required)."""

    user_intent: str
    user_reply_language: str
    open_gaps: tuple[str, ...] = ()
    step_back_notes: tuple[str, ...] = ()


__all__ = ["IngressDistillToolArgs"]
