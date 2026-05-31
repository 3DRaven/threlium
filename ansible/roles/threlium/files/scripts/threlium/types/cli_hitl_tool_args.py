"""Аргументы tool ``confirm_cli_hitl`` (cli_resume HITL classifier).

После :class:`~threlium.types.litellm_tool_call.LiteLlmToolCallArgumentsWire`
и ``validate_tool_args_json`` — см. ``cli_hitl_tool_bridge``.
"""
from __future__ import annotations

import msgspec


class ConfirmCliHitlToolArgs(msgspec.Struct, frozen=True):
    """Решение пользователя по privileged CLI (fail-closed при confirmed=false)."""

    confirmed: bool
    interpretation: str | None = None


__all__ = ["ConfirmCliHitlToolArgs"]
