"""Parse tool_calls → :class:`ConfirmCliHitlToolArgs` для ``cli_resume``.

Тонкие обёртки над общим каркасом :mod:`threlium.litellm_tool_bridge`
(``docs/TYPES.md`` § tool bridge).
"""
from __future__ import annotations

from litellm.types.utils import Message

from threlium.litellm_tool_bridge import parse_single_tool, parse_tool_args_from_wire
from threlium.litellm_tool_spec import load_tool_spec, tool_spec_parameters
from threlium.types import PromptPath
from threlium.types.cli_hitl_tool_args import ConfirmCliHitlToolArgs
from threlium.types.cli_hitl_tool_function import (
    CliHitlBridgeError,
    CliHitlToolFunctionName,
)
from threlium.types.litellm_tool_call import LiteLlmToolCallArgumentsWire

_CONTEXT = "cli_hitl_resume"


def parse_confirm_cli_hitl_from_wire(
    wire: LiteLlmToolCallArgumentsWire,
) -> ConfirmCliHitlToolArgs:
    """jsonschema + msgspec по wire args tool ``confirm_cli_hitl``."""
    spec = load_tool_spec(PromptPath.CLI_RESUME_CONFIRM_CLI_HITL_TOOL_SPEC)
    schema = tool_spec_parameters(spec)
    return parse_tool_args_from_wire(
        wire,
        schema=schema,
        args_type=ConfirmCliHitlToolArgs,
        bridge_error=CliHitlBridgeError,
        context=_CONTEXT,
    )


def parse_confirm_cli_hitl_assistant(assistant: Message) -> ConfirmCliHitlToolArgs:
    """Распарсить assistant message после ``require_tool_calls_response``."""
    return parse_single_tool(
        assistant,
        expected=CliHitlToolFunctionName.CONFIRM_CLI_HITL,
        tool_spec_path=PromptPath.CLI_RESUME_CONFIRM_CLI_HITL_TOOL_SPEC,
        args_type=ConfirmCliHitlToolArgs,
        bridge_error=CliHitlBridgeError,
        context=_CONTEXT,
    )


def parse_confirm_cli_hitl(msg: Message) -> ConfirmCliHitlToolArgs:
    """Alias: полный parse от assistant message (включая require_single_tool_call)."""
    return parse_confirm_cli_hitl_assistant(msg)


__all__ = [
    "parse_confirm_cli_hitl",
    "parse_confirm_cli_hitl_assistant",
    "parse_confirm_cli_hitl_from_wire",
]
