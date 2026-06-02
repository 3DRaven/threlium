"""Generic single-tool bridge: assistant tool_call → msgspec args (``docs/TYPES.md`` § tool bridge).

Единый каркас для доменных ``*_tool_bridge`` модулей. Контракт идентичен у всех:
``require_single_tool_call`` → ``*ToolFunctionName.parse_tool_call`` / ``assert_matches``
→ ``load_tool_spec`` → ``validate_tool_args_json`` (jsonschema) → ``msgspec.convert``.
Любая ошибка схемы/валидации заворачивается в доменный ``bridge_error``.
"""
from __future__ import annotations

from typing import TypeVar

import jsonschema
import msgspec
from litellm.types.utils import Message

from threlium.litellm_tool_response import require_single_tool_call
from threlium.litellm_tool_spec import (
    load_tool_spec,
    tool_spec_parameters,
    validate_tool_args_json,
)
from threlium.types import PromptPath
from threlium.types._core import ToolFunctionNameBase
from threlium.types.litellm_tool_call import LiteLlmToolCallArgumentsWire

_ArgsT = TypeVar("_ArgsT")


def parse_tool_args_from_wire(
    wire: LiteLlmToolCallArgumentsWire,
    *,
    schema: dict[str, object],
    args_type: type[_ArgsT],
    bridge_error: type[Exception],
    context: str,
) -> _ArgsT:
    """jsonschema-валидация wire JSON + ``msgspec.convert`` → ``args_type`` или ``bridge_error``."""
    try:
        args_dict = validate_tool_args_json(schema, wire)
    except jsonschema.ValidationError as exc:
        raise bridge_error(f"{context}: arguments failed jsonschema") from exc
    try:
        return msgspec.convert(args_dict, type=args_type)
    except (RuntimeError, ValueError, msgspec.ValidationError) as exc:
        raise bridge_error(f"{context}: invalid arguments") from exc


def parse_single_tool(
    assistant: Message,
    *,
    expected: ToolFunctionNameBase,
    tool_spec_path: PromptPath,
    args_type: type[_ArgsT],
    bridge_error: type[Exception],
    context: str,
    **spec_jinja_vars: object,
) -> _ArgsT:
    """Полный single-tool разбор assistant message → ``args_type`` (см. модульный docstring).

    ``expected`` — ожидаемый член доменного ``ToolFunctionNameBase``; класс берётся
    из ``type(expected)``. ``spec_jinja_vars`` пробрасываются в ``load_tool_spec``
    (напр. ``distill_max_chars`` для ingress_distill).
    """
    tc = require_single_tool_call(assistant, context=context)
    name = type(expected).parse_tool_call(tc)
    name.assert_matches(expected)
    spec = load_tool_spec(tool_spec_path, **spec_jinja_vars)
    schema = tool_spec_parameters(spec)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    return parse_tool_args_from_wire(
        wire,
        schema=schema,
        args_type=args_type,
        bridge_error=bridge_error,
        context=context,
    )


__all__ = ["parse_single_tool", "parse_tool_args_from_wire"]
