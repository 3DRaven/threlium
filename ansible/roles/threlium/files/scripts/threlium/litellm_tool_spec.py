"""Загрузка Jinja tool_spec и валидация аргументов tool_calls (общий для reasoning / LightRAG)."""
from __future__ import annotations

import json
from typing import cast

import jsonschema
from litellm.types.utils import ChatCompletionMessageToolCall, Message

from threlium.prompts import render_prompt
from threlium.types import PromptPath
from threlium.types.litellm_tool_call import LiteLlmToolCallArgumentsWire


def load_tool_spec(prompt_path: PromptPath, /, **jinja_vars: object) -> dict[str, object]:
    """Собрать один OpenAI tool dict из ``tool_spec.j2``.

    ``jinja_vars`` — переменные шаблона (напр. ``distill_max_chars`` для ingress_distill);
    единая точка загрузки tool-spec с валидацией ``function.name`` / ``function.parameters``.
    """
    rendered = render_prompt(prompt_path, **jinja_vars)
    raw = json.loads(rendered)
    if not isinstance(raw, dict):
        raise RuntimeError(f"{prompt_path}: tool spec JSON must be an object")
    spec = cast(dict[str, object], raw)
    func = spec.get("function")
    if not isinstance(func, dict):
        raise RuntimeError(f"{prompt_path}: function must be an object")
    fn = cast(dict[str, object], func)
    name_o = fn.get("name")
    params_o = fn.get("parameters")
    if not isinstance(name_o, str) or not name_o.strip():
        raise RuntimeError(f"{prompt_path}: function.name must be a non-empty string")
    if not isinstance(params_o, dict):
        raise RuntimeError(f"{prompt_path}: function.parameters must be an object")
    return spec


def tool_spec_parameters(spec: dict[str, object]) -> dict[str, object]:
    """JSON Schema из загруженного tool spec."""
    func = spec["function"]
    if not isinstance(func, dict):
        raise RuntimeError("tool spec: function must be an object")
    params = func.get("parameters")
    if not isinstance(params, dict):
        raise RuntimeError("tool spec: function.parameters must be an object")
    return cast(dict[str, object], params)


def first_tool_call(msg: Message) -> ChatCompletionMessageToolCall | None:
    """Первый tool_call из assistant message (или None)."""
    tcs = msg.tool_calls
    if not tcs:
        return None
    return tcs[0]


def tool_call_arguments_wire_from_tool_call(
    tc: ChatCompletionMessageToolCall,
) -> LiteLlmToolCallArgumentsWire:
    """Сырой JSON args из tool_call (общий wire-класс)."""
    return LiteLlmToolCallArgumentsWire.from_tool_call(tc)


def validate_tool_args_json(
    schema: dict[str, object],
    wire: LiteLlmToolCallArgumentsWire,
) -> dict[str, object]:
    """jsonschema.validate → dict для msgspec.convert."""
    args = json.loads(wire.value)
    jsonschema.validate(instance=args, schema=schema)
    if not isinstance(args, dict):
        raise RuntimeError("tool args JSON must be an object")
    return cast(dict[str, object], args)


__all__ = [
    "first_tool_call",
    "load_tool_spec",
    "tool_call_arguments_wire_from_tool_call",
    "tool_spec_parameters",
    "validate_tool_args_json",
]
