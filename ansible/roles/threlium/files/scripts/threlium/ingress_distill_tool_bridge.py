"""Parse tool_calls → :class:`IngressDistillToolArgs` для ingress distill.

Caller предзагружает tool spec с jinja-vars (``distill_max_chars``) и передаёт
готовую JSON-схему — поэтому используется wire-вариант общего каркаса
:func:`~threlium.litellm_tool_bridge.parse_tool_args_from_wire`.
"""
from __future__ import annotations

from litellm.types.utils import Message

from threlium.litellm_tool_bridge import parse_tool_args_from_wire
from threlium.litellm_tool_response import require_single_tool_call
from threlium.types.ingress_distill_tool_args import IngressDistillToolArgs
from threlium.types.ingress_distill_tool_function import (
    IngressDistillBridgeError,
    IngressDistillToolFunctionName,
)
from threlium.types.litellm_tool_call import LiteLlmToolCallArgumentsWire

_CONTEXT = "ingress_distill"


def parse_ingress_distill_from_wire(
    wire: LiteLlmToolCallArgumentsWire,
    *,
    schema: dict[str, object],
) -> IngressDistillToolArgs:
    return parse_tool_args_from_wire(
        wire,
        schema=schema,
        args_type=IngressDistillToolArgs,
        bridge_error=IngressDistillBridgeError,
        context=_CONTEXT,
    )


def parse_ingress_distill_assistant(
    assistant: Message,
    *,
    schema: dict[str, object],
) -> IngressDistillToolArgs:
    tc = require_single_tool_call(assistant, context=_CONTEXT)
    name = IngressDistillToolFunctionName.parse_tool_call(tc)
    name.assert_matches(IngressDistillToolFunctionName.INGRESS_DISTILL)
    wire = LiteLlmToolCallArgumentsWire.from_tool_call(tc)
    return parse_ingress_distill_from_wire(wire, schema=schema)


__all__ = [
    "parse_ingress_distill_assistant",
    "parse_ingress_distill_from_wire",
]
