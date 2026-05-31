"""Parse tool_calls → :class:`IngressDistillToolArgs` для ingress distill."""
from __future__ import annotations

import jsonschema
import msgspec
from litellm.types.utils import Message

from threlium.litellm_tool_response import require_single_tool_call
from threlium.litellm_tool_spec import (
    tool_spec_parameters,
    validate_tool_args_json,
)
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
    try:
        args_dict = validate_tool_args_json(schema, wire)
    except jsonschema.ValidationError as exc:
        raise IngressDistillBridgeError(
            f"{_CONTEXT}: ingress_distill arguments failed jsonschema"
        ) from exc
    try:
        return msgspec.convert(args_dict, type=IngressDistillToolArgs)
    except (RuntimeError, ValueError, msgspec.ValidationError) as exc:
        raise IngressDistillBridgeError(
            f"{_CONTEXT}: invalid ingress_distill arguments"
        ) from exc


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
