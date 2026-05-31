"""Имя OpenAI tool для ingress distill."""
from __future__ import annotations

from enum import StrEnum
from typing import Self

from litellm.types.utils import ChatCompletionMessageToolCall


class IngressDistillBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для ingress distill."""


class IngressDistillToolFunctionName(StrEnum):
    INGRESS_DISTILL = "ingress_distill"

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise IngressDistillBridgeError("tool_call without function.name")
        raw = func.name.strip()
        try:
            return cls(raw)
        except ValueError as exc:
            raise IngressDistillBridgeError(
                f"unknown ingress distill tool function.name={raw!r}"
            ) from exc

    def assert_matches(self, expected: IngressDistillToolFunctionName) -> None:
        if self != expected:
            raise IngressDistillBridgeError(
                f"expected tool {expected.value!r}, got {self.value!r}"
            )


__all__ = ["IngressDistillBridgeError", "IngressDistillToolFunctionName"]
