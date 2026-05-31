"""Имя OpenAI tool для классификации HITL-ответа в ``cli_resume``."""
from __future__ import annotations

from enum import StrEnum
from typing import Self

from litellm.types.utils import ChatCompletionMessageToolCall


class CliHitlBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для HITL classifier."""


class CliHitlToolFunctionName(StrEnum):
    CONFIRM_CLI_HITL = "confirm_cli_hitl"

    @classmethod
    def parse_tool_call(cls, tc: ChatCompletionMessageToolCall) -> Self:
        func = tc.function
        if func is None or not func.name:
            raise CliHitlBridgeError("tool_call without function.name")
        raw = func.name.strip()
        try:
            return cls(raw)
        except ValueError as exc:
            raise CliHitlBridgeError(
                f"unknown HITL tool function.name={raw!r}"
            ) from exc

    def assert_matches(self, expected: CliHitlToolFunctionName) -> None:
        if self != expected:
            raise CliHitlBridgeError(
                f"expected tool {expected.value!r}, got {self.value!r}"
            )


__all__ = ["CliHitlBridgeError", "CliHitlToolFunctionName"]
