"""Имя OpenAI tool для классификации HITL-ответа в ``cli_resume``."""
from __future__ import annotations

from enum import nonmember

from ._core import ToolFunctionNameBase


class CliHitlBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для HITL classifier."""


class CliHitlToolFunctionName(ToolFunctionNameBase):
    CONFIRM_CLI_HITL = "confirm_cli_hitl"

    _bridge_error = nonmember(CliHitlBridgeError)
    _label = nonmember("HITL")


__all__ = ["CliHitlBridgeError", "CliHitlToolFunctionName"]
