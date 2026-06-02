"""Имя OpenAI tool для ingress distill."""
from __future__ import annotations

from enum import nonmember

from ._core import ToolFunctionNameBase


class IngressDistillBridgeError(RuntimeError):
    """Ошибка bridge tool_calls → args для ingress distill."""


class IngressDistillToolFunctionName(ToolFunctionNameBase):
    INGRESS_DISTILL = "ingress_distill"

    _bridge_error = nonmember(IngressDistillBridgeError)
    _label = nonmember("ingress distill")


__all__ = ["IngressDistillBridgeError", "IngressDistillToolFunctionName"]
