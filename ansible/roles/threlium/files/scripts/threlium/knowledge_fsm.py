"""Parse-фабрики для стадий knowledge system.

Уровень 1 (docs/TYPES.md): JSON разбирается только здесь; стадии
вызывают ``parse_*`` и работают с готовыми VO.
"""
from __future__ import annotations

import json
import logging

import msgspec

from threlium.types.knowledge_stage import (
    FormalReasonStagePayload,
    MemoryQueryStagePayload,
)

log = logging.getLogger(__name__)


def parse_formal_reason_payload(text: str) -> FormalReasonStagePayload | None:
    """Parse JSON body → FormalReasonStagePayload or None on failure."""
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        log.warning("formal_reason: payload is not valid JSON")
        return None
    try:
        return msgspec.convert(raw, type=FormalReasonStagePayload)
    except (msgspec.ValidationError, TypeError) as e:
        log.warning("formal_reason: payload validation failed: %s", e)
        return None


def parse_memory_query_payload(text: str) -> MemoryQueryStagePayload | None:
    """Parse JSON body → MemoryQueryStagePayload or None on failure."""
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        log.warning("memory_query: payload is not valid JSON")
        return None
    try:
        return msgspec.convert(raw, type=MemoryQueryStagePayload)
    except (msgspec.ValidationError, TypeError) as e:
        log.warning("memory_query: payload validation failed: %s", e)
        return None


__all__ = [
    "parse_formal_reason_payload",
    "parse_memory_query_payload",
]
