"""Parse-фабрики для стадий knowledge system.

Уровень 1 (docs/TYPES.md): JSON разбирается только здесь; стадии
вызывают ``parse_*`` и работают с готовыми VO.
"""
from __future__ import annotations

from threlium.types import parse_json_payload
from threlium.types.knowledge_stage import (
    FormalReasonResultPayload,
    FormalReasonStagePayload,
    MemoryQueryStagePayload,
)


def parse_formal_reason_payload(text: str) -> FormalReasonStagePayload | None:
    """Parse JSON body → FormalReasonStagePayload or None on failure."""
    return parse_json_payload(text, FormalReasonStagePayload, log_ctx="formal_reason")


def parse_formal_reason_result_payload(text: str) -> FormalReasonResultPayload | None:
    """Parse ``<system>`` JSON from ``formal_reason`` → :class:`FormalReasonResultPayload`."""
    return parse_json_payload(
        text, FormalReasonResultPayload, log_ctx="formal_reason_result"
    )


def parse_memory_query_payload(text: str) -> MemoryQueryStagePayload | None:
    """Parse JSON body → MemoryQueryStagePayload or None on failure."""
    return parse_json_payload(text, MemoryQueryStagePayload, log_ctx="memory_query")


__all__ = [
    "parse_formal_reason_payload",
    "parse_formal_reason_result_payload",
    "parse_memory_query_payload",
]
