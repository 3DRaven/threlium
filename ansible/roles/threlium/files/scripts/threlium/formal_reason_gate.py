"""Formal_reason gate: read ``<system origin=formal_reason>`` on reasoning ingress."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.knowledge_fsm import parse_formal_reason_result_payload
from threlium.mime_reform import (
    iter_system_parts,
    part_origin_stage,
    system_leaf_part_text,
)
from threlium.types import (
    FormalReasonErrorKind,
    FormalReasonOutcome,
    FormalReasonResultPayload,
    FsmStage,
    MailHeaderName,
    RfcMessageIdWire,
)

_HDR = MailHeaderName


def compute_formal_reason_outcome(
    *,
    error_kind: FormalReasonErrorKind,
    conforms: bool,
    violations: int,
    has_query_error: bool,
    has_derived_error: bool,
) -> FormalReasonOutcome:
    if error_kind is not FormalReasonErrorKind.NONE:
        return FormalReasonOutcome.TECHNICAL_FAILED
    if has_query_error or has_derived_error:
        return FormalReasonOutcome.TECHNICAL_FAILED
    if not conforms or violations > 0:
        return FormalReasonOutcome.SHACL_NEGATIVE
    return FormalReasonOutcome.PASSED


def _message_id_wire(msg: EmailMessage) -> str | None:
    mid = RfcMessageIdWire.parse_present_from_email(msg, _HDR.MESSAGE_ID)
    return mid.value if mid is not None else None


def require_formal_reason_result_payload(
    text: str,
    *,
    content_id: str,
    message_id: str | None = None,
) -> FormalReasonResultPayload:
    """Строгий разбор ``FormalReasonResultPayload``; при ошибке — ``RuntimeError``."""
    parsed = parse_formal_reason_result_payload(text)
    if parsed is None:
        mid = message_id or "?"
        raise RuntimeError(
            "formal_reason gate: invalid FormalReasonResultPayload in "
            f"<system origin=formal_reason> (message_id={mid!r}, content_id={content_id!r})"
        )
    return parsed


def formal_reason_result_from_reasoning_envelope(
    msg: EmailMessage,
) -> FormalReasonResultPayload | None:
    """Последний валидный ``<system origin=formal_reason>`` на письме enrich_fast→reasoning.

  На практике после splice в дельте одна такая часть; при обходе берётся последний
  в ``iter_system_parts``. Любая непустая formal_reason ``<system>`` обязана парситься.
    """
    mid = _message_id_wire(msg)
    last: FormalReasonResultPayload | None = None
    for cid, part in iter_system_parts(msg):
        if part_origin_stage(part) is not FsmStage.FORMAL_REASON:
            continue
        body = system_leaf_part_text(part).strip()
        if not body:
            continue
        last = require_formal_reason_result_payload(
            body, content_id=cid.value, message_id=mid
        )
    return last


def delta_had_formal_reason(delta_msgs: list[EmailMessage]) -> bool:
    """В окне enrich_fast-дельты было письмо ``From: formal_reason@localhost``."""
    for dm in delta_msgs:
        if FsmStage.try_from_mailbox(dm.get(_HDR.FROM.value)) is FsmStage.FORMAL_REASON:
            return True
    return False


def assert_formal_reason_relay_after_splice(
    spliced: EmailMessage,
    *,
    delta_msgs: list[EmailMessage],
    message_id: str | None = None,
) -> None:
    """После formal_reason в дельте на конверте reasoning должна быть relayed ``<system>``."""
    if not delta_had_formal_reason(delta_msgs):
        return
    mid = message_id or _message_id_wire(spliced)
    found = False
    for cid, part in iter_system_parts(spliced):
        if part_origin_stage(part) is not FsmStage.FORMAL_REASON:
            continue
        found = True
        body = system_leaf_part_text(part).strip()
        if body:
            require_formal_reason_result_payload(
                body, content_id=cid.value, message_id=mid
            )
    if not found:
        raise RuntimeError(
            "formal_reason gate: delta had formal_reason@localhost but spliced envelope "
            f"has no <system origin=formal_reason> (message_id={mid or '?'!r})"
        )


def formal_reason_gate_active(msg: EmailMessage) -> bool:
    r = formal_reason_result_from_reasoning_envelope(msg)
    return r is not None and r.outcome is FormalReasonOutcome.TECHNICAL_FAILED


__all__ = [
    "assert_formal_reason_relay_after_splice",
    "compute_formal_reason_outcome",
    "delta_had_formal_reason",
    "formal_reason_gate_active",
    "formal_reason_result_from_reasoning_envelope",
    "require_formal_reason_result_payload",
]
