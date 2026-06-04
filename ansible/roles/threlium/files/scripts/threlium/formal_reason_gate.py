"""Formal_reason gate: ``FormalReasonResultPayload`` from IRT delta (reasoning hop window)."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.enrich_context import message_inner_from_email
from threlium.irt_chain import IrtAncestorSnapshot
from threlium.knowledge_fsm import parse_formal_reason_result_payload
from threlium.logutil import logger
from threlium.mail import email_message_from_path
from threlium.mime_reform import (
    iter_system_parts,
    part_origin_stage,
    system_leaf_part_text,
    system_part_text,
)
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.types import (
    FormalReasonErrorKind,
    FormalReasonOutcome,
    FormalReasonResultPayload,
    FsmStage,
    MailHeaderName,
    NotmuchMessageIdInner,
    RfcMessageIdWire,
)

_HDR = MailHeaderName
log = logger.bind(component="formal_reason_gate")


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
            f"<system> (message_id={mid!r}, content_id={content_id!r})"
        )
    return parsed


def formal_reason_result_from_formal_reason_email(
    msg: EmailMessage,
) -> FormalReasonResultPayload | None:
    """``FormalReasonResultPayload`` из единственной ``<system>`` на письме formal_reason→enrich_fast."""
    mid = _message_id_wire(msg)
    try:
        body = system_part_text(msg).strip()
    except RuntimeError:
        return None
    if not body:
        return None
    parts = iter_system_parts(msg)
    cid = parts[0][0].value if parts else "<system>"
    return require_formal_reason_result_payload(
        body, content_id=cid, message_id=mid
    )


def _latest_formal_reason_output_snap(
    leaf_inner: NotmuchMessageIdInner,
) -> IrtAncestorSnapshot | None:
    """Новейший hop ``formal_reason@`` в IRT-окне (лист reasoning → watermark ``To: reasoning``)."""
    found: IrtAncestorSnapshot | None = None
    for snap in iter_irt_ancestors_filtered(leaf_inner):
        if snap.is_addressed_to_fsm_stage(FsmStage.REASONING):
            break
        if snap.is_sent_from_fsm_stage(FsmStage.FORMAL_REASON):
            found = snap
            break
    return found


def formal_reason_result_from_irt_delta(
    leaf_inner: NotmuchMessageIdInner,
) -> FormalReasonResultPayload | None:
    """Machine payload последнего ``formal_reason`` в дельте текущего reasoning-hop."""
    snap = _latest_formal_reason_output_snap(leaf_inner)
    if snap is None:
        return None
    try:
        hop_msg = email_message_from_path(snap.path)
    except OSError as exc:
        log.warning(
            "formal_reason_irt_load_failed",
            path=str(snap.path),
            message_id=snap.message_id_inner.value,
            exc_msg=str(exc),
        )
        return None
    return formal_reason_result_from_formal_reason_email(hop_msg)


def formal_reason_result_from_reasoning_envelope(
    msg: EmailMessage,
) -> FormalReasonResultPayload | None:
    """Последний relayed ``<system origin=formal_reason>`` на spliced enrich_fast→reasoning.

    Используется ``assert_formal_reason_relay_after_splice`` (fast path); gate — IRT (см.
    :func:`formal_reason_result_from_irt_delta`).
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
    inner = message_inner_from_email(msg)
    if inner is None:
        log.debug("formal_reason_gate_inactive", reason="no_message_id_inner")
        return False
    r = formal_reason_result_from_irt_delta(inner)
    active = r is not None and r.outcome is FormalReasonOutcome.TECHNICAL_FAILED
    log.debug(
        "formal_reason_gate",
        active=active,
        outcome=r.outcome.value if r is not None else None,
        leaf_inner=inner.value,
    )
    return active


__all__ = [
    "assert_formal_reason_relay_after_splice",
    "compute_formal_reason_outcome",
    "delta_had_formal_reason",
    "formal_reason_gate_active",
    "formal_reason_result_from_formal_reason_email",
    "formal_reason_result_from_irt_delta",
    "formal_reason_result_from_reasoning_envelope",
    "require_formal_reason_result_payload",
]
