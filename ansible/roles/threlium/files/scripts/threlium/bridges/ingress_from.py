"""Проверка ``From:`` bridge→ingress (внешняя граница)."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.types import MailHeaderName, NotmuchBridgeFromLocalhost


def bridge_from_email(msg: EmailMessage) -> NotmuchBridgeFromLocalhost | None:
    raw = msg.get(MailHeaderName.FROM)
    if raw is None:
        return None
    normalized = str(raw).strip().lower()
    for bridge in NotmuchBridgeFromLocalhost:
        if normalized == bridge.value.lower():
            return bridge
    return None


def require_bridge_from_email(msg: EmailMessage) -> NotmuchBridgeFromLocalhost:
    bridge = bridge_from_email(msg)
    if bridge is None:
        raw = msg.get(MailHeaderName.FROM)
        raise RuntimeError(
            f"ingress: expected bridge From ({', '.join(b.value for b in NotmuchBridgeFromLocalhost)}), "
            f"got {raw!r}"
        )
    return bridge
