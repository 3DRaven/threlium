"""Bridge → ingress: ``<system>`` → ``EnrichUserQueryText`` (callee ingress, не bridge)."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.mime_reform import EnrichPartId, EnrichContentId, _iter_relay_leaf_parts, system_part_text
from threlium.types import EnrichUserQueryText, IngressExternalBodyText


def assert_bridge_input_has_no_user_query(msg: EmailMessage) -> None:
    """Fail-fast: bridge→ingress не должен нести ``<user-query>`` (создаёт ingress)."""
    target = EnrichContentId.from_part_id(EnrichPartId.USER_QUERY)
    for cid, _part in _iter_relay_leaf_parts(msg):
        if cid == target:
            raise RuntimeError(
                "FSM-инвариант: bridge→ingress не должен содержать <user-query>-часть"
            )


def enrich_user_query_from_bridge_system(msg: EmailMessage) -> EnrichUserQueryText:
    """``system_part_text`` → VO для distill и attach на ingress→enrich."""
    raw = system_part_text(msg)
    return EnrichUserQueryText.from_external_body(
        IngressExternalBodyText.parse(raw)
    )
