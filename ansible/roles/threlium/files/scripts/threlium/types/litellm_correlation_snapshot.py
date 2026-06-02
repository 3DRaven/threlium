"""Typed snapshot HTTP-заголовков e2e-корреляции LiteLLM (``extra_headers`` dict)."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Self

import msgspec

from threlium.mail_header_names import MailHeaderName
from threlium.types.litellm_correlation_header import LitellmCorrelationHeader


class LitellmCorrelationSnapshot(msgspec.Struct, frozen=True, kw_only=True):
    """Снимок белого списка ключей из :func:`~threlium.litellm_correlation_headers._assemble_litellm_correlation_dict`."""

    route_wire: str
    call_site: str
    thread_root_mid: str | None = None
    from_hdr: str | None = None
    to_hdr: str | None = None
    message_id_hdr: str | None = None
    in_reply_to_hdr: str | None = None

    @classmethod
    def from_mapping(cls, headers: Mapping[str, str]) -> Self:
        route = headers.get(MailHeaderName.ROUTE.value)
        call_site = headers.get(LitellmCorrelationHeader.CALL_SITE.value)
        if not route or not call_site:
            raise RuntimeError(
                "LitellmCorrelationSnapshot: missing X-Threlium-Route or X-Threlium-Call-Site"
            )
        return cls(
            route_wire=route,
            call_site=call_site,
            thread_root_mid=headers.get(LitellmCorrelationHeader.THREAD_ROOT_MID.value),
            from_hdr=headers.get(MailHeaderName.FROM.value),
            to_hdr=headers.get(MailHeaderName.TO.value),
            message_id_hdr=headers.get(MailHeaderName.MESSAGE_ID.value),
            in_reply_to_hdr=headers.get(MailHeaderName.IN_REPLY_TO.value),
        )

    def as_dict(self) -> dict[str, str]:
        """Обратно в ``dict`` для TLS / ``extra_headers`` (только непустые поля)."""
        out: dict[str, str] = {
            MailHeaderName.ROUTE.value: self.route_wire,
            LitellmCorrelationHeader.CALL_SITE.value: self.call_site,
        }
        if self.thread_root_mid:
            out[LitellmCorrelationHeader.THREAD_ROOT_MID.value] = self.thread_root_mid
        if self.from_hdr:
            out[MailHeaderName.FROM.value] = self.from_hdr
        if self.to_hdr:
            out[MailHeaderName.TO.value] = self.to_hdr
        if self.message_id_hdr:
            out[MailHeaderName.MESSAGE_ID.value] = self.message_id_hdr
        if self.in_reply_to_hdr:
            out[MailHeaderName.IN_REPLY_TO.value] = self.in_reply_to_hdr
        return out
