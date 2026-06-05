"""Snowflake-MID для isomorph: уникальный MID независимо от тела + водяной знак тред-непрерывности.

Зачем (см. docs/E2E_PARALLEL_ISOLATION_REPORT §5-bis): контент-адресуемый glue-MID = ``hash(reply)`` даёт
КОЛЛИЗИЮ при идентичных ответах (два теста/хода → один MID → notmuch сливает треды). Snowflake — уникальный
63-битный k-сортируемый id (``time|instance|seq``) → MID уникален независимо от тела, коллизий нет в корне.

Egress кладёт ``snowflake`` glue в НЕВИДИМЫЙ водяной знак ответа (`encode_mid_safe`); мост следующего хода
декодит его из last-assistant (`decode_glue_snowflake`) → точный ``In-Reply-To`` БЕЗ content-голосования.

``instance`` = in-process wrapping-счётчик (mod 512) + партиция по процессу (старший бит 10-битного поля:
мост 0..511, egress 512..1023) — разные процессы НЕ коллизируют, lock-free (свой генератор на минт, без
общего мутабельного состояния). Единственная оговорка — часы не идут назад (NTP *slew* безопасен).
"""
from __future__ import annotations

import itertools
import re
from typing import Final

from snowflake import SnowflakeGenerator

from threlium.invisible_task_mid import decode_mid_safe, encode_mid_safe
from threlium.types import IsomorphSnowflakeId, RfcMessageIdWire

#: Кастомный epoch (2024-01-01T00:00:00Z, ms): старшие (временные) биты малы → короткий int/знак на годы.
_EPOCH_MS: Final[int] = 1_704_067_200_000

#: База instance по процессу (старший бит 10-битного поля). Мост и egress — разные процессы.
_INSTANCE_BASE_BRIDGE: Final[int] = 0
_INSTANCE_BASE_EGRESS: Final[int] = 512
#: Ширина in-process счётчика внутри партиции процесса (9 бит = 512 значений).
_INSTANCE_SPAN: Final[int] = 512

_counter: Final["itertools.count[int]"] = itertools.count()


def _mint(instance_base: int) -> int:
    """Сминтить snowflake: instance = base + (счётчик mod 512); свой генератор на минт → lock-free."""
    instance = instance_base + (next(_counter) % _INSTANCE_SPAN)
    sf = next(SnowflakeGenerator(instance, epoch=_EPOCH_MS))
    if sf is None:  # переполнение seq в одну мс на свежем генераторе — недостижимо, но тип Optional
        raise RuntimeError("snowflake_mid: generator yielded None")
    return sf


def mint_egress_snowflake() -> int:
    """Уникальный snowflake для glue-MID ответа egress (партиция egress: instance 512..1023)."""
    return _mint(_INSTANCE_BASE_EGRESS)


def mint_bridge_snowflake() -> int:
    """Уникальный snowflake для ingress-MID моста (партиция bridge: instance 0..511)."""
    return _mint(_INSTANCE_BASE_BRIDGE)


def snowflake_to_mid(snowflake: int) -> RfcMessageIdWire:
    """``snowflake`` → канонический ``<b62(IsomorphSnowflakeId)@localhost>``."""
    return RfcMessageIdWire.from_native(IsomorphSnowflakeId(v=1, snowflake=snowflake))


def mid_to_snowflake(mid: RfcMessageIdWire) -> int | None:
    """Канонический snowflake-MID → ``snowflake`` (``None``, если это не snowflake-MID)."""
    raw = mid.value if mid is not None else None
    if not raw:
        return None
    try:
        native = RfcMessageIdWire.native_from_canonical_str(raw, IsomorphSnowflakeId)
    except Exception:  # noqa: BLE001 — чужой/контент-MID (другой VO) → не snowflake
        return None
    return native.snowflake


def watermark_reply(reply_text: str, glue_snowflake: int) -> str:
    """Дописать невидимый водяной знак ``glue_snowflake`` в конец ответа (клиент вернёт его в истории)."""
    return reply_text + encode_mid_safe(glue_snowflake)


def decode_glue_snowflake(assistant_text: str) -> int | None:
    """Извлечь ``glue_snowflake`` из водяного знака last-assistant (``None``, если знака нет)."""
    return decode_mid_safe(assistant_text)


#: E2E-ONLY: тест шлёт ГОТОВЫЙ thread-root MID прямо в теле (prompt/user) как ``E2E_MID:<b62@localhost>``.
#: Мост в e2e-режиме берёт его как ingress-MID — без content-hash, поэтому НЕ зависит от реконструкции тела
#: Cline / даты / шаблона системного промпта (устраняет date-drift точного хеша). В ПРОДЕ не вызывается.
_E2E_MID_RE: Final = re.compile(r"E2E_MID:(<?[0-9A-Za-z]+@localhost>?)")


def extract_e2e_explicit_mid(tail_body: str) -> tuple[RfcMessageIdWire | None, str]:
    """E2E-ONLY: вынуть явный ``E2E_MID:<...@localhost>`` из хвоста → (MID, хвост-без-токена).

    Тест генерит MID ТЕМ ЖЕ кодом, что egress (`snowflake_to_mid`), и шлёт его в теле; мост использует
    напрямую как thread-root. Нет токена → ``(None, tail_body)``.
    """
    m = _E2E_MID_RE.search(tail_body)
    if m is None:
        return None, tail_body
    mid = RfcMessageIdWire.parse_threlium_canonical_optional(m.group(1))
    cleaned = (tail_body[: m.start()] + tail_body[m.end():]).strip()
    return mid, cleaned
