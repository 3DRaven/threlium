"""Batch dedup bridge→ingress по уже известным ``Message-ID`` в notmuch."""
from __future__ import annotations

from collections.abc import Iterable

import notmuch2  # pyright: ignore[reportMissingImports]

import threlium.nm as nm
from threlium.types import NotmuchMessageIdInner


def filter_known_message_ids_in_db(
    db: notmuch2.Database,
    candidates: Iterable[NotmuchMessageIdInner],
) -> set[NotmuchMessageIdInner]:
    """Подмножество ``candidates``, уже присутствующих в индексе notmuch."""
    known: set[NotmuchMessageIdInner] = set()
    for mid_nm in candidates:
        if nm.notmuch_index_has_message_id_in_db(db, mid_nm):
            known.add(mid_nm)
    return known
