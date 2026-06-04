"""Pack-хелперы summarize_context: гранулярные ``<history>`` units → батчи по токенам.

Фаза A (этот модуль, чистые функции без LLM):

  * ``build_history_by_mid`` — units (oldest→newest) → ``dict[mid → list[HistoryPart]]``,
    порядок вставки = порядок обработки (отдельной очереди нет);
  * ``split_oversized_in_place`` — большой CID **делится** на несколько меньших history
    CID (не trim!); письма на диске не меняются;
  * ``pack_next_fitted`` / ``consume_fitted`` — жадный префикс под ``content_budget`` и
    его удаление после успешного LLM-раунда.

Фаза B (цикл LLM с prior summary) живёт в ``states/summarize_context.py``.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from threlium.context_token_count import count_tokens
from threlium.mime_reform import EnrichContentId
from threlium.types import NotmuchMessageIdInner, SummarizeHistoryUnit


@dataclass(frozen=True)
class HistoryPart:
    """Единица обработки summarize: контент-адресный CID + тело ``<history>``-части."""

    cid: EnrichContentId
    text: str


def build_history_by_mid(
    units: list[SummarizeHistoryUnit],
) -> dict[NotmuchMessageIdInner, list[HistoryPart]]:
    """Units → ``dict[mid → [HistoryPart]]``; порядок вставки = порядок units (oldest→newest)."""
    out: dict[NotmuchMessageIdInner, list[HistoryPart]] = {}
    for u in units:
        inner = NotmuchMessageIdInner.parse(u.source_mid)
        out.setdefault(inner, []).append(
            HistoryPart(cid=EnrichContentId(value=u.cid), text=u.text)
        )
    return out


def _split_text_by_tokens(tokenizer: Any, text: str, chunk_token_limit: int) -> list[str]:
    ids = tokenizer.encode(text)
    if len(ids) <= chunk_token_limit:
        return [text]
    step = max(1, chunk_token_limit)
    return [tokenizer.decode(ids[i : i + step]) for i in range(0, len(ids), step)]


def split_oversized_in_place(
    history_by_mid: dict[NotmuchMessageIdInner, list[HistoryPart]],
    tokenizer: Any,
    chunk_token_limit: int,
) -> None:
    """Заменить части > ``chunk_token_limit`` токенов на несколько меньших history CID."""
    for inner in list(history_by_mid.keys()):
        new_parts: list[HistoryPart] = []
        for p in history_by_mid[inner]:
            if count_tokens(tokenizer, p.text) <= chunk_token_limit:
                new_parts.append(p)
                continue
            for frag in _split_text_by_tokens(tokenizer, p.text, chunk_token_limit):
                if frag.strip():
                    new_parts.append(
                        HistoryPart(cid=EnrichContentId.from_history_body(frag), text=frag)
                    )
        history_by_mid[inner] = new_parts


def pack_next_fitted(
    history_by_mid: dict[NotmuchMessageIdInner, list[HistoryPart]],
    tokenizer: Any,
    content_budget: int,
) -> list[HistoryPart]:
    """Жадный префикс (в порядке dict) под ``content_budget``; минимум одна часть (прогресс)."""
    fitted: list[HistoryPart] = []
    used = 0
    for inner in history_by_mid:
        for p in history_by_mid[inner]:
            t = count_tokens(tokenizer, p.text)
            if fitted and used + t > content_budget:
                return fitted
            fitted.append(p)
            used += t
    return fitted


def consume_fitted(
    history_by_mid: dict[NotmuchMessageIdInner, list[HistoryPart]],
    fitted: list[HistoryPart],
) -> None:
    """Удалить обработанный префикс из dict (pop с начала списков; пустые mid — ``del``)."""
    remaining = list(fitted)
    for inner in list(history_by_mid.keys()):
        parts = history_by_mid[inner]
        while parts and remaining and parts[0] is remaining[0]:
            parts.pop(0)
            remaining.pop(0)
        if not parts:
            del history_by_mid[inner]
        if not remaining:
            break


def history_by_mid_empty(
    history_by_mid: dict[NotmuchMessageIdInner, list[HistoryPart]],
) -> bool:
    return not any(parts for parts in history_by_mid.values())


__all__ = [
    "HistoryPart",
    "build_history_by_mid",
    "consume_fitted",
    "history_by_mid_empty",
    "pack_next_fitted",
    "split_oversized_in_place",
]
