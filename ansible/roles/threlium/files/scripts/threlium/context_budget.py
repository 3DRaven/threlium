"""Двухуровневая оптимизация контекстного бюджета (MCKP + per-message scoring).

Level 1: MCKP (Multiple-Choice Knapsack) между бакетами — scipy.optimize.milp.
Level 2: Per-message value scoring внутри unified_mail_context для динамического tier-assignment.

После унификации history/system «содержательность» письма — наличие ``<history>``-части
(:func:`threlium.mime_reform.message_has_history`), а не таблица ``To:``-стадий. Базовый вес
сообщения — ``X-Threlium-Content-Score`` его ``<history>``-части (скоринг отправителя),
потребитель домножает на recency/size. Семантику источника даёт ``X-Threlium-Origin`` /
конвертный ``From:`` (метка ``[from: ...]``), без enum-типов и SERVICE-классификации.

docs/TYPES.md: доменные enum, frozen dataclasses, typed containers.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from enum import StrEnum

import numpy as np
from scipy.optimize import LinearConstraint, Bounds, milp

from threlium.mail_header_names import MailHeaderName
from threlium.mime_reform import EnrichPartId, history_part_text, iter_history_parts
from threlium.types.content_score import ThreliumContentScoreWire

_HDR = MailHeaderName


def message_content_score(msg: EmailMessage) -> float:
    """Базовый вес = ``X-Threlium-Content-Score`` первой ``<history>``-части письма.

    Скоринг отправителя (источник проставил базовый вес из ``settings.history.score_for``).
    Нет части/заголовка → нейтральный вес (fallback :meth:`ThreliumContentScoreWire.as_score`).
    """
    for _cid, part in iter_history_parts(msg):
        return ThreliumContentScoreWire.parse(part.get(_HDR.CONTENT_SCORE.value)).as_score()
    return ThreliumContentScoreWire.parse(None).as_score()


def message_origin_label(msg: EmailMessage) -> str:
    """Метка источника для аннотации ``[from: ...]`` в mail_context.j2.

    ``X-Threlium-Origin`` первой ``<history>``-части (штампует enrich_fast при сплайсе),
    иначе local-part конвертного ``From:`` (для писем полного enrich без relay-копии).
    """
    for _cid, part in iter_history_parts(msg):
        origin = part.get(_HDR.ORIGIN.value)
        if origin and str(origin).strip():
            return str(origin).strip().split("@", 1)[0]
        break
    frm = msg.get(_HDR.FROM.value) or ""
    return frm.split("@", 1)[0].strip() or "?"


class BucketConfigTier(StrEnum):
    """Дискретные конфигурации рендеринга бакета для MCKP."""

    FULL = "full"
    MEDIUM = "medium"
    COMPACT = "compact"
    EMPTY = "empty"


@dataclass(frozen=True)
class BucketConfig:
    """Одна возможная конфигурация бакета для Level 1 MCKP."""

    bucket: EnrichPartId
    tier: BucketConfigTier
    weight: int
    value: float
    tier1_count: int
    tier2_count: int


@dataclass(frozen=True)
class ContextMessageTierAssignment:
    """Tier-assignment одного сообщения (Level 2 результат)."""

    chronological_index: int
    value: float
    assigned_tier: int
    body_chars: int
    origin: str


def normalize_weights(raw: dict[EnrichPartId, float]) -> dict[EnrichPartId, float]:
    """Любые >= 0 значения → [0, 1], сумма = 1. Все нули → равномерное."""
    total = sum(max(0.0, v) for v in raw.values())
    if total == 0:
        n = len(raw)
        return {k: 1.0 / n for k in raw}
    return {k: max(0.0, v) / total for k, v in raw.items()}


def history_body_chars(msg: EmailMessage) -> int:
    """Длина тела первой ``<history>``-части (для scoring/оценки веса)."""
    for _cid, part in iter_history_parts(msg):
        return len(history_part_text(part))
    part = msg.get_body(preferencelist=("plain", "html"))
    return len(part.get_content()) if part else 0


def message_value(
    pos_from_end: int,
    total: int,
    content_score: float,
    body_chars: int,
) -> float:
    """Ценность сообщения для tier-assignment: recency × content_score × size_penalty. O(1)."""
    if total == 0:
        return 0.0
    recency = pos_from_end / total
    size_penalty = 1.0 if body_chars < 5000 else 5000 / max(1, body_chars)
    return recency * max(0.0, content_score) * size_penalty


def score_messages(
    messages: list[EmailMessage],
) -> tuple[ContextMessageTierAssignment, ...]:
    """Level 2: score все сообщения по content-score их ``<history>``-части (desc by value)."""
    total = len(messages)
    scored: list[ContextMessageTierAssignment] = []
    for idx, msg in enumerate(messages):
        pos_from_end = total - idx
        body_chars = history_body_chars(msg)
        val = message_value(pos_from_end, total, message_content_score(msg), body_chars)
        scored.append(ContextMessageTierAssignment(
            chronological_index=idx,
            value=val,
            assigned_tier=3,
            body_chars=body_chars,
            origin=message_origin_label(msg),
        ))
    return tuple(scored)


def assign_tiers(
    scored: tuple[ContextMessageTierAssignment, ...],
    tier1_count: int,
    tier2_count: int,
) -> tuple[ContextMessageTierAssignment, ...]:
    """Assign tier 1/2/3 based on value ranking. Preserves chronological_index."""
    by_value = sorted(scored, key=lambda x: x.value, reverse=True)
    result: list[ContextMessageTierAssignment] = []
    for rank, assignment in enumerate(by_value):
        if rank < tier1_count:
            tier = 1
        elif rank < tier1_count + tier2_count:
            tier = 2
        else:
            tier = 3
        result.append(ContextMessageTierAssignment(
            chronological_index=assignment.chronological_index,
            value=assignment.value,
            assigned_tier=tier,
            body_chars=assignment.body_chars,
            origin=assignment.origin,
        ))
    return tuple(sorted(result, key=lambda x: x.chronological_index))


def estimate_unified_weight(
    scored: tuple[ContextMessageTierAssignment, ...],
    tier1_count: int,
    tier2_count: int,
    preview_chars: int,
    header_chars: int = 100,
) -> int:
    """Оценка веса unified_mail_context без Jinja-рендеринга.

    Использует body_chars из scored для арифметической аппроксимации вместо полного
    Jinja-рендеринга mail_context.j2. Все сообщения содержательные (есть ``<history>``).
    """
    tiered = assign_tiers(scored, tier1_count, tier2_count)
    total = 0
    for t in tiered:
        if t.assigned_tier == 1:
            total += t.body_chars + header_chars
        elif t.assigned_tier == 2:
            total += preview_chars + header_chars
        else:
            total += header_chars
    return total


def solve_mckp(
    bucket_configs: dict[EnrichPartId, list[BucketConfig]],
    capacity: int,
    priorities: dict[EnrichPartId, float],
) -> dict[EnrichPartId, BucketConfig]:
    """Level 1: MCKP via scipy.optimize.milp.

    Fast path: если суммарный full < capacity, возвращает full для всех.
    """
    norm_priorities = normalize_weights(priorities)

    full_total = 0
    full_configs: dict[EnrichPartId, BucketConfig] = {}
    for bucket_id, configs in bucket_configs.items():
        full_cfg = next((c for c in configs if c.tier == BucketConfigTier.FULL), configs[0])
        full_configs[bucket_id] = full_cfg
        full_total += full_cfg.weight

    if full_total <= capacity:
        return full_configs

    buckets = list(bucket_configs.keys())
    n_buckets = len(buckets)

    flat_configs: list[BucketConfig] = []
    group_indices: list[list[int]] = []
    idx = 0
    for bucket_id in buckets:
        configs = bucket_configs[bucket_id]
        group = []
        for cfg in configs:
            flat_configs.append(cfg)
            group.append(idx)
            idx += 1
        group_indices.append(group)

    n_vars = len(flat_configs)

    c = np.array([-cfg.value * norm_priorities.get(cfg.bucket, 0.0) for cfg in flat_configs])

    weights = np.array([[cfg.weight for cfg in flat_configs]], dtype=float)
    capacity_constraint = LinearConstraint(A=weights, ub=[capacity])

    A_groups = np.zeros((n_buckets, n_vars), dtype=float)
    for i, group in enumerate(group_indices):
        for j in group:
            A_groups[i, j] = 1.0
    group_constraint = LinearConstraint(A=A_groups, lb=np.ones(n_buckets), ub=np.ones(n_buckets))

    bounds = Bounds(lb=0, ub=1)
    integrality = np.ones(n_vars)

    constraints = [capacity_constraint, group_constraint]

    res = milp(c=c, constraints=constraints, bounds=bounds, integrality=integrality)

    if res.success:
        chosen: dict[EnrichPartId, BucketConfig] = {}
        for i, bucket_id in enumerate(buckets):
            for j in group_indices[i]:
                if round(res.x[j]) == 1:
                    chosen[bucket_id] = flat_configs[j]
                    break
            else:
                chosen[bucket_id] = full_configs[bucket_id]
        return chosen

    return full_configs
