"""Двухуровневая оптимизация контекстного бюджета (MCKP + per-message scoring).

Level 1: MCKP (Multiple-Choice Knapsack) между бакетами — scipy.optimize.milp.
Level 2: Per-message value scoring внутри unified_mail_context для динамического tier-assignment.

docs/TYPES.md: доменные enum, frozen dataclasses, typed containers.
"""
from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
from enum import StrEnum

import numpy as np
from scipy.optimize import LinearConstraint, Bounds, milp

from threlium.mime_reform import EnrichPartId
from threlium.types import FsmStage

class ContextMessageType(StrEnum):
    """Тип сообщения в контексте — определяет value-weight при scoring."""

    USER_INPUT = "user_input"
    AGENT_RESPONSE = "agent_response"
    CONTEXT_SUMMARY = "context_summary"
    TOOL_OBSERVATION = "tool_observation"
    SYSTEM = "system"
    SERVICE = "service"


# Единая таблица: канонический ``To:`` → роль в enrich-контексте.
#
# Whitelist unified IRT-хвоста (:func:`message_in_unified_mail_context`) = все ключи,
# кроме ``SERVICE``. Стадии без строки здесь не попадают в ``<unified-mail-context>``
# (``classify_message_type`` → ``SYSTEM``).
#
# --- В unified (содержательное тело для LLM) ---
#
# ingress — ввод пользователя (мост email/matrix/telegram), ответы субагента
#   (subagent_end→ingress), reflect, ошибки cli, наблюдения cli_exec (выход уходит
#   с ``From: cli_exec``, но в Maildir фиксируется как письмо ``To: ingress``).
# egress_router — итоговый текст перед наружу: response_finalize (compose) и
#   cli_hitl_out (HITL bridge); одного блока достаточно, без egress_* и archive.
# cli_exec — входящее задание на исполнение (payload после cli_intent); stdout/stderr
#   смотри в следующем по IRT ``To: ingress`` от cli_exec.
# formal_reason — payload от reasoning (SHACL/RDF-рассуждение); отчёт уходит в
#   enrich_fast и не дублируется здесь, но входное письмо нужно в треде («проверь логику»).
# memory_query — формулировка запроса к графу от reasoning; ответ — observation в
#   enrich_fast, в IRT остаётся исходная постановка на ``To: memory_query``.
# summarize_memory — итог ``summarize_context`` (сжатый хвост треда); высокий tier-score.
#
# --- SERVICE (явно не в unified; схлопывание в mail_context.j2) ---
#
# enrich — триггер цикла и монолитный MIME enrich→reasoning; контекст в отдельных
#   Content-ID, не в IRT-хвосте.
# enrich_fast — аддитивный relay observation/plan/memory между reasoning и вспомогательными
#   стадиями: каждый хоп — отдельная MIME-часть с уникальным Content-ID <family@inner-mid>,
#   накапливается в хвосте E_prev (не перезапись). reasoning группирует по семейству.
# response_observe / response_edit / response_append — CRDT-наблюдения и правки ответа.
# reflect — служебный переход (шаблон continue/final); смысл попадает в следующий ingress.
# summarize_context — переполнение контекста → пакетное summarize; не история треда.
#
# --- Нет строки (не unified; отдельный канал или дублирует whitelist) ---
#
# reasoning — ``To: reasoning`` несёт multipart enrich (graph/unified/…); дублировал бы
#   отдельные MIME-части и раздувал бы бюджет.
# response_finalize — черновик собирается в handler; наружу идёт через egress_router
#   (IRT egress часто на glue MID ingress, не на finalize).
# thread_memory / global_memory — отдельные бакеты ``build_unified_email_messages``,
#   не IRT-хвост ``all_messages``.
# subagent_intent / subagent_end — границы субагента; результат subagent_end→ingress.
# cli_intent / cli_resume — маршрутизация CLI; содержание — cli_exec или ingress.
# cli_hitl_out — запрос подтверждения → egress_router (включён через egress_router).
# egress_email / egress_telegram / egress_matrix — доставка и sent_raw; достаточно egress_router.
# archive — audit egress; IRT-glue ответа пользователя, не для LLM-контекста.
CONTEXT_ROLE_BY_TO_STAGE: dict[FsmStage, ContextMessageType] = {
    FsmStage.INGRESS: ContextMessageType.USER_INPUT,
    FsmStage.EGRESS_ROUTER: ContextMessageType.AGENT_RESPONSE,
    FsmStage.CLI_EXEC: ContextMessageType.TOOL_OBSERVATION,
    FsmStage.FORMAL_REASON: ContextMessageType.AGENT_RESPONSE,
    FsmStage.MEMORY_QUERY: ContextMessageType.AGENT_RESPONSE,
    FsmStage.SUMMARIZE_MEMORY: ContextMessageType.CONTEXT_SUMMARY,
    FsmStage.ENRICH: ContextMessageType.SERVICE,
    FsmStage.ENRICH_FAST: ContextMessageType.SERVICE,
    FsmStage.RESPONSE_OBSERVE: ContextMessageType.SERVICE,
    FsmStage.RESPONSE_EDIT: ContextMessageType.SERVICE,
    FsmStage.RESPONSE_APPEND: ContextMessageType.SERVICE,
    FsmStage.TASKS_UPSERT: ContextMessageType.SERVICE,
    FsmStage.REFLECT: ContextMessageType.SERVICE,
    FsmStage.SUMMARIZE_CONTEXT: ContextMessageType.SERVICE,
}

SERVICE_TRANSITION_STAGES: frozenset[FsmStage] = frozenset(
    stage
    for stage, role in CONTEXT_ROLE_BY_TO_STAGE.items()
    if role is ContextMessageType.SERVICE
)


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
    msg_type: ContextMessageType


@dataclass(frozen=True)
class ContextMessageTypeWeights:
    """Веса типов сообщений — typed container вместо dict[str, float]."""

    user_input: float
    agent_response: float
    context_summary: float
    tool_observation: float
    system: float
    service: float

    def weight_for(self, msg_type: ContextMessageType) -> float:
        return getattr(self, msg_type.value)


def normalize_weights(raw: dict[EnrichPartId, float]) -> dict[EnrichPartId, float]:
    """Любые >= 0 значения → [0, 1], сумма = 1. Все нули → равномерное."""
    total = sum(max(0.0, v) for v in raw.values())
    if total == 0:
        n = len(raw)
        return {k: 1.0 / n for k in raw}
    return {k: max(0.0, v) / total for k, v in raw.items()}


def _normalize_type_weights(weights: ContextMessageTypeWeights) -> ContextMessageTypeWeights:
    """Нормализовать type weights к [0, 1]."""
    raw = [
        max(0.0, weights.user_input),
        max(0.0, weights.agent_response),
        max(0.0, weights.context_summary),
        max(0.0, weights.tool_observation),
        max(0.0, weights.system),
        max(0.0, weights.service),
    ]
    total = sum(raw)
    if total == 0:
        n = len(raw)
        raw = [1.0 / n] * n
    else:
        raw = [v / total for v in raw]
    return ContextMessageTypeWeights(
        user_input=raw[0],
        agent_response=raw[1],
        context_summary=raw[2],
        tool_observation=raw[3],
        system=raw[4],
        service=raw[5],
    )


def to_stage_in_unified_role(stage: FsmStage | None) -> bool:
    """``To``-стадия входит в unified: есть в :data:`CONTEXT_ROLE_BY_TO_STAGE` и роль не SERVICE.

    Предикат **enrich**: фильтр IRT-хвоста треда (:func:`iter_irt_ancestors_filtered`).
    ``thread_memory`` / ``global_memory`` сюда **не** входят — они собираются
    отдельными бакетами :func:`build_unified_email_messages`, а не из IRT-цепочки.
    Для drain (глобальный скан union-индекса) используется более широкий
    :func:`content_indexable_to_stage`.
    """
    if stage is None:
        return False
    role = CONTEXT_ROLE_BY_TO_STAGE.get(stage)
    return role is not None and role is not ContextMessageType.SERVICE


def message_in_unified_mail_context(msg: EmailMessage) -> bool:
    """IRT-хвост: ``To:`` есть в :data:`CONTEXT_ROLE_BY_TO_STAGE` и роль не SERVICE."""
    return to_stage_in_unified_role(FsmStage.try_from_incoming_to(msg))


# Стадии, чьё ``To:`` несёт содержательную нагрузку для графа LightRAG.
#
# База — те же не-SERVICE ключи :data:`CONTEXT_ROLE_BY_TO_STAGE`, что и unified
# enrich-роль (:func:`to_stage_in_unified_role`), плюс выделенные memory-ящики
# ``thread_memory`` / ``global_memory``.
#
# Фундаментальное отличие от enrich: enrich собирает контекст обходом IRT-цепочки
# треда (:func:`iter_irt_ancestors_filtered`) — thread/subagent-локально, с дедупом
# и отдельными memory-бакетами, которые в IRT-хвост не включаются. Drain же
# сканирует весь union-индекс глобально (notmuch search), поэтому memory-письма
# индексируются напрямую, а не как отдельные бакеты.
CONTENT_INDEXABLE_STAGES: frozenset[FsmStage] = frozenset(
    {
        stage
        for stage, role in CONTEXT_ROLE_BY_TO_STAGE.items()
        if role is not ContextMessageType.SERVICE
    }
    | {FsmStage.THREAD_MEMORY, FsmStage.GLOBAL_MEMORY}
)


def content_indexable_stages() -> frozenset[FsmStage]:
    """Whitelist стадий для LightRAG-drain (см. :data:`CONTENT_INDEXABLE_STAGES`)."""
    return CONTENT_INDEXABLE_STAGES


def content_indexable_to_stage(stage: FsmStage | None) -> bool:
    """``To``-стадия письма несёт содержательную нагрузку для LightRAG-графа (drain)."""
    return stage is not None and stage in CONTENT_INDEXABLE_STAGES


def classify_message_type(msg: EmailMessage) -> ContextMessageType:
    """Тип для scoring — из :data:`CONTEXT_ROLE_BY_TO_STAGE`, иначе SYSTEM."""
    stage = FsmStage.try_from_incoming_to(msg)
    if stage is None:
        return ContextMessageType.SYSTEM
    return CONTEXT_ROLE_BY_TO_STAGE.get(stage, ContextMessageType.SYSTEM)


def message_value(
    pos_from_end: int,
    total: int,
    msg_type: ContextMessageType,
    body_chars: int,
    type_weights: ContextMessageTypeWeights,
) -> float:
    """Вычислить ценность сообщения для tier-assignment. O(1)."""
    if total == 0:
        return 0.0
    recency = pos_from_end / total
    type_w = type_weights.weight_for(msg_type)
    size_penalty = 1.0 if body_chars < 5000 else 5000 / max(1, body_chars)
    return recency * type_w * size_penalty


def score_messages(
    messages: list[EmailMessage],
    type_weights: ContextMessageTypeWeights,
) -> tuple[ContextMessageTierAssignment, ...]:
    """Level 2: score все сообщения, вернуть sorted by value desc."""
    normalized_tw = _normalize_type_weights(type_weights)
    total = len(messages)
    scored: list[ContextMessageTierAssignment] = []
    for idx, msg in enumerate(messages):
        pos_from_end = total - idx
        msg_type = classify_message_type(msg)
        part = msg.get_body(preferencelist=("plain", "html"))
        body_chars = len(part.get_content()) if part else 0
        val = message_value(pos_from_end, total, msg_type, body_chars, normalized_tw)
        scored.append(ContextMessageTierAssignment(
            chronological_index=idx,
            value=val,
            assigned_tier=3,
            body_chars=body_chars,
            msg_type=msg_type,
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
            msg_type=assignment.msg_type,
        ))
    return tuple(sorted(result, key=lambda x: x.chronological_index))


def estimate_unified_weight(
    scored: tuple[ContextMessageTierAssignment, ...],
    tier1_count: int,
    tier2_count: int,
    preview_chars: int,
    header_chars: int = 100,
    service_marker_chars: int = 50,
) -> int:
    """Оценка веса unified_mail_context без Jinja-рендеринга.

    Использует body_chars и msg_type из scored для арифметической аппроксимации
    вместо полного Jinja-рендеринга mail_context.j2.
    """
    tiered = assign_tiers(scored, tier1_count, tier2_count)
    total = 0
    for t in tiered:
        if t.msg_type == ContextMessageType.SERVICE:
            total += service_marker_chars
        elif t.assigned_tier == 1:
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
