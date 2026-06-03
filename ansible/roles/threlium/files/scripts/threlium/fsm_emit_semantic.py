"""Семантические обёртки над низкоуровневым :func:`emit_transition_preserving_payload`.

Дефолты билдера (IRT из MID входа, декремент hop) живут здесь,
а не в ``fsm_emit`` — см. ``docs/TYPES.md``.
"""
from __future__ import annotations

from copy import deepcopy
from email.message import EmailMessage

from threlium.fsm_emit import (
    ManagedFsmHeaderPatch,
    ManagedFsmHeaderValue,
    advance_hop_budget_for_simple_step,
    attach_request_echo_history,
    build_fsm_step_to_stage,
    emit_transition_preserving_payload,
    _build_history_only_envelope,
    irt_wire_from_incoming_message_id,
)
from threlium.mime_reform import (
    EnrichContentId,
    _make_inline_text_part,
    attach_user_query_part,
    iter_history_parts,
)
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    EnrichCalleeHistoryText,
    EnrichRequestEchoText,
    EnrichUserQueryText,
    FsmStage,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    IngressDistillHistoryPart,
    MailHeaderName,
    PromptPath,
    ThreliumContentScoreWire,
)


def managed_patch_simple_fsm_step(
    incoming: EmailMessage, settings: ThreliumSettings,
) -> dict[MailHeaderName, ManagedFsmHeaderValue]:
    """Карта managed-заголовков: IRT из MID входа, декремент hop."""
    patch: dict[MailHeaderName, ManagedFsmHeaderValue] = {}

    irt = irt_wire_from_incoming_message_id(incoming)
    if irt is not None and irt.value.strip():
        patch[MailHeaderName.IN_REPLY_TO] = irt

    patch[MailHeaderName.HOP_BUDGET] = advance_hop_budget_for_simple_step(
        HopBudgetLine.parse_from_email(incoming), settings
    )

    return patch


def emit_transition_simple_step_preserving_payload(
    incoming: EmailMessage,
    *,
    to_addr: FsmStage,
    from_stage: FsmStage,
    settings: ThreliumSettings,
) -> EmailMessage:
    """Переход с сохранением тела; IRT / hop как простой шаг."""
    return emit_transition_preserving_payload(
        incoming,
        to_addr=to_addr,
        from_stage=from_stage,
        managed_headers=managed_patch_simple_fsm_step(incoming, settings),
    )


def emit_to_enrich(
    incoming: EmailMessage,
    from_stage: FsmStage,
    *,
    user_query: EnrichUserQueryText,
    callee_history: EnrichCalleeHistoryText | None = None,
    request_echo: EnrichRequestEchoText | None = None,
    relay_history_from: EmailMessage | None = None,
    settings: ThreliumSettings,
    managed_headers: ManagedFsmHeaderPatch | None = None,
) -> EmailMessage:
    """Единый choke-point callee → ``enrich@`` (``<user-query>`` обязателен)."""
    headers = (
        managed_headers
        if managed_headers is not None
        else managed_patch_simple_fsm_step(incoming, settings)
    )
    out = _build_history_only_envelope(
        incoming,
        to_addr=FsmStage.ENRICH,
        from_stage=from_stage,
        managed_headers=headers,
    )
    attach_user_query_part(out, user_query)
    if relay_history_from is not None:
        for _cid, part in iter_history_parts(relay_history_from):
            out.attach(deepcopy(part))
    elif callee_history is not None and callee_history.value.strip():
        score = ThreliumContentScoreWire.from_score(
            settings.history.score_for(from_stage)
        )
        body = callee_history.value
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(body),
                body,
                score=score,
            )
        )
    if request_echo is not None and request_echo.value.strip():
        attach_request_echo_history(
            out,
            incoming=incoming,
            echo_text=request_echo.value,
            from_stage=from_stage,
            settings=settings,
        )
    return out


def emit_bridge_distill_to_enrich(
    incoming: EmailMessage,
    from_stage: FsmStage,
    *,
    user_query: EnrichUserQueryText,
    settings: ThreliumSettings,
    distill_parts: tuple[IngressDistillHistoryPart, ...],
) -> EmailMessage:
    """ingress (bridge-only): distill ``<history>`` + ``<user-query>`` from bridge system → enrich."""
    out = _build_history_only_envelope(
        incoming,
        to_addr=FsmStage.ENRICH,
        from_stage=from_stage,
        managed_headers=managed_patch_simple_fsm_step(incoming, settings),
    )
    attach_user_query_part(out, user_query)
    score = ThreliumContentScoreWire.from_score(settings.history.score_for(from_stage))
    for hp in distill_parts:
        out.attach(
            _make_inline_text_part(
                EnrichContentId.from_history_body(hp.text),
                hp.text,
                score=score,
            )
        )
    return out


def emit_to_enrich_fast(
    incoming: EmailMessage,
    from_stage: FsmStage,
    *,
    settings: ThreliumSettings,
    history: str | None = None,
    request_echo: str | None = None,
    system: str | None = None,
    subject_line: FsmTransitionPlainSubjectLine | None = None,
) -> EmailMessage:
    """Единый choke-point перехода tool-callee → ``enrich_fast@`` (CONTEXT §3)."""
    return build_fsm_step_to_stage(
        incoming,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=from_stage,
        history=history,
        request_echo=request_echo,
        system=system,
        subject_line=subject_line,
        settings=settings,
    )


def emit_preserving_to_enrich_fast(
    incoming: EmailMessage,
    from_stage: FsmStage,
    *,
    settings: ThreliumSettings,
) -> EmailMessage:
    """Mutator relay: сохранить payload, перейти в ``enrich_fast@``."""
    return emit_transition_simple_step_preserving_payload(
        incoming,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=from_stage,
        settings=settings,
    )


def emit_enrich_validation_error(
    incoming: EmailMessage,
    *,
    from_stage: FsmStage,
    settings: ThreliumSettings,
    user_query: EnrichUserQueryText,
    prompt_path: PromptPath,
    **template_vars: object,
) -> EmailMessage:
    """Validation-ошибка mutator-стадии → ``enrich@`` с notice в ``<history>``."""
    body = render_prompt(prompt_path, **template_vars).strip()
    return emit_to_enrich(
        incoming,
        from_stage,
        user_query=user_query,
        callee_history=EnrichCalleeHistoryText.parse(body),
        settings=settings,
    )


def managed_patch_subagent_push_to_enrich(
    incoming: EmailMessage,
    *,
    hop_budget: HopBudgetLine,
) -> ManagedFsmHeaderPatch:
    """subagent_intent → enrich: непрерывный IRT + изолированный hop."""
    patch: ManagedFsmHeaderPatch = {MailHeaderName.HOP_BUDGET: hop_budget}
    irt = irt_wire_from_incoming_message_id(incoming)
    if irt is not None and irt.value.strip():
        patch = {MailHeaderName.IN_REPLY_TO: irt, **patch}
    return patch


# Removed: emit_ingress_validation_error → emit_enrich_validation_error
