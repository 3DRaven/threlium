"""Семантические обёртки над низкоуровневым :func:`emit_transition_preserving_payload`.

Дефолты билдера (IRT из MID входа, декремент hop) живут здесь,
а не в ``fsm_emit`` — см. ``docs/TYPES.md``.
"""
from __future__ import annotations

from email.message import EmailMessage

from threlium.fsm_emit import (
    HDR_HOP_BUDGET,
    ManagedFsmHeaderPatch,
    ManagedFsmHeaderValue,
    advance_hop_budget_for_simple_step,
    build_fsm_plain_to_stage,
    build_fsm_step_to_stage,
    emit_transition_preserving_payload,
    irt_wire_from_incoming_message_id,
)
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    FsmStage,
    FsmTransitionPlainBody,
    FsmTransitionPlainSubjectLine,
    HopBudgetLine,
    MailHeaderName,
    PromptPath,
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
    """Единый choke-point перехода tool-callee → ``enrich_fast@`` (CONTEXT §3).

    Обёртка над :func:`build_fsm_step_to_stage` с ``to_addr=ENRICH_FAST``; семантика
    ``history`` / ``request_echo`` / ``system`` — как у билдера (callee владеет историей).
    """
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
    """Mutator relay: сохранить payload, перейти в ``enrich_fast@`` (tasks_upsert, response_*)."""
    return emit_transition_simple_step_preserving_payload(
        incoming,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=from_stage,
        settings=settings,
    )


def emit_ingress_validation_error(
    incoming: EmailMessage,
    *,
    from_stage: FsmStage,
    settings: ThreliumSettings,
    prompt_path: PromptPath,
    **template_vars: object,
) -> EmailMessage:
    """Стадия отбила входной payload → ``ingress`` с отрендеренной ошибкой.

    Единый путь для validation-ошибок mutator-стадий (tasks_upsert, response_edit):
    ``render_prompt(prompt_path, **template_vars)`` → ``<plain>`` на ingress как
    простой шаг (IRT из MID входа, декремент hop).
    """
    body = render_prompt(prompt_path, **template_vars).strip()
    return build_fsm_plain_to_stage(
        incoming,
        to_addr=FsmStage.INGRESS,
        from_stage=from_stage,
        body=FsmTransitionPlainBody.parse(body),
        settings=settings,
    )


def managed_patch_subagent_push_to_ingress(
    incoming: EmailMessage,
    *,
    hop_budget: HopBudgetLine,
) -> ManagedFsmHeaderPatch:
    """subagent_intent → ingress: непрерывный IRT + изолированный hop."""
    patch: ManagedFsmHeaderPatch = {MailHeaderName.HOP_BUDGET: hop_budget}
    irt = irt_wire_from_incoming_message_id(incoming)
    if irt is not None and irt.value.strip():
        patch = {MailHeaderName.IN_REPLY_TO: irt, **patch}
    return patch
