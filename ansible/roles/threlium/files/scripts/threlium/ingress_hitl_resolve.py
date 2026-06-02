"""Резолв источника CLI intent по цепочке ``In-Reply-To`` (лист → корень).

Барьер: узел с ``To:`` = ``FsmStage.CLI_RESUME`` — выше по цепочке не ищем.
Payload JSON: первый предок (без листа) с ``To:`` = ``FsmStage.CLI_INTENT`` до того же барьера;
тело intent читается из ``<system>``-части (``system_part_text_from_path``), не из первого
``text/plain`` (``docs/CONTEXT_CONTRACT.md`` §2).

HITL-детекция (ранее ``resolve_hitl_parent_from_in_reply_to_ancestors``) удалена:
используется прямой lookup родителя через
:func:`~threlium.types.ingress_hitl.classify_hitl_parent_notmuch` в ``ingress.main``.
"""
from __future__ import annotations

from pathlib import Path

from threlium.irt_chain import iter_in_reply_to_ancestors_from_inner_id
from threlium.types.fsm_stage import FsmStage
from threlium.types import NotmuchMessageIdInner


def find_cli_intent_maildir_path_from_in_reply_to_ancestors(
    start_inner: NotmuchMessageIdInner,
) -> Path | None:
    """Первое письмо с ``To: cli_intent@localhost`` среди предков (лист исключён), до барьера ``cli_resume``."""
    ancestors = iter_in_reply_to_ancestors_from_inner_id(start_inner)
    if not ancestors:
        return None
    for snap in ancestors[1:]:
        if snap.is_addressed_to_fsm_stage(FsmStage.CLI_RESUME):
            return None
        if snap.is_addressed_to_fsm_stage(FsmStage.CLI_INTENT):
            if not snap.path.is_file():
                raise RuntimeError(
                    "FSM-инвариант: файл письма cli_intent отсутствует на диске по пути из индекса "
                    f"path={snap.path!r}"
                )
            return snap.path
    return None
