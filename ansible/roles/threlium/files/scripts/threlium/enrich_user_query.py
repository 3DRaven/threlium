"""Загрузка ``<user-query>`` из IRT-предков (callee-only, не enrich consumer)."""
from __future__ import annotations

from email.message import EmailMessage

from threlium.mail import email_message_from_path
from threlium.mime_reform import require_enrich_user_query_text
from threlium.nm import require_fsm_message_id
from threlium.thread_context_filter import iter_irt_ancestors_filtered
from threlium.types import EnrichUserQueryText, FsmStage, NotmuchMessageIdInner


def load_enrich_user_query_from_thread_irt(
    leaf_inner: NotmuchMessageIdInner,
) -> EnrichUserQueryText | None:
    """Последний ancestor ``To: enrich@`` с ``<user-query>`` (лист→корень)."""
    for snap in iter_irt_ancestors_filtered(leaf_inner):
        if not snap.is_addressed_to_fsm_stage(FsmStage.ENRICH):
            continue
        try:
            m = email_message_from_path(snap.path)
        except OSError:
            continue
        try:
            return require_enrich_user_query_text(m)
        except RuntimeError:
            continue
    return None


def require_enrich_user_query_for_reenrich(
    msg: EmailMessage,
    *,
    stage_label: str,
) -> EnrichUserQueryText:
    """Callee re-enrich: обязательный parent ``<user-query>`` из треда."""
    _mid_w, inner = require_fsm_message_id(msg, stage_label)
    loaded = load_enrich_user_query_from_thread_irt(inner)
    if loaded is None:
        raise RuntimeError(
            f"{stage_label}: no ancestor To:enrich@ with <user-query> in thread"
        )
    return loaded
