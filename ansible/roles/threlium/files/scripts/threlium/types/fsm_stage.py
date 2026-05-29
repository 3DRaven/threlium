"""Замкнутый набор FSM-стадий: local-part = ``value``, mailbox = ``<value>@localhost``.

Синхронизировать с ``ansible/roles/threlium/vars/main.yml`` → ``threlium_fsm_mailbox_stages[].id``.
"""
from __future__ import annotations

from email.message import EmailMessage
from email.utils import getaddresses
from enum import StrEnum
from typing import Self

from threlium.mail_header_names import MailHeaderName

_HDR = MailHeaderName


class FsmStage(StrEnum):
    INGRESS = "ingress"
    ENRICH = "enrich"
    REASONING = "reasoning"
    REFLECT = "reflect"
    THREAD_MEMORY = "thread_memory"
    GLOBAL_MEMORY = "global_memory"
    SUBAGENT_INTENT = "subagent_intent"
    SUBAGENT_END = "subagent_end"
    CLI_INTENT = "cli_intent"
    CLI_HITL_OUT = "cli_hitl_out"
    CLI_RESUME = "cli_resume"
    CLI_EXEC = "cli_exec"
    RESPONSE_APPEND = "response_append"
    RESPONSE_EDIT = "response_edit"
    RESPONSE_OBSERVE = "response_observe"
    TASKS_UPSERT = "tasks_upsert"
    ENRICH_FAST = "enrich_fast"
    RESPONSE_FINALIZE = "response_finalize"
    EGRESS_ROUTER = "egress_router"
    EGRESS_EMAIL = "egress_email"
    EGRESS_TELEGRAM = "egress_telegram"
    EGRESS_MATRIX = "egress_matrix"
    FORMAL_REASON = "formal_reason"
    MEMORY_QUERY = "memory_query"
    SUMMARIZE_CONTEXT = "summarize_context"
    SUMMARIZE_MEMORY = "summarize_memory"
    ARCHIVE = "archive"

    @property
    def rfc822_mailbox(self) -> str:
        return f"{self.value}@localhost"

    @classmethod
    def parse(cls, raw: str | None) -> FsmStage:
        """Local-part или полный ``local@localhost`` (без ``+``); только известные стадии."""
        s = str(raw).strip() if raw is not None else ""
        if not s:
            raise ValueError("FsmStage.parse: empty")
        if "@" in s:
            local_s, _, dom = s.partition("@")
            local = local_s.strip()
            dom_l = dom.strip().lower()
            if dom_l != "localhost":
                raise ValueError(f"FsmStage.parse: domain must be localhost: {raw!r}")
            if "+" in local:
                raise ValueError("FsmStage.parse: plus addressing forbidden")
            if not local:
                raise ValueError(f"FsmStage.parse: empty local part: {raw!r}")
            key = local
        else:
            key = s
        try:
            return cls(key)
        except ValueError as e:
            raise ValueError(f"FsmStage.parse: unknown stage {raw!r}") from e

    @classmethod
    def from_incoming_to(cls, msg: EmailMessage) -> Self:
        """Извлечь стадию из канонического ``To:`` входного письма.

        FSM-инвариант (`docs/FSM.md §4`, `docs/ORCHESTRATION.md §3`): каждое письмо,
        попадающее на вход стадии, адресовано ровно одной стадии как
        ``<stage>@localhost``. Плюс-адресация запрещена — параллелизм обеспечивается
        `systemd`-template'ом воркера, а не заголовками. Нарушение инварианта —
        жёсткая ошибка: билдер или bridge собрал письмо неверно либо fdm.conf
        мисроутил.
        """
        tos = msg.get_all(_HDR.TO, [])
        addrs = [a for _, a in getaddresses(tos) if a]
        if len(addrs) != 1:
            raise RuntimeError(
                f"FSM-инвариант нарушен: ожидается ровно одно To:, получено {len(addrs)} ({tos!r})"
            )
        email_addr = addrs[0]
        if "@" not in email_addr:
            raise RuntimeError(f"FSM-инвариант нарушен: To:={email_addr!r} без домена")
        local, domain = email_addr.split("@", 1)
        if domain != "localhost":
            raise RuntimeError(
                f"FSM-инвариант нарушен: To: должен быть @localhost, получено @{domain}"
            )
        if "+" in local:
            raise RuntimeError(
                f"FSM-инвариант нарушен: плюс-адресация в To: запрещена "
                f"(параллелизм обеспечивается systemd template воркера, "
                f"см. ORCHESTRATION.md §3); получено {email_addr!r}"
            )
        if not local:
            raise RuntimeError(
                f"FSM-инвариант нарушен: пустая локальная часть To:={email_addr!r}"
            )
        return cls.parse(local)

    @classmethod
    def _try_stage_from_single_addr(cls, email_addr: str) -> Self | None:
        if "@" not in email_addr:
            return None
        local, domain = email_addr.split("@", 1)
        if domain != "localhost" or "+" in local or not local.strip():
            return None
        try:
            return cls.parse(local)
        except ValueError:
            return None

    @classmethod
    def try_from_incoming_to(cls, msg: EmailMessage) -> Self | None:
        """Как :meth:`from_incoming_to`, но ``None`` если ``To:`` не ровно одна FSM-стадия @localhost."""
        addrs = [a for _, a in getaddresses(msg.get_all(_HDR.TO, [])) if a]
        return cls._try_stage_from_single_addr(addrs[0]) if len(addrs) == 1 else None

    @classmethod
    def try_from_to_header_value(cls, raw: str | None) -> Self | None:
        """Стадия из сырого значения заголовка ``To:`` (``None`` если не ровно одна FSM-стадия @localhost)."""
        if raw is None:
            return None
        addrs = [a for _, a in getaddresses([raw]) if a]
        return cls._try_stage_from_single_addr(addrs[0]) if len(addrs) == 1 else None
