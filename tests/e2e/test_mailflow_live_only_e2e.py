"""Почтовый mailflow на **уже поднятом** e2e-стеке: этот модуль не поднимает compose и не вызывает Ansible.

**Общие условия.** Стек должен быть запущен заранее (docker compose / отдельные утилиты вне этого
репозитория; см. ``docs/E2E.md`` §5). Тесты с хоста машины открывают SMTP и IMAP на проброшенные порты GreenMail. Имя compose-проекта
берётся из переменной окружения, если задана, иначе автообнаруживается среди здоровых проектов с
префиксом e2e. Если подходящего стека нет — тест пропускается, чтобы файл можно было включать в
общую выборку CI. Для длинных ожиданий удобен запуск с неперехваченным выводом, чтобы видеть
периодические сообщения о прогрессе.

**Роли адресов.** Письма «от пользователя» уходят на ящик, с которого в продукте забирает почту
fetchmail. Ответы агента тест читает из отдельного ящика того же сервера (учётная запись для
входящих ответов в live-режиме). Тестовый LLM-mock узнаёт свой ответ по характерной теме и фрагменту
тела письма.

---

**Диалог в два оборота.** Проверяется полный обмен без специальных тем: пользователь → агент →
пользователь → агент.

1. Запоминается, сколько писем уже лежит во входящих ящика ответов (чтобы не перепутать со старыми).
2. Отправляется первое письмо от пользовательского адреса на адрес приёма с уникальным идентификатором
   и темой.
3. Ожидается первое **новое** письмо в ящике ответов: оно должно выглядеть как ответ тестового мока
   по теме и телу, а заголовок ответа на предыдущее письмо должен ссылаться на исходное сообщение
   пользователя.
4. Отправляется второе письмо пользователя как ответ в тред: тема с префиксом «Re:», заголовки
   ответа и ссылки на цепочку указывают на **первый** ответ агента (как в обычном почтовом клиенте).
5. Ожидается второй ответ агента с теми же признаками мока, но ссылка «на предыдущее» должна уже
   вести на **второе** письмо пользователя — цепочка обсуждения не оборвалась после первого круга.

---

**Усечённая цепочка SUBAGENT_TABLE.** Проверяется ветка с вложенным кадром: ответ агента в pytest@,
затем по **STATE** (content-flag ``saw_subagent_result``) — POP-результат дочернего L1-кадра вернулся
в L0 reasoning-промпт (строго сильнее «папка ``subagent_end`` существует»). Без docker-exec/notmuch.

---

**Локальная память (MEMORY_TABLE, ветки thread_memory / global_memory / reflect).** Метка сценария в
теме; проверка по **STATE** (content-flag ``saw_thread_memory_note`` / ``saw_global_memory_note`` /
``saw_reflect_summary``) — персистнутая заметка/итог reflect вернулись в reasoning-промпт; ответ
агента в pytest@ как барьер. Без docker-exec/notmuch (E2E.md §3.6.2).

---

**Полная матрица SUBAGENT_TABLE с подтверждением CLI (HITL).** Письмо с маркером в теле; HITL-письмо
в pytest@ (IMAP UID SEARCH по треду); ответ «yes»; финальный ответ агента; проверка по **STATE**
(content-flag ``saw_hitl_cli_echo``) — привилегированная cli-команда выполнилась после подтверждения и
её вывод вернулся в reasoning. Без docker-exec/notmuch.

---

**CLI: sandbox allow / отказ после HITL.** WireMock (``cli_intent_allow_echo``, ``hitl_matrix_resume_no``).

---

Нужен живой Docker Compose с системой под тестом, GreenMail и сервисом-заглушкой LLM (тот же файл
compose, что в ``tests/e2e/compose``). Подъём стека и ``site.yml`` — вне этого модуля.

Обнаружение проекта: первый healthy compose-проект ``threlium_e2e_*`` с контейнером ``sut``.

Если стека нет — тесты **пропускаются**, а не падают: их можно держать в общей выборке CI.
"""
from __future__ import annotations

from tests.e2e.mail_wire import e2e_parse_rfc822, e2e_serialize_rfc822, e2e_smtp_send
from email.header import decode_header
from email.message import EmailMessage

from pathlib import Path

import imaplib
import re

import uuid

import pytest
import requests
import smtplib

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .toolkit import (
    E2EComposeRuntime,
    TIMEOUT_POLL_LIVE_MAIL,
    TIMEOUT_POLL_SHORT,
    E2E_FETCHMAIL_PASS,
    E2E_FETCHMAIL_USER,
    E2E_GREENMAIL_REPLY_USER,
    E2E_REPLY_BODY_SNIPPET,
    REPO_ROOT,
    e2e_refresh_hop_budget_default,
    e2e_refresh_hop_budget_sub,
    e2e_dense_threlium_ctx_body,
    e2e_greenmail_mailbox_address,
    e2e_thread_root_mid_for_message_id,
    poll_until,
    rfc_first_message_id_in_in_reply_to_header,
)
from .wiremock_client import (
    assert_wiremock_zero_unmatched_requests,
    prepare_wiremock_scenario,
    wiremock_admin_base,
    wiremock_public_base,
    wiremock_state_thread_root_property,
)

# Каталоги стабов и ``stub_tag`` этого модуля; корреляция LiteLLM — ``X-Threlium-Thread-Root``
# (= canonical MID старейшего в треде письма с ``tag:route``, см. ``e2e_thread_root_mid_for_message_id``).
_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
_LIVE_ONLY_ROOT = _WIREMOCK_STUBS_ROOT / "test_mailflow_live_only_e2e"

LIVE_TWO_TURN_STUB_TAG = "stub-mailflow-live-two-turn-01"
LIVE_TWO_TURN_CORRELATOR = "e2e-live-tw-anchor-01"
LIVE_TWO_TURN_STUB_DIR = _LIVE_ONLY_ROOT / "two_turn"
LIVE_TWO_TURN_SUBJECT1 = f"e2e live dialog turn1 {LIVE_TWO_TURN_CORRELATOR}"
LIVE_TWO_TURN_SUBJECT2 = "Re: e2e reply turn2 partb-marker"


LIVE_SUBAGENT_SHALLOW_STUB_TAG = "stub-mailflow-live-sat-shallow-01"
LIVE_SUBAGENT_SHALLOW_SUBJECT = "e2e_subagent_table_chain live sat-shallow-01"
LIVE_SUBAGENT_SHALLOW_STUB_DIR = _LIVE_ONLY_ROOT / "subagent_table_shallow"

LIVE_SUBAGENT_BUDGET_EXHAUSTED_STUB_TAG = "stub-mailflow-live-sat-budget-exhausted-01"
LIVE_SUBAGENT_BUDGET_EXHAUSTED_SUBJECT = "e2e_subagent_budget_exhausted live sat-budget-exhausted-01"
LIVE_SUBAGENT_BUDGET_EXHAUSTED_STUB_DIR = _LIVE_ONLY_ROOT / "subagent_budget_exhausted"
# Plain subagent_intent body uses "X-Threlium-Hop-Budget exhausted"; L1 reasoning after
# enrich_fast relay gets reasoning/budget_exhausted.j2 (remaining < 1), visible in WM journal.
E2E_SUBAGENT_BUDGET_EXHAUSTED_NOTICE = "hop-budget for this thread is exhausted"

LIVE_MEMORY_THREAD_STUB_TAG = "stub-mailflow-live-mem-thread-01"
LIVE_MEMORY_THREAD_SUBJECT = "e2e_memory_thread_live live mem-thread-01"
LIVE_MEMORY_THREAD_STUB_DIR = _LIVE_ONLY_ROOT / "memory_thread"

LIVE_GLOBAL_MEMORY_STUB_TAG = "stub-mailflow-live-global-mem-01"
LIVE_GLOBAL_MEMORY_SUBJECT = "e2e_global_memory_live live global-mem-01"
LIVE_GLOBAL_MEMORY_STUB_DIR = _LIVE_ONLY_ROOT / "global_memory"

LIVE_REFLECT_CYCLE_STUB_TAG = "stub-mailflow-live-reflect-cyc-01"
LIVE_REFLECT_CYCLE_SUBJECT = "e2e_reflect_cycle_live live reflect-cyc-01"
LIVE_REFLECT_CYCLE_STUB_DIR = _LIVE_ONLY_ROOT / "reflect_cycle"

LIVE_HITL_MATRIX_STUB_TAG = "stub-mailflow-live-hitl-mx-01"
LIVE_HITL_MATRIX_BODY_ANCHOR_LINE = "e2e_subagent_hitl_matrix full cycle body hitl-mx-01"
LIVE_HITL_MATRIX_STUB_DIR = _LIVE_ONLY_ROOT / "hitl_matrix"
LIVE_HITL_MATRIX_SUBJECT = "e2e subagent+HITL matrix live hitlmx01"

LIVE_CLI_ALLOW_STUB_TAG = "stub-mailflow-live-cli-allow-01"
LIVE_CLI_ALLOW_STUB_DIR = _LIVE_ONLY_ROOT / "cli_intent_allow_echo"
LIVE_CLI_ALLOW_SUBJECT = "e2e_cli_intent_allow live cli-allow-01"

LIVE_HITL_RESUME_NO_STUB_TAG = "stub-mailflow-live-hitl-mx-no-01"
LIVE_HITL_RESUME_NO_STUB_DIR = _LIVE_ONLY_ROOT / "hitl_matrix_resume_no"
E2E_HITL_RESUME_NO_BODY_SNIPPET = "e2e hitl resume no:"
LIVE_CLI_NOT_CONFIRMED_SUBJECT = "CLI command not confirmed"

def _two_turn_debug_route_wire(label: str, wire: str) -> None:
    """stderr: длина коррелятора; для b62-wire — декод :class:`IngressRouteB62Wire` (если доступен ``threlium``)."""
    if wire.strip().startswith("<"):
        t = wire[-36:] if len(wire) > 36 else wire
        log.debug(
            "two_turn_thread_root_correlator",
            label=label,
            wire_len=len(wire),
            wire_tail=t,
        )
        return
    summary: str
    try:
        from threlium.types import IngressRouteB62Wire  # noqa: PLC0415

        summary = repr(IngressRouteB62Wire.decode_b62_wire(wire))
    except Exception as exc:
        summary = f"<decode skipped or failed: {exc}>"
    tail = wire[-28:] if len(wire) > 28 else wire
    log.debug(
        "two_turn_route_wire",
        label=label,
        wire_len=len(wire),
        wire_tail=tail,
        decoded=summary,
    )


def _two_turn_debug_wiremock_contexts(wm_base: str, label: str) -> None:
    """stderr: список имён контекстов State Extension (GET ``/state-extension/contexts``)."""
    admin = wiremock_admin_base(wm_base).rstrip("/")
    url = f"{admin}/state-extension/contexts"
    try:
        r = requests.get(url, timeout=float(TIMEOUT_POLL_SHORT))
        r.raise_for_status()
        ctx = r.json()
    except Exception as exc:
        log.debug("two_turn_wm_contexts_get_failed", label=label, error=repr(exc))
        return
    if not isinstance(ctx, list):
        log.debug("two_turn_wm_contexts_bad_json", label=label, ctx_repr=repr(ctx)[:200])
        return
    log.debug("two_turn_wm_contexts", label=label, count=len(ctx))
    for i, name in enumerate(ctx):
        if isinstance(name, str):
            t = name[-36:] if len(name) > 36 else name
            log.debug(
                "two_turn_wm_context_entry",
                label=label,
                index=i,
                name_len=len(name),
                name_tail=t,
            )
        else:
            log.debug("two_turn_wm_context_entry", label=label, index=i, name=repr(name))


def _decode_subject(raw: str) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out: list[str] = []
    for text, enc in parts:
        if isinstance(text, bytes):
            out.append(text.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(text))
    return "".join(out)


_LIVE_KIND_TO_STUB: dict[str, tuple[Path, str]] = {
    "two_turn": (LIVE_TWO_TURN_STUB_DIR, LIVE_TWO_TURN_STUB_TAG),
    "subagent_shallow": (LIVE_SUBAGENT_SHALLOW_STUB_DIR, LIVE_SUBAGENT_SHALLOW_STUB_TAG),
    "subagent_budget_exhausted": (
        LIVE_SUBAGENT_BUDGET_EXHAUSTED_STUB_DIR,
        LIVE_SUBAGENT_BUDGET_EXHAUSTED_STUB_TAG,
    ),
    "memory_thread": (LIVE_MEMORY_THREAD_STUB_DIR, LIVE_MEMORY_THREAD_STUB_TAG),
    "global_memory": (LIVE_GLOBAL_MEMORY_STUB_DIR, LIVE_GLOBAL_MEMORY_STUB_TAG),
    "reflect_cycle": (LIVE_REFLECT_CYCLE_STUB_DIR, LIVE_REFLECT_CYCLE_STUB_TAG),
    "hitl_matrix": (LIVE_HITL_MATRIX_STUB_DIR, LIVE_HITL_MATRIX_STUB_TAG),
    "cli_intent_allow_echo": (LIVE_CLI_ALLOW_STUB_DIR, LIVE_CLI_ALLOW_STUB_TAG),
    "hitl_matrix_resume_no": (LIVE_HITL_RESUME_NO_STUB_DIR, LIVE_HITL_RESUME_NO_STUB_TAG),
}

def _live_prepare_wiremock(
    rt: E2EComposeRuntime, *, kind: str, correlation_key: str
) -> None:
    """Зарегистрировать статические стабы из репозитория (делегирует в ``prepare_wiremock_scenario``)."""
    try:
        stub_dir, stub_tag = _LIVE_KIND_TO_STUB[kind]
    except KeyError as e:
        raise ValueError(f"unknown live wiremock kind: {kind!r}") from e
    base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    prepare_wiremock_scenario(
        base,
        stub_dir=stub_dir,
        stub_tag=stub_tag,
        correlation_key=correlation_key,
    )


def _plain_body(msg) -> str:
    if msg.is_multipart():
        for p in msg.walk():
            if p.get_content_type() == "text/plain":
                pl = p.get_payload(decode=True)
                if isinstance(pl, bytes):
                    return pl.decode("utf-8", errors="replace")
                return str(pl or "")
    pl = msg.get_payload(decode=True)
    if isinstance(pl, bytes):
        return pl.decode("utf-8", errors="replace")
    return str(pl or "")


def _references_angle_inners(refs: str | None) -> list[str]:
    """Все inner ``Message-ID`` из заголовка ``References`` (порядок сохраняется)."""
    if not refs:
        return []
    return [m.group(1).strip() for m in re.finditer(r"<([^>]+)>", str(refs))]


def _greenmail_row_is_agent_mail_in_root_thread(
    *,
    root_inner: str,
    in_reply_to: str | None,
    references: str | None,
) -> bool:
    """Тот же внешний тред, что и корневое письмо пользователя (GreenMail): по IRT либо по References.

    Сначала прямой ответ на корень (первый MID в ``In-Reply-To`` == ``user_mid``). Если агент отвечает
    на промежуточное письмо — корень остаётся в ``References`` (сложный случай).

    ``egress_email`` ставит на SMTP ``In-Reply-To`` = сырой ``reply_target`` из маршрута (см. код стадии);
    это тот же inner, что в SMTP-инъекции теста — сравнение с ``root_inner`` без канонизации.
    """
    ri = root_inner.strip().lower()
    irt_first = rfc_first_message_id_in_in_reply_to_header(in_reply_to)
    if irt_first is not None and irt_first.lower() == ri:
        return True
    if irt_first is None or irt_first.lower() != ri:
        for inner in _references_angle_inners(references):
            if inner.lower() == ri:
                return True
    return False


def _pytest_inbox_max_uid(host: str, port: int, *, user: str, password: str) -> int:
    """Максимальный UID в INBOX — дешёвый baseline для UID-фильтрации."""
    with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
        imap.login(user, password)
        imap.select("INBOX")
        _, data = imap.uid("search", None, "ALL")
        uids = data[0].split() if data and data[0] else []
        imap.logout()
    return int(uids[-1]) if uids else 0


def _pytest_inbox_rows(
    host: str,
    port: int,
    *,
    user: str,
    password: str,
    since_uid: int = 0,
    subject: str = "",
    in_reply_to: str = "",
    in_thread_of: str = "",
) -> list[tuple[str, str, str, str, str, str]]:
    """Сообщения (uid, Message-ID, Subject, In-Reply-To, References, plain body).

    Серверная фильтрация через IMAP SEARCH — O(1) вместо O(всех сообщений).

    *since_uid*    — только UID > since_uid.
    *subject*      — ``SUBJECT "<val>"``.
    *in_reply_to*  — ``HEADER In-Reply-To "<val>"``.
    *in_thread_of* — ``OR HEADER In-Reply-To "<val>" HEADER References "<val>"``
                     (взаимоисключает *in_reply_to*).
    """
    parts: list[str] = []
    if since_uid > 0:
        parts.append(f"UID {since_uid + 1}:*")
    if subject:
        parts.append(f'SUBJECT "{subject}"')
    if in_reply_to:
        parts.append(f'HEADER In-Reply-To "{in_reply_to.strip().strip("<>")}"')
    elif in_thread_of:
        clean = in_thread_of.strip().strip("<>")
        parts.append(f'OR HEADER In-Reply-To "{clean}" HEADER References "{clean}"')
    criteria = " ".join(parts) if parts else "ALL"

    with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
        imap.login(user, password)
        imap.select("INBOX")
        _, data = imap.uid("search", None, criteria)
        uids = data[0].split() if data and data[0] else []
        rows: list[tuple[str, str, str, str, str, str]] = []
        for uid in uids:
            _, raw_data = imap.uid("fetch", uid, "(RFC822)")
            if not raw_data or not isinstance(raw_data[0], tuple):
                continue
            msg = e2e_parse_rfc822(raw_data[0][1])
            rows.append(
                (
                    uid.decode("ascii") if isinstance(uid, bytes) else str(uid),
                    (msg.get("Message-ID") or "").strip(),
                    _decode_subject(msg.get("Subject") or ""),
                    (msg.get("In-Reply-To") or "").strip(),
                    (msg.get("References") or "").strip(),
                    _plain_body(msg),
                )
            )
        imap.logout()
    return rows


def _smtp_send(host: str, port: int, msg: EmailMessage) -> None:
    e2e_smtp_send(host, port, msg, timeout=float(TIMEOUT_POLL_SHORT))


def _debug_greenmail_smtp_payload(label: str, msg: EmailMessage) -> None:
    """Что pytest реально передаёт в GreenMail по SMTP: число физических строк In-Reply-To и блок заголовков."""
    irt = msg.get_all("In-Reply-To") or []
    refs = msg.get_all("References") or []
    log.debug(
        "greenmail_smtp_in_reply_to",
        label=label,
        count=len(irt),
        values=irt,
    )
    log.debug(
        "greenmail_smtp_references",
        label=label,
        count=len(refs),
        values=refs,
    )
    raw = e2e_serialize_rfc822(msg)
    head, sep, _rest = raw.partition(b"\r\n\r\n")
    if not sep:
        head, sep, _rest = raw.partition(b"\n\n")
    text = head.decode("utf-8", errors="replace")
    n_irt_lines = sum(
        1 for line in text.splitlines() if line.lower().startswith("in-reply-to:")
    )
    log.debug(
        "greenmail_smtp_in_reply_to_lines",
        label=label,
        physical_lines=n_irt_lines,
    )
    log.debug(
        "greenmail_smtp_headers",
        label=label,
        headers=clip_log_body(text, max_len=4000),
    )


def test_live_mailflow_two_turn_dialog_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """1) пользователь пишет → 2) ответ агента в pytest@ → 3) ответ пользователя в тред → 4) второй ответ агента с IRT на шаг 3."""
    rt = e2e_runtime
    # Уникальный MID на прогон (параллельные pytest и повторные запуски).
    user1_mid = f"e2e-live-turn1-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user1_mid)
    # Второе письмо в том же треде: ``resolve_route_from_thread_oldest_route_tag`` всегда даёт
    # ``Message-ID`` старейшего с ``tag:route`` — это первый пользовательский вход (user1), не user2.
    user2_mid = f"e2e-live-turn2-{uuid.uuid4().hex}@localhost"
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="two_turn", correlation_key=correlation_key)
        log.debug(
            "two_turn_setup",
            wm_base=wm_base,
            user1_mid=user1_mid,
            user2_mid=user2_mid,
        )
        _two_turn_debug_route_wire("thread-root correlator (pytest)", correlation_key)
        _two_turn_debug_wiremock_contexts(wm_base, "after seed (prepare)")
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj1 = LIVE_TWO_TURN_SUBJECT1
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m1 = EmailMessage()
        m1["From"] = from_addr
        m1["To"] = to_addr
        m1["Subject"] = subj1
        m1["Message-ID"] = f"<{user1_mid}>"
        m1.set_content(
            e2e_dense_threlium_ctx_body(
                head="User first body e2e-live-tw-anchor-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m1)
        _two_turn_debug_wiremock_contexts(wm_base, "after SMTP m1 (bridge may not have run yet)")

        u1i = user1_mid.strip().strip("<>")

        def _first_reply() -> tuple[str, str, str, str] | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=u1i,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, msg_id, subj, irt, _refs, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return msg_id, subj, irt, body
            return None

        r1 = poll_until(_first_reply, timeout=TIMEOUT_POLL_SHORT, desc="live mailflow: first agent reply in pytest@ INBOX")
        assert r1 is not None
        agent1_mid, _, agent1_irt, _ = r1
        assert user1_mid in (agent1_irt or "").replace("<", "").replace(">", "")

        subj2 = LIVE_TWO_TURN_SUBJECT2
        irt = agent1_mid.strip()
        if not irt.startswith("<"):
            irt = f"<{irt.strip('<>')}>"

        baseline_uid2 = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m2 = EmailMessage()
        m2["From"] = from_addr
        m2["To"] = to_addr
        m2["Subject"] = subj2
        m2["Message-ID"] = f"<{user2_mid}>"
        m2["In-Reply-To"] = irt
        m2["References"] = irt
        m2.set_content(
            e2e_dense_threlium_ctx_body(
                head="User second body e2e-live-tw-anchor-01 partb",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m2)

        u2i = user2_mid.strip().strip("<>")

        def _second_reply() -> tuple[str, str, str, str] | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid2, subject="", in_reply_to=u2i,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, msg_id, subj, irt2, _refs, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return msg_id, subj, irt2, body
            return None

        r2 = poll_until(_second_reply, timeout=TIMEOUT_POLL_SHORT, desc="live mailflow: second agent reply in pytest@ INBOX")
        assert r2 is not None
        _, _, agent2_irt, _ = r2
        inner_u2 = user2_mid.strip().strip("<>")
        assert inner_u2.lower() in (agent2_irt or "").lower(), (
            f"expected In-Reply-To to reference user follow-up {inner_u2!r}, got {agent2_irt!r}"
        )
    finally:
        # Не удалять контекст WM здесь—хук unmatched после return; иначе поздний LiteLLM без State.
        assert_wiremock_zero_unmatched_requests(wm_base)




def test_live_subagent_table_shallow_chain_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """SUBAGENT_TABLE (усечённо): L0 → subagent_intent → L1 → egress_router (POP) → ответ в pytest@.

    Маркер Subject = ``E2E_SUBAGENT_TABLE_LIVE_SUBJECT_MARKER`` (тот же wire, что в ``reference_l0/threlium_e2e_l0.py``).
    """
    rt = e2e_runtime
    user_mid = f"e2e-sat-live-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="subagent_shallow", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj = LIVE_SUBAGENT_SHALLOW_SUBJECT
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="SUBAGENT_TABLE shallow chain body sat-shallow-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_LIVE_MAIL,
            desc="live SUBAGENT_TABLE: agent reply after nested frame",
        )
        # subagent-поток по STATE (без docker-exec notmuch): POP-результат дочернего L1-фрейма
        # ('e2e L1 subagent frame result (POP to L0)') вернулся в L0 reasoning-промпт после subagent_end —
        # content-flag saw_subagent_result на post-subagent reasoning-стабе, строго СИЛЬНЕЕ «SUBAGENT_END-папка
        # существует» (доказывает, что результат субагента вернулся в родительский контур). Прямое чтение
        # после ответа GreenMail (контур завершён) — time-independent; маршрут enforced unmatched-guard.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_subagent_result") == "1"
        ), "L1 subagent POP result must reach L0 reasoning prompt (state saw_subagent_result)"
    finally:
        # Контекст WM не удалять здесь — см. two_turn finally.
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_subagent_budget_exhausted_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """L1 ``subagent_intent`` when hop budget exhausted → enrich notice (``budget_exhausted.j2``)."""
    rt = e2e_runtime
    user_mid = f"e2e-sat-budget-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        e2e_refresh_hop_budget_sub(rt.project_name, budget_sub=4, repo_root=REPO_ROOT)
        _live_prepare_wiremock(rt, kind="subagent_budget_exhausted", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj = LIVE_SUBAGENT_BUDGET_EXHAUSTED_SUBJECT
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="SUBAGENT_TABLE budget exhausted body sat-budget-exhausted-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_LIVE_MAIL,
            desc="live SUBAGENT_TABLE: agent reply after budget exhausted nested frame",
        )
        # budget-exhausted-поток по STATE (без journal-скана и docker-exec notmuch): notice enrich_fast
        # ('hop-budget for this thread is exhausted') долетел до L1 reasoning-промпта после блока бюджета —
        # content-flag saw_budget_exhausted_notice на L1-стабе (104), та же семантика, что прежний
        # journal find_wiremock_requests_by_body_contains, но дёшево из state. Прямое чтение после ответа
        # GreenMail (контур завершён) — time-independent; маршрут enforced unmatched-guard.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_budget_exhausted_notice")
            == "1"
        ), "budget exhausted notice must reach L1 reasoning prompt (state saw_budget_exhausted_notice)"
    finally:
        e2e_refresh_hop_budget_default(rt.project_name, repo_root=REPO_ROOT)
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_memory_table_thread_memory_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """Документ ``MEMORY_TABLE.md`` §1 (локальная память): L0 → ``thread_memory`` → … → ответ пользователю.

    Маркер Subject — ``E2E_MEMORY_THREAD_LIVE_SUBJECT_MARKER``; корневой ``Message-ID`` с префиксом
    ``E2E_MEMORY_THREAD_LIVE_MSGID_PREFIX``. WireMock: первый вызов reasoning — ``thread_memory``, второй —
    ``egress_router`` с дефолтным L0 ответом.
    """
    rt = e2e_runtime
    user_mid = f"e2e-mem-tm-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="memory_thread", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj = LIVE_MEMORY_THREAD_SUBJECT
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="MEMORY_TABLE §1 live body mem-thread-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_SHORT,
            desc="live MEMORY_TABLE: agent e2e reply in pytest@ INBOX",
        )
        # thread_memory-поток по STATE (без docker-exec notmuch): персистнутая локальная заметка
        # ('e2e live thread_memory note') вернулась в reasoning-промпт после стадии thread_memory —
        # content-flag saw_thread_memory_note на post-tm reasoning-стабе, строго СИЛЬНЕЕ «THREAD_MEMORY-папка
        # существует» (доказывает, что заметка вернулась в контур, а не только что стадия отметилась).
        # Маршрут ingress→thread_memory→reasoning→finalize→egress→archive enforced фазовыми стабами +
        # unmatched-guard + ответным письмом выше. Прямое чтение после ответа GreenMail — time-independent.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_thread_memory_note") == "1"
        ), "thread_memory note must reach reasoning prompt (state saw_thread_memory_note)"
    finally:
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_memory_table_global_memory_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """MEMORY_TABLE §2 (глобальная память): L0 → ``global_memory`` → … → ответ пользователю."""
    rt = e2e_runtime
    user_mid = f"e2e-global-mem-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="global_memory", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj = LIVE_GLOBAL_MEMORY_SUBJECT
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="MEMORY_TABLE §2 live body global-mem-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_SHORT,
            desc="live MEMORY_TABLE §2: agent e2e reply in pytest@ INBOX",
        )
        # global_memory-поток по STATE (без docker-exec notmuch): персистнутая кросс-тред заметка
        # ('e2e live global_memory note') вернулась в reasoning-промпт после стадии global_memory —
        # content-flag saw_global_memory_note, строго СИЛЬНЕЕ «GLOBAL_MEMORY-папка существует». Прямое
        # чтение после ответа GreenMail (контур завершён) — time-independent; маршрут enforced unmatched-guard.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_global_memory_note") == "1"
        ), "global_memory note must reach reasoning prompt (state saw_global_memory_note)"
    finally:
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_reflect_then_egress_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """MEMORY_TABLE §3: первый hop reasoning → ``reflect``, второй → ``egress_router`` → ответ."""
    rt = e2e_runtime
    user_mid = f"e2e-reflect-cyc-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="reflect_cycle", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        subj = LIVE_REFLECT_CYCLE_SUBJECT
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="MEMORY_TABLE §3 live body reflect-cyc-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, subject="", in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_SHORT,
            desc="live MEMORY_TABLE §3: agent e2e reply in pytest@ INBOX",
        )
        # reflect-поток по STATE (без docker-exec notmuch): вывод reflect ('e2e live reflect summary')
        # вернулся во второй reasoning-hop после стадии reflect — content-flag saw_reflect_summary, строго
        # СИЛЬНЕЕ «REFLECT-папка существует» (доказывает, что reflect-итог вернулся в контур). Прямое чтение
        # после ответа GreenMail (контур завершён) — time-independent; маршрут enforced unmatched-guard.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_reflect_summary") == "1"
        ), "reflect summary must reach the second reasoning hop (state saw_reflect_summary)"
    finally:
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_subagent_hitl_matrix_full_cycle_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """Полный путь ``docs/SUBAGENT_TABLE.md`` (L0→L1→L2, HITL на L2): WireMock даёт L1→subagent, L2→cli_intent.

    Маркер тела ``E2E_SUBAGENT_HITL_MATRIX_BODY_MARKER``, корневой ``Message-ID`` с префиксом
    ``E2E_SUBAGENT_HITL_MATRIX_LIVE_MSGID_PREFIX``. Далее: HITL-письмо в pytest@, ``yes``, финальный e2e reply.
    """
    rt = e2e_runtime
    user_mid = f"e2e-hitl-mx-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        # После budget-exhausted теста в threlium.yaml может остаться ``budget_sub: 4``.
        e2e_refresh_hop_budget_default(rt.project_name, repo_root=REPO_ROOT)
        _live_prepare_wiremock(rt, kind="hitl_matrix", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        run_tag = uuid.uuid4().hex[:12]
        subj = f"{LIVE_HITL_MATRIX_SUBJECT} [{run_tag}]"
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head=(
                    f"{LIVE_HITL_MATRIX_BODY_ANCHOR_LINE}\n"
                    "Expect CLI HITL then final reply."
                ),
                correlation_key=correlation_key,
            )
        )
        _debug_greenmail_smtp_payload("hitl_matrix_root", m)
        _smtp_send(smtp_h, smtp_p, m)

        root_inner = user_mid.strip().strip("<>")

        def _hitl_mail() -> tuple[str, str, str, str] | None:
            """Новейшее в pytest@ письмо в треде корня: IRT на корень или корень в References (GreenMail)."""
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid, in_thread_of=root_inner,
            )
            if not rows:
                return None
            for _imap_id, msg_id, subj_l, irt, refs, _body in reversed(rows):
                if not _greenmail_row_is_agent_mail_in_root_thread(
                    root_inner=root_inner,
                    in_reply_to=irt,
                    references=refs,
                ):
                    continue
                return msg_id, subj_l, irt, refs
            return None

        hitl = poll_until(
            _hitl_mail,
            timeout=TIMEOUT_POLL_LIVE_MAIL,
            interval=1.5,
            desc="live SUBAGENT+HITL: HITL confirm mail in pytest@ INBOX",
        )
        assert hitl is not None
        hitl_mid_raw, hitl_subj, _hitl_irt_imap, hitl_refs_imap = hitl
        hitl_mid = hitl_mid_raw.strip()
        if not hitl_mid.startswith("<"):
            hitl_mid = f"<{hitl_mid.strip('<>')}>"

        baseline_uid_after_hitl = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        yes_mid = f"e2e-hitl-yes-{uuid.uuid4().hex}@localhost"
        # LiteLLM / FSM держат ``X-Threlium-Route`` от корня треда (oldest route tag), не от ``yes_mid``.
        # Пересидить State по yes_mid — сломать матчеры WireMock на фазе после ``cli_resume``.

        m_yes = EmailMessage()
        m_yes["From"] = from_addr
        m_yes["To"] = to_addr
        # Тот же ``Re:``, что ожидает цепочка ответа на фактический Subject HITL (не фиктивный маркер).
        m_yes["Subject"] = f"Re: {hitl_subj.strip()}"
        m_yes["Message-ID"] = f"<{yes_mid}>"
        # Явный родитель по RFC (ответ на HITL); только ``References`` без IRT — нетипично для клиента.
        m_yes["In-Reply-To"] = hitl_mid
        # References: как в IMAP у HITL (цепочка исходящих), иначе только корень + HITL (inner как в инъекции).
        _hr = (hitl_refs_imap or "").strip()
        m_yes["References"] = (
            f"{_hr} {hitl_mid}".strip() if _hr else f"<{user_mid}> {hitl_mid}"
        )
        m_yes.set_content(
            e2e_dense_threlium_ctx_body(head="yes", correlation_key=correlation_key)
        )
        _debug_greenmail_smtp_payload("hitl_matrix_yes", m_yes)
        _smtp_send(smtp_h, smtp_p, m_yes)

        yes_inner = yes_mid.strip().strip("<>")

        def _final_agent_reply() -> tuple[str, str] | None:
            """Финальный ответ агента после HITL «yes»: новое письмо с маркерами e2e в корневом треде.

            Ожидается ``In-Reply-To`` на письмо пользователя ``yes`` (``yes_inner``), когда цепочка
            IRT восстановлена через subagent_end (см. SUBAGENT_TABLE). Отбор: корневой тред, маркеры
            тела/темы, не само HITL-письмо.
            """
            rows = _pytest_inbox_rows(
                imap_h, imap_p, user=imap_user, password=imap_pass,
                since_uid=baseline_uid_after_hitl, subject="",
                in_thread_of=root_inner,
            )
            if not rows:
                return None
            for _imap_id, msg_id, subj_l, irt, refs, body in reversed(rows):
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                mid_strip = msg_id.strip().strip("<>").lower()
                hitl_mid_strip = hitl_mid.strip().strip("<>").lower()
                if mid_strip == hitl_mid_strip:
                    continue
                return irt, body
            return None

        fin = poll_until(
            _final_agent_reply,
            timeout=TIMEOUT_POLL_LIVE_MAIL,
            interval=1.5,
            desc="live SUBAGENT+HITL: final agent e2e reply after user yes",
        )
        assert fin is not None
        agent_irt, _ = fin
        assert yes_inner.lower() in (agent_irt or "").lower(), (
            f"expected final reply In-Reply-To to reference user yes message {yes_inner!r}, got {agent_irt!r}"
        )
        # HITL+cli-поток по STATE (без docker-exec notmuch): после подтверждения пользователем ('yes')
        # привилегированная cli-команда (echo 'e2e-hitl-cli-xyzzy') выполнилась в cli_exec и её вывод
        # вернулся в reasoning-промпт — content-flag saw_hitl_cli_echo на post-cli reasoning-стабе, строго
        # СИЛЬНЕЕ «CLI_EXEC/CLI_RESUME-папки существуют» (доказывает, что HITL-resume провёл cli и результат
        # вернулся в контур). Прямое чтение после финального ответа GreenMail (выше) — time-independent;
        # маршрут SUBAGENT→CLI_INTENT→HITL_OUT→RESUME→CLI_EXEC→reasoning→egress enforced фазовыми стабами +
        # unmatched-guard; финальный IRT на письмо 'yes' проверен выше.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_hitl_cli_echo") == "1"
        ), "post-HITL cli_exec echo output must reach reasoning (state saw_hitl_cli_echo)"
    finally:
        # Контекст WM не удалять здесь — см. two_turn finally.
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_cli_intent_allow_echo_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """``cli_intent`` allow (``echo``) → ``cli_exec`` → … → ``egress_email``."""
    rt = e2e_runtime
    user_mid = f"e2e-cli-allow-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="cli_intent_allow_echo", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = LIVE_CLI_ALLOW_SUBJECT
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head="e2e live cli_intent allow echo body cli-allow-01",
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        uinner = user_mid.strip().strip("<>")

        def _agent_reply() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h,
                imap_p,
                user=imap_user,
                password=imap_pass,
                since_uid=baseline_uid,
                subject="",
                in_reply_to=uinner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _, _, _, _, body = row
                if E2E_REPLY_BODY_SNIPPET.lower() not in body.lower():
                    continue
                return True
            return None

        poll_until(
            _agent_reply,
            timeout=TIMEOUT_POLL_SHORT,
            desc="live cli_intent allow echo: agent e2e reply in pytest@ INBOX",
        )
        # cli-поток по STATE (без docker-exec notmuch): эхо-вывод cli_exec ('e2e-cli-allow-xyzzy') дошёл до
        # reasoning — content-flag saw_cli_echo на post-cli reasoning-стабе, строго СИЛЬНЕЕ «CLI_EXEC-папка
        # существует» (доказывает, что вывод вернулся в контур, а не только что стадия отметилась).
        # Маршрут ingress→enrich→reasoning→cli_intent→cli_exec→finalize→egress→archive enforced фазовыми
        # стабами (egress gated hasProperty round1_ledger_done) + unmatched-guard + ответным письмом выше.
        # Прямое чтение: ассерт после ответа GreenMail (контур завершён) — time-independent.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_cli_echo") == "1"
        ), "cli_exec echo output must reach reasoning (state saw_cli_echo)"
    finally:
        assert_wiremock_zero_unmatched_requests(wm_base)


def test_live_hitl_user_rejects_cli_on_running_stack(e2e_runtime: E2EComposeRuntime) -> None:
    """HITL до ``cli_intent``; пользователь отвечает ``no`` → без ``cli_exec``; письмо с отказом."""
    rt = e2e_runtime
    user_mid = f"e2e-hitl-no-{uuid.uuid4().hex}@localhost"
    correlation_key = e2e_thread_root_mid_for_message_id(user_mid)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    try:
        _live_prepare_wiremock(rt, kind="hitl_matrix_resume_no", correlation_key=correlation_key)
        smtp_h, smtp_p = rt.greenmail_smtp_host, rt.greenmail_smtp_port
        imap_h, imap_p = rt.greenmail_imap_host, rt.greenmail_imap_port
        to_addr = e2e_greenmail_mailbox_address(E2E_FETCHMAIL_USER)
        from_addr = e2e_greenmail_mailbox_address("pytest")
        imap_user = E2E_GREENMAIL_REPLY_USER
        imap_pass = E2E_FETCHMAIL_PASS

        run_tag = uuid.uuid4().hex[:12]
        subj = f"{LIVE_HITL_MATRIX_SUBJECT} [{run_tag}]"
        baseline_uid = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        m = EmailMessage()
        m["From"] = from_addr
        m["To"] = to_addr
        m["Subject"] = subj
        m["Message-ID"] = f"<{user_mid}>"
        m.set_content(
            e2e_dense_threlium_ctx_body(
                head=(
                    f"{LIVE_HITL_MATRIX_BODY_ANCHOR_LINE}\n"
                    "Expect CLI HITL then user decline (no)."
                ),
                correlation_key=correlation_key,
            )
        )
        _smtp_send(smtp_h, smtp_p, m)

        root_inner = user_mid.strip().strip("<>")

        def _hitl_mail() -> tuple[str, str, str, str] | None:
            rows = _pytest_inbox_rows(
                imap_h,
                imap_p,
                user=imap_user,
                password=imap_pass,
                since_uid=baseline_uid,
                in_thread_of=root_inner,
            )
            if not rows:
                return None
            for _imap_id, msg_id, subj_l, irt, refs, _body in reversed(rows):
                if not _greenmail_row_is_agent_mail_in_root_thread(
                    root_inner=root_inner,
                    in_reply_to=irt,
                    references=refs,
                ):
                    continue
                return msg_id, subj_l, irt, refs
            return None

        hitl = poll_until(
            _hitl_mail,
            timeout=TIMEOUT_POLL_SHORT,
            interval=1.5,
            desc="live HITL resume no: HITL confirm mail in pytest@ INBOX",
        )
        assert hitl is not None
        hitl_mid_raw, hitl_subj, _hitl_irt_imap, hitl_refs_imap = hitl
        hitl_mid = hitl_mid_raw.strip()
        if not hitl_mid.startswith("<"):
            hitl_mid = f"<{hitl_mid.strip('<>')}>"

        baseline_uid_after_hitl = _pytest_inbox_max_uid(imap_h, imap_p, user=imap_user, password=imap_pass)

        no_mid = f"e2e-hitl-no-reply-{uuid.uuid4().hex}@localhost"
        m_no = EmailMessage()
        m_no["From"] = from_addr
        m_no["To"] = to_addr
        m_no["Subject"] = f"Re: {hitl_subj.strip()}"
        m_no["Message-ID"] = f"<{no_mid}>"
        m_no["In-Reply-To"] = hitl_mid
        _hr = (hitl_refs_imap or "").strip()
        m_no["References"] = (
            f"{_hr} {hitl_mid}".strip() if _hr else f"<{user_mid}> {hitl_mid}"
        )
        m_no.set_content(
            e2e_dense_threlium_ctx_body(head="no", correlation_key=correlation_key)
        )
        _smtp_send(smtp_h, smtp_p, m_no)

        def _decline_notice_imap() -> bool | None:
            rows = _pytest_inbox_rows(
                imap_h,
                imap_p,
                user=imap_user,
                password=imap_pass,
                since_uid=baseline_uid_after_hitl,
                in_thread_of=root_inner,
            )
            if not rows:
                return None
            for row in reversed(rows):
                _, _msg_id, _subj_l, _irt, _refs, body = row
                body_l = body.lower()
                if E2E_HITL_RESUME_NO_BODY_SNIPPET.lower() not in body_l and (
                    "user did not confirm the cli command" not in body_l
                ):
                    continue
                return True
            return None

        poll_until(
            _decline_notice_imap,
            timeout=TIMEOUT_POLL_SHORT,
            interval=1.0,
            desc="live HITL resume no: decline reply in pytest@ INBOX (GreenMail)",
        )

        # HITL-reject-поток по STATE (без docker-exec notmuch): после ответа пользователя 'no' cli_exec НЕ
        # запускался; enrich_fast сформировал decline-notice ('user did not confirm the CLI command'), и она
        # вернулась в reasoning-промпт после reject — content-flag saw_decline_notice на post-reject
        # reasoning-стабе (072), строго СИЛЬНЕЕ прежних «ENRICH_FAST-папка содержит токен» + «папки стадий
        # существуют» (доказывает, что отказ долетел до контура, а не только осел в Maildir). Прямое чтение
        # после decline-ответа GreenMail (выше) — time-independent; маршрут enforced фазовыми стабами +
        # unmatched-guard (route-wire в finally).
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_decline_notice") == "1"
        ), "HITL decline notice must reach reasoning prompt (state saw_decline_notice)"
    finally:
        assert_wiremock_zero_unmatched_requests(
            wm_base,
            x_threlium_route_wire=correlation_key,
        )
