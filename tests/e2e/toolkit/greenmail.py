"""GreenMail IMAP/SMTP waits (host-first)."""
from __future__ import annotations

import email
import imaplib
import re
import shlex
import smtplib
import time
import uuid
from email.header import decode_header
from email.message import EmailMessage
from pathlib import Path
from typing import Any

from tests.e2e.log import clip_log_body, log
from tests.e2e.mail_wire import e2e_parse_rfc822, e2e_smtp_send

from .bridges.email import (
    e2e_thread_root_mid_for_message_id,
    notmuch_id_search_term,
    rfc_first_message_id_in_in_reply_to_header,
)
from .constants import (
    E2E_FETCHMAIL_PASS,
    E2E_FETCHMAIL_USER,
    E2E_GREENMAIL_READINESS_PROBE_FROM,
    E2E_GREENMAIL_REPLY_USER,
    E2E_IMAP_PROCESSED_FOLDER,
    E2E_REPLY_BODY,
    E2E_REPLY_BODY_SNIPPET,
    E2E_REPLY_SUBJECT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .poll import poll_until, poll_until_backoff
from .remote_boot import REMOTE_PROBE_LOGGER_BOOT, REMOTE_RFC822_PARSER_BOOT
from .runtime import _compose_container, _mapped_port, discover_runtime, service_exec

def _decoded_email_subject(msg: Any) -> str:
    """Subject из заголовка письма в виде строки (RFC 2047 decode)."""
    raw = msg.get("Subject") if hasattr(msg, "get") else ""
    if raw is None:
        return ""
    if not isinstance(raw, str):
        raw = str(raw)
    parts = decode_header(raw)
    out: list[str] = []
    for text, enc in parts:
        if isinstance(text, bytes):
            out.append(text.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(str(text))
    return "".join(out)


def e2e_greenmail_mailbox_address(local_part_or_address: str) -> str:
    """RFC5322-адрес ящика для SMTP/IMAP к GreenMail e2e (``GREENMAIL_OPTS``: ``user:secret@localhost``, …)."""
    s = (local_part_or_address or "").strip()
    if not s:
        raise ValueError("e2e_greenmail_mailbox_address: empty")
    if "@" in s:
        return s
    return f"{s}@localhost"


def _greenmail_imap_expunge_folder(imap: imaplib.IMAP4, folder: str) -> int:
    """Удалить все письма в ``folder`` (``\\Deleted`` + ``EXPUNGE``). Папки нет → 0."""
    typ, _ = imap.select(folder)
    if typ != "OK":
        return 0
    _, data = imap.search(None, "ALL")
    uids = data[0].split() if data and data[0] else []
    for uid in uids:
        imap.store(uid, "+FLAGS", "\\Deleted")
    if uids:
        imap.expunge()
    return len(uids)


def wait_for_greenmail_ready(project_name: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> tuple[str, int]:
    host, port = _mapped_port(project_name, "greenmail", 3025)

    def _probe() -> tuple[str, int] | None:
        with smtplib.SMTP(host=host, port=port, timeout=int(TIMEOUT_POLL_SHORT)) as smtp:
            code, _ = smtp.ehlo()
        return (host, port) if 200 <= code < 400 else None

    return poll_until_backoff(_probe, timeout=timeout, desc=f"greenmail SMTP ready {host}:{port}")


def wait_for_greenmail_inbox_message_host(
    host: str,
    port: int,
    *,
    user: str = E2E_FETCHMAIL_USER,
    password: str = E2E_FETCHMAIL_PASS,
    message_id: str | None = None,
    subject: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Ожидает появление письма в INBOX GreenMail через host-side IMAP."""

    def _probe() -> bool | None:
        with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
            imap.login(user, password)
            _, data = imap.select("INBOX")
            count = int((data[0] or b"0").decode("utf-8"))
            if count <= 0:
                imap.logout()
                return None
            if not message_id and not subject:
                imap.logout()
                return True

            _, ids_data = imap.search(None, "ALL")
            ids = ids_data[0].split() if ids_data and ids_data[0] else []
            for msg_id in ids:
                _, raw_data = imap.fetch(msg_id, "(BODY.PEEK[HEADER])")
                cell = raw_data[0] if raw_data and raw_data[0] else None
                if not isinstance(cell, tuple) or len(cell) < 2:
                    continue
                raw_header = cell[1]
                if not isinstance(raw_header, (bytes, bytearray)):
                    continue
                msg = e2e_parse_rfc822(raw_header)
                if message_id and msg.get("Message-ID", "").strip("<>") != message_id.strip("<>"):
                    continue
                if subject and _decoded_email_subject(msg) != subject:
                    continue
                imap.logout()
                return True
            imap.logout()
            return None

    poll_until_backoff(_probe, timeout=timeout, desc=f"greenmail host IMAP inbox message on {host}:{port}")


def wait_for_greenmail_inbox_message_seen_host(
    host: str,
    port: int,
    *,
    user: str = E2E_FETCHMAIL_USER,
    password: str = E2E_FETCHMAIL_PASS,
    message_id: str | None = None,
    subject: str | None = None,
    timeout: float | None = None,
) -> None:
    """Ждёт письмо с якорями в INBOX GreenMail с флагом ``\\Seen``.

    Письмо остаётся на сервере; бридж после FETCH обычно выставляет ``\\Seen`` —
    это подтверждает забор с control node (проброшенный IMAP), без гонки «UNSEEN до probe».

    Не используйте при включённом ``bridges.email.imap_processed_folder`` (UID MOVE):
    обработанное письмо уходит из INBOX и здесь никогда не найдётся — берите
    :func:`wait_for_greenmail_inbox_message_gone_host`.
    """
    if timeout is None:
        timeout = TIMEOUT_POLL_SHORT

    def _imap_response_has_seen(flag_dat: list | None) -> bool:
        if not flag_dat:
            return False
        for item in flag_dat:
            if isinstance(item, bytes) and b"\\Seen" in item:
                return True
            if isinstance(item, tuple):
                for x in item:
                    if isinstance(x, bytes) and b"\\Seen" in x:
                        return True
        return False

    def _probe() -> bool | None:
        with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
            imap.login(user, password)
            imap.select("INBOX")
            _, ids_data = imap.search(None, "ALL")
            ids = ids_data[0].split() if ids_data and ids_data[0] else []
            for msg_uid in ids:
                _, raw_data = imap.fetch(msg_uid, "(BODY.PEEK[HEADER])")
                cell = raw_data[0] if raw_data and raw_data[0] else None
                if not isinstance(cell, tuple) or len(cell) < 2:
                    continue
                raw_header = cell[1]
                if not isinstance(raw_header, (bytes, bytearray)):
                    continue
                msg = e2e_parse_rfc822(raw_header)
                if message_id and msg.get("Message-ID", "").strip("<>") != message_id.strip("<>"):
                    continue
                if subject and _decoded_email_subject(msg) != subject:
                    continue
                _, flag_dat = imap.fetch(msg_uid, "(FLAGS)")
                imap.logout()
                if _imap_response_has_seen(flag_dat):
                    return True
                return None
            imap.logout()
            return None

    anchor = ""
    if message_id:
        anchor += f" mid={message_id!r}"
    if subject:
        anchor += f" subj={subject!r}"
    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc=f"greenmail host IMAP message Seen (bridge pickup){anchor} on {host}:{port}",
    )


def wait_for_greenmail_inbox_message_gone_host(
    host: str,
    port: int,
    *,
    user: str = E2E_FETCHMAIL_USER,
    password: str = E2E_FETCHMAIL_PASS,
    message_id: str | None = None,
    subject: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Ожидает **обработку** письма в INBOX GreenMail (host-side IMAP).

    IMAP bridge в SUT помечает обработанные письма ``\\Seen``,
    поэтому ищем среди UNSEEN — когда письмо пропало из UNSEEN, bridge его обработал.
    """

    def _probe() -> bool | None:
        with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
            imap.login(user, password)
            imap.select("INBOX")

            _, ids_data = imap.search(None, "UNSEEN")
            ids = ids_data[0].split() if ids_data and ids_data[0] else []
            if not ids:
                imap.logout()
                return True
            if not message_id and not subject:
                imap.logout()
                return None

            for msg_id in ids:
                _, raw_data = imap.fetch(msg_id, "(BODY.PEEK[HEADER])")
                cell = raw_data[0] if raw_data and raw_data[0] else None
                if not isinstance(cell, tuple) or len(cell) < 2:
                    continue
                raw_header = cell[1]
                if not isinstance(raw_header, (bytes, bytearray)):
                    continue
                msg = e2e_parse_rfc822(raw_header)
                if message_id and msg.get("Message-ID", "").strip("<>") != message_id.strip("<>"):
                    continue
                if subject and _decoded_email_subject(msg) != subject:
                    continue
                imap.logout()
                return None
            imap.logout()
            return True

    poll_until_backoff(_probe, timeout=timeout, desc=f"greenmail host IMAP inbox message gone on {host}:{port}")


def run_greenmail_host_readiness_probe(
    project_name: str,
    *,
    smtp_timeout: float = TIMEOUT_POLL_SHORT,
    imap_timeout: float | None = None,
    wiremock_seed_base: str | None = None,
    through_agent_mailbox: bool = False,
) -> str:
    """Проверка GreenMail с хоста: SMTP → доставка в INBOX по IMAP.

    По умолчанию (**``through_agent_mailbox=False``**) письмо уходит на отдельный тестовый ящик
    ``E2E_GREENMAIL_REPLY_USER`` (в compose: ``pytest:secret@localhost``), который **не**
    забирает fetchmail Threlium — SUT/notmuch не трогаются, WireMock не сидится под probe.

    При **``through_agent_mailbox=True``** — прежнее поведение: ``To`` = ``E2E_FETCHMAIL_USER``
    (``test@…``), ожидание забора бриджем (письмо ушло из INBOX через UID MOVE); если задан ``wiremock_seed_base``, до SMTP
    вызывается :func:`tests.e2e.wiremock_client.wiremock_state_seed_context` под ожидаемый
    ``X-Threlium-Thread-Root`` (см. ``docs/TESTING.md`` §4.4.x).

    Returns inner ``Message-ID`` (без угловых скобок) — тот же идентификатор, что в
    ``Message-ID: <…>`` на проволке.
    """
    gm_smtp_host, gm_smtp_port = wait_for_greenmail_ready(project_name, timeout=smtp_timeout)

    rt = discover_runtime(project_name)

    probe_msg_id = f"e2e-readiness-{uuid.uuid4().hex[:8]}@localhost"
    probe_subject = f"e2e greenmail readiness probe {uuid.uuid4().hex[:6]}"

    rcpt_local = E2E_FETCHMAIL_USER if through_agent_mailbox else E2E_GREENMAIL_REPLY_USER
    imap_user = rcpt_local
    imap_pass = E2E_FETCHMAIL_PASS

    if through_agent_mailbox and wiremock_seed_base:
        from tests.e2e.wiremock_client import wiremock_state_seed_context

        ck = e2e_thread_root_mid_for_message_id(probe_msg_id)
        wiremock_state_seed_context(wiremock_seed_base, ck)

    msg = EmailMessage()
    msg["From"] = E2E_GREENMAIL_READINESS_PROBE_FROM
    msg["To"] = e2e_greenmail_mailbox_address(rcpt_local)
    msg["Subject"] = probe_subject
    msg["Message-ID"] = f"<{probe_msg_id}>"
    msg.set_content("readiness probe")

    with smtplib.SMTP(gm_smtp_host, gm_smtp_port, timeout=int(TIMEOUT_POLL_SHORT)) as smtp:
        e2e_smtp_send(gm_smtp_host, gm_smtp_port, msg, timeout=float(TIMEOUT_POLL_SHORT))

    if through_agent_mailbox:
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            user=imap_user,
            password=imap_pass,
            message_id=probe_msg_id,
            subject=probe_subject,
            timeout=imap_timeout or TIMEOUT_POLL_SHORT,
        )
        log_tail = "SMTP→IMAP bridge pickup (test@, gone from INBOX)"
    else:
        wait_for_greenmail_inbox_message_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            user=imap_user,
            password=imap_pass,
            message_id=probe_msg_id,
            subject=probe_subject,
            timeout=imap_timeout or TIMEOUT_POLL_SHORT,
        )
        log_tail = f"SMTP→IMAP INBOX (isolated {rcpt_local}@, no SUT fetchmail)"

    log.info("greenmail_readiness_ok", log_tail=log_tail, project_name=project_name)
    return probe_msg_id


def wait_for_greenmail_inbox_message(
    project_name: str,
    *,
    user: str = E2E_FETCHMAIL_USER,
    password: str = E2E_FETCHMAIL_PASS,
    message_id: str | None = None,
    subject: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
    repo_root: Path | None = None,
) -> None:
    """Wait for a message to appear in GreenMail INBOX (SUT-side IMAP).

    When ``message_id`` / ``subject`` are provided, the probe matches by
    those anchors (header inspection) instead of a bare ``count > 0``.
    """
    root = repo_root or REPO_ROOT

    need_filter = bool(message_id or subject)
    if need_filter:
        mid_check = (
            f"if msg.get('Message-ID','').strip('<>') != {message_id.strip('<>')!r}: continue\n"
            if message_id else ""
        )
        subj_check = (
            f"if msg.get('Subject','') != {subject!r}: continue\n"
            if subject else ""
        )
        script = (
            "import imaplib, email\n"
            + REMOTE_PROBE_LOGGER_BOOT
            + "m = imaplib.IMAP4('greenmail', 3143)\n"
            f"m.login({user!r}, {password!r})\n"
            "_, data = m.select('INBOX')\n"
            "count = int((data[0] or b'0').decode())\n"
            "if count <= 0:\n"
            "    m.logout(); _probe_out.info('INBOX_COUNT=0'); raise SystemExit(2)\n"
            "_, ids_data = m.search(None, 'ALL')\n"
            "ids = ids_data[0].split() if ids_data and ids_data[0] else []\n"
            "for uid in ids:\n"
            "    _, raw = m.fetch(uid, '(BODY.PEEK[HEADER])')\n"
            "    cell = raw[0] if raw and raw[0] else None\n"
            "    if not isinstance(cell, tuple) or len(cell) < 2: continue\n"
            "    hdr = cell[1]\n"
            "    if not isinstance(hdr, (bytes, bytearray)): continue\n"
            + REMOTE_RFC822_PARSER_BOOT
            + "    msg = parse_rfc822(hdr)\n"
            f"    {mid_check}"
            f"    {subj_check}"
            "    m.logout(); _probe_out.info('INBOX_COUNT=%d MATCH=1' % (count,)); raise SystemExit(0)\n"
            "m.logout(); _probe_out.info('INBOX_COUNT=%d MATCH=0' % (count,)); raise SystemExit(2)\n"
        )
    else:
        script = (
            "import imaplib\n"
            + REMOTE_PROBE_LOGGER_BOOT
            + "m = imaplib.IMAP4('greenmail', 3143)\n"
            f"m.login({user!r}, {password!r})\n"
            "_, data = m.select('INBOX')\n"
            "count = int((data[0] or b'0').decode('utf-8'))\n"
            "m.logout()\n"
            "_probe_out.info('INBOX_COUNT=%d' % (count,))\n"
            "raise SystemExit(0 if count > 0 else 2)\n"
        )

    probe_cmd = ["bash", "-lc", f"python3 - <<'PY'\n{script}PY"]

    snap: dict[str, str] = {"inbox": "?", "rc": "?"}

    def _probe() -> bool | None:
        r = service_exec(project_name, "sut", probe_cmd, repo_root=root, timeout=30)
        snap["rc"] = str(r.returncode)
        for line in (r.stdout or "").splitlines():
            if line.startswith("INBOX_COUNT="):
                snap["inbox"] = line.split("=", 1)[1].strip()
                break
        return True if r.returncode == 0 else None

    def _extra() -> str:
        return f"IMAP_INBOX={snap['inbox']} probe_exit={snap['rc']}"

    anchor_desc = ""
    if message_id:
        anchor_desc += f" mid={message_id!r}"
    if subject:
        anchor_desc += f" subj={subject!r}"
    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc=f"greenmail inbox message present (SUT-side){anchor_desc}",
        progress_extra=_extra,
    )


def wait_for_greenmail_inbox_message_gone(
    project_name: str,
    *,
    user: str = E2E_FETCHMAIL_USER,
    password: str = E2E_FETCHMAIL_PASS,
    message_id: str | None = None,
    subject: str | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
    repo_root: Path | None = None,
) -> None:
    """Wait for a message to be processed in GreenMail INBOX (SUT-side IMAP).

    IMAP bridge marks processed messages ``\\Seen``; this function searches
    UNSEEN — when the target message is no longer UNSEEN, bridge has processed it.
    Without anchors falls back to ``UNSEEN count == 0``.
    """
    root = repo_root or REPO_ROOT

    need_filter = bool(message_id or subject)
    if need_filter:
        mid_check = (
            f"if msg.get('Message-ID','').strip('<>') != {message_id.strip('<>')!r}: continue\n"
            if message_id else ""
        )
        subj_check = (
            f"if msg.get('Subject','') != {subject!r}: continue\n"
            if subject else ""
        )
        script = (
            "import imaplib, email\n"
            + REMOTE_PROBE_LOGGER_BOOT
            + "m = imaplib.IMAP4('greenmail', 3143)\n"
            f"m.login({user!r}, {password!r})\n"
            "m.select('INBOX')\n"
            "_, ids_data = m.search(None, 'UNSEEN')\n"
            "ids = ids_data[0].split() if ids_data and ids_data[0] else []\n"
            "if not ids:\n"
            "    m.logout(); _probe_out.info('UNSEEN=0 GONE=1'); raise SystemExit(0)\n"
            "for uid in ids:\n"
            "    _, raw = m.fetch(uid, '(BODY.PEEK[HEADER])')\n"
            "    cell = raw[0] if raw and raw[0] else None\n"
            "    if not isinstance(cell, tuple) or len(cell) < 2: continue\n"
            "    hdr = cell[1]\n"
            "    if not isinstance(hdr, (bytes, bytearray)): continue\n"
            + REMOTE_RFC822_PARSER_BOOT
            + "    msg = parse_rfc822(hdr)\n"
            f"    {mid_check}"
            f"    {subj_check}"
            "    m.logout(); _probe_out.info('UNSEEN=%d GONE=0' % (len(ids),)); raise SystemExit(2)\n"
            "m.logout(); _probe_out.info('UNSEEN=%d GONE=1' % (len(ids),)); raise SystemExit(0)\n"
        )
    else:
        script = (
            "import imaplib\n"
            + REMOTE_PROBE_LOGGER_BOOT
            + "m = imaplib.IMAP4('greenmail', 3143)\n"
            f"m.login({user!r}, {password!r})\n"
            "m.select('INBOX')\n"
            "_, ids_data = m.search(None, 'UNSEEN')\n"
            "ids = ids_data[0].split() if ids_data and ids_data[0] else []\n"
            "count = len(ids)\n"
            "m.logout()\n"
            "_probe_out.info('UNSEEN=%d' % (count,))\n"
            "raise SystemExit(0 if count == 0 else 2)\n"
        )

    probe_cmd = ["bash", "-lc", f"python3 - <<'PY'\n{script}PY"]

    snap: dict[str, str] = {"inbox": "?", "rc": "?"}

    def _probe() -> bool | None:
        r = service_exec(project_name, "sut", probe_cmd, repo_root=root, timeout=30)
        snap["rc"] = str(r.returncode)
        for line in (r.stdout or "").splitlines():
            if line.startswith("INBOX_COUNT="):
                snap["inbox"] = line.split("=", 1)[1].strip()
                break
        return True if r.returncode == 0 else None

    def _extra() -> str:
        return f"IMAP_INBOX={snap['inbox']} probe_exit={snap['rc']}"

    anchor_desc = ""
    if message_id:
        anchor_desc += f" mid={message_id!r}"
    if subject:
        anchor_desc += f" subj={subject!r}"
    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc=f"greenmail inbox message gone (SUT-side IMAP IDLE){anchor_desc}",
        progress_extra=_extra,
    )


def wait_for_greenmail_user_reply(
    project_name: str,
    *,
    user: str = E2E_GREENMAIL_REPLY_USER,
    password: str = E2E_FETCHMAIL_PASS,
    reply_in_reply_to: str | None = None,
    route_wire: str | None = None,
    canonical_id: str | None = None,
    raw_id: str | None = None,
    subject_substring: str = E2E_REPLY_SUBJECT,
    body_substring: str = E2E_REPLY_BODY_SNIPPET,
    timeout: float = TIMEOUT_POLL_SHORT,
    repo_root: Path | None = None,
) -> None:
    """Wait for an agent reply in GreenMail INBOX, optionally correlated to a thread.

    **Корреляция сценария SMTP inject → ответ в pytest@** (parallel-safe): на внешнем письме после
    ``egress_email`` служебные ``X-Threlium-*`` сняты; первый токен ``In-Reply-To`` совпадает с **исходным**
    ``Message-ID`` входящей инъекции (inner без скобок). Передайте ``raw_id`` из фикстуры mailflow
    либо явный ``reply_in_reply_to`` с тем же inner. Это соответствует ``reply_target_rfc_message_id``
    в ``EmailIngressRoute`` и ``MESSAGES.md`` §2.

    Приоритет якоря: ``reply_in_reply_to``, затем ``raw_id``, затем ``canonical_id``. Полезный порядок для
    инъекции — **raw_id**: ``canonical_external_msgid`` (например из ``canonical_id`` в тесте) — это b62-форма,
    тогда как ``In-Reply-To`` на GreenMail содержит **непосредственный** MID из ``smtp_inject.py``.

    ``route_wire``: устарел для проверки ответа по IMAP — wire Route не попадает на внешний SMTP. Если задан
    **только** ``route_wire`` (без якоря выше), выполняется лишь отбор по subject/body (без тредовой
    привязки, небезопасно при параллельных прогонах). Для b62 в notmuch см.
    :func:`e2e_smtp_inject_ingress_route_wire_for_message_id` по ``raw_id`` инъекции; устаревший
    :func:`e2e_smtp_inject_ingress_route_wire` — только без ``reply_target``.

    When no IRT anchor is given and ``route_wire`` is absent, the function falls back to subject/body
    matching (not parallel-safe).

    Ответ агента приходит на ``EmailIngressRoute.origin`` (smtp inject: ``pytest@localhost``); IMAP по умолчанию —
    ``E2E_GREENMAIL_REPLY_USER`` (``pytest``), не ``E2E_FETCHMAIL_USER`` (входящая инъекция в ящик ``test``).
    """
    root = repo_root or REPO_ROOT

    irt_anchor: str | None = None
    if reply_in_reply_to is not None and str(reply_in_reply_to).strip():
        irt_anchor = str(reply_in_reply_to).strip().strip("<>").lower()
    elif raw_id is not None and str(raw_id).strip():
        irt_anchor = str(raw_id).strip().strip("<>").lower()
    elif canonical_id is not None and str(canonical_id).strip():
        irt_anchor = str(canonical_id).strip().strip("<>").lower()
    # ``route_wire`` в одиночку: без якоря (устаревшая подсказка; на внешнем SMTP Route нет).

    if irt_anchor is not None:
        # Тот же уровень отступа, что и тело ``for msg_id in reversed(ids):`` (8 пробелов).
        anchor_check = (
            f"        _irt_raw = (msg.get('In-Reply-To') or '').strip()\n"
            f"        _m = re.search(r'<([^>]+)>', _irt_raw)\n"
            f"        _irt_first = (_m.group(1).strip().lower() if _m else '') or ''\n"
            f"        if _irt_first != {irt_anchor!r}:\n"
            f"            continue\n"
        )
    else:
        anchor_check = ""

    py_body = f"""import imaplib
import re
import sys
{REMOTE_PROBE_LOGGER_BOOT}{REMOTE_RFC822_PARSER_BOOT}from email.header import decode_header

def _decode_subject(raw: str) -> str:
    if not raw:
        return ""
    parts = decode_header(raw)
    out = []
    for text, enc in parts:
        if isinstance(text, bytes):
            out.append(text.decode(enc or "utf-8", errors="replace"))
        else:
            out.append(text)
    return "".join(out)

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

def check() -> bool:
    sn = {(subject_substring or "").lower()!r}
    bn = {(body_substring or "").lower()!r}
    m = imaplib.IMAP4("greenmail", 3143)
    m.login({user!r}, {password!r})
    m.select("INBOX")
    _, data = m.search(None, "ALL")
    ids = data[0].split() if data and data[0] else []
    for msg_id in reversed(ids):
        _, raw_data = m.fetch(msg_id, "(RFC822)")
        if not raw_data or not isinstance(raw_data[0], tuple):
            continue
        raw = raw_data[0][1]
        msg = parse_rfc822(raw)
{anchor_check}\
        subj = _decode_subject(msg.get("Subject") or "").lower()
        body = _plain_body(msg).lower()
        if (sn and sn in subj) or (bn and bn in body):
            m.logout()
            _probe_out.info("GREENMAIL_REPLY_OK=1")
            return True
    m.logout()
    return False

raise SystemExit(0 if check() else 1)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py_body + "\nPY\n"]
    snap = {"rc": "?"}

    def _probe() -> bool | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        snap["rc"] = str(r.returncode)
        return True if r.returncode == 0 else None

    def _extra() -> str:
        return f"greenmail_reply_probe_exit={snap['rc']}"

    anchor_desc = ""
    if irt_anchor is not None:
        anchor_desc = f" in_reply_to_anchor={irt_anchor!r}"
    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc=f"greenmail INBOX reply (thread-correlated){anchor_desc}",
        progress_extra=_extra,
    )


def _imap_message_plain_body(msg: Any) -> str:
    """``text/plain`` тело письма (или payload non-multipart) как строка."""
    if msg.is_multipart():
        for p in msg.walk():
            if p.get_content_type() == "text/plain":
                pl = p.get_payload(decode=True)
                if isinstance(pl, bytes):
                    return pl.decode("utf-8", errors="replace")
                return str(pl or "")
        return ""
    pl = msg.get_payload(decode=True)
    if isinstance(pl, bytes):
        return pl.decode("utf-8", errors="replace")
    return str(pl or "")


def greenmail_wait_agent_reply_message_id(
    host: str,
    port: int,
    *,
    in_reply_to_anchor: str,
    user: str = E2E_GREENMAIL_REPLY_USER,
    password: str = E2E_FETCHMAIL_PASS,
    body_substring: str = E2E_REPLY_BODY_SNIPPET,
    since_uid: int = 0,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> str:
    """Дождаться ответа агента в INBOX GreenMail (host-side IMAP) и вернуть его ``Message-ID``.

    Ответ коррелируется по первому токену ``In-Reply-To`` == ``in_reply_to_anchor`` (inner MID
    исходной инъекции, без скобок) — так же, как :func:`wait_for_greenmail_user_reply`; тело
    дополнительно проверяется по ``body_substring``. Возвращается ``Message-ID`` ответа агента
    в угловых скобках — пригоден как ``in_reply_to`` следующего письма пользователя.

    Реалистичный threading: пользователь отвечает на письмо бота, а не на собственную инъекцию.
    Egress glue-record (см. :mod:`threlium.egress_self_archive`) держит IRT-цепочку непрерывной —
    ход вверх по In-Reply-To из нового хода проходит через ``tasks_upsert`` прошлого хода, поэтому
    per-frame task-ledger наследуется без ручного сброса WireMock-латча.
    """
    anchor = in_reply_to_anchor.strip().strip("<>")
    found: dict[str, str] = {}

    def _probe() -> bool | None:
        with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
            imap.login(user, password)
            imap.select("INBOX")
            crit = f'HEADER In-Reply-To "{anchor}"'
            if since_uid > 0:
                crit = f"UID {since_uid + 1}:* {crit}"
            _, data = imap.uid("search", None, crit)
            uids = data[0].split() if data and data[0] else []
            for uid in reversed(uids):
                _, raw_data = imap.uid("fetch", uid, "(RFC822)")
                if not raw_data or not isinstance(raw_data[0], tuple):
                    continue
                msg = e2e_parse_rfc822(raw_data[0][1])
                m = re.search(r"<([^>]+)>", msg.get("In-Reply-To") or "")
                first = m.group(1).strip().lower() if m else ""
                if first != anchor.lower():
                    continue
                body = _imap_message_plain_body(msg)
                if body_substring and body_substring.lower() not in body.lower():
                    continue
                mid = (msg.get("Message-ID") or "").strip()
                if mid:
                    found["mid"] = mid if mid.startswith("<") else f"<{mid.strip('<>')}>"
                    imap.logout()
                    return True
            imap.logout()
            return None

    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc=f"greenmail agent reply Message-ID (in_reply_to={anchor!r}) on {host}:{port}",
    )
    return found["mid"]


def imap_list_uids_in_folder(
    host: str,
    port: int,
    *,
    user: str,
    password: str,
    folder: str,
) -> list[int]:
    """UID-ы писем в папке ``folder`` (``UID SEARCH ALL``)."""
    import imaplib

    with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
        imap.login(user, password)
        typ, _ = imap.select(folder, readonly=True)
        if typ != "OK":
            raise RuntimeError(f"IMAP SELECT {folder!r} failed: {typ}")
        typ, data = imap.uid("SEARCH", None, "ALL")
        if typ != "OK":
            raise RuntimeError(f"IMAP UID SEARCH ALL failed: {typ} {data!r}")
        raw = data[0] if data else b""
        if not raw:
            return []
        return sorted(int(x) for x in raw.decode().split())


def assert_imap_inner_mid_in_folder(
    host: str,
    port: int,
    *,
    user: str,
    password: str,
    folder: str,
    inner_mid: str,
) -> None:
    """Письмо с ``Message-ID`` ``inner_mid`` присутствует в ``folder``."""
    import imaplib

    needle = inner_mid.strip().strip("<>")
    with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
        imap.login(user, password)
        typ, _ = imap.select(folder, readonly=True)
        if typ != "OK":
            raise AssertionError(f"IMAP SELECT {folder!r} failed: {typ}")
        typ, data = imap.uid("SEARCH", None, "HEADER", "Message-ID", f"<{needle}>")
        if typ != "OK":
            raise AssertionError(f"IMAP UID SEARCH Message-ID failed: {typ}")
        uids = data[0].split() if data and data[0] else []
        assert uids, (
            f"expected Message-ID {needle!r} in IMAP folder {folder!r}, got no UIDs"
        )


def assert_imap_inner_mid_not_in_inbox(
    host: str,
    port: int,
    *,
    user: str,
    password: str,
    inner_mid: str,
) -> None:
    """После ``UID MOVE`` письма нет в INBOX (но может быть в processed)."""
    import imaplib

    needle = inner_mid.strip().strip("<>")
    with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
        imap.login(user, password)
        typ, _ = imap.select("INBOX", readonly=True)
        if typ != "OK":
            raise AssertionError(f"IMAP SELECT INBOX failed: {typ}")
        typ, data = imap.uid("SEARCH", None, "HEADER", "Message-ID", f"<{needle}>")
        if typ != "OK":
            raise AssertionError(f"IMAP UID SEARCH Message-ID failed: {typ}")
        uids = data[0].split() if data and data[0] else []
        assert not uids, (
            f"Message-ID {needle!r} still in INBOX (uids={uids!r}); expected UID MOVE"
        )
