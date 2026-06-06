"""SUT / GreenMail cleanup between scenarios."""
from __future__ import annotations

import imaplib
import json
import os
import re
import shlex
import time

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .bridges.email import notmuch_id_search_term
from .constants import (
    E2E_FETCHMAIL_PASS,
    E2E_FETCHMAIL_USER,
    E2E_GREENMAIL_REPLY_USER,
    E2E_IMAP_PROCESSED_FOLDER,
    E2E_REMOTE_POSIX_HOME,
    E2E_REMOTE_THRELIUM_HOME,
    E2E_SUT_NOTMUCH_BASH_EXPORT,
    THRELIUM_E2E_SKIP_SUT_MAILDIR_FLUSH_ENV,
    TIMEOUT_POLL_SHORT,
    _MATRIX_E2E_STUB_TAGS,
    _STUB_TAG_TO_PREFIXES,
)
from .greenmail import _greenmail_imap_expunge_folder, e2e_greenmail_mailbox_address
from .runtime import E2EComposeRuntime, service_exec

def _stub_tag_uses_telegram_bridge_cleanup(stub_tag: str) -> bool:
    return stub_tag.startswith("stub-telegram-wiremock-live-e2e")


def e2e_flush_greenmail_inboxes(rt: E2EComposeRuntime) -> None:
    """EXPUNGE GreenMail IMAP: ``INBOX`` (``test@``, ``pytest@``) и ``Threlium.Processed`` (``test@``).

    Without this, ``threlium-bridge@email`` picks up stale messages from previous
    runs after SUT Maildir/notmuch flush.  The bridge now drops replies whose
    immediate ``In-Reply-To`` parent is missing from the wiped notmuch index
    (``orphan_skip``), so stale replies no longer feed ``irt_chain.py`` and the
    enrich worker no longer enters a restart loop.  Flushing is still required:
    stale root messages would otherwise be re-delivered as duplicates and the
    IMAP UID watermark must be reset between independent test sessions.

    ``Threlium.Processed`` (UID MOVE после fetch) тоже чистится: после wipe
    notmuch мост стартует с ``effective_start=1`` и иначе снова обрабатывает
    всё, что осталось в INBOX или накопилось в processed-папке между сессиями.
    """
    host, port = rt.greenmail_imap_host, rt.greenmail_imap_port
    flush_specs: list[tuple[str, str, tuple[str, ...]]] = [
        (E2E_FETCHMAIL_USER, E2E_FETCHMAIL_PASS, ("INBOX", E2E_IMAP_PROCESSED_FOLDER)),
        (E2E_GREENMAIL_REPLY_USER, E2E_FETCHMAIL_PASS, ("INBOX",)),
    ]
    for user, password, folders in flush_specs:
        try:
            with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
                imap.login(user, password)
                for folder in folders:
                    try:
                        n = _greenmail_imap_expunge_folder(imap, folder)
                        log.info("greenmail_flush", user=user, folder=folder, expunged=n)
                    except Exception as folder_exc:
                        log.warning(
                            "greenmail_flush_folder_skipped",
                            user=user,
                            folder=folder,
                            error=repr(folder_exc),
                        )
                imap.logout()
        except Exception as exc:
            log.warning("greenmail_flush_skipped", user=user, error=repr(exc))


def e2e_flush_sut_fsm_maildirs(rt: E2EComposeRuntime) -> None:
    """Очистить Maildir, notmuch DB и LightRAG на SUT перед тестовой сессией.

    Полный wipe: файлы Maildir, Xapian индекс notmuch, LightRAG storage — всё пересоздаётся
    engine при старте. Без wipe накопленные данные замедляют LightRAG indexing (173MB+, 60s+ на документ).
    """
    raw = os.environ.get(THRELIUM_E2E_SKIP_SUT_MAILDIR_FLUSH_ENV, "")
    if str(raw).strip().lower() in ("1", "true", "yes", "on"):
        log.info("sut_maildir_flush_skipped", env=THRELIUM_E2E_SKIP_SUT_MAILDIR_FLUSH_ENV)
        return
    th = shlex.quote(E2E_REMOTE_THRELIUM_HOME)
    nm_cfg = shlex.quote(E2E_REMOTE_POSIX_HOME + "/.notmuch-config")
    home_q = shlex.quote(E2E_REMOTE_POSIX_HOME)
    notmuch_cmd = "export HOME=" + home_q + " NOTMUCH_CONFIG=" + nm_cfg + "; notmuch new"
    su_wrap = shlex.quote(notmuch_cmd)
    script = f"""set -eu
TH={th}
if [ -d "$TH/stages" ]; then
  find "$TH/stages" \\
    \\( -path '*/Maildir/new/*' -o -path '*/Maildir/cur/*' \\
    -o -path '*/Maildir/tmp/*' \\) \\
    -type f ! -name '.*' -delete 2>/dev/null || true
fi
# LightRAG accumulates data across runs (173MB+); entity extraction slows to 60s+ per document.
rm -rf "$TH/lightrag" 2>/dev/null || true
# notmuch Xapian index — stale thread IDs interfere with isolation; recreated by `notmuch new`.
rm -rf "$TH/stages/.notmuch" 2>/dev/null || true
# LightRAG KV/doc-status теперь в Redis (localhost) — чистим вместе с файловым lightrag-каталогом,
# иначе индекс/кэш прошлой сессии переживёт wipe и сломает изоляцию прогона.
redis-cli flushall >/dev/null 2>&1 || true
su - {E2E_THRELIUM_USER} -s /bin/bash -c {su_wrap} </dev/null || true
echo "[e2e] SUT flushed: Maildir + lightrag(files+redis) + notmuch DB wiped, notmuch new done"
"""
    completed = service_exec(
        rt.project_name,
        "sut",
        ["bash", "-lc", script],
        repo_root=rt.repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if completed.returncode != 0:
        log.warning(
            "sut_maildir_flush_warning",
            rc=completed.returncode,
            stdout_snippet=(completed.stdout or "")[:600],
        )


def e2e_clean_sut_messages_for_test(rt: E2EComposeRuntime, stub_tag: str, correlation_key: str | None = None) -> None:
    """Очистить сообщения из предыдущих запусков конкретного E2E теста на SUT.

    Использует неполные Message-ID префиксы, соответствующие stub_tag,
    декодирует канонические Message-ID в notmuch, вычисляет thread ID,
    удаляет все файлы в найденных тредах и обновляет индекс.
    Исключает из удаления тред, соответствующий correlation_key, чтобы сохранить
    сообщения текущей сессии в многошаговых тестах.
    """
    prefixes = _STUB_TAG_TO_PREFIXES.get(stub_tag, [])
    bridge_matrix = stub_tag in _MATRIX_E2E_STUB_TAGS
    bridge_telegram = _stub_tag_uses_telegram_bridge_cleanup(stub_tag)
    if not prefixes and not bridge_matrix and not bridge_telegram:
        return

    corr_search_term = ""
    if correlation_key:
        try:
            corr_search_term = notmuch_id_search_term(correlation_key)
        except ValueError:
            log.warning(
                "sut_message_cleanup_skip_active_thread",
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )

    py_script = f"""import os, subprocess, re, base62, msgspec

prefixes = {prefixes!r}
bridge_matrix = {bridge_matrix!r}
bridge_telegram = {bridge_telegram!r}
corr_search_term = {corr_search_term!r}
env = os.environ.copy()
env["HOME"] = "/home/threlium"
env["NOTMUCH_CONFIG"] = "/home/threlium/.notmuch-config"

active_thread_id = None
if corr_search_term:
    proc_active = subprocess.run(
        ["notmuch", "search", "--output=threads", corr_search_term],
        capture_output=True,
        text=True,
        env=env,
    )
    if proc_active.returncode == 0:
        lines = proc_active.stdout.splitlines()
        if lines:
            active_thread_id = lines[0].strip()

proc = subprocess.run(["notmuch", "search", "--output=messages", "*"], capture_output=True, text=True, env=env)
if proc.returncode != 0:
    print("notmuch search failed:", proc.stderr)
    raise SystemExit(1)

message_ids = [line.strip() for line in proc.stdout.splitlines() if line.strip()]
threads_to_delete = set()

for mid in message_ids:
    bracketed_mid = mid if mid.startswith("<") else f"<{{mid}}>"
    match = re.fullmatch(r"<\\s*([^>]+)\\s*>", bracketed_mid.strip())
    if not match:
        continue
    inner = match.group(1).strip()
    left, _, right = inner.rpartition("@")
    if right not in ("localhost", "internal"):
        if any(inner.startswith(p) for p in prefixes):
            threads_to_delete.add(bracketed_mid)
        continue
    try:
        payload = base62.decodebytes(left)
        decoded = msgspec.json.decode(payload)
    except Exception:
        if any(inner.startswith(p) for p in prefixes):
            threads_to_delete.add(bracketed_mid)
        continue

    matched = False
    if "message_id" in decoded and isinstance(decoded["message_id"], str):
        if any(decoded["message_id"].startswith(p) for p in prefixes):
            matched = True
    elif "room_id" in decoded and isinstance(decoded["room_id"], str):
        room_id = decoded["room_id"]
        if bridge_matrix and room_id.startswith("!e2e_"):
            matched = True
        elif any(room_id.startswith(p) for p in prefixes):
            matched = True
    elif "chat_id" in decoded and bridge_telegram:
        matched = True
    elif "event_id" in decoded and isinstance(decoded["event_id"], str):
        if any(decoded["event_id"].startswith(p) for p in prefixes):
            matched = True

    if matched:
        threads_to_delete.add(bracketed_mid)

deleted_files = 0
for mid in threads_to_delete:
    proc_thread = subprocess.run(["notmuch", "search", "--output=threads", f"id:{{mid}}"], capture_output=True, text=True, env=env)
    if proc_thread.returncode != 0:
        continue
    for thread_line in proc_thread.stdout.splitlines():
        thread_id = thread_line.strip()
        if not thread_id:
            continue
        if active_thread_id and thread_id == active_thread_id:
            print(f"[cleanup] Skipping active thread: {{thread_id}}")
            continue
        proc_files = subprocess.run(["notmuch", "search", "--output=files", thread_id], capture_output=True, text=True, env=env)
        if proc_files.returncode != 0:
            continue
        for file_path in proc_files.stdout.splitlines():
            file_path = file_path.strip()
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    print(f"[cleanup] Deleted: {{file_path}}")
                    deleted_files += 1
                except Exception as e:
                    print(f"[cleanup] Failed to delete {{file_path}}: {{e}}")

if deleted_files > 0:
    subprocess.run(["notmuch", "new"], env=env)
    print(f"[cleanup] Done: deleted {{deleted_files}} files and updated notmuch")
else:
    print("[cleanup] No messages found for stub_tag={stub_tag}")
"""

    cmd = ["/home/threlium/threlium/agent/.venv/bin/python3", "-c", py_script]
    completed = service_exec(
        rt.project_name,
        "sut",
        cmd,
        repo_root=rt.repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if completed.returncode != 0:
        log.warning(
            "sut_message_cleanup_error",
            stub_tag=stub_tag,
            rc=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
        )
    else:
        out = (completed.stdout or "").strip()
        if out:
            log.info("sut_message_cleanup_success", stub_tag=stub_tag, output=out)
