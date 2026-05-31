"""Общие утилиты для e2e: poll, docker SDK exec, дампы при падении."""
from __future__ import annotations

import json
import contextlib
import fcntl
import hashlib
import imaplib
import os
import re
import shlex
import shutil
import smtplib
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from email import message_from_bytes
from email.header import decode_header
from email.message import EmailMessage
from datetime import datetime, timezone
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import TypeVar

from threlium.types import (
    EmailIngressRoute,
    EmailNativeId,
    ExternalRfcMidWire,
    FsmStage,
    IngressRouteB62Wire,
    MatrixNativeId,
    MatrixRoomEventId,
    MatrixRoomId,
    NotmuchMessageIdInner,
    NotmuchQueryField,
    NotmuchTag,
    RfcMessageIdWire,
    TelegramNativeId,
)
from threlium.lightrag_drain_query import lightrag_drain_pending_search
from tests.e2e.log import clip_log_body, log

import docker  # type: ignore[import-not-found]
from tenacity import (
    RetryError,
    Retrying,
    retry_if_exception_type,
    retry_if_result,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from .sut_user_systemd import (
    E2E_SUT_THRELIUM_USER_UNIT_JOURNAL,
    E2E_THRELIUM_USER,
    E2E_THRELIUM_USER_JOURNALCTL_PREFIX,
    E2E_THRELIUM_USER_JOURNAL_TRANSPORT_MATCH,
    e2e_sut_threlium_user_journal_rotate_vacuum_bash,
    e2e_sut_threlium_user_workers_idle_probe_bash,
    e2e_sut_threlium_user_workers_stall_diag_bash,
    e2e_start_threlium_user_pipeline_bash,
    e2e_stop_threlium_user_pipeline_bash,
    e2e_threlium_user_unit_journalctl_bash,
)

T = TypeVar("T")

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_DIR = REPO_ROOT / "tests" / "e2e" / "compose"

# Heredoc на SUT: ``StreamHandler(sys.stdout)`` + ``%(message)s`` — по строке на ``.info`` (протокол парсинга stdout).
_E2E_REMOTE_PROBE_LOGGER_BOOT = (
    "import logging, sys\n"
    "_probe_out = logging.getLogger('threlium.e2e.remote')\n"
    "if not _probe_out.handlers:\n"
    "    _h = logging.StreamHandler(sys.stdout)\n"
    "    _h.setFormatter(logging.Formatter('%(message)s'))\n"
    "    _probe_out.addHandler(_h)\n"
    "    _probe_out.setLevel(logging.INFO)\n"
    "    _probe_out.propagate = False\n"
)


def rfc_first_message_id_in_in_reply_to_header(value: str | None) -> str | None:
    """Первый токен ``<…>`` из заголовка ``In-Reply-To`` (RFC 5322). Пусто → ``None``."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    m = re.search(r"<([^>]+)>", s)
    return m.group(1).strip() if m else None


def canonical_external_msgid(raw_message_id: str) -> str:
    """Возвращает каноничный inner-id для внешнего email Message-ID (без ``<…>``).

    `docs/MESSAGES.md` §2: на границе ingress для email входящий
    ``Message-ID:`` приводится к ``<base62(EmailNativeId{v:1, message_id:<raw>})@localhost>`` через
    :meth:`RfcMessageIdWire.from_native`;
    для **email-моста** в индекс попадает :func:`email_ingress_notmuch_id_inner`, не эта форма.
    Для проверок notmuch после моста используйте :func:`email_ingress_notmuch_id_inner`.
    """
    canonical_full = RfcMessageIdWire.from_native(
        EmailNativeId(v=1, message_id=raw_message_id)
    ).value
    return f"{RfcMessageIdWire.threlium_fs_id_left(canonical_full)}@localhost"


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


def email_ingress_notmuch_id_inner(raw_message_id: str) -> str:
    """Inner ``Message-ID`` в union-notmuch после email-моста и индексации продуктом.

    Мост заменяет входящий идентификатор на :meth:`RfcMessageIdWire.from_native`
    с :class:`EmailNativeId` (``v=1``, строка ``inner`` из угловых скобок), как в
    :func:`threlium.bridges.email._bridge_wire_from_angle_inner`.
    """
    inner = raw_message_id.strip().strip("<>")
    return (
        RfcMessageIdWire.from_native(EmailNativeId(v=1, message_id=inner))
        .value.strip("<>")
        .strip()
    )


def notmuch_id_search_term(inner_or_bracketed: str) -> str:
    """Предикат ``id:"…"`` для ``notmuch count/search`` (экранирование inner ``Message-ID``)."""
    mid = NotmuchMessageIdInner.from_optional_raw(inner_or_bracketed)
    if mid is None:
        raise ValueError(f"notmuch_id_search_term: invalid message-id: {inner_or_bracketed!r}")
    return mid.as_notmuch_term()


E2E_COMPOSE_FILE_NAME = "docker-compose.yml"
E2E_COMPOSE_FILE = COMPOSE_DIR / E2E_COMPOSE_FILE_NAME


def e2e_compose_coord_dir() -> Path:
    """Каталог координаторов shared compose: стабилен между отдельными вызовами ``pytest`` (тот же checkout)."""
    workspace_hash = hashlib.sha256(str(REPO_ROOT.resolve()).encode()).hexdigest()[:12]
    d = Path(tempfile.gettempdir()) / f"threlium_e2e_compose_coord_{workspace_hash}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def e2e_compose_coord_paths() -> tuple[Path, Path, Path]:
    """``(lock_path, ready_flag_path, runtime_json_path)`` для лидера/фолловеров ``compose_stack``."""
    d = e2e_compose_coord_dir()
    return (
        d / "e2e_compose_setup.lock",
        d / "e2e_compose_ready.flag",
        d / "e2e_shared_runtime.json",
    )


def e2e_controller_hint_path() -> Path:
    """Путь подсказки контроллера pytest (``sessionfinish``): от ``cwd``, как раньше в ``conftest``."""
    workspace_hash = hashlib.sha256(str(Path.cwd()).encode()).hexdigest()[:12]
    return Path(tempfile.gettempdir()) / f"threlium_e2e_{workspace_hash}.json"


def e2e_controller_hint_write(
    project_name: str,
    *,
    runtime_json_path: Path | None = None,
) -> None:
    hint = e2e_controller_hint_path()
    hint.write_text(
        json.dumps({
            "project_name": project_name,
            "pid": os.getpid(),
            "cwd": str(Path.cwd()),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "runtime_json": str(runtime_json_path) if runtime_json_path else None,
        })
    )


def e2e_controller_hint_read() -> str | None:
    try:
        hint = e2e_controller_hint_path()
        if hint.is_file():
            data = json.loads(hint.read_text())
            return data.get("project_name") or None
    except (OSError, ValueError, KeyError):
        pass
    return None


def e2e_controller_hint_cleanup() -> None:
    try:
        e2e_controller_hint_path().unlink(missing_ok=True)
    except OSError:
        pass


# Cockpit (HTTPS :9090), Caddy в e2e (HTTP :8080 на проброшенном порту).
E2E_SUT_COCKPIT_PORT = 9090

# Сервисы shared compose (tests/e2e/compose/docker-compose.yml) — для проверки «живости» стека.
E2E_SHARED_COMPOSE_SERVICES: tuple[str, ...] = ("sut", "greenmail", "wiremock")
# Предсобранный SUT (docker commit после site.yml); должен совпадать
# с дефолтом THRELIUM_E2E_BAKE_IMAGE в bake_e2e_sut_image.sh.
E2E_BAKED_SUT_IMAGE = "threlium/e2e-sut:baked"
# Образ сервиса `sut` в compose (подстановка в docker-compose.yml); дефолт совпадает с YAML.
E2E_SUT_IMAGE_ENV = "THRELIUM_E2E_SUT_IMAGE"
E2E_DEFAULT_SUT_IMAGE = E2E_BAKED_SUT_IMAGE
E2E_REBUILD_BAKED_IMAGE_ENV = "THRELIUM_E2E_REBUILD_BAKED_IMAGE"
E2E_AUTO_BAKE_IF_MISSING_ENV = "THRELIUM_E2E_AUTO_BAKE_IF_MISSING"
E2E_BAKE_SCRIPT = REPO_ROOT / "tests" / "e2e" / "scripts" / "bake_e2e_sut_image.sh"
E2E_PROJECT = os.environ.get("THRELIUM_E2E_PROJECT", "threlium_e2e")
E2E_REMOTE_REPO_PATH = os.environ.get(
    "THRELIUM_E2E_REMOTE_REPO_PATH",
    f"/home/{E2E_THRELIUM_USER}/threlium/agent",
)
E2E_REMOTE_THRELIUM_HOME = os.environ.get(
    "THRELIUM_E2E_REMOTE_THRELIUM_HOME",
    f"/home/{E2E_THRELIUM_USER}/threlium/data",
)
E2E_REMOTE_POSIX_HOME = os.environ.get(
    "THRELIUM_E2E_REMOTE_POSIX_HOME",
    f"/home/{E2E_THRELIUM_USER}",
)
# User journal / user systemd на SUT: ``tests/e2e/sut_user_systemd.py`` (реэкспорт выше).
# docker exec от root: без явного HOME/NOTMUCH_CONFIG notmuch читает не тот индекс.
E2E_SUT_NOTMUCH_BASH_EXPORT = (
    "export HOME="
    + shlex.quote(E2E_REMOTE_POSIX_HOME)
    + " NOTMUCH_CONFIG="
    + shlex.quote(E2E_REMOTE_POSIX_HOME + "/.notmuch-config")
)
# WireMock (compose ``wiremock``): OpenAI-совместимые стабы + Matrix; хост-порт через
# _mapped_port("wiremock", E2E_WIREMOCK_CONTAINER_PORT).
E2E_WIREMOCK_CONTAINER_PORT: int = 8080
E2E_FETCHMAIL_USER = os.environ.get("THRELIUM_E2E_FETCHMAIL_USER", "test")
E2E_FETCHMAIL_PASS = os.environ.get("THRELIUM_E2E_FETCHMAIL_PASS", "secret")
# Ответ агента msmtp шлёт на ``EmailIngressRoute.origin`` (для smtp_inject — ``pytest@localhost``).
E2E_GREENMAIL_REPLY_USER = os.environ.get("THRELIUM_E2E_GREENMAIL_REPLY_USER", "pytest")
# Общий флаг с live-сценариями: пропуск очистки Maildir на SUT (отладка).
THRELIUM_E2E_SKIP_SUT_MAILDIR_FLUSH_ENV = "THRELIUM_E2E_LIVE_SKIP_SUT_MAILDIR_FLUSH"


def e2e_greenmail_mailbox_address(local_part_or_address: str) -> str:
    """RFC5322-адрес ящика для SMTP/IMAP к GreenMail e2e (``GREENMAIL_OPTS``: ``user:secret@localhost``, …)."""
    s = (local_part_or_address or "").strip()
    if not s:
        raise ValueError("e2e_greenmail_mailbox_address: empty")
    if "@" in s:
        return s
    return f"{s}@localhost"


# Синхронно с ansible/roles/threlium/vars/main.yml threlium_fsm_mailbox_stages (id);
# порядок — как в ``FsmStage`` (определение enum).
E2E_FSM_MAILBOX_STAGE_IDS: tuple[str, ...] = tuple(s.value for s in FsmStage)
E2E_ANSIBLE_INVENTORY_PATH = "inventory/e2e/hosts.yml"
E2E_ANSIBLE_CONFIG_NAME = "ansible-e2e.cfg"

# Системные юниты mail-archive web (site.yml → mail_archive_web.yml): Cockpit.
# Дамп при падении e2e / ansible (см. dump_failure_artifacts).
E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT = r"""set +e
echo '=== systemctl list-units caddy* --all --no-pager ==='
systemctl list-units 'caddy*' --all --no-pager 2>&1 || true
echo ''
echo '=== systemctl list-units cockpit* --all --no-pager ==='
systemctl list-units 'cockpit*' --all --no-pager 2>&1 || true
echo ''
echo '=== systemctl status cockpit.service --no-pager -l ==='
systemctl status cockpit.service --no-pager -l 2>&1 || true
echo ''
echo '=== systemctl status cockpit.socket --no-pager -l ==='
systemctl status cockpit.socket --no-pager -l 2>&1 || true
echo ''
echo '=== journalctl -u caddy.service --no-pager -n 50 ==='
journalctl -u caddy.service --no-pager -n 50 2>&1 || true
echo ''
echo '=== journalctl -u cockpit.service -u cockpit-ws.service --no-pager -n 50 ==='
journalctl -u cockpit.service -u cockpit-ws.service --no-pager -n 50 2>&1 || true
echo ''
echo '=== journalctl -u cockpit.socket --no-pager -n 40 ==='
journalctl -u cockpit.socket --no-pager -n 40 2>&1 || true
echo ''
echo '=== ss LISTEN :9090 or :8080 ==='
ss -tlnp 2>/dev/null | grep -E ':(9090|8080)' || true
echo ''
echo '=== systemctl list-units --failed --no-pager (system scope) ==='
systemctl list-units --failed --no-pager 2>&1 || true
"""

# Единый таймаут поведенческих ожиданий e2e (сек.): ``THRELIUM_E2E_POLL_SHORT``, дефолт 30.
# Долгим исключением остаётся только ``THRELIUM_E2E_TIMEOUT_ANSIBLE`` (playbook / wipe_sync).
TIMEOUT_POLL_SHORT = float(os.environ.get("THRELIUM_E2E_POLL_SHORT", "30"))
TIMEOUT_ANSIBLE_PLAYBOOK = int(os.environ.get("THRELIUM_E2E_TIMEOUT_ANSIBLE", str(20 * 60)))
POLL_INTERVAL = float(os.environ.get("THRELIUM_E2E_POLL_INTERVAL", "2.0"))
# Ответ контура после WireMock L0-стабов (паритет ``reference_l0/threlium_e2e_l0.py``).
E2E_REPLY_SUBJECT = "e2e reply"
E2E_REPLY_BODY = "ok from llm-mock"
E2E_REPLY_BODY_SNIPPET = E2E_REPLY_BODY
# Маркер в Subject входящего письма для «цепочки SUBAGENT_TABLE» в L0-mock (см. ``threlium_e2e_l0.py``).
E2E_SUBAGENT_TABLE_LIVE_SUBJECT_MARKER = "e2e_subagent_table_chain"
# Полный цикл SUBAGENT_TABLE + CLI HITL (live e2e + ``threlium_e2e_l0.py``): маркер в теле, префикс корневого Message-ID.
E2E_SUBAGENT_HITL_MATRIX_BODY_MARKER = "e2e_subagent_hitl_matrix"
E2E_SUBAGENT_HITL_MATRIX_LIVE_MSGID_PREFIX = "e2e-hitl-mx-"
# Live e2e MEMORY_TABLE.md §1 (thread_memory): Subject и префикс корневого Message-ID (см. ``threlium_e2e_l0.py``).
E2E_MEMORY_THREAD_LIVE_SUBJECT_MARKER = "e2e_memory_thread_live"
E2E_MEMORY_THREAD_LIVE_MSGID_PREFIX = "e2e-mem-tm-"

# Сколько строк с ``correlation_key`` добавлять к телу письма (доп. попадание маркера в чанки тела;
# для ``/embeddings`` на WireMock см. префикс чанка ``Subject:`` в ``threlium_email_chunking_func``).
_E2E_DENSE_CORR_SEGMENTS = 4

E2E_CTX_TRIM_HEAD_MARKER = "E2E-CTX-TRIM-HEAD-MARKER"
E2E_CTX_TRIM_TAIL_MARKER = "E2E-CTX-TRIM-TAIL-MARKER"
# Запас на JSON-обёртку chat/completions + XML-секции промпта reasoning
# (envelope, knowledge_graph, mail_context и пр. — каждая MIME-часть имеет
# собственный бюджет context_max_chars).
E2E_CTX_TRIM_JOURNAL_SLACK_CHARS = 12000

E2E_SUMMARY_MARKER = "E2E-SUM-CONTEXT-MARKER"
E2E_SUM_ORIG_HEAD_MARKER = "E2E-SUM-ORIG-HEAD-MARKER"
E2E_SUM_ORIG_PAD_MARKER = "E2E-SUM-ORIG-PAD-MARKER"
E2E_SUMMARIZE_LLM_NEEDLE = "context summarizer"


def e2e_dense_threlium_ctx_body(*, head: str, correlation_key: str) -> str:
    """Текст письма: ``head`` + много строк с тем же ``correlation_key`` подряд.

    Чанкер LightRAG добавляет к каждому чанку строку ``Subject: …`` (см.
    ``threlium/lightrag_chunking.py``); стабы ``/embeddings`` в основном матчятся по ней.
    Плотные повторяющиеся строки остаются полезны для попадания маркера в чанки и для журнала WireMock.
    Для e2e-корреляции LiteLLM ``correlation_key`` в сиде State совпадает с canonical
    thread-root MID (см. :func:`e2e_thread_root_mid_for_message_id`).
    """
    lines = [head.rstrip("\n")] if head.strip() else []
    lines.extend(
        f"e2e_ctx_seg_{i:03d} {correlation_key}" for i in range(_E2E_DENSE_CORR_SEGMENTS)
    )
    return "\n".join(lines) + "\n"


def e2e_oversized_context_trim_body(
    *,
    head: str,
    correlation_key: str,
    pad_chars: int = 60_000,
) -> str:
    """Тело письма для e2e trim: HEAD в начале, TAIL в конце, между ними padding."""
    pad = max(0, pad_chars)
    core = (
        f"{E2E_CTX_TRIM_HEAD_MARKER}\n"
        f"{head.rstrip()}\n"
        f"{'X' * pad}\n"
        f"{E2E_CTX_TRIM_TAIL_MARKER}\n"
        f"e2e_ctx_tail {correlation_key}\n"
    )
    return core


def e2e_oversized_context_trim_prior_turn_body(
    *,
    head: str,
    correlation_key: str,
    pad_chars: int = 25_000,
) -> str:
    """Предыдущий ход треда для trim e2e: HEAD + padding (без TAIL — он на текущем ходе)."""
    pad = max(0, pad_chars)
    return (
        f"{E2E_CTX_TRIM_HEAD_MARKER}\n"
        f"{head.rstrip()}\n"
        f"{'X' * pad}\n"
        f"e2e_ctx_prior {correlation_key}\n"
    )


def e2e_oversized_context_trim_current_turn_body(
    *,
    head: str,
    correlation_key: str,
) -> str:
    """Текущий ход для trim e2e: TAIL-маркер (HEAD/pad уже в unified с прошлого ingress)."""
    return (
        f"{E2E_CTX_TRIM_TAIL_MARKER}\n"
        f"{head.rstrip()}\n"
        f"e2e_ctx_tail {correlation_key}\n"
    )


def e2e_summarize_overflow_inject_body(
    *,
    head: str,
    correlation_key: str,
    pad_chars: int = 25_000,
) -> str:
    """Тело для e2e summarize overflow: HEAD + длинный PAD (исключается после суммаризации)."""
    pad = max(0, pad_chars)
    return (
        f"{E2E_SUM_ORIG_HEAD_MARKER}\n"
        f"{head.rstrip()}\n"
        f"{E2E_SUM_ORIG_PAD_MARKER}\n"
        f"{'P' * pad}\n"
        f"e2e_sum_tail {correlation_key}\n"
    )


def e2e_smtp_inject_ingress_route_wire_for_message_id(
    *,
    raw_message_id: str,
    origin: str = "pytest@localhost",
) -> str:
    """B62-wire ``X-Threlium-Route`` как после IMAP-моста (см. ``bridges.email._build_canonical``).

    ``raw_message_id`` — inner ``Message-ID`` инъекции (как в ``smtp_inject`` / ``--message-id``), со или без
    угловых скобок. ``origin`` — адрес отправителя (по умолчанию как в ``smtp_inject.py``).
    Для ``wiremock_state_seed_context`` и проверок LiteLLM используйте эту строку: она совпадает с
    ``reply_target_rfc_message_id`` + ``origin`` в JSON маршрута на письме в notmuch.
    """
    rt = ExternalRfcMidWire.parse_optional(str(raw_message_id).strip())
    route = EmailIngressRoute(
        channel="email",
        origin=str(origin).strip(),
        reply_target_rfc_message_id=rt,
    )
    return IngressRouteB62Wire.from_ingress_route(route).value.strip()


def e2e_thread_root_mid_for_message_id(raw_message_id: str) -> str:
    """Уголковый ``Message-ID`` старейшего в notmuch-треде письма с ``tag:route`` (как ``X-Threlium-Thread-Root``).

    Продукт берёт то же значение, что ``resolved.message_id_inner`` у
    :func:`~threlium.ingress_route_resolve.resolve_route_from_thread_oldest_route_tag_under_db`
    (один коррелятор на весь тред, все каналы). Тест подбирает ``raw_message_id`` / вход стаба
    так, что после ингресса этим ``Message-ID`` оказывается именно то письмо; для SMTP-инъекции
    это каноника ``email_ingress_notmuch_id_inner`` (короче route wire — лимит WireMock State).
    """
    inner = email_ingress_notmuch_id_inner(raw_message_id)
    return f"<{inner}>"


def e2e_matrix_thread_root_mid_for_sync_event(*, room_id: str, event_id: str) -> str:
    """Уголковый ``Message-ID`` корня matrix-треда по ``room_id`` + ``event_id`` из ответа ``/sync``.

    Совпадает с :mod:`threlium.bridges.matrix` (``RfcMessageIdWire.from_native(MatrixNativeId(v=1, …))``)
    и с ``X-Threlium-Thread-Root`` для LiteLLM / WireMock State ``correlation_key``.
    """
    native = MatrixNativeId(
        v=1,
        room_id=MatrixRoomId(room_id.strip()),
        event_id=MatrixRoomEventId(event_id.strip()),
    )
    mid_wire = RfcMessageIdWire.from_native(native)
    inner = NotmuchMessageIdInner.from_present_wire(mid_wire)
    return inner.as_angle_bracket_header()


def e2e_matrix_generate_room_ids() -> tuple[str, str]:
    """Сгенерировать уникальную пару ``(room_id, event_id)`` для Matrix e2e теста.

    ``room_id`` — ``!e2e_<hex>:mock``, ``event_id`` — ``$evt_<hex>``.
    Используется для ``register_room`` в WireMock State и вычисления ``correlation_key``.
    """
    room_id = f"!e2e_{uuid.uuid4().hex[:16]}:mock"
    event_id = f"$evt_{uuid.uuid4().hex[:20]}"
    return room_id, event_id


def e2e_telegram_generate_update_bundle(
    *,
    with_forum_topic: bool,
) -> tuple[int, int, int, int | None]:
    """Уникальные ``(chat_id, message_id, update_id, message_thread_id)`` для Telegram e2e.

    ``message_thread_id`` — ``None`` в личке; в forum topic — положительное int, отличное от
    ``message_id``, чтобы не путать смыслы полей.
    """
    chat_id = int(uuid.uuid4().int % 900_000_000) + 100_000_000
    message_id = int(uuid.uuid4().int % 90_000) + 10_000
    update_id = int(uuid.uuid4().int % 900_000_000) + 100_000_000
    mtid: int | None
    if with_forum_topic:
        mtid = int(uuid.uuid4().int % 90_000) + 50_000
        while mtid == message_id:
            mtid = int(uuid.uuid4().int % 90_000) + 50_000
    else:
        mtid = None
    return chat_id, message_id, update_id, mtid


def e2e_telegram_thread_root_mid_for_message(
    *,
    chat_id: int,
    message_id: int,
    message_thread_id: int | None,
) -> str:
    """Уголковый ``Message-ID`` корня треда (как ``X-Threlium-Thread-Root`` / WireMock ``correlation_key``).

    Совпадает с :mod:`threlium.bridges.telegram` (``RfcMessageIdWire.from_native(TelegramNativeId(…))``).
    """
    native = TelegramNativeId(
        v=1,
        chat_id=chat_id,
        message_id=message_id,
        message_thread_id=message_thread_id,
    )
    mid_wire = RfcMessageIdWire.from_native(native)
    inner = NotmuchMessageIdInner.from_present_wire(mid_wire)
    return inner.as_angle_bracket_header()


def e2e_smtp_inject_ingress_route_wire() -> str:
    """Устаревший b62 **без** ``reply_target_rfc_message_id`` (в JSON будет ``null``).

    **Не совпадает** с реальным ``X-Threlium-Route`` после моста для SMTP-инъекции: мост всегда кладёт
    ``reply_target_rfc_message_id`` из входящего ``Message-ID``. Для WireMock State и корреляции
    LiteLLM вызывайте :func:`e2e_smtp_inject_ingress_route_wire_for_message_id`.

    Оставлен для редких проверок «только origin» / обратной совместимости импортов.
    """
    return IngressRouteB62Wire.from_ingress_route(
        EmailIngressRoute(channel="email", origin="pytest@localhost")
    ).value.strip()


@dataclass(frozen=True)
class E2EComposeRuntime:
    """Нормализованный runtime-контекст e2e-стека, поднятого через Testcontainers."""

    project_name: str
    repo_root: Path
    greenmail_smtp_host: str
    greenmail_smtp_port: int
    greenmail_imap_host: str
    greenmail_imap_port: int
    wiremock_host: str
    wiremock_port: int
    sut_fresh_bake: bool = False


def e2e_flush_greenmail_inboxes(rt: E2EComposeRuntime) -> None:
    """EXPUNGE all messages from GreenMail IMAP inboxes (``test@``, ``pytest@``).

    Without this, ``threlium-bridge@email`` picks up stale messages from previous
    runs after SUT Maildir/notmuch flush.  The bridge now drops replies whose
    immediate ``In-Reply-To`` parent is missing from the wiped notmuch index
    (``orphan_skip``), so stale replies no longer feed ``irt_chain.py`` and the
    enrich worker no longer enters a restart loop.  Flushing is still required:
    stale root messages would otherwise be re-delivered as duplicates and the
    IMAP UID watermark must be reset between independent test sessions.
    """
    accounts = [
        (E2E_FETCHMAIL_USER, E2E_FETCHMAIL_PASS),
        (E2E_GREENMAIL_REPLY_USER, E2E_FETCHMAIL_PASS),
    ]
    host, port = rt.greenmail_imap_host, rt.greenmail_imap_port
    for user, password in accounts:
        try:
            with imaplib.IMAP4(host, port, timeout=int(TIMEOUT_POLL_SHORT)) as imap:
                imap.login(user, password)
                imap.select("INBOX")
                _, data = imap.search(None, "ALL")
                uids = data[0].split() if data[0] else []
                for uid in uids:
                    imap.store(uid, "+FLAGS", "\\Deleted")
                imap.expunge()
                imap.logout()
            log.info("greenmail_flush", user=user, expunged=len(uids))
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
su - {E2E_THRELIUM_USER} -s /bin/bash -c {su_wrap} </dev/null || true
echo "[e2e] SUT flushed: Maildir + lightrag + notmuch DB wiped, notmuch new done"
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


# Детерминированный bootstrap-корпус для e2e: реальный knowledge/ — десятки .md разной длины,
# у каждого свой chunks_count, поэтому число bootstrap chat/embedding-вызовов к WireMock плавает.
# В e2e оставляем ровно один синтетический документ: фиксированное имя => фиксированный
# doc_id (knowledge:bootstrap:md5(rel)[:16]) и стабильный (минимальный) набор вызовов индексации.
E2E_KNOWLEDGE_PROBE_FILENAME = "e2e_bootstrap_probe.md"
_E2E_KNOWLEDGE_PROBE_CONTENT = (
    "# E2E Bootstrap Probe\n"
    "\n"
    "Deterministic single-document corpus for the knowledge bootstrap indexing e2e.\n"
    "Threlium routes ingress mail through the FSM pipeline and indexes knowledge here.\n"
)


def e2e_install_deterministic_knowledge_corpus(rt: E2EComposeRuntime) -> None:
    """Заменить knowledge/ на SUT одним детерминированным probe-документом (e2e-среда, навсегда).

    Вызывать в cold-reset preflight **после** :func:`e2e_flush_sut_fsm_maildirs` (тот стирает
    ``$TH/lightrag``) и **до** старта engine: при старте bootstrap проиндексирует ровно probe.
    Бэкапа/возврата нет — это только тестовая среда; настоящий корпus возвращается lишь полным
    rebake образа.
    """
    th = shlex.quote(E2E_REMOTE_THRELIUM_HOME)
    script = f"""set -eu
TH={th}
KN="$TH/knowledge"
rm -rf "$KN"
mkdir -p "$KN"
cat > "$KN/{E2E_KNOWLEDGE_PROBE_FILENAME}" <<'PROBE'
{_E2E_KNOWLEDGE_PROBE_CONTENT}PROBE
rm -f "$TH/lightrag/kv_store_doc_status.json" 2>/dev/null || true
chown -R {E2E_THRELIUM_USER}:{E2E_THRELIUM_USER} "$KN"
echo "[e2e] deterministic knowledge corpus: $(find "$KN" -name '*.md' | wc -l) doc(s)"
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
            "sut_knowledge_corpus_install_warning",
            rc=completed.returncode,
            stdout_snippet=(completed.stdout or "")[:600],
        )
    else:
        log.info("sut_knowledge_corpus_deterministic", detail=(completed.stdout or "").strip())


def e2e_stop_threlium_user_pipeline_services(rt: E2EComposeRuntime) -> None:
    """Остановить на SUT ``threlium-engine`` и активные ``threlium-work@*`` / ``threlium-sweep@*`` (user systemd).

    Вызывается только из координированного preflight pytest перед полным сбросом WireMock/Maildir,
    чтобы не было HTTP к WM без сидированного State.
    """
    completed = service_exec(
        rt.project_name,
        "sut",
        ["bash", "-lc", e2e_stop_threlium_user_pipeline_bash()],
        repo_root=rt.repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if completed.returncode != 0:
        log.warning(
            "sut_pipeline_stop_warning",
            rc=completed.returncode,
            stdout_snippet=(completed.stdout or "")[:800],
        )


def e2e_sut_threlium_user_journal_rotate_and_vacuum(rt: E2EComposeRuntime) -> None:
    """Ротация и vacuum user-journal ``threlium`` на SUT (cold reset).

    Вызывать **после** :func:`e2e_stop_threlium_user_pipeline_services`, пока user systemd ещё жив.
    """
    completed = service_exec(
        rt.project_name,
        "sut",
        ["bash", "-lc", e2e_sut_threlium_user_journal_rotate_vacuum_bash()],
        repo_root=rt.repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    tail = (completed.stdout or "").strip()
    if tail:
        log.debug(
            "sut_journal_rotate_tail",
            body=clip_log_body(tail, max_len=2000),
        )
    if completed.returncode != 0:
        log.warning(
            "sut_journal_rotate_warning",
            rc=completed.returncode,
            stderr_snippet=(completed.stderr or "")[:600],
        )


def e2e_start_threlium_user_pipeline_services(rt: E2EComposeRuntime) -> None:
    """Запустить ``threlium-engine.service`` на SUT (user systemd) после cold-reset окружения."""
    completed = service_exec(
        rt.project_name,
        "sut",
        ["bash", "-lc", e2e_start_threlium_user_pipeline_bash()],
        repo_root=rt.repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if completed.returncode != 0:
        raise RuntimeError(
            "e2e: failed to start threlium-engine.service on SUT after pre-run reset; "
            f"rc={completed.returncode} stdout={(completed.stdout or '')[-1200:]!r}"
        )


def _diag(message: str) -> None:
    log.debug("mailflow_diag", detail=message)


def resolve_e2e_sut_image() -> str:
    """Тег образа `sut` для compose: явный THRELIUM_E2E_SUT_IMAGE или предсобранный по умолчанию."""
    return os.environ.get(E2E_SUT_IMAGE_ENV, E2E_BAKED_SUT_IMAGE).strip()


def e2e_rebuild_baked_image_requested() -> bool:
    """Принудительный полный bake перед тестами (THRELIUM_E2E_REBUILD_BAKED_IMAGE)."""
    raw = os.environ.get(E2E_REBUILD_BAKED_IMAGE_ENV)
    if raw is None or not str(raw).strip():
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _e2e_auto_bake_if_missing() -> bool:
    raw = os.environ.get(E2E_AUTO_BAKE_IF_MISSING_ENV)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


@contextlib.contextmanager
def _e2e_bake_image_lock() -> Iterator[None]:
    lock_path = Path(tempfile.gettempdir()) / "threlium_e2e_bake_image.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _docker_image_exists_locally(image: str) -> bool:
    try:
        r = subprocess.run(
            ["docker", "image", "inspect", image],
            check=False,
            capture_output=True,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _run_bake_for_e2e_sut_image(image_tag: str) -> None:
    env = os.environ.copy()
    env["THRELIUM_E2E_BAKE_IMAGE"] = image_tag
    subprocess.run(
        [str(E2E_BAKE_SCRIPT)],
        check=True,
        cwd=str(REPO_ROOT),
        env=env,
    )


def ensure_e2e_sut_image_exists(*, force_rebuild: bool = False) -> tuple[str, bool]:
    """Гарантирует наличие локального образа SUT для e2e; при необходимости запускает bake.

    Returns ``(image_tag, did_bake)`` — *did_bake* is ``True`` when this call actually
    executed the bake script (forced or auto), ``False`` when the image already existed.
    """
    image = resolve_e2e_sut_image()
    if force_rebuild:
        with _e2e_bake_image_lock():
            _diag(
                f"SUT image rebuild ({E2E_REBUILD_BAKED_IMAGE_ENV}=1 or wipe_bake): {image} "
                f"(fresh upstream + full site.yml + docker commit)"
            )
            _run_bake_for_e2e_sut_image(image)
        return image, True
    if _docker_image_exists_locally(image):
        return image, False
    if image != E2E_BAKED_SUT_IMAGE:
        return image, False
    if not _e2e_auto_bake_if_missing():
        raise RuntimeError(
            f"e2e SUT image {image!r} not found locally. Either run "
            f"`pytest -n0 tests/e2e/wipe_bake.py` (or set {E2E_REBUILD_BAKED_IMAGE_ENV}=1 in CI) "
            f"to bake it from upstream, run {E2E_BAKE_SCRIPT} manually with "
            f"THRELIUM_E2E_BAKE_IMAGE={image}, or allow auto-bake "
            f"(unset {E2E_AUTO_BAKE_IF_MISSING_ENV} or set to 1)."
        )
    with _e2e_bake_image_lock():
        if _docker_image_exists_locally(image):
            return image, False
        _diag(
            f"SUT image {image!r} missing; running bake ({E2E_AUTO_BAKE_IF_MISSING_ENV} defaults to enabled)"
        )
        _run_bake_for_e2e_sut_image(image)
    return image, True


def mailflow_diag_block(title: str, body: str, *, max_chars: int = 20000) -> None:
    """Многострочный дамп в stderr для анализа mailflow (IMAP bridge / notmuch / systemd)."""
    truncated = body if len(body) <= max_chars else body[:max_chars] + "\n... [mailflow_diag_block truncated] ...\n"
    log.debug(
        "mailflow_diag_block",
        title=title,
        body=clip_log_body(truncated, max_len=max_chars),
    )


def mailflow_log_phase(message: str) -> None:
    """Короткая метка фазы mailflow-теста (время относительно фикстуры — в сообщении)."""
    _diag(f"[mailflow] {message}")


def poll_until(
    fn: Callable[[], T | None],
    *,
    timeout: float,
    interval: float = POLL_INTERVAL,
    desc: str = "condition",
) -> T:
    """Fixed-interval poll backed by tenacity. Returns first non-None result from *fn*."""
    _diag(f"poll start: {desc} (timeout={timeout}s)")
    report_at = time.monotonic() + min(10.0, max(3.0, float(timeout) / 4.0))

    def _before_sleep(retry_state: Any) -> None:
        nonlocal report_at
        now = time.monotonic()
        if now >= report_at:
            _diag(f"poll progress: {desc} (attempt #{retry_state.attempt_number})")
            report_at = now + min(10.0, max(3.0, float(timeout) / 4.0))

    try:
        result = Retrying(
            retry=retry_if_result(lambda r: r is None) | retry_if_exception_type(Exception),
            stop=stop_after_delay(timeout),
            wait=wait_fixed(interval),
            before_sleep=_before_sleep,
        )(fn)
    except RetryError as e:
        _diag(f"poll timeout: {desc}")
        last = e.last_attempt.exception() if e.last_attempt.failed else None
        msg = f"timeout waiting for {desc} ({timeout}s)"
        if last:
            msg += f": {last!r}"
        raise TimeoutError(msg) from last
    _diag(f"poll done: {desc}")
    return result  # type: ignore[return-value]


def poll_until_backoff(
    fn: Callable[[], T | None],
    *,
    timeout: float,
    desc: str = "condition",
    progress_extra: Callable[[], str] | None = None,
) -> T:
    """Exponential-backoff poll backed by tenacity. Returns first non-None result from *fn*."""
    _diag(f"poll(backoff) start: {desc} (timeout={timeout}s)")
    report_at = time.monotonic() + min(10.0, max(3.0, float(timeout) / 4.0))

    def _before_sleep(retry_state: Any) -> None:
        nonlocal report_at
        now = time.monotonic()
        if now >= report_at:
            extra = ""
            if progress_extra is not None:
                try:
                    extra = f" | {progress_extra()}"
                except Exception as pe:
                    extra = f" | (progress_extra failed: {pe!r})"
            _diag(f"poll(backoff) progress: {desc}{extra} (attempt #{retry_state.attempt_number})")
            report_at = now + min(10.0, max(3.0, float(timeout) / 4.0))

    try:
        result = Retrying(
            retry=retry_if_result(lambda r: r is None) | retry_if_exception_type(Exception),
            stop=stop_after_delay(timeout),
            wait=wait_exponential(multiplier=0.25, min=0.5, max=5),
            before_sleep=_before_sleep,
        )(fn)
    except RetryError as e:
        _diag(f"poll(backoff) timeout: {desc}")
        last = e.last_attempt.exception() if e.last_attempt.failed else None
        msg = f"timeout waiting for {desc} ({timeout}s)"
        if last:
            msg += f": {last!r}"
        raise TimeoutError(msg) from last
    _diag(f"poll(backoff) done: {desc}")
    return result  # type: ignore[return-value]


def _docker_client() -> Any:
    return docker.from_env()


def _compose_container(project_name: str, service: str) -> Any:
    client = _docker_client()
    containers = client.containers.list(
        all=True,
        filters={
            "label": [
                f"com.docker.compose.project={project_name}",
                f"com.docker.compose.service={service}",
            ]
        },
    )
    if not containers:
        raise RuntimeError(f"container not found for compose service={service!r}, project={project_name!r}")
    running = [c for c in containers if c.status == "running"]
    return running[0] if running else containers[0]


def _compose_project_containers(project_name: str) -> list[Any]:
    client = _docker_client()
    return client.containers.list(
        all=True,
        filters={"label": [f"com.docker.compose.project={project_name}"]},
    )


def e2e_shared_compose_stack_is_healthy(project_name: str) -> bool:
    """True, если для ``project_name`` все сервисы из ``E2E_SHARED_COMPOSE_SERVICES`` имеют running-контейнер."""
    try:
        containers = _compose_project_containers(project_name)
    except Exception:
        return False
    by_service: dict[str, list[Any]] = {}
    for c in containers:
        labels = c.labels or {}
        svc = labels.get("com.docker.compose.service") or ""
        if not svc:
            continue
        by_service.setdefault(svc, []).append(c)
    for required in E2E_SHARED_COMPOSE_SERVICES:
        running = [c for c in by_service.get(required, []) if getattr(c, "status", None) == "running"]
        if not running:
            return False
    return True


def discover_live_e2e_project_name() -> str | None:
    """Имя уже поднятого e2e compose-проекта **без** фикстуры ``compose_stack`` / bake.

    Используется сценариями «только проверки на живом стеке» (см. ``test_mailflow_live_only_e2e``).

    Первый *healthy* проект среди *running* контейнеров ``service=sut``, чей
    ``com.docker.compose.project`` начинается с ``{E2E_PROJECT}_`` (лексикографически первый).

    Политика: один shared-стек после ``wipe_bake`` / ``compose_stack``.

    ``None`` — Docker недоступен или нет ни одного healthy стека с нужным префиксом.
    """
    try:
        client = _docker_client()
        containers = client.containers.list(filters={"status": "running"})
    except Exception:
        return None
    candidates: set[str] = set()
    prefix = f"{E2E_PROJECT}_"
    for c in containers:
        labels = c.labels or {}
        if labels.get("com.docker.compose.service") != "sut":
            continue
        pn = labels.get("com.docker.compose.project") or ""
        if isinstance(pn, str) and pn.startswith(prefix):
            candidates.add(pn)
    for pn in sorted(candidates):
        if e2e_shared_compose_stack_is_healthy(pn):
            return pn
    return None


def discover_compose_projects_for_e2e_compose_dir() -> list[str]:
    """Уникальные ``com.docker.compose.project`` для контейнеров из каталога ``COMPOSE_DIR``.

    Совпадение по ``com.docker.compose.project.working_dir`` (Compose v2) или по
    ``com.docker.compose.project.config_files``, если рабочая директория в лейблах пуста.
    Так снимаются стеки с любым ``docker compose -p`` (включая ``threlium_dbg``,
    ``threlium_e2e_bake``), а не только ``{E2E_PROJECT}_*``.
    """
    compose_dir = COMPOSE_DIR.resolve()
    compose_file = E2E_COMPOSE_FILE.resolve()
    client = _docker_client()
    try:
        containers = client.containers.list(
            all=True,
            filters={"label": ["com.docker.compose.project"]},
        )
    except Exception:
        return []
    projects: set[str] = set()
    for c in containers:
        labels = c.labels or {}
        pn = (labels.get("com.docker.compose.project") or "").strip()
        if not pn:
            continue
        wd_raw = (labels.get("com.docker.compose.project.working_dir") or "").strip()
        if wd_raw:
            try:
                if Path(wd_raw).resolve() == compose_dir:
                    projects.add(pn)
                    continue
            except OSError:
                pass
        cfg = (labels.get("com.docker.compose.project.config_files") or "").strip()
        if not cfg:
            continue
        for part in (p.strip() for p in cfg.split(",") if p.strip()):
            try:
                if Path(part).resolve() == compose_file:
                    projects.add(pn)
                    break
            except OSError:
                tail = "tests/e2e/compose/docker-compose.yml"
                if part.replace("\\", "/").endswith(tail):
                    projects.add(pn)
                    break
    return sorted(projects)


def discover_stale_compose_projects(*, project_prefix: str = E2E_PROJECT) -> list[str]:
    """Совместимость API: *project_prefix* игнорируется.

    См. :func:`discover_compose_projects_for_e2e_compose_dir`.
    """
    _ = project_prefix
    return discover_compose_projects_for_e2e_compose_dir()


def stop_compose_projects_for_e2e_compose_dir(
    *, timeout: int = int(TIMEOUT_POLL_SHORT)
) -> list[str]:
    """``docker compose down`` для всех проектов из ``COMPOSE_DIR`` (любой ``-p``)."""
    stale_projects = discover_compose_projects_for_e2e_compose_dir()
    if not stale_projects:
        return []

    cleaned: list[str] = []
    warnings: list[str] = []
    for project_name in stale_projects:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(E2E_COMPOSE_FILE),
                "-p",
                project_name,
                "down",
                "--remove-orphans",
                "--volumes",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(COMPOSE_DIR),
        )
        remaining = _compose_project_containers(project_name)
        if remaining:
            names = ", ".join(sorted(c.name for c in remaining))
            raise RuntimeError(
                "failed to cleanup stale e2e compose project "
                f"{project_name!r}; remaining containers: {names}\n"
                f"docker compose down exit={result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        if result.returncode != 0:
            warnings.append(
                f"cleanup warning for {project_name!r}: "
                f"docker compose down exit={result.returncode}"
            )
        cleaned.append(project_name)

    for warning in warnings:
        log.warning("compose_cleanup_warning", detail=warning)
    return cleaned


def stop_stale_compose_projects(
    *, project_prefix: str = E2E_PROJECT, timeout: int = int(TIMEOUT_POLL_SHORT)
) -> list[str]:
    """Останавливает все compose-проекты из каталога ``COMPOSE_DIR`` до нового прогона.

    Имя сохранено по истории; *project_prefix* игнорируется — см.
    :func:`stop_compose_projects_for_e2e_compose_dir`.
    """
    _ = project_prefix
    return stop_compose_projects_for_e2e_compose_dir(timeout=timeout)


def compose_down_project(project_name: str, *, timeout: int = int(TIMEOUT_POLL_SHORT)) -> None:
    """``docker compose down --remove-orphans --volumes`` for a single project."""
    subprocess.run(
        [
            "docker", "compose",
            "-f", str(E2E_COMPOSE_FILE),
            "-p", project_name,
            "down", "--remove-orphans", "--volumes",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(COMPOSE_DIR),
    )


def cleanup_stale_bundle_archives(*, artifacts_root: Path | None = None) -> int:
    """Удаляет старые post-deploy bundle архивы из ansible/artifacts."""
    root = artifacts_root or (REPO_ROOT / "ansible" / "artifacts")
    if not root.exists():
        _diag(f"bundle cleanup skipped: {root} does not exist")
        return 0

    removed = 0
    for archive_path in root.rglob("threlium-bundle-*.tar.gz"):
        if not archive_path.is_file():
            continue
        try:
            archive_path.unlink()
            removed += 1
        except OSError as e:
            _diag(f"bundle cleanup warning: failed to remove {archive_path}: {e!r}")

    _diag(f"bundle cleanup done: removed={removed} root={root}")
    return removed


def _mapped_port(project_name: str, service: str, container_port: int) -> tuple[str, int]:
    c = _compose_container(project_name, service)
    c.reload()
    key = f"{container_port}/tcp"
    binding = (c.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}).get(key)
    if not binding:
        raise RuntimeError(
            f"port mapping not found for service={service!r}, port={container_port}, project={project_name!r}"
        )
    host_ip = binding[0].get("HostIp") or "127.0.0.1"
    if host_ip in ("0.0.0.0", "::"):
        host_ip = "127.0.0.1"
    return host_ip, int(binding[0]["HostPort"])


def discover_runtime(project_name: str, *, repo_root: Path | None = None) -> E2EComposeRuntime:
    smtp_host, smtp_port = _mapped_port(project_name, "greenmail", 3025)
    imap_host, imap_port = _mapped_port(project_name, "greenmail", 3143)
    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    return E2EComposeRuntime(
        project_name=project_name,
        repo_root=repo_root or REPO_ROOT,
        greenmail_smtp_host=smtp_host,
        greenmail_smtp_port=smtp_port,
        greenmail_imap_host=imap_host,
        greenmail_imap_port=imap_port,
        wiremock_host=wm_host,
        wiremock_port=wm_port,
    )


def wait_for_wiremock_ready(project_name: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> tuple[str, int]:
    """Ждём, пока WireMock Admin API отвечает на ``GET /__admin/mappings``."""
    host, port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    import urllib.error
    import urllib.request

    def _probe() -> tuple[str, int] | None:
        try:
            with urllib.request.urlopen(
                f"http://{host}:{port}/__admin/mappings",
                timeout=float(TIMEOUT_POLL_SHORT),
            ) as r:
                if r.status == 200:
                    return (host, port)
        except (urllib.error.URLError, OSError, TimeoutError):
            return None
        return None

    return poll_until(_probe, timeout=timeout, desc=f"wiremock admin ready http://{host}:{port}")


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
                msg = message_from_bytes(raw_header)
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
                msg = message_from_bytes(raw_header)
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
                msg = message_from_bytes(raw_header)
                if message_id and msg.get("Message-ID", "").strip("<>") != message_id.strip("<>"):
                    continue
                if subject and _decoded_email_subject(msg) != subject:
                    continue
                imap.logout()
                return None
            imap.logout()
            return True

    poll_until_backoff(_probe, timeout=timeout, desc=f"greenmail host IMAP inbox message gone on {host}:{port}")


# From заголовок SMTP readiness-письма.
# Сид WireMock State: :func:`e2e_thread_root_mid_for_message_id` по probe_msg_id.
E2E_GREENMAIL_READINESS_PROBE_FROM = "pytest-readiness@localhost"


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
        from .wiremock_client import wiremock_state_seed_context

        ck = e2e_thread_root_mid_for_message_id(probe_msg_id)
        wiremock_state_seed_context(wiremock_seed_base, ck)

    msg = EmailMessage()
    msg["From"] = E2E_GREENMAIL_READINESS_PROBE_FROM
    msg["To"] = e2e_greenmail_mailbox_address(rcpt_local)
    msg["Subject"] = probe_subject
    msg["Message-ID"] = f"<{probe_msg_id}>"
    msg.set_content("readiness probe")

    with smtplib.SMTP(gm_smtp_host, gm_smtp_port, timeout=int(TIMEOUT_POLL_SHORT)) as smtp:
        smtp.send_message(msg)

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
            + _E2E_REMOTE_PROBE_LOGGER_BOOT
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
            "    msg = email.message_from_bytes(hdr)\n"
            f"    {mid_check}"
            f"    {subj_check}"
            "    m.logout(); _probe_out.info('INBOX_COUNT=%d MATCH=1' % (count,)); raise SystemExit(0)\n"
            "m.logout(); _probe_out.info('INBOX_COUNT=%d MATCH=0' % (count,)); raise SystemExit(2)\n"
        )
    else:
        script = (
            "import imaplib\n"
            + _E2E_REMOTE_PROBE_LOGGER_BOOT
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
            + _E2E_REMOTE_PROBE_LOGGER_BOOT
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
            "    msg = email.message_from_bytes(hdr)\n"
            f"    {mid_check}"
            f"    {subj_check}"
            "    m.logout(); _probe_out.info('UNSEEN=%d GONE=0' % (len(ids),)); raise SystemExit(2)\n"
            "m.logout(); _probe_out.info('UNSEEN=%d GONE=1' % (len(ids),)); raise SystemExit(0)\n"
        )
    else:
        script = (
            "import imaplib\n"
            + _E2E_REMOTE_PROBE_LOGGER_BOOT
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


def service_exec(
    project_name: str,
    service: str,
    argv: list[str],
    *,
    repo_root: Path | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    del repo_root, timeout
    _diag(f"exec start: service={service} argv={argv[:3]}...")
    container = _compose_container(project_name, service)
    result = container.exec_run(cmd=argv, stdout=True, stderr=True, tty=False, demux=False)
    output = result.output or b""
    if isinstance(output, bytes):
        text_out = output.decode("utf-8", errors="replace")
    else:
        text_out = str(output)
    completed = subprocess.CompletedProcess(args=argv, returncode=int(result.exit_code), stdout=text_out, stderr="")
    _diag(f"exec done: service={service} rc={completed.returncode}")
    return completed


def compose_logs(project_name: str, *, repo_root: Path | None = None) -> str:
    del repo_root
    client = _docker_client()
    containers = client.containers.list(
        all=True,
        filters={"label": [f"com.docker.compose.project={project_name}"]},
    )
    if not containers:
        return f"(no containers found for compose project {project_name})\n"
    parts: list[str] = []
    for c in sorted(containers, key=lambda it: it.name):
        parts.append(f"--- {c.name} ({c.status}) ---\n")
        try:
            parts.append(c.logs(stdout=True, stderr=True, tail=500).decode("utf-8", errors="replace"))
        except Exception as e:  # pragma: no cover
            parts.append(f"(failed to fetch logs: {e!r})\n")
    return "".join(parts)


def mailflow_wait_fsm_maildir_activity(
    project_name: str,
    *,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
    message_id: str | None = None,
    subject: str | None = None,
) -> None:
    """Ждём признаков FSM: файлы в ``new`` у ingress/enrich/reasoning **или** письмо в notmuch по якорю.

    Если задан только ``message_id``, notmuch-ветка использует ``id:…`` без subject.
    Если заданы оба — запрос ``id:… and subject:'…'``. Если ``message_id`` нет — только Maildir.
    """
    root = repo_root or REPO_ROOT
    if message_id is None:
        query_py = "None"
    elif subject is None:
        query_py = repr(notmuch_id_search_term(message_id))
    else:
        query_py = repr(f"{notmuch_id_search_term(message_id)} and subject:'{subject}'")
    py = f"""import os, pathlib, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}for _envf in ("{E2E_REMOTE_REPO_PATH}/env/threlium.env",):
    try:
        with open(_envf) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))
    except OSError:
        pass
TH = os.environ.get("THRELIUM_HOME", "{E2E_REMOTE_THRELIUM_HOME}")
tot = 0
for sid in ("ingress", "enrich", "reasoning"):
    p = pathlib.Path(TH) / "stages" / sid / "Maildir" / "new"
    if p.is_dir():
        tot += sum(1 for x in p.iterdir() if x.is_file() and not x.name.startswith("."))
_probe_out.info("FSM_NEW_TOTAL=" + str(tot))
if tot > 0:
    sys.exit(0)
q = {query_py}
if q is None:
    sys.exit(1)
os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
r = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((r.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_MATCH=" + str(n))
sys.exit(0 if n > 0 else 1)
"""

    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY\n"]

    def _probe() -> bool | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        return True if r.returncode == 0 else None

    def _extra() -> str:
        r2 = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        return (r2.stdout or "").strip().replace("\n", " ")[:400]

    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc="FSM Maildir activity or notmuch anchor",
        progress_extra=_extra,
    )


def mailflow_fsm_maildir_systemd_snapshot(project_name: str, *, repo_root: Path | None = None) -> str:
    """Снимок Maildir (stages/) + журнал threlium user-юнитов (``runuser`` + ``--user-unit``) и широкий хвост."""
    root = repo_root or REPO_ROOT
    ids_bash = " ".join(E2E_FSM_MAILBOX_STAGE_IDS)
    script = f"""
set +e
if [ -f {E2E_REMOTE_REPO_PATH}/env/threlium.env ]; then
  set -a
  # shellcheck disable=SC1091
  . {E2E_REMOTE_REPO_PATH}/env/threlium.env
  set +a
fi
TH="${{THRELIUM_HOME:-{E2E_REMOTE_THRELIUM_HOME}}}"
echo "THRELIUM_HOME=$TH"
echo "--- Maildir file counts (new/cur/tmp, non-hidden; stages/* only) ---"
for sub in $(for id in {ids_bash}; do echo "stages/$id/Maildir"; done); do
  base="$TH/$sub"
  for box in new cur tmp; do
    d="$base/$box"
    if [ -d "$d" ]; then
      n=$(find "$d" -maxdepth 1 -type f ! -name '.*' 2>/dev/null | wc -l)
    else
      n=0
    fi
    echo "$sub/$box: $n"
  done
done
echo "--- threlium user-unit journals (runuser {E2E_THRELIUM_USER}, --user-unit) ---"
{E2E_SUT_THRELIUM_USER_UNIT_JOURNAL}
echo "--- journal broad tail (all units) ---"
journalctl -n 200 --no-pager 2>&1 || true
echo "--- LightRAG working_dir (INDEX §5b) ---"
if [ -d "$TH/lightrag" ]; then
  ls -la "$TH/lightrag" 2>&1 | head -n 40 || true
else
  echo "(no $TH/lightrag)"
fi
"""
    r = service_exec(project_name, "sut", ["bash", "-lc", script], repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    return (r.stdout or "") + (r.stderr or "") + f"\n(exit {r.returncode})\n"


def dump_failure_artifacts(project_name: str, *, repo_root: Path | None = None) -> str:
    """Собирает вывод диагностик в одну строку для stderr теста.

    Широкий хвост журнала — **от root** (``journalctl -n`` без ``--user``).
    Стабильные threlium user-юниты — через ``runuser`` + ``--user-unit``
    (:data:`E2E_SUT_THRELIUM_USER_UNIT_JOURNAL` в ``sut_user_systemd``), затем широкий хвост.
    Системный стек mail-archive (Cockpit):
    :data:`E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT`. Снимок Maildir —
    ``mailflow_fsm_maildir_systemd_snapshot``.
    Если после стабилизации enrich остаются аномалии в заголовках — см. ``journalctl``
    для failed ``threlium-work@`` / ``threlium-bridge@`` и ``~/.fdm.conf``.
    """
    chunks: list[str] = []
    root = repo_root or REPO_ROOT

    chunks.append("=== fsm maildir + systemd snapshot ===\n")
    chunks.append(mailflow_fsm_maildir_systemd_snapshot(project_name, repo_root=root))

    chunks.append("=== container logs ===\n")
    chunks.append(compose_logs(project_name, repo_root=root))

    for label, cmd in [
        (
            "mail-archive system units (cockpit)",
            ["bash", "-lc", E2E_SUT_MAIL_ARCHIVE_SYSTEM_DIAG_SCRIPT],
        ),
        (f"loginctl {E2E_THRELIUM_USER}", ["bash", "-lc", f"loginctl show-user {E2E_THRELIUM_USER} 2>&1 || true"]),
        (
            "journal threlium user-units + broad tail",
            ["bash", "-lc", E2E_SUT_THRELIUM_USER_UNIT_JOURNAL + "\necho '--- journal broad tail ---'\njournalctl -n 200 --no-pager 2>&1 || true"],
        ),
        (
            "notmuch count",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>&1 || true"],
        ),
        (
            "fdm.conf rendered in sut",
            [
                "bash",
                "-lc",
                f"echo '--- readlink ~/.fdm.conf ---'; readlink -f {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
                f"echo '--- {E2E_REMOTE_POSIX_HOME}/.fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
                f"echo '--- repo config/fdm.conf ---'; "
                f"sed -n '1,260p' {E2E_REMOTE_REPO_PATH}/config/fdm.conf 2>&1 || true",
            ],
        ),
        ("threlium.yaml", ["bash", "-lc", f"cat {E2E_REMOTE_THRELIUM_HOME}/config/threlium.yaml 2>&1 || true"]),
        ("threlium.env", ["bash", "-lc", f"cat {E2E_REMOTE_REPO_PATH}/env/threlium.env 2>&1 || true"]),
        (
            "bridge-email systemd service",
            ["bash", "-lc", _email_bridge_systemd_diag_script()],
        ),
        ("msmtprc", ["bash", "-lc", f"cat {E2E_REMOTE_POSIX_HOME}/.msmtprc 2>&1 || true"]),
        (
            "journal threlium-bridge@email (fdm path)",
            [
                "bash",
                "-lc",
                f"{e2e_threlium_user_unit_journalctl_bash('threlium-bridge@email.service', 120)}",
            ],
        ),
    ]:
        chunks.append(f"\n=== {label} ===\n")
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
        chunks.append(r.stdout + r.stderr + f"\n(exit {r.returncode})\n")

    chunks.append("\n=== greenmail (if any) ===\n")
    try:
        greenmail = _compose_container(project_name, "greenmail")
        chunks.append(greenmail.logs(stdout=True, stderr=True, tail=500).decode("utf-8", errors="replace"))
    except Exception as e:  # pragma: no cover
        chunks.append(f"(failed to fetch greenmail logs: {e!r})\n")

    chunks.append("\n=== wiremock (OpenAI/Matrix stubs) ===\n")
    try:
        from .wiremock_client import describe_wiremock_admin_state

        wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
        chunks.append(describe_wiremock_admin_state(wm_host, wm_port, project_name=project_name))
    except Exception as e:  # pragma: no cover
        chunks.append(f"(failed to describe wiremock state: {e!r})\n")
    return "".join(chunks)


def tcp_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=float(TIMEOUT_POLL_SHORT)):
            return True
    except OSError:
        return False


def ensure_e2e_ansible_collections(*, repo_root: Path | None = None) -> None:
    """Ставит Galaxy-коллекции для e2e.

    Нужны ``community.docker`` (inventory), ``community.general``
    (``archive`` в site.yml) и ``ansible.posix`` (``authorized_key`` при SSH-hardening).
    """
    root = repo_root or REPO_ROOT
    ansible_dir = root / "ansible"
    coll_install_root = ansible_dir / "collections"
    requirements = coll_install_root / "requirements.yml"
    ac = coll_install_root / "ansible_collections"
    marker_docker = ac / "community" / "docker" / "plugins" / "connection" / "docker.py"
    marker_general = ac / "community" / "general" / "plugins" / "modules" / "archive.py"
    marker_posix = ac / "ansible" / "posix" / "plugins" / "modules" / "authorized_key.py"
    if marker_docker.is_file() and marker_general.is_file() and marker_posix.is_file():
        return
    if not requirements.is_file():
        raise RuntimeError(
            f"e2e Ansible collections requirements missing: {requirements} "
            "(need community.docker + community.general + ansible.posix; see file contents)"
        )
    coll_install_root.mkdir(parents=True, exist_ok=True)
    if not shutil.which("ansible-galaxy"):
        raise RuntimeError("ansible-galaxy not on PATH (install ansible-core extras e2e)")
    t0 = time.monotonic()
    _diag("ansible-galaxy collection install (e2e) start")
    gal_env = {
        **os.environ,
        "ANSIBLE_CONFIG": str((ansible_dir / E2E_ANSIBLE_CONFIG_NAME).resolve()),
    }
    r = subprocess.run(
        [
            "ansible-galaxy",
            "collection",
            "install",
            "-r",
            str(requirements),
            "-p",
            str(coll_install_root),
            "--force",
        ],
        cwd=str(ansible_dir),
        check=False,
        text=True,
        timeout=int(TIMEOUT_POLL_SHORT),
        stdout=sys.stderr,
        stderr=sys.stderr,
        env=gal_env,
    )
    if r.returncode != 0:
        raise RuntimeError(
            "ansible-galaxy collection install failed (e2e needs collections from requirements.yml). "
            f"command: ansible-galaxy collection install -r {requirements} -p {coll_install_root}"
        )
    if not marker_docker.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but docker connection plugin missing: " + str(marker_docker)
        )
    if not marker_general.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but community.general.archive missing: " + str(marker_general)
        )
    if not marker_posix.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but ansible.posix.authorized_key missing: " + str(marker_posix)
        )
    _diag(f"ansible-galaxy collection install (e2e) done (elapsed={time.monotonic() - t0:.1f}s)")


def run_e2e_site_playbook(
    project_name: str,
    *,
    checkout: str,
    repo_root: Path | None = None,
    ansible_tags: str | None = None,
    ansible_extra_vars: dict[str, Any] | None = None,
) -> None:
    """``site.yml`` в контейнер ``sut`` (e2e inventory).

    По умолчанию прогоняет полный плейбук: задачи ``deploy`` и ``deploy``+``refresh``; блоки с ``never``+``refresh`` (чистка harness) без явного ``--tags refresh`` не выполняются.
    При ``ansible_tags="refresh"`` (``wipe_sync``): цепочка файлов/env/шаблонов (``deploy``+``refresh`` в ``site.yml``, **без** ``pip``) + harness (``never``+``refresh``); без apt и без полного acceptance.

    * ``THRELIUM_E2E_ANSIBLE_TAGS``      — ``--tags`` (например ``refresh`` для sync кода/env/шаблонов + harness e2e);
    * ``THRELIUM_E2E_ANSIBLE_SKIP_TAGS`` — ``--skip-tags`` (например ``refresh`` при необходимости).

    Пустые / не заданные — полный ``site.yml`` без фильтрации по тегам.

    Явный аргумент ``ansible_tags`` переопределяет env.

    ``ansible_extra_vars`` — дополнительный JSON-файл ``-e @…`` **после**
    переменных инвентаря ``inventory/e2e/group_vars/threlium_hosts.yml`` (см. symlink на
    ``group_vars/e2e.yml``) и ``e2e_sut_container_id`` (перекрывает переменные для одного прогона).

    Вывод ``ansible-playbook`` наследует stdio pytest (без перенаправления всего в ``stderr``);
    при наличии ``stdbuf(1)`` — построчная буферизация. Уровень Ansible: env
    ``THRELIUM_E2E_ANSIBLE_VERBOSITY`` — ``0`` без ``-v``, ``1``…``4`` → ``-v``…``-vvvv``
    (по умолчанию ``1``, чтобы в ``pytest -s`` были видны ход задач между длинными ``apt``).
    """
    del checkout
    root = repo_root or REPO_ROOT
    started = time.monotonic()
    _diag("ansible deploy start")
    container_id = _compose_container(project_name, "sut").id
    if not container_id:
        raise RuntimeError(f"sut container id is empty for compose project {project_name!r}")
    cmd: list[str] = ["ansible-playbook"]
    verb_raw = os.environ.get("THRELIUM_E2E_ANSIBLE_VERBOSITY", "1").strip()
    if verb_raw and verb_raw != "0":
        try:
            vn = min(max(int(verb_raw), 1), 4)
        except ValueError:
            vn = 1
        cmd.append("-" + ("v" * vn))
    cmd.extend(
        [
            "playbooks/site.yml",
            "-i",
            E2E_ANSIBLE_INVENTORY_PATH,
            "-e",
            f"e2e_sut_container_id={container_id}",
        ]
    )
    extra_file: Path | None = None
    if ansible_extra_vars:
        tf = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="ansible-e2e-extra-",
            delete=False,
            encoding="utf-8",
        )
        with tf:
            json.dump(ansible_extra_vars, tf)
        extra_file = Path(tf.name)
        cmd.extend(["-e", f"@{extra_file}"])
    if ansible_tags is not None:
        ansible_tags_val = ansible_tags.strip()
    else:
        ansible_tags_val = os.environ.get("THRELIUM_E2E_ANSIBLE_TAGS", "").strip()
    ansible_skip_tags = os.environ.get("THRELIUM_E2E_ANSIBLE_SKIP_TAGS", "").strip()
    if ansible_tags_val:
        cmd += ["--tags", ansible_tags_val]
    if ansible_skip_tags:
        cmd += ["--skip-tags", ansible_skip_tags]
    if not shutil.which("ansible-playbook"):
        raise RuntimeError(
            "ansible-playbook not found on host; "
            "install it in test environment (e.g. pip install ansible-core)."
        )
    ansible_dir = root / "ansible"
    e2e_cfg = (ansible_dir / E2E_ANSIBLE_CONFIG_NAME).resolve()
    if not e2e_cfg.is_file():
        raise RuntimeError(f"missing e2e ansible config: {e2e_cfg}")
    ensure_e2e_ansible_collections(repo_root=root)
    run_env = {
        **os.environ,
        "ANSIBLE_CONFIG": str(e2e_cfg),
        "PYTHONUNBUFFERED": "1",
    }
    exec_cmd = list(cmd)
    if shutil.which("stdbuf"):
        exec_cmd = ["stdbuf", "-oL", "-eL", *exec_cmd]
    try:
        r = subprocess.run(
            exec_cmd,
            check=False,
            text=True,
            timeout=TIMEOUT_ANSIBLE_PLAYBOOK,
            cwd=str(ansible_dir),
            env=run_env,
        )
    finally:
        if extra_file is not None:
            extra_file.unlink(missing_ok=True)
    if r.returncode != 0:
        raise RuntimeError(
            "ansible-playbook failed (see streamed logs above).\ncommand: "
            + " ".join(exec_cmd)
            + "\n"
            + dump_failure_artifacts(project_name, repo_root=root)
        )
    _diag(f"ansible deploy done (elapsed={time.monotonic() - started:.1f}s)")


_E2E_LEAVE_STACK_RUNNING_ENV = "THRELIUM_E2E_LEAVE_STACK_RUNNING"
_E2E_DEFAULT_HOP_BUDGET = {"budget_root": 256, "budget_sub": 256}


def e2e_refresh_hop_budget_sub(
    project_name: str,
    *,
    budget_sub: int,
    repo_root: Path | None = None,
) -> None:
    """Redeploy ``threlium.yaml`` ``hop.budget_sub`` via ansible ``refresh`` (restarts engine)."""
    os.environ[_E2E_LEAVE_STACK_RUNNING_ENV] = "1"
    hop = dict(_E2E_DEFAULT_HOP_BUDGET)
    hop["budget_sub"] = budget_sub
    run_e2e_site_playbook(
        project_name,
        checkout="/unused",
        repo_root=repo_root or REPO_ROOT,
        ansible_tags="refresh",
        ansible_extra_vars={"threlium_hop": hop},
    )


def e2e_refresh_hop_budget_default(
    project_name: str,
    *,
    repo_root: Path | None = None,
) -> None:
    """Restore e2e inventory ``hop`` defaults (``budget_sub=256``) after a scoped override."""
    os.environ[_E2E_LEAVE_STACK_RUNNING_ENV] = "1"
    run_e2e_site_playbook(
        project_name,
        checkout="/unused",
        repo_root=repo_root or REPO_ROOT,
        ansible_tags="refresh",
    )


def copy_repo_and_run_ansible(
    project_name: str,
    *,
    checkout: str,
    repo_root: Path | None = None,
) -> None:
    """Устаревшее имя; используйте ``run_e2e_site_playbook``."""
    run_e2e_site_playbook(project_name, checkout=checkout, repo_root=repo_root)


def smtp_inject_inbound(
    project_name: str,
    *,
    checkout: str,
    repo_root: Path | None = None,
    message_id: str | None = None,
    subject: str | None = None,
    body: str | None = None,
    in_reply_to: str | None = None,
) -> None:
    """Отправляет письмо в GreenMail по SMTP с хоста pytest (localhost:mapped_port)."""
    del checkout
    started = time.monotonic()
    _diag("smtp inject start")
    root = repo_root or REPO_ROOT
    script = root / "tests" / "e2e" / "smtp_inject.py"
    host, port = wait_for_greenmail_ready(project_name, timeout=TIMEOUT_POLL_SHORT)

    cmd: list[str] = [sys.executable, str(script), host, str(port)]
    if message_id is not None:
        cmd += ["--message-id", message_id]
    if subject is not None:
        cmd += ["--subject", subject]
    if body is not None:
        cmd += ["--body", body]
    if in_reply_to is not None:
        cmd += ["--in-reply-to", in_reply_to]

    r = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if r.returncode != 0:
        raise RuntimeError(f"smtp inject from host failed: {r.stdout}{r.stderr}")
    _diag(f"smtp inject done (elapsed={time.monotonic() - started:.1f}s)")
    out = (r.stdout or "").strip()
    if out:
        _diag(f"smtp inject host stdout: {out[:500]}")


def _email_bridge_systemd_diag_script() -> str:
    """Снимок юнита bridge-email (Python IMAP IDLE bridge)."""
    return f"""\
set +e
echo "=== threlium-bridge@.service (unit file) ==="
cat {E2E_REMOTE_POSIX_HOME}/.config/systemd/user/threlium-bridge@.service 2>&1 || true
echo "=== journalctl --user-unit threlium-bridge@email.service (runuser {E2E_THRELIUM_USER}, last 120) ==="
{e2e_threlium_user_unit_journalctl_bash("threlium-bridge@email.service", 120)}
echo "=== journalctl broad tail (root, last 40) ==="
journalctl -n 40 --no-pager 2>&1 || true
"""


def poll_notmuch_positive(project_name: str, *, repo_root: Path | None = None) -> str:
    root = repo_root or REPO_ROOT

    def _count() -> str | None:
        r = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>/dev/null || echo 0"],
            repo_root=root,
            timeout=30,
        )
        if r.returncode != 0:
            return None
        try:
            n = int((r.stdout or "").strip())
        except ValueError:
            return None
        if n > 0:
            return (r.stdout or "").strip()
        return None

    return poll_until(_count, timeout=TIMEOUT_POLL_SHORT, desc="notmuch count > 0")


def poll_lightrag_indexed_positive(
    project_name: str, *, repo_root: Path | None = None, timeout: float | None = None
) -> None:
    """Wait until LightRAG drain has actually inserted data into vectordb.

    Polls ``vdb_chunks.json`` file size inside the SUT: a file > 4 bytes means
    nano-vectordb has at least one stored vector (empty JSON list ``[]`` is 2-3 bytes).
    This is more reliable than checking ``tag:lightrag_indexed`` which gets applied by
    fdm on archive copies before the drain runs.
    """
    root = repo_root or REPO_ROOT
    w = float(timeout) if timeout is not None else float(TIMEOUT_POLL_SHORT)
    cmd = [
        "bash", "-lc",
        "stat --printf='%s' /home/threlium/threlium/data/lightrag/vdb_chunks.json 2>/dev/null || echo 0",
    ]

    def _probe() -> str | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=30)
        if r.returncode != 0:
            return None
        try:
            sz = int((r.stdout or "").strip())
        except ValueError:
            return None
        return str(sz) if sz > 10 else None

    poll_until(_probe, timeout=w, interval=2.0, desc="vdb_chunks.json size > 10 bytes")


def wait_for_notmuch_message(
    project_name: str,
    *,
    message_id: str,
    subject: str | None = None,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Дождаться появления письма в notmuch по ``id:…``.

    Если передан ``subject``, запрос сужается ``and subject:'…'`` (устаревший режим).
    Для e2e-дезамбiguaции достаточно ``message_id`` (inner id / якорь треда).
    """
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(message_id)
    query = f"{id_term} and subject:'{subject}'" if subject is not None else id_term
    py_body = (
        "import os, subprocess, sys\n"
        + _E2E_REMOTE_PROBE_LOGGER_BOOT
        + f"os.environ.setdefault('HOME', {E2E_REMOTE_POSIX_HOME!r})\n"
        f"os.environ.setdefault('NOTMUCH_CONFIG', {E2E_REMOTE_POSIX_HOME + '/.notmuch-config'!r})\n"
        f"query = {query!r}\n"
        "r = subprocess.run(['notmuch', 'count', query], capture_output=True, text=True)\n"
        "raw = (r.stdout or '0').strip() or '0'\n"
        "_probe_out.info('NOTMUCH_QUERY_COUNT=' + raw)\n"
        "try:\n"
        "    n = int(raw)\n"
        "except ValueError:\n"
        "    n = 0\n"
        "raise SystemExit(0 if n > 0 else 1)\n"
    )
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py_body + "\nPY\n"]

    snap: dict[str, str] = {"q": "?", "rc": "?"}

    def _count() -> bool | None:
        r = service_exec(project_name, "sut", cmd, repo_root=root, timeout=30)
        snap["rc"] = str(r.returncode)
        snap["q"] = "?"
        for line in (r.stdout or "").splitlines():
            if line.startswith("NOTMUCH_QUERY_COUNT="):
                snap["q"] = line.split("=", 1)[1].strip()
                break
        return True if r.returncode == 0 else None

    def _extra() -> str:
        r2 = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count '*' 2>/dev/null || echo notmuch_err"],
            repo_root=root,
            timeout=30,
        )
        total = (r2.stdout or "").strip().replace("\n", " ")[:200]
        return (
            f"query={query!r} matched_count={snap['q']} last_probe_exit={snap['rc']} "
            f"notmuch_count_star={total}"
        )

    poll_until_backoff(
        _count,
        timeout=timeout,
        desc=f"notmuch query {query!r} > 0",
        progress_extra=_extra,
    )


def reset_mda_pipeline_diag(project_name: str, *, repo_root: Path | None = None) -> None:
    """Перед mailflow: ранее очищался ``maildrop.log``; с fdm отдельного LDA-лога нет — no-op."""
    root = repo_root or REPO_ROOT
    service_exec(project_name, "sut", ["bash", "-lc", "true"], repo_root=root, timeout=30)


def reset_maildrop_debug_log(project_name: str, *, repo_root: Path | None = None) -> None:
    """Совместимость: см. :func:`reset_mda_pipeline_diag`."""
    reset_mda_pipeline_diag(project_name, repo_root=repo_root)


def mailflow_pipeline_diag(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
) -> None:
    """Диагностика после mailflow: notmuch (тред в архиве), хвост LLM-лога, journal threlium."""
    root = repo_root or REPO_ROOT
    snap = mailflow_fsm_maildir_systemd_snapshot(project_name, repo_root=root)
    mailflow_diag_block("mailflow: fsm maildir + systemd snapshot", snap, max_chars=30000)
    mf = service_exec(
        project_name,
        "sut",
        [
            "bash",
            "-lc",
            f"echo '--- readlink ~/.fdm.conf ---'; readlink -f {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
            f"echo '--- {E2E_REMOTE_POSIX_HOME}/.fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_POSIX_HOME}/.fdm.conf 2>&1 || true; "
            f"echo '--- repo config/fdm.conf ---'; sed -n '1,260p' {E2E_REMOTE_REPO_PATH}/config/fdm.conf 2>&1 || true",
        ],
        repo_root=root,
        timeout=30,
    )
    mailflow_diag_block("mailflow: rendered fdm.conf in sut", mf.stdout + mf.stderr, max_chars=30000)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
def _paths(raw: str) -> list[str]:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return []
    out: list[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, str):
                s = item.strip()
            elif isinstance(item, dict):
                s = str(item.get("path") or item.get("file") or item.get("filename") or "").strip()
            else:
                s = ""
            if s:
                out.append(s)
    return out
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=30", "--output=files", "--format=json", id_q],
    capture_output=True,
    text=True,
)
_probe_out.info("--- notmuch search id anchor (files, up to 30) ---")
_probe_out.info(json.dumps(_paths(p.stdout), ensure_ascii=False, indent=2) or "(empty)")
p2 = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p2.stdout)
if tid:
    p3 = subprocess.run(
        ["notmuch", "search", "--limit=50", "--output=files", "--format=json", tid],
        capture_output=True,
        text=True,
    )
    _probe_out.info("--- notmuch thread in union index (files, up to 50) ---")
    _probe_out.info(json.dumps(_paths(p3.stdout), ensure_ascii=False, indent=2) or "(empty)")
sys.exit(0)
"""
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    mailflow_diag_block("mailflow: notmuch thread / stages slice", r.stdout + r.stderr, max_chars=25000)

    try:
        from .wiremock_client import describe_wiremock_admin_state

        wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
        wm_state = describe_wiremock_admin_state(wm_host, wm_port, project_name=project_name)
    except Exception as e:  # pragma: no cover
        wm_state = f"(failed to describe wiremock state: {e!r})"
    mailflow_diag_block("mailflow: wiremock admin + journal tail", wm_state, max_chars=12000)

    md_log_r = service_exec(
        project_name,
        "sut",
        [
            "bash",
            "-lc",
            f"{e2e_threlium_user_unit_journalctl_bash('threlium-bridge@email.service', 200)}",
        ],
        repo_root=root,
        timeout=30,
    )
    mailflow_diag_block("mailflow: journal threlium-bridge@email (fdm)", md_log_r.stdout + md_log_r.stderr, max_chars=20000)

    j = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", E2E_SUT_THRELIUM_USER_UNIT_JOURNAL + "\necho '--- journal broad tail ---'\njournalctl -n 120 --no-pager 2>&1 || true"],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    mailflow_diag_block("mailflow: journal (threlium user-units + broad tail)", j.stdout + j.stderr, max_chars=20000)


def assert_notmuch_thread_fully_in_stages(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """Все сообщения notmuch-треда якорного id должны быть видны в union index.

    `docs/INDEX.md` §1, §10 решение 7: union-notmuch root = ``stages/``;
    отдельного ``archive/Maildir`` нет. Каждое настроенное письмо должно быть
    в ``stages/<stage>/Maildir/cur`` (после ``nm_settle()``) или ``new`` (между
    ``insert`` и стартом worker'а).

    Опрос: после reasoning → egress цепочка может ещё обрабатываться секунды;
    ждём ``count(thread) == count(thread and not tag:unread)`` до ``settle_timeout``.
    """
    root = repo_root or REPO_ROOT
    if settle_timeout is not None:
        w = float(settle_timeout)
    else:
        w = float(TIMEOUT_POLL_SHORT)
    _unread_term = NotmuchTag.UNREAD.as_tag_query_term()
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
c_full = subprocess.run(["notmuch", "count", tid], capture_output=True, text=True)
q_settled = f"{{tid}} and not {_unread_term}"
c_settled = subprocess.run(["notmuch", "count", q_settled], capture_output=True, text=True)
try:
    n_full = int((c_full.stdout or "0").strip() or "0")
except ValueError:
    n_full = 0
try:
    n_settled = int((c_settled.stdout or "0").strip() or "0")
except ValueError:
    n_settled = 0
_probe_out.info(
    "NOTMUCH_THREAD_COUNTS tid="
    + repr(tid)
    + " full="
    + str(n_full)
    + " settled="
    + str(n_settled)
)
if n_full <= 0:
    sys.exit(3)
if n_full != n_settled:
    sys.exit(4)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]

    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread settled (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread did not fully settle in union stages index within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None


def assert_notmuch_thread_has_messages_in_folders(
    project_name: str,
    *,
    anchor_message_id: str,
    stage_folder_ids: Sequence[str],
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """В треде якорного ``Message-ID`` есть хотя бы одно сообщение в каждом ``folder:<id>/Maildir``."""
    if not stage_folder_ids:
        return
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    stages_json = json.dumps([str(s).strip() for s in stage_folder_ids if str(s).strip()])
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
stages = json.loads({repr(stages_json)})
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
missing = []
for sid in stages:
    q = f'{{tid}} AND folder:"{{sid}}/Maildir"'
    c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
    try:
        n = int((c.stdout or "0").strip() or "0")
    except ValueError:
        n = 0
    if n < 1:
        missing.append(sid)
        _probe_out.info("NOTMUCH_STAGE_FOLDER_EMPTY stage=" + repr(sid) + " q=" + repr(q))
if missing:
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread stage folders (anchor={anchor_message_id!r}, stages={list(stage_folder_ids)!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread missing expected stage Maildir folder hits within "
            f"{w}s (anchor id={anchor_message_id!r}, wanted folders={list(stage_folder_ids)!r}).{out}"
        ) from None


def poll_notmuch_thread_in_stage_folder(
    project_name: str,
    *,
    anchor_message_id: str,
    stage_folder_id: str,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """Poll до появления сообщения из треда *anchor_message_id* в ``folder:<stage>/Maildir``.

    Изолированная замена journalctl-проверок маршрутизации: привязка по ``Message-ID``
    конкретного теста, без глобального grep по журналу.
    """
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    sid_lit = repr(str(stage_folder_id).strip())
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
sid = {sid_lit}
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
q = f'{{tid}} AND folder:"{{sid}}/Maildir"'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
if n < 1:
    _probe_out.info("NOTMUCH_STAGE_FOLDER_EMPTY stage=" + repr(sid) + " q=" + repr(q))
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        return True if r.returncode == 0 else None

    poll_until(
        _probe,
        timeout=w,
        interval=2.0,
        desc=(
            f"notmuch thread in stage folder "
            f"(anchor={anchor_message_id!r}, stage={stage_folder_id!r})"
        ),
    )


def assert_notmuch_folder_contains_body_token(
    project_name: str,
    *,
    stage_folder_id: str,
    body_token: str,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
    min_count: int = 1,
) -> None:
    """Хотя бы ``min_count`` сообщений в ``folder:<stage>/Maildir`` с подстрокой ``body_token`` в теле (notmuch)."""
    tok = str(body_token).strip()
    if not tok:
        raise ValueError("assert_notmuch_folder_contains_body_token: empty body_token")
    sid = str(stage_folder_id).strip()
    if not sid:
        raise ValueError("assert_notmuch_folder_contains_body_token: empty stage_folder_id")
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    tok_lit = repr(tok)
    sid_lit = repr(sid)
    mc = int(min_count)
    py = f"""import os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
tok = {tok_lit}
sid = {sid_lit}
want = {mc}
q = f'folder:"{{sid}}/Maildir" "{{tok}}"'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_FOLDER_TOKEN_COUNT n=" + str(n) + " q=" + repr(q))
sys.exit(0 if n >= want else 6)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch folder={stage_folder_id!r} body contains {tok[:48]!r}…",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        raise AssertionError(
            f"notmuch folder {stage_folder_id!r} had fewer than {min_count} hits for body token "
            f"within {w}s (token={tok!r}).{out}"
        ) from None


def assert_notmuch_thread_tag_count(
    project_name: str,
    *,
    anchor_message_id: str,
    tag: str,
    min_count: int = 1,
    repo_root: Path | None = None,
    poll_timeout: float | None = None,
) -> None:
    """В треде якорного ``Message-ID`` не меньше ``min_count`` сообщений с ``tag:<tag>``."""
    tag_val = str(tag).strip().removeprefix("tag:")
    if not tag_val:
        raise ValueError("assert_notmuch_thread_tag_count: empty tag")
    root = repo_root or REPO_ROOT
    w = float(poll_timeout) if poll_timeout is not None else float(TIMEOUT_POLL_SHORT)
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    tag_lit = repr(tag_val)
    mc = int(min_count)
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
tag = {tag_lit}
want = {mc}
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
try:
    payload = json.loads((p.stdout or "").strip() or "[]")
except json.JSONDecodeError:
    payload = []
tid = ""
if isinstance(payload, list) and payload:
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or "").strip()
if not tid:
    sys.exit(2)
if not tid.startswith("thread:"):
    tid = f"thread:{{tid}}"
q = f'{{tid}} tag:{{tag}}'
c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
try:
    n = int((c.stdout or "0").strip() or "0")
except ValueError:
    n = 0
_probe_out.info("NOTMUCH_THREAD_TAG_COUNT n=" + str(n) + " q=" + repr(q))
sys.exit(0 if n >= want else 6)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread tag:{tag_val!r} count>={min_count} (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        raise AssertionError(
            f"notmuch thread had fewer than {min_count} messages with tag:{tag_val!r} within {w}s "
            f"(anchor id={anchor_message_id!r}).{out}"
        ) from None


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
{_E2E_REMOTE_PROBE_LOGGER_BOOT}from email import message_from_bytes
from email.header import decode_header

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
        msg = message_from_bytes(raw)
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
                msg = message_from_bytes(raw_data[0][1])
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


def _e2e_log_sut_workers_stall_diag(project_name: str, *, repo_root: Path, banner: str) -> None:
    """Снимок SUT при таймауте ``wait_for_sut_threlium_user_workers_idle`` (list-units + journal)."""
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", e2e_sut_threlium_user_workers_stall_diag_bash()],
        repo_root=repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    body = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
    cap = 25_000
    if len(body) > cap:
        body = body[:cap] + "\n… (truncated)"
    log.debug(
        "sut_workers_stall_diag",
        banner=banner,
        body=clip_log_body(body, max_len=cap),
    )


def wait_for_sut_threlium_user_workers_idle(
    project_name: str,
    *,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Дождаться отсутствия активных user-unit ``threlium-work@*`` и ``threlium-sweep@*``.

    Нужно перед mailflow на живом SUT: иначе долетающие LiteLLM со старым ``X-Threlium-Route``
    дают unmatched после холодного прогона или до ``wiremock_state_reset_all_contexts`` в ``pytest_sessionfinish``.

    При ``TimeoutError`` в лог уходит :func:`~tests.e2e.sut_user_systemd.e2e_sut_threlium_user_workers_stall_diag_bash`.
    """
    root = repo_root or REPO_ROOT
    script = e2e_sut_threlium_user_workers_idle_probe_bash()

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", script],
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        if r.returncode != 0:
            return None
        try:
            line = (r.stdout or "").strip().splitlines()[-1]
            n = int(line)
        except (ValueError, IndexError):
            return None
        return True if n == 0 else None

    try:
        poll_until_backoff(
            _probe,
            timeout=timeout,
            desc="sut: threlium-work@ / threlium-sweep@ idle (user systemd)",
        )
    except TimeoutError as e:
        _e2e_log_sut_workers_stall_diag(
            project_name,
            repo_root=root,
            banner=f"sut workers idle TIMEOUT diag (timeout={timeout}s): {e}",
        )
        raise


def wait_for_wiremock_global_unmatched_zero(
    project_name: str,
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """``GET /__admin/requests/unmatched`` пуст (глобально по инстансу)."""
    from .wiremock_client import wiremock_public_base, wiremock_unmatched_requests_count

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = wiremock_public_base(wm_host, wm_port)

    def _probe() -> bool | None:
        try:
            if wiremock_unmatched_requests_count(public_base) == 0:
                return True
        except Exception:
            return None
        return None

    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc="wiremock: zero global unmatched requests",
    )


def assert_wiremock_mailflow_received_chat_completion_posts(
    project_name: str,
    *,
    stub_tag: str,
    anchor_message_id: str = "e2e-inbound@localhost",
    repo_root: Path | None = None,
    min_posts: int = 1,
) -> None:
    """Проверка журнала WireMock: POST ``/chat/completions`` с ``stub_tag`` и якорем в теле/headers.

    Источник истины — Admin API ``GET /__admin/requests`` (записи с ``metadata.threlium_e2e_stub_tag``).
    ``anchor_message_id`` — canonical thread-root MID (``X-Threlium-Thread-Root`` у запроса к LiteLLM
    и сид State), см. :func:`e2e_thread_root_mid_for_message_id`.
    ``diag_callback`` перед ошибкой — :func:`mailflow_pipeline_diag`.
    """
    from .wiremock_client import assert_wiremock_stub_received_min_chat_completions

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_stub_received_min_chat_completions(
        public_base,
        stub_tag=stub_tag,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )


def assert_wiremock_mailflow_zero_unmatched(
    project_name: str,
    *,
    anchor_message_id: str,
    correlation_route_wire: str | None = None,
    repo_root: Path | None = None,
) -> None:
    """Журнал ``GET /__admin/requests/unmatched`` пуст (с опросом до ``TIMEOUT_POLL_SHORT``).

    Нормативно — **глобально** по инстансу (``correlation_route_wire`` не передаётся); параметр
    оставлен для совместимости и особых случаев (например узкий фильтр при отладке ``pytest -n>1``).
    """
    from .wiremock_client import assert_wiremock_zero_unmatched_requests

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_zero_unmatched_requests(
        public_base,
        diag_callback=_diag,
        x_threlium_route_wire=correlation_route_wire,
    )


def assert_wiremock_mailflow_min_embedding_posts(
    project_name: str,
    *,
    anchor_message_id: str,
    min_posts: int,
    repo_root: Path | None = None,
) -> None:
    """≥ ``min_posts`` успешных POST ``/embeddings`` с якорем ``X-Threlium-Thread-Root`` (полный журнал)."""
    from .wiremock_client import assert_wiremock_min_embedding_posts_matching_anchor

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_min_embedding_posts_matching_anchor(
        public_base,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )


def assert_wiremock_mailflow_min_rerank_posts(
    project_name: str,
    *,
    anchor_message_id: str,
    min_posts: int,
    repo_root: Path | None = None,
) -> None:
    """>=``min_posts`` POST ``/rerank`` (200) with anchor ``X-Threlium-Thread-Root``."""
    from .wiremock_client import assert_wiremock_min_rerank_posts_matching_anchor

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_min_rerank_posts_matching_anchor(
        public_base,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )


def assert_notmuch_mailflow_thread_has_lightrag_indexed(
    project_name: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """В треде якорного id есть хотя бы одно сообщение с ``tag:lightrag_indexed`` (индексация LightRAG прошла)."""
    root = repo_root or REPO_ROOT
    if settle_timeout is not None:
        w = float(settle_timeout)
    else:
        w = float(TIMEOUT_POLL_SHORT)
    rag_term = NotmuchTag.LIGHTRAG_INDEXED.as_tag_query_term()
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
anchor = {anchor_message_id!r}
id_q = {_id_q_lit}
rag_term = {rag_term!r}
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
q_rag = tid + " and " + rag_term
c_rag = subprocess.run(["notmuch", "count", q_rag], capture_output=True, text=True)
try:
    n_rag = int((c_rag.stdout or "0").strip() or "0")
except ValueError:
    n_rag = 0
_probe_out.info("LIGHTRAG_INDEXED_IN_THREAD=" + str(n_rag) + " tid=" + repr(tid))
if n_rag < 1:
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"notmuch thread has lightrag_indexed (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "notmuch thread has no messages tagged lightrag_indexed within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None


def assert_notmuch_thread_lightrag_index_filter(
    project_name: str,
    *,
    anchor_message_id: str,
    indexed_stages: tuple[FsmStage, ...],
    excluded_stages: tuple[FsmStage, ...],
    repo_root: Path | None = None,
    settle_timeout: float | None = None,
) -> None:
    """Селективная индексация LightRAG в треде якоря (push-down whitelist).

    Для каждой ``indexed_stages``: в треде есть письмо ``to:<stage>`` И оно
    ``tag:lightrag_indexed`` (content-indexable стадия попала в drain).
    Для каждой ``excluded_stages``: письма ``to:<stage>`` в треде есть, но НИ
    одно НЕ ``tag:lightrag_indexed`` (SERVICE/не-whitelist стадия отсечена
    селектором :func:`threlium.lightrag_drain_query.lightrag_drain_pending_search`
    и в граф не попала). Drain при этом доходит до idle — отсечённые письма не
    «застревают» в pending.
    """
    root = repo_root or REPO_ROOT
    w = float(settle_timeout) if settle_timeout is not None else float(TIMEOUT_POLL_SHORT)
    rag_term = NotmuchTag.LIGHTRAG_INDEXED.as_tag_query_term()
    indexed_to_terms = [
        NotmuchQueryField.TO.term(s.rfc822_mailbox) for s in indexed_stages
    ]
    excluded_to_terms = [
        NotmuchQueryField.TO.term(s.rfc822_mailbox) for s in excluded_stages
    ]
    _id_q_lit = repr(notmuch_id_search_term(anchor_message_id))
    py = f"""import json, os, subprocess, sys
{_E2E_REMOTE_PROBE_LOGGER_BOOT}os.environ.setdefault("HOME", "{E2E_REMOTE_POSIX_HOME}")
os.environ.setdefault("NOTMUCH_CONFIG", "{E2E_REMOTE_POSIX_HOME}/.notmuch-config")
id_q = {_id_q_lit}
rag_term = {rag_term!r}
indexed_terms = {indexed_to_terms!r}
excluded_terms = {excluded_to_terms!r}
def _count(q: str) -> int:
    c = subprocess.run(["notmuch", "count", q], capture_output=True, text=True)
    try:
        return int((c.stdout or "0").strip() or "0")
    except ValueError:
        return 0
def _first_thread(raw: str) -> str:
    try:
        payload = json.loads((raw or "").strip() or "[]")
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, list) or not payload:
        return ""
    first = payload[0]
    if isinstance(first, str):
        tid = first.strip()
    elif isinstance(first, dict):
        tid = str(first.get("thread") or first.get("thread_id") or first.get("threadid") or "").strip()
    else:
        tid = ""
    if not tid:
        return ""
    return tid if tid.startswith("thread:") else f"thread:{{tid}}"
p = subprocess.run(
    ["notmuch", "search", "--limit=1", "--output=threads", "--format=json", id_q],
    capture_output=True,
    text=True,
)
tid = _first_thread(p.stdout)
if not tid:
    _probe_out.info("NOTMUCH_THREAD_RESOLVE_FAIL stdout=" + repr(p.stdout) + " stderr=" + repr(p.stderr))
    sys.exit(2)
problems = []
for term in indexed_terms:
    n_all = _count(tid + " and " + term)
    n_idx = _count(tid + " and " + term + " and " + rag_term)
    _probe_out.info("INDEXED_STAGE term=" + repr(term) + " all=" + str(n_all) + " indexed=" + str(n_idx))
    if n_all < 1:
        problems.append("missing stage " + term)
    elif n_idx < 1:
        problems.append("not indexed " + term)
for term in excluded_terms:
    n_all = _count(tid + " and " + term)
    n_idx = _count(tid + " and " + term + " and " + rag_term)
    _probe_out.info("EXCLUDED_STAGE term=" + repr(term) + " all=" + str(n_all) + " indexed=" + str(n_idx))
    if n_all < 1:
        problems.append("missing stage " + term)
    elif n_idx > 0:
        problems.append("unexpectedly indexed " + term)
if problems:
    _probe_out.info("INDEX_FILTER_PROBLEMS=" + repr(problems) + " tid=" + repr(tid))
    sys.exit(5)
sys.exit(0)
"""
    cmd = ["bash", "-lc", "python3 <<'PY'\n" + py + "\nPY"]
    last: dict[str, Any] = {"r": None}

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            cmd,
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        last["r"] = r
        return True if r.returncode == 0 else None

    try:
        poll_until(
            _probe,
            timeout=w,
            interval=2.0,
            desc=f"lightrag index filter (anchor={anchor_message_id!r})",
        )
    except TimeoutError:
        r = last["r"]
        out = ""
        if r is not None:
            out = f"\nscript output:\n{r.stdout}\n{r.stderr}"
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=root)
        raise AssertionError(
            "lightrag selective indexing invariant violated within "
            f"{w}s (anchor id={anchor_message_id!r}).{out}"
        ) from None


# ---------------------------------------------------------------------------
# Mailflow scenario infrastructure: shared arrange / assert for email pipeline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MailflowScenarioSpec:
    """Declarative config for a full email-mailflow e2e scenario.

    Encapsulates the variable parts so that the fixture (arrange) and assertion
    (act+assert) code can be shared across tests with different WireMock stubs.
    """

    label: str
    raw_id_prefix: str
    stub_dir: Path
    stub_tag: str
    body_head: str
    body_override: str | None = None
    oversized_trim_body: bool = False
    summarize_overflow_body: bool = False
    min_chat_completion_posts: int = 1
    # Cold-reset SUT: один probe в knowledge/ → меньше drain/bootstrap embeddings на тред.
    min_embedding_posts: int = 5
    min_rerank_posts: int = 1
    warmup_body_extra: str = ""
    expect_notmuch_stage_folders: tuple[str, ...] | None = None
    reply_subject_needle: str | None = None
    reply_body_needle: str | None = None
    assert_thread_no_unread: bool = False


def _wait_rag_drain_idle(project_name: str, *, label: str) -> None:
    """Poll until the LightRAG pending selector returns empty (drain finished)."""
    selector = lightrag_drain_pending_search()
    cmd = [
        "bash", "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; "
        f"notmuch count '{selector}' 2>/dev/null || echo 99",
    ]

    def _probe() -> str | None:
        r = service_exec(project_name, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
        if r.returncode != 0:
            return None
        try:
            n = int((r.stdout or "").strip())
        except ValueError:
            return None
        return "0" if n == 0 else None

    poll_until(_probe, timeout=TIMEOUT_POLL_SHORT, interval=2.0, desc="rag pending count == 0")
    mailflow_log_phase(f"{label}: rag drain idle (no pending messages)")


def _inject_rag_warmup(
    project_name: str,
    *,
    rt: E2EComposeRuntime,
    wm_base: str,
    stub_tag: str,
    body_head: str,
    body_extra: str,
    label: str,
) -> None:
    """Ensure vectordb has data for rerank; inject warm-up only if needed.

    If vectordb already contains data (from a previous test in the same session),
    skip injection entirely. Otherwise inject a warm-up message through the agent
    mailbox and wait for LightRAG drain to populate the vectordb.
    """
    from .wiremock_client import (  # noqa: PLC0415
        composite_context_key,
        wiremock_state_seed_context,
    )

    cmd = [
        "bash", "-lc",
        "stat --printf='%s' /home/threlium/threlium/data/lightrag/vdb_chunks.json 2>/dev/null || echo 0",
    ]
    r = service_exec(project_name, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
    try:
        sz = int((r.stdout or "").strip())
    except ValueError:
        sz = 0
    if sz > 10:
        _wait_rag_drain_idle(project_name, label=label)
        mailflow_log_phase(f"{label}: vectordb already has data ({sz} bytes), skip warmup")
        return

    warmup_id = f"e2e-rag-warmup-{uuid.uuid4().hex[:12]}@localhost"
    warmup_corr = e2e_thread_root_mid_for_message_id(warmup_id)
    warmup_ctx = composite_context_key(stub_tag, warmup_corr)
    wiremock_state_seed_context(wm_base, warmup_ctx)

    warmup_body = e2e_dense_threlium_ctx_body(
        head=body_head, correlation_key=warmup_corr
    )
    if body_extra:
        warmup_body = warmup_body.rstrip("\n") + "\n" + body_extra + "\n"
    smtp_inject_inbound(
        project_name,
        checkout="/unused",
        repo_root=REPO_ROOT,
        message_id=warmup_id,
        body=warmup_body,
    )
    mailflow_log_phase(f"{label}: rag warmup injected mid={warmup_id!r}")

    wait_for_greenmail_inbox_message_gone_host(
        rt.greenmail_imap_host,
        rt.greenmail_imap_port,
        message_id=warmup_id,
    )
    mailflow_log_phase(f"{label}: rag warmup picked up (gone from INBOX, pipeline complete)")

    poll_lightrag_indexed_positive(project_name, repo_root=REPO_ROOT)
    _wait_rag_drain_idle(project_name, label=label)
    mailflow_log_phase(f"{label}: rag warmup indexed in vectordb")


@contextlib.contextmanager
def mailflow_inject_and_wait(
    spec: MailflowScenarioSpec,
    deployed_stack: str,
) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Arrange phase: prepare WireMock → inject email → wait bridge pickup (gone from INBOX) + FSM activity.

    Yields ``(project_name, raw_id, canonical_id, nm_inner, stub_tag, correlation_key)``.
    Teardown не чистит журнал WireMock (оставлен для ручной отладки).
    """
    from .wiremock_client import (  # noqa: PLC0415
        prepare_wiremock_scenario,
        teardown_wiremock_scenario,
        wiremock_public_base,
    )

    needs_prior_thread_turn = spec.summarize_overflow_body or spec.oversized_trim_body
    seed_id: str | None = None
    main_in_reply_to: str | None = None
    if needs_prior_thread_turn:
        seed_id = f"{spec.raw_id_prefix}seed-{uuid.uuid4().hex}@localhost"
        correlation_key = e2e_thread_root_mid_for_message_id(seed_id)
    raw_id = f"{spec.raw_id_prefix}{uuid.uuid4().hex}@localhost"
    if not needs_prior_thread_turn:
        correlation_key = e2e_thread_root_mid_for_message_id(raw_id)
    nm_inner = email_ingress_notmuch_id_inner(raw_id)
    canonical_id = canonical_external_msgid(raw_id)
    t0 = time.monotonic()
    mailflow_log_phase(
        f"{spec.label}: start (project={deployed_stack}) "
        f"message_id={raw_id!r} correlation_key={correlation_key!r}"
    )
    rt = discover_runtime(deployed_stack, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    prepare_wiremock_scenario(
        wm_base,
        stub_dir=spec.stub_dir,
        stub_tag=spec.stub_tag,
        correlation_key=correlation_key,
    )

    if spec.min_rerank_posts > 0:
        _inject_rag_warmup(
            deployed_stack,
            rt=rt,
            wm_base=wm_base,
            stub_tag=spec.stub_tag,
            body_head=spec.body_head,
            body_extra=spec.warmup_body_extra,
            label=spec.label,
        )
        mailflow_log_phase(
            f"{spec.label}: lightrag vectordb has indexed data (+{time.monotonic() - t0:.1f}s)"
        )

    reset_maildrop_debug_log(deployed_stack, repo_root=REPO_ROOT)

    if seed_id is not None:
        if spec.summarize_overflow_body:
            seed_body = e2e_summarize_overflow_inject_body(
                head=f"{spec.body_head} (prior thread turn seed)",
                correlation_key=correlation_key,
            )
        elif spec.oversized_trim_body:
            seed_body = e2e_oversized_context_trim_prior_turn_body(
                head=f"{spec.body_head} (prior thread turn seed)",
                correlation_key=correlation_key,
            )
        else:
            seed_body = e2e_dense_threlium_ctx_body(
                head=f"{spec.body_head} (prior thread turn seed)",
                correlation_key=correlation_key,
            )
        smtp_inject_inbound(
            deployed_stack,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=seed_id,
            body=seed_body,
        )
        mailflow_log_phase(
            f"{spec.label}: prior-turn seed injected mid={seed_id!r} (+{time.monotonic() - t0:.1f}s)"
        )
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            message_id=seed_id,
        )
        seed_nm_inner = email_ingress_notmuch_id_inner(seed_id)
        mailflow_wait_fsm_maildir_activity(
            deployed_stack,
            repo_root=REPO_ROOT,
            message_id=seed_nm_inner,
        )
        wait_for_notmuch_message(
            deployed_stack, message_id=seed_nm_inner, repo_root=REPO_ROOT
        )
        mailflow_log_phase(
            f"{spec.label}: prior-turn seed indexed mid={seed_id!r} (+{time.monotonic() - t0:.1f}s)"
        )
        wait_for_greenmail_user_reply(
            deployed_stack,
            raw_id=seed_id,
            repo_root=REPO_ROOT,
        )
        assert_notmuch_thread_fully_in_stages(
            deployed_stack,
            anchor_message_id=seed_nm_inner,
            repo_root=REPO_ROOT,
        )
        mailflow_log_phase(
            f"{spec.label}: prior-turn seed pipeline settled mid={seed_id!r} "
            f"(+{time.monotonic() - t0:.1f}s)"
        )
        # Реалистичный threading: основной ход тредится на ОТВЕТ агента (egress glue-record),
        # а не на собственную seed-инъекцию. Тогда IRT-цепочка основного хода проходит через
        # ``tasks_upsert`` seed-хода → per-frame task-ledger наследуется и finalize-gate
        # проходит без ручного сброса WireMock-латча ``phase_tasks_ledger_done``.
        main_in_reply_to = greenmail_wait_agent_reply_message_id(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            in_reply_to_anchor=seed_id,
        )
        mailflow_log_phase(
            f"{spec.label}: prior-turn agent reply mid={main_in_reply_to!r} "
            f"(+{time.monotonic() - t0:.1f}s)"
        )

    if spec.body_override is not None:
        inject_body = spec.body_override
    elif spec.oversized_trim_body:
        if seed_id is not None:
            inject_body = e2e_oversized_context_trim_current_turn_body(
                head=spec.body_head, correlation_key=correlation_key
            )
        else:
            inject_body = e2e_oversized_context_trim_body(
                head=spec.body_head, correlation_key=correlation_key
            )
    elif spec.summarize_overflow_body:
        if seed_id is not None:
            inject_body = e2e_dense_threlium_ctx_body(
                head=spec.body_head, correlation_key=correlation_key
            )
        else:
            inject_body = e2e_summarize_overflow_inject_body(
                head=spec.body_head, correlation_key=correlation_key
            )
    else:
        inject_body = e2e_dense_threlium_ctx_body(
            head=spec.body_head, correlation_key=correlation_key
        )
    smtp_inject_inbound(
        deployed_stack,
        checkout="/unused",
        repo_root=REPO_ROOT,
        message_id=raw_id,
        body=inject_body,
        **({"in_reply_to": main_in_reply_to} if main_in_reply_to is not None else {}),
    )
    mailflow_log_phase(f"{spec.label}: after smtp_inject_inbound (+{time.monotonic() - t0:.1f}s)")
    wait_for_greenmail_inbox_message_gone_host(
        rt.greenmail_imap_host,
        rt.greenmail_imap_port,
        message_id=raw_id,
    )
    mailflow_log_phase(
        f"{spec.label}: after wait_for_greenmail_inbox_message_gone_host (+{time.monotonic() - t0:.1f}s)"
    )
    snap = mailflow_fsm_maildir_systemd_snapshot(deployed_stack, repo_root=REPO_ROOT)
    mailflow_diag_block(
        f"{spec.label}: fsm maildir + systemd snapshot after IMAP IDLE pickup",
        snap,
        max_chars=30000,
    )
    mailflow_wait_fsm_maildir_activity(
        deployed_stack,
        repo_root=REPO_ROOT,
        message_id=nm_inner,
    )
    try:
        yield deployed_stack, raw_id, canonical_id, nm_inner, spec.stub_tag, correlation_key
    finally:
        teardown_wiremock_scenario(
            wm_base, correlation_key=correlation_key, stub_tag=spec.stub_tag
        )


def assert_full_mailflow_pipeline(
    spec: MailflowScenarioSpec,
    *,
    project: str,
    raw_id: str,
    nm_inner: str,
    stub_tag: str,
    correlation_key: str,
) -> None:
    """Assert phase: notmuch indexed → WireMock coverage → FSM stages → user reply → zero unmatched."""
    t0 = time.monotonic()
    mailflow_log_phase(
        f"{spec.label}: wait_for_notmuch_message nm_inner={nm_inner!r} "
        f"correlation_key_tail={correlation_key[-24:]!r}"
    )
    wait_for_notmuch_message(project, message_id=nm_inner, repo_root=REPO_ROOT)
    mailflow_log_phase(f"{spec.label}: notmuch OK (+{time.monotonic() - t0:.1f}s)")
    mailflow_pipeline_diag(project, anchor_message_id=nm_inner, repo_root=REPO_ROOT)
    assert_wiremock_mailflow_received_chat_completion_posts(
        project,
        stub_tag=stub_tag,
        anchor_message_id=correlation_key,
        repo_root=REPO_ROOT,
        min_posts=spec.min_chat_completion_posts,
    )
    assert_wiremock_mailflow_min_embedding_posts(
        project,
        anchor_message_id=correlation_key,
        min_posts=spec.min_embedding_posts,
        repo_root=REPO_ROOT,
    )
    if spec.min_rerank_posts > 0:
        assert_wiremock_mailflow_min_rerank_posts(
            project,
            anchor_message_id=correlation_key,
            min_posts=spec.min_rerank_posts,
            repo_root=REPO_ROOT,
        )
    # Multi-hop scenarios (empty-ledger bounce, tasks_upsert, …) finish only after egress.
    # Wait for the user-visible reply before requiring the notmuch thread to be fully settled.
    wait_for_greenmail_user_reply(
        project,
        raw_id=raw_id,
        repo_root=REPO_ROOT,
        **({"subject_substring": spec.reply_subject_needle} if spec.reply_subject_needle is not None else {}),
        **({"body_substring": spec.reply_body_needle} if spec.reply_body_needle is not None else {}),
    )
    assert_notmuch_thread_fully_in_stages(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    assert_notmuch_mailflow_thread_has_lightrag_indexed(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    if spec.expect_notmuch_stage_folders:
        assert_notmuch_thread_has_messages_in_folders(
            project,
            anchor_message_id=nm_inner,
            stage_folder_ids=spec.expect_notmuch_stage_folders,
            repo_root=REPO_ROOT,
        )
    if spec.assert_thread_no_unread:
        assert_notmuch_thread_has_no_unread(
            project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
        )
    assert_wiremock_mailflow_zero_unmatched(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    mailflow_log_phase(f"{spec.label}: pipeline checks OK (+{time.monotonic() - t0:.1f}s)")


# ---------------------------------------------------------------------------
# IMAP processed-folder helpers (email bridge UID MOVE)
# ---------------------------------------------------------------------------

E2E_IMAP_PROCESSED_FOLDER = "Threlium.Processed"


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


def _iter_notmuch_mbox_show_messages(mbox_text: str) -> Iterator[EmailMessage]:
    """RFC822-письма из ``notmuch show --format=mbox`` (полные заголовки, в т.ч. ``X-Threlium-*``).

    ``--format=json`` отдаёт урезанный ``headers`` без служебных полей Threlium; mbox — полный конверт.
    """
    for block in re.split(r"(?=^From )", mbox_text, flags=re.MULTILINE):
        block = block.strip()
        if not block:
            continue
        lines = block.splitlines(keepends=True)
        if lines and lines[0].startswith("From "):
            rfc822 = "".join(lines[1:])
        else:
            rfc822 = block
        if rfc822.strip():
            yield message_from_bytes(rfc822.encode("utf-8", errors="replace"))


def _notmuch_mbox_show_route_b62_for_message(
    mbox_text: str,
    *,
    message_id_inner: str,
) -> str | None:
    """``X-Threlium-Route`` (b62 wire) для письма с данным inner ``Message-ID`` в выводе mbox."""
    from threlium.mail_header_names import MailHeaderName

    needle = NotmuchMessageIdInner.parse(message_id_inner)
    hdr = MailHeaderName.ROUTE.value
    for msg in _iter_notmuch_mbox_show_messages(mbox_text):
        mid_w = RfcMessageIdWire.parse_present_from_email(msg, MailHeaderName.MESSAGE_ID)
        mid_inner = NotmuchMessageIdInner.from_optional_wire(mid_w)
        if mid_inner is None or not mid_inner.equals_case_insensitive(needle):
            continue
        route = msg.get(hdr)
        if route is None:
            route = msg.get("X-Threlium-Route")
        if route is None:
            continue
        s = str(route).strip()
        return s if s else None
    return None


def email_ingress_imap_checkpoint_from_notmuch(
    project: str,
    *,
    nm_inner: str,
    repo_root: Path | None = None,
) -> tuple[int | None, int]:
    """``(imap_uidvalidity, imap_uid)`` из ``X-Threlium-Route`` ingress-письма в notmuch."""
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(nm_inner)
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch show --format=mbox {shlex.quote(id_term)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    text = (r.stdout or "").strip()
    if not text:
        return None, 0
    route_b62 = _notmuch_mbox_show_route_b62_for_message(text, message_id_inner=nm_inner)
    if not route_b62:
        return None, 0
    route_w = IngressRouteB62Wire.decode_b62_wire(route_b62)
    if not isinstance(route_w, EmailIngressRoute):
        return None, 0
    uid = route_w.imap_uid
    uiv = route_w.imap_uidvalidity
    return (int(uiv) if uiv is not None else None, int(uid) if uid is not None else 0)


def restart_email_bridge_service(project: str, *, repo_root: Path | None = None) -> None:
    """``systemctl --user restart threlium-bridge@email`` на SUT."""
    from .sut_user_systemd import E2E_THRELIUM_USER

    root = repo_root or REPO_ROOT
    cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-bridge@email.service",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    assert r.returncode == 0, f"bridge restart failed: {(r.stderr or r.stdout)!r}"


def assert_notmuch_thread_has_no_unread(
    project: str,
    *,
    anchor_message_id: str,
    repo_root: Path | None = None,
) -> None:
    """После успешного прогона в треде нет ``tag:unread`` (``nm_settle``)."""
    root = repo_root or REPO_ROOT
    id_term = notmuch_id_search_term(anchor_message_id)
    q = f"thread:{anchor_message_id} and tag:unread"
    cmd = [
        "bash",
        "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; notmuch count {shlex.quote(q)}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=root, timeout=int(TIMEOUT_POLL_SHORT))
    count = (r.stdout or "0").strip().splitlines()[-1].strip()
    assert count == "0", (
        f"expected no unread in thread (anchor={anchor_message_id!r}), count={count!r}"
    )
