"""E2e constants, stub-tag cleanup maps, paths."""
from __future__ import annotations

import os
import shlex
from pathlib import Path

from threlium.types import FsmStage

from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

REPO_ROOT = Path(__file__).resolve().parents[3]
COMPOSE_DIR = REPO_ROOT / "tests" / "e2e" / "compose"
E2E_COMPOSE_FILE_NAME = "docker-compose.yml"
E2E_COMPOSE_FILE = COMPOSE_DIR / E2E_COMPOSE_FILE_NAME
E2E_SUT_COCKPIT_PORT = 9090
E2E_SHARED_COMPOSE_SERVICES: tuple[str, ...] = ("sut", "greenmail", "wiremock")
E2E_BAKED_SUT_IMAGE = "threlium/e2e-sut:baked"
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
E2E_SUT_NOTMUCH_BASH_EXPORT = (
    "export HOME="
    + shlex.quote(E2E_REMOTE_POSIX_HOME)
    + " NOTMUCH_CONFIG="
    + shlex.quote(E2E_REMOTE_POSIX_HOME + "/.notmuch-config")
)
E2E_WIREMOCK_CONTAINER_PORT: int = 8080
E2E_FETCHMAIL_USER = os.environ.get("THRELIUM_E2E_FETCHMAIL_USER", "test")
E2E_FETCHMAIL_PASS = os.environ.get("THRELIUM_E2E_FETCHMAIL_PASS", "secret")
E2E_IMAP_PROCESSED_FOLDER = os.environ.get(
    "THRELIUM_E2E_IMAP_PROCESSED_FOLDER", "Threlium.Processed"
)
E2E_GREENMAIL_REPLY_USER = os.environ.get("THRELIUM_E2E_GREENMAIL_REPLY_USER", "pytest")
THRELIUM_E2E_SKIP_SUT_MAILDIR_FLUSH_ENV = "THRELIUM_E2E_LIVE_SKIP_SUT_MAILDIR_FLUSH"
E2E_FSM_MAILBOX_STAGE_IDS: tuple[str, ...] = tuple(s.value for s in FsmStage)
E2E_ANSIBLE_INVENTORY_PATH = "inventory/e2e/hosts.yml"
E2E_ANSIBLE_CONFIG_NAME = "ansible-e2e.cfg"
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
TIMEOUT_POLL_SHORT = float(os.environ.get("THRELIUM_E2E_POLL_SHORT", "30"))
TIMEOUT_POLL_LIVE_MAIL = float(os.environ.get("THRELIUM_E2E_POLL_LIVE_MAIL", "120"))
TIMEOUT_ANSIBLE_PLAYBOOK = int(os.environ.get("THRELIUM_E2E_TIMEOUT_ANSIBLE", str(20 * 60)))
POLL_INTERVAL = float(os.environ.get("THRELIUM_E2E_POLL_INTERVAL", "2.0"))
E2E_REPLY_SUBJECT = "e2e reply"
E2E_REPLY_BODY = "ok from llm-mock"
E2E_REPLY_BODY_SNIPPET = E2E_REPLY_BODY
E2E_SUBAGENT_TABLE_LIVE_SUBJECT_MARKER = "e2e_subagent_table_chain"
E2E_SUBAGENT_HITL_MATRIX_BODY_MARKER = "e2e_subagent_hitl_matrix"
E2E_SUBAGENT_HITL_MATRIX_LIVE_MSGID_PREFIX = "e2e-hitl-mx-"
E2E_MEMORY_THREAD_LIVE_SUBJECT_MARKER = "e2e_memory_thread_live"
E2E_MEMORY_THREAD_LIVE_MSGID_PREFIX = "e2e-mem-tm-"
_E2E_DENSE_CORR_SEGMENTS = 1
E2E_CTX_TRIM_HEAD_MARKER = "E2E-CTX-TRIM-HEAD-MARKER"
E2E_CTX_TRIM_TAIL_MARKER = "E2E-CTX-TRIM-TAIL-MARKER"
E2E_CTX_TRIM_JOURNAL_SLACK_CHARS = 12000
E2E_SUMMARY_MARKER = "E2E-SUM-CONTEXT-MARKER"
E2E_SUM_ORIG_HEAD_MARKER = "E2E-SUM-ORIG-HEAD-MARKER"
E2E_SUM_ORIG_PAD_MARKER = "E2E-SUM-ORIG-PAD-MARKER"
E2E_SUMMARIZE_LLM_NEEDLE = "context summarizer"

E2E_KNOWLEDGE_PROBE_FILENAME = "e2e_bootstrap_probe.md"
# Детерминированный probe-документ живёт в тестовых ресурсах (репо), а не инлайн-строкой: cold-reset
# заменяет им весь запечённый knowledge-корпус перед reindex (см. e2e_install_deterministic_knowledge_corpus).
_E2E_KNOWLEDGE_PROBE_CONTENT = (
    REPO_ROOT / "tests" / "e2e" / "fixtures" / E2E_KNOWLEDGE_PROBE_FILENAME
).read_text(encoding="utf-8")
E2E_BOOTSTRAP_THREAD_ROOT = "e2e-bootstrap"
_E2E_LIGHTRAG_DOC_STATUS = f"{E2E_REMOTE_THRELIUM_HOME}/lightrag/kv_store_doc_status.json"
E2E_GREENMAIL_READINESS_PROBE_FROM = "pytest-readiness@localhost"
_E2E_LEAVE_STACK_RUNNING_ENV = "THRELIUM_E2E_LEAVE_STACK_RUNNING"
_E2E_DEFAULT_HOP_BUDGET = {"budget_root": 256, "budget_sub": 256}
