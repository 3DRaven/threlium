"""Knowledge bootstrap and LightRAG warmup helpers."""
from __future__ import annotations

import os
import shlex
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .constants import (
    E2E_BOOTSTRAP_THREAD_ROOT,
    E2E_KNOWLEDGE_PROBE_FILENAME,
    E2E_REMOTE_REPO_PATH,
    E2E_REMOTE_THRELIUM_HOME,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    _E2E_KNOWLEDGE_PROBE_CONTENT,
    _E2E_LIGHTRAG_DOC_STATUS,
)
from .poll import poll_until
from .pipeline import e2e_start_threlium_user_pipeline_services
from .runtime import E2EComposeRuntime, service_exec

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


def e2e_wait_engine_active(
    project: str,
    *,
    repo_root: Path | None = None,
    timeout: float = 60.0,
) -> None:
    """Poll until ``threlium-engine.service`` is active on SUT."""
    cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user is-active threlium-engine.service",
    ]
    root = repo_root or REPO_ROOT

    def _check() -> str | None:
        r = service_exec(project, "sut", cmd, repo_root=root, timeout=15)
        if (r.stdout or "").strip() == "active":
            return "active"
        return None

    poll_until(_check, timeout=timeout, interval=2.0, desc="threlium-engine.service active")


def bootstrap_embedding_entries(wm_base: str) -> list[dict]:
    """WireMock journal: bootstrap embedding requests (``X-Threlium-Thread-Root: e2e-bootstrap``)."""
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        journal_entries_for_stub_tag_with_header,
    )

    return journal_entries_for_stub_tag_with_header(
        wm_base,
        stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        header_name="X-Threlium-Thread-Root",
        header_value=E2E_BOOTSTRAP_THREAD_ROOT,
        url_contains="/embeddings",
    )


def bootstrap_embedding_entry_ids(wm_base: str) -> set[str]:
    return {
        str(e.get("id") or "")
        for e in bootstrap_embedding_entries(wm_base)
        if e.get("id")
    }


def _wait_bootstrap_embeddings_in_wiremock(wm_base: str) -> None:
    def _seen() -> bool | None:
        return True if bootstrap_embedding_entries(wm_base) else None

    poll_until(
        _seen,
        timeout=TIMEOUT_POLL_SHORT,
        interval=2.0,
        desc=f"bootstrap embeddings (X-Threlium-Thread-Root={E2E_BOOTSTRAP_THREAD_ROOT!r})",
    )


def e2e_clear_doc_status_and_restart_engine(
    project: str,
    *,
    repo_root: Path | None = None,
) -> None:
    """Wipe LightRAG state (redis KV + faiss) and restart engine → forces bootstrap RE-EMBED.

    LightRAG doc_status / full_docs / chunks / entities живут в **Redis** (ключи ``doc_status:*``,
    ``full_docs:*``, ``text_chunks:*``, ``*entities*``, ``llm_response_cache:*``) + faiss-индексы в
    ``$THRELIUM_HOME/lightrag``. Старый ``rm -f kv_store_doc_status.json`` был no-op (JSON-файла нет —
    стор в Redis), поэтому рестарт ловил ``Duplicate document detected (filename)`` и пропускал
    embedding-вызов → нет ``X-Threlium-Thread-Root: e2e-bootstrap`` в журнале WireMock → таймаут.
    Полный wipe (как в cold-reset :func:`e2e_flush_sut_fsm_maildirs`) — engine при старте бутстрапит
    probe-корпус заново и реально дёргает embeddings. **Serial-only** (общий Redis/LightRAG/engine):
    модуль bootstrap пропускается под xdist (см. ``test_knowledge_bootstrap_live_e2e``).
    """
    root = repo_root or REPO_ROOT
    lightrag_dir = shlex.quote(f"{E2E_REMOTE_THRELIUM_HOME}/lightrag")
    service_exec(
        project,
        "sut",
        [
            "bash",
            "-lc",
            f"redis-cli flushall >/dev/null 2>&1 || true; "
            f"rm -rf {lightrag_dir}/* 2>/dev/null || true; "
            f"echo '[e2e] lightrag wiped (redis flushall + faiss files)'",
        ],
        repo_root=root,
        timeout=15,
    )
    restart_cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-engine.service",
    ]
    service_exec(project, "sut", restart_cmd, repo_root=root, timeout=30)
    e2e_wait_engine_active(project, repo_root=root, timeout=90.0)


def e2e_bootstrap_reindex_and_wait(rt: E2EComposeRuntime) -> None:
    """Clean WM journal, force bootstrap re-index of probe corpus, wait for embedding in journal."""
    from tests.e2e.wiremock_client import reset_request_journal, wiremock_public_base  # noqa: PLC0415

    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    reset_request_journal(wm_base)
    e2e_clear_doc_status_and_restart_engine(rt.project_name, repo_root=rt.repo_root)
    _wait_bootstrap_embeddings_in_wiremock(wm_base)


def e2e_bootstrap_scenario(rt: E2EComposeRuntime) -> Iterator[E2EComposeRuntime]:
    """Bootstrap reindex in test body; start full pipeline on exit."""
    e2e_bootstrap_reindex_and_wait(rt)
    try:
        yield rt
    finally:
        e2e_start_threlium_user_pipeline_services(rt)
