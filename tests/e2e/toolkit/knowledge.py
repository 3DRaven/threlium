"""Knowledge bootstrap and LightRAG warmup helpers."""
from __future__ import annotations

import shlex
from pathlib import Path

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .constants import (
    E2E_BOOTSTRAP_THREAD_ROOT,
    E2E_KNOWLEDGE_PROBE_FILENAME,
    E2E_REMOTE_THRELIUM_HOME,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    _E2E_KNOWLEDGE_PROBE_CONTENT,
)
from .poll import poll_until
from .runtime import E2EComposeRuntime, service_exec

def e2e_install_deterministic_knowledge_corpus(rt: E2EComposeRuntime) -> None:
    """Заменить knowledge/ на SUT одним детерминированным probe-документом (e2e-среда, навсегда).

    Вызывать в cold-reset preflight **после** :func:`e2e_flush_sut_fsm_maildirs` (тот стирает
    ``$TH/lightrag``) и **до** старта engine: при старте bootstrap проиндексирует ровно probe.
    Бэкапа/возврата нет — это только тестовая среда; настоящий корпус возвращается лишь полным
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


def _wait_bootstrap_embeddings_in_wiremock(wm_base: str) -> None:
    def _seen() -> bool | None:
        return True if bootstrap_embedding_entries(wm_base) else None

    poll_until(
        _seen,
        timeout=TIMEOUT_POLL_SHORT,
        interval=2.0,
        desc=f"bootstrap embeddings (X-Threlium-Thread-Root={E2E_BOOTSTRAP_THREAD_ROOT!r})",
    )


def e2e_restart_threlium_engine_only(project: str, *, repo_root: Path | None = None) -> None:
    """Рестарт ``threlium-engine`` БЕЗ flushall lightrag — упражняет идемпотентность bootstrap (LightRAG
    dedup по doc_status в Redis: второй старт не должен пере-эмбедить уже проиндексированный probe-корпус)."""
    root = repo_root or REPO_ROOT
    restart_cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-engine.service",
    ]
    service_exec(project, "sut", restart_cmd, repo_root=root, timeout=30)
    e2e_wait_engine_active(project, repo_root=root, timeout=90.0)


def _wait_bootstrap_doc_status_persisted(project: str, *, repo_root: Path | None = None) -> None:
    """Дождаться, что bootstrap ЗАПИСАЛ probe в redis ``doc_status`` (не только embedding-request в журнал).

    Барьер перед последующим рестартом без flushall: embedding-request попадает в журнал ДО того, как
    LightRAG персистит ``doc_status``; рестарт в этом окне видит пустой doc_status → пере-эмбедит (ложный
    dedup-fail). Поллим redis, пока probe не появится в ``doc_status:*``."""
    root = repo_root or REPO_ROOT
    cmd = [
        "bash", "-lc",
        "for k in $(redis-cli --scan --pattern 'doc_status:*' 2>/dev/null); do "
        "redis-cli get \"$k\" 2>/dev/null; done",
    ]

    def _probe() -> bool | None:
        r = service_exec(project, "sut", cmd, repo_root=root, timeout=15)
        text = (r.stdout or "") + (r.stderr or "")
        return True if E2E_KNOWLEDGE_PROBE_FILENAME in text else None

    poll_until(_probe, timeout=TIMEOUT_POLL_SHORT, desc="redis doc_status has bootstrap probe")


def e2e_wait_bootstrap_indexed(rt: E2EComposeRuntime) -> None:
    """Барьер: дождаться, что свеже-стартовавший engine проиндексировал probe-корпус.

    Вызывать в cold-reset ПОСЛЕ старта engine на чистом сторе (lightrag wiped +
    :func:`e2e_install_deterministic_knowledge_corpus`). Два сигнала: (1) bootstrap-embedding
    (``X-Threlium-Thread-Root: e2e-bootstrap``) дошёл до WireMock; (2) probe персистнут в redis
    ``doc_status`` — это и есть готовность индекса. Второй барьер обязателен перед идемпотентным
    рестартом (:func:`e2e_restart_threlium_engine_only`): рестарт в окне до персиста увидел бы пустой
    doc_status → пере-эмбедил бы (ложный dedup-fail, нет ``[DUPLICATE:filename]``).
    """
    from tests.e2e.wiremock_client import wiremock_public_base  # noqa: PLC0415

    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    _wait_bootstrap_embeddings_in_wiremock(wm_base)
    _wait_bootstrap_doc_status_persisted(rt.project_name, repo_root=rt.repo_root)
