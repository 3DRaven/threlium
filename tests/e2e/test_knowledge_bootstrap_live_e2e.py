"""E2E live-only smoke test: bootstrap knowledge + infrastructure checks.

Проверяет на уже поднятом стеке (без inject):
- P1: файлы knowledge/*.md на месте в $THRELIUM_HOME/knowledge/
- B1: WireMock journal содержит embedding-запросы с X-Threlium-Thread-Root: e2e-bootstrap
- R4: все промпты reasoning/memory_query, reasoning/logic_validate и observation на месте
- B2: повторный restart engine не генерирует новых embedding-запросов к WireMock

Тип: @pytest.mark.e2e_live — если стека нет, тест пропускается.

xdist_group=engine_restart — при pytest -n N все тесты этого модуля
группируются в одном воркере, чтобы restart engine не прерывал
параллельные mailflow-тесты на том же SUT.
"""
from __future__ import annotations

import pytest

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .helpers import (
    REPO_ROOT,
    discover_live_e2e_project_name,
    discover_runtime,
    poll_until,
    service_exec,
    wait_for_sut_threlium_user_workers_idle,
)
from .wiremock_client import (
    journal_entries_for_stub_tag_with_header,
    wiremock_public_base,
    THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
)

pytestmark = pytest.mark.xdist_group(name="engine_restart")

_KNOWLEDGE_FILES = [
    "shacl_sparql.md",
    "sparql_functions.md",
    "turtle_syntax.md",
]

_KNOWLEDGE_PROMPTS = [
    "reasoning/logic_validate/tool_spec.j2",
    "reasoning/logic_validate/email_body.j2",
    "reasoning/logic_validate/email_subject.j2",
    "reasoning/memory_query/tool_spec.j2",
    "reasoning/memory_query/email_body.j2",
    "reasoning/memory_query/email_subject.j2",
    "logic_validate/observation.j2",
    "memory_query/observation.j2",
]

_THRELIUM_HOME = f"/home/{E2E_THRELIUM_USER}/threlium/data"
_BOOTSTRAP_THREAD_ROOT = "e2e-bootstrap"
_LIGHTRAG_DOC_STATUS = f"{_THRELIUM_HOME}/lightrag/kv_store_doc_status.json"


def _bootstrap_embedding_entries(wm_base: str) -> list[dict]:
    """WireMock journal entries: embedding requests from bootstrap (by thread-root header)."""
    return journal_entries_for_stub_tag_with_header(
        wm_base,
        stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        header_name="X-Threlium-Thread-Root",
        header_value=_BOOTSTRAP_THREAD_ROOT,
        url_contains="/embeddings",
    )


def _wait_engine_active(project: str, *, timeout: float = 60.0) -> None:
    cmd = [
        "bash",
        "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user is-active threlium-engine.service",
    ]

    def _check() -> str | None:
        r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=15)
        if (r.stdout or "").strip() == "active":
            return "active"
        return None

    poll_until(_check, timeout=timeout, interval=2.0, desc="threlium-engine.service active")


def _clear_doc_status_and_restart_engine(project: str) -> None:
    """Remove LightRAG doc_status so bootstrap re-indexes, then restart engine."""
    rm_cmd = [
        "bash", "-lc",
        f"rm -f {_LIGHTRAG_DOC_STATUS}",
    ]
    service_exec(project, "sut", rm_cmd, repo_root=REPO_ROOT, timeout=10)

    restart_cmd = [
        "bash", "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-engine.service",
    ]
    service_exec(project, "sut", restart_cmd, repo_root=REPO_ROOT, timeout=30)
    _wait_engine_active(project, timeout=90.0)


@pytest.fixture(scope="module")
def live_bootstrap_runtime():
    """E2EComposeRuntime for a live stack; force fresh bootstrap by clearing doc_status."""
    pn = discover_live_e2e_project_name()
    if not pn:
        pytest.skip(
            "No live e2e stack: start compose (pytest tests/e2e / wipe_bake)."
        )
    try:
        rt = discover_runtime(pn)
    except Exception as e:
        pytest.skip(f"live e2e stack not reachable: {e}")

    wait_for_sut_threlium_user_workers_idle(pn, repo_root=REPO_ROOT, timeout=60.0)
    _clear_doc_status_and_restart_engine(pn)
    import time
    time.sleep(30)
    return pn, rt


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_knowledge_files_deployed(live_bootstrap_runtime) -> None:
    """P1: knowledge/*.md files exist on SUT in $THRELIUM_HOME/knowledge/."""
    project, _rt = live_bootstrap_runtime
    for fname in _KNOWLEDGE_FILES:
        path = f"{_THRELIUM_HOME}/knowledge/{fname}"
        cmd = ["bash", "-lc", f"test -f {path} && echo OK || echo MISSING"]
        r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=10)
        output = (r.stdout or "").strip()
        assert output == "OK", (
            f"Knowledge file missing on SUT: {path}\n"
            f"stdout={r.stdout!r} stderr={r.stderr!r}"
        )
    log.info("knowledge_files_deployed", count=len(_KNOWLEDGE_FILES))


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_knowledge_prompts_deployed(live_bootstrap_runtime) -> None:
    """R4: all reasoning/memory_query, reasoning/logic_validate and observation prompts on SUT."""
    project, _rt = live_bootstrap_runtime
    for rel in _KNOWLEDGE_PROMPTS:
        path = f"{_THRELIUM_HOME}/prompts/{rel}"
        cmd = ["bash", "-lc", f"test -f {path} && echo OK || echo MISSING"]
        r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=10)
        output = (r.stdout or "").strip()
        assert output == "OK", (
            f"Prompt file missing on SUT: {path}\n"
            f"stdout={r.stdout!r} stderr={r.stderr!r}"
        )
    log.info("knowledge_prompts_deployed", count=len(_KNOWLEDGE_PROMPTS))


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_bootstrap_knowledge_called_wiremock(live_bootstrap_runtime) -> None:
    """B1: WireMock received embedding requests with X-Threlium-Thread-Root: e2e-bootstrap."""
    project, rt = live_bootstrap_runtime
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)

    all_entries = journal_entries_for_stub_tag_with_header(
        wm_base,
        stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        header_name="X-Threlium-Thread-Root",
        header_value=_BOOTSTRAP_THREAD_ROOT,
    )
    log.info(
        "bootstrap_embedding_direct_check",
        total_all_header_matched=len(all_entries),
    )

    entries = _bootstrap_embedding_entries(wm_base)
    assert entries, (
        f"No embedding requests with X-Threlium-Thread-Root={_BOOTSTRAP_THREAD_ROOT!r} "
        f"found in WireMock journal (stub_tag={THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG!r}, "
        f"url_contains=/embeddings). "
        f"All entries matching header (any url): {len(all_entries)}."
    )
    log.info("bootstrap_knowledge_wiremock_verified", count=len(entries))


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_bootstrap_idempotent_on_restart(live_bootstrap_runtime) -> None:
    """B2: restart engine -> no new embedding requests with bootstrap thread-root."""
    project, rt = live_bootstrap_runtime
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)

    count_before = len(_bootstrap_embedding_entries(wm_base))
    log.info("bootstrap_idempotent_pre_restart", count_before=count_before)

    wait_for_sut_threlium_user_workers_idle(project, repo_root=REPO_ROOT, timeout=60.0)

    restart_cmd = [
        "bash", "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-engine.service",
    ]
    service_exec(project, "sut", restart_cmd, repo_root=REPO_ROOT, timeout=30)
    _wait_engine_active(project, timeout=90.0)

    import time
    time.sleep(10)

    count_after = len(_bootstrap_embedding_entries(wm_base))
    log.info("bootstrap_idempotent_post_restart", count_before=count_before, count_after=count_after)
    assert count_after == count_before, (
        f"Bootstrap generated new embedding requests after restart: "
        f"before={count_before}, after={count_after}. "
        f"LightRAG deduplication did not prevent re-indexing."
    )
