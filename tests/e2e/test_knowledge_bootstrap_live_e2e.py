"""E2E live-only: **детерминированная** bootstrap-индексация knowledge + инфраструктурные проверки.

Реальный задеплоенный корпус knowledge/*.md насчитывает десятки документов разной длины:
число chunk'ов (а значит entity-extraction chat + embedding вызовов к WireMock) от запуска
к запуску плавает, индексация долгая, а счётчики B1/B2 — недетерминированы.

Детерминизм обеспечивается **на этапе подготовки прогона** (cold reset в ``conftest``,
:func:`tests.e2e.helpers.e2e_install_deterministic_knowledge_corpus`): на SUT ``knowledge/``
заменяется одним синтетическим probe-документом **навсегда** (тестовая среда, без бэкапа —
настоящий корпус возвращается только полным rebake). Поэтому фикстура здесь лишь:

1. останавливает user-pipeline на SUT (чтобы не мешали залипшие auto-restart воркеры);
2. чистит журнал WireMock (исключаем e2e-bootstrap записи прошлых модулей);
3. удаляет LightRAG ``doc_status`` и рестартит engine → bootstrap переиндексирует probe;
4. ждёт появления bootstrap-embedding в журнале WireMock.

Проверки:
- P1: на SUT в ``knowledge/`` — ровно один probe-документ (детерминированный корпус);
- doc_status: содержит probe-документ после bootstrap;
- B1: WireMock journal содержит embedding-запросы с ``X-Threlium-Thread-Root: e2e-bootstrap``;
- R4: промпты reasoning/memory_query, reasoning/formal_reason и observation на месте;
- B2: повторный restart engine не генерирует новых embedding-запросов (LightRAG dedup).

Тип: @pytest.mark.e2e_live — если стека нет, тест пропускается.

xdist_group=engine_restart — при pytest -n N все тесты этого модуля группируются в одном
воркере, чтобы restart engine не пересекался с параллельными mailflow-тестами.
"""
from __future__ import annotations

import pytest

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .helpers import (
    E2EComposeRuntime,
    E2E_KNOWLEDGE_PROBE_FILENAME,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    discover_runtime,
    e2e_start_threlium_user_pipeline_services,
    poll_until,
    service_exec,
    wait_for_sut_threlium_user_workers_idle,
)
from .wiremock_client import (
    journal_entries_for_stub_tag_with_header,
    reset_request_journal,
    wiremock_public_base,
    THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
)

pytestmark = pytest.mark.xdist_group(name="engine_restart")

_KNOWLEDGE_PROMPTS = [
    "reasoning/formal_reason/tool_spec.j2",
    "reasoning/formal_reason/email_body.j2",
    "reasoning/formal_reason/email_subject.j2",
    "reasoning/memory_query/tool_spec.j2",
    "reasoning/memory_query/email_body.j2",
    "reasoning/memory_query/email_subject.j2",
    "formal_reason/observation_passed.j2",
    "formal_reason/observation_fatal.j2",
    "formal_reason/observation_supplemental_error.j2",
    "formal_reason/observation_shacl_negative.j2",
    "memory_query/observation.j2",
]

_THRELIUM_HOME = f"/home/{E2E_THRELIUM_USER}/threlium/data"
_BOOTSTRAP_THREAD_ROOT = "e2e-bootstrap"
_LIGHTRAG_DOC_STATUS = f"{_THRELIUM_HOME}/lightrag/kv_store_doc_status.json"
_KNOWLEDGE_DIR = f"{_THRELIUM_HOME}/knowledge"


def _bootstrap_embedding_entries(wm_base: str) -> list[dict]:
    """WireMock journal entries: embedding requests from bootstrap (by thread-root header)."""
    return journal_entries_for_stub_tag_with_header(
        wm_base,
        stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        header_name="X-Threlium-Thread-Root",
        header_value=_BOOTSTRAP_THREAD_ROOT,
        url_contains="/embeddings",
    )


def _bootstrap_embedding_entry_ids(wm_base: str) -> set[str]:
    return {
        str(e.get("id") or "")
        for e in _bootstrap_embedding_entries(wm_base)
        if e.get("id")
    }


def _wait_bootstrap_embeddings_in_wiremock(wm_base: str) -> None:
    """Poll WM journal until engine bootstrap posted at least one embedding."""

    def _seen() -> bool | None:
        return True if _bootstrap_embedding_entries(wm_base) else None

    poll_until(
        _seen,
        timeout=TIMEOUT_POLL_SHORT,
        interval=2.0,
        desc=f"bootstrap embeddings (X-Threlium-Thread-Root={_BOOTSTRAP_THREAD_ROOT!r})",
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
def live_bootstrap_runtime(compose_stack: E2EComposeRuntime):
    """Live stack + свежий детерминированный bootstrap (корпус уже подменён в cold reset).

    Обязательно после ``compose_stack``: session cold reset делает ``DELETE /__admin/requests``.
    Без этой зависимости module-фикстура поднималась раньше autouse compose_stack, bootstrap
    embeddings попадали в журнал, cold reset их стирал, тело теста видело пустой journal.
    """
    pn = compose_stack.project_name
    rt = discover_runtime(pn)

    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    # compose_stack/cold reset уже остановил pipeline, подменил knowledge на probe и поднял engine.
    # Здесь только: чистый журнал → rm doc_status → restart (переиндексация probe) → ждём embedding.
    reset_request_journal(wm_base)
    _clear_doc_status_and_restart_engine(pn)
    _wait_bootstrap_embeddings_in_wiremock(wm_base)
    try:
        yield pn, rt
    finally:
        e2e_start_threlium_user_pipeline_services(rt)


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_knowledge_files_deployed(live_bootstrap_runtime) -> None:
    """P1: на SUT в knowledge/ — ровно один probe-документ (детерминированный корпус)."""
    project, _rt = live_bootstrap_runtime

    probe_path = f"{_KNOWLEDGE_DIR}/{E2E_KNOWLEDGE_PROBE_FILENAME}"
    r = service_exec(
        project, "sut",
        ["bash", "-lc", f"test -f {probe_path} && echo OK || echo MISSING"],
        repo_root=REPO_ROOT, timeout=10,
    )
    assert (r.stdout or "").strip() == "OK", (
        f"probe corpus document missing on SUT: {probe_path}\n"
        f"stdout={r.stdout!r} stderr={r.stderr!r}"
    )

    r = service_exec(
        project, "sut",
        ["bash", "-lc", f"find {_KNOWLEDGE_DIR} -name '*.md' | wc -l"],
        repo_root=REPO_ROOT, timeout=10,
    )
    active_md = int((r.stdout or "0").strip())
    assert active_md == 1, (
        f"expected exactly 1 active knowledge/*.md (deterministic probe), got {active_md}"
    )
    log.info("knowledge_files_deployed", active_md=active_md)


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_knowledge_docs_indexed_in_lightrag(live_bootstrap_runtime) -> None:
    """LightRAG ``doc_status`` содержит probe-документ после bootstrap."""
    project, _rt = live_bootstrap_runtime
    cmd = [
        "bash",
        "-lc",
        f"test -f {_LIGHTRAG_DOC_STATUS} && cat {_LIGHTRAG_DOC_STATUS}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
    text = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, f"doc_status unreadable: {text[:400]!r}"
    assert E2E_KNOWLEDGE_PROBE_FILENAME in text, (
        f"expected {E2E_KNOWLEDGE_PROBE_FILENAME!r} in lightrag doc_status; snippet={text[:500]!r}"
    )
    log.info("knowledge_docs_in_doc_status", doc=E2E_KNOWLEDGE_PROBE_FILENAME)


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_knowledge_prompts_deployed(live_bootstrap_runtime) -> None:
    """R4: all reasoning/memory_query, reasoning/formal_reason and observation prompts on SUT."""
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

    ids_before = _bootstrap_embedding_entry_ids(wm_base)
    assert ids_before, "expected bootstrap embedding journal entries before restart"
    log.info("bootstrap_idempotent_pre_restart", count_before=len(ids_before))

    wait_for_sut_threlium_user_workers_idle(project, repo_root=REPO_ROOT)

    restart_cmd = [
        "bash", "-lc",
        f"runuser -u {E2E_THRELIUM_USER} -- env "
        f"XDG_RUNTIME_DIR=/run/user/$(id -u {E2E_THRELIUM_USER}) "
        "systemctl --user restart threlium-engine.service",
    ]
    service_exec(project, "sut", restart_cmd, repo_root=REPO_ROOT, timeout=30)
    _wait_engine_active(project, timeout=90.0)

    # Дать bootstrap отработать (no-op при dedup): если бы re-index случился, embedding-вызовы
    # появились бы в этом окне. Окно < единого e2e-таймаута.
    import time
    time.sleep(8)

    ids_after = _bootstrap_embedding_entry_ids(wm_base)
    new_ids = ids_after - ids_before
    log.info(
        "bootstrap_idempotent_post_restart",
        count_before=len(ids_before),
        count_after=len(ids_after),
        new_ids=sorted(new_ids),
    )
    assert not new_ids, (
        f"Bootstrap generated new embedding requests after restart: new_ids={sorted(new_ids)!r}. "
        f"LightRAG deduplication did not prevent re-indexing."
    )
