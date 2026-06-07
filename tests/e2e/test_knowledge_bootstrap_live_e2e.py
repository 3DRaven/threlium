"""E2E: детерминированная bootstrap-индексация knowledge + инфраструктурные проверки.

Детерминизм: probe-корпус ``e2e_bootstrap_probe.md`` ставится при cold reset
(:func:`tests.e2e.toolkit.knowledge.e2e_install_deterministic_knowledge_corpus` в ``wipe_sync`` /
``fsts_between_test_reset``), не в pytest-fixture.

Reindex probe — :func:`tests.e2e.toolkit.knowledge.e2e_bootstrap_reindex_and_wait` из тела теста
(``e2e_runtime`` для этого модуля только discover, без pipeline).

Проверки:
- P1: на SUT в ``knowledge/`` — ровно один probe-документ;
- doc_status: probe после bootstrap;
- B1: WM journal — embedding с ``X-Threlium-Thread-Root: e2e-bootstrap``;
- R4: промпты reasoning/memory_query, formal_reason и observation на месте;
- B2: повторный restart engine — без новых embedding (LightRAG dedup).

Serial-only (skip под xdist, E2E.md §5): reindex дёргает ``redis-cli flushall`` + рестарт ОБЩЕГО
engine — это снесло бы LightRAG/FSM-состояние параллельных воркеров. Валидируется в ``-n0``;
под ``-n N`` модуль показывается как ``skipped``, не ломая остальных.
"""
from __future__ import annotations

import os
import time

import pytest

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .toolkit import (
    E2EComposeRuntime,
    E2E_BOOTSTRAP_THREAD_ROOT,
    E2E_KNOWLEDGE_PROBE_FILENAME,
    REPO_ROOT,
    bootstrap_embedding_entries,
    bootstrap_embedding_entry_ids,
    e2e_bootstrap_reindex_and_wait,
    e2e_start_threlium_user_pipeline_services,
    e2e_wait_engine_active,
    service_exec,
    wait_for_sut_threlium_user_workers_idle,
)
from .wiremock_client import (
    THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
    journal_entries_for_stub_tag_with_header,
    wiremock_public_base,
)

pytestmark = pytest.mark.skipif(
    os.environ.get("PYTEST_XDIST_WORKER") is not None,
    reason=(
        "knowledge bootstrap reindex does redis flushall + restarts the shared engine "
        "→ serial only (-n0); validated outside xdist (E2E.md §5)"
    ),
)

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
_LIGHTRAG_DOC_STATUS = f"{_THRELIUM_HOME}/lightrag/kv_store_doc_status.json"
_KNOWLEDGE_DIR = f"{_THRELIUM_HOME}/knowledge"


def test_knowledge_files_deployed(e2e_runtime: E2EComposeRuntime) -> None:
    """P1: на SUT в knowledge/ — ровно один probe-документ (детерминированный корпус)."""
    project = e2e_runtime.project_name

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


def test_knowledge_docs_indexed_in_lightrag(e2e_runtime: E2EComposeRuntime) -> None:
    """LightRAG ``doc_status`` содержит probe-документ после bootstrap.

    LightRAG KV/doc-status хранится в **Redis** (не в ``kv_store_doc_status.json`` — файла нет),
    поэтому читаем значения ключей ``doc_status:*`` через ``redis-cli``: в content_summary каждой
    записи лежит ``Subject: <filename>``.
    """
    e2e_bootstrap_reindex_and_wait(e2e_runtime)
    project = e2e_runtime.project_name
    cmd = [
        "bash",
        "-lc",
        "for k in $(redis-cli --scan --pattern 'doc_status:*' 2>/dev/null); do "
        "redis-cli get \"$k\" 2>/dev/null; done",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
    text = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, f"redis doc_status unreadable: {text[:400]!r}"
    assert E2E_KNOWLEDGE_PROBE_FILENAME in text, (
        f"expected {E2E_KNOWLEDGE_PROBE_FILENAME!r} in redis doc_status:*; snippet={text[:500]!r}"
    )
    log.info("knowledge_docs_in_doc_status", doc=E2E_KNOWLEDGE_PROBE_FILENAME)
    e2e_start_threlium_user_pipeline_services(e2e_runtime)


def test_knowledge_prompts_deployed(e2e_runtime: E2EComposeRuntime) -> None:
    """R4: all reasoning/memory_query, reasoning/formal_reason and observation prompts on SUT."""
    project = e2e_runtime.project_name
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


def test_bootstrap_knowledge_called_wiremock(e2e_runtime: E2EComposeRuntime) -> None:
    """B1: WireMock received embedding requests with X-Threlium-Thread-Root: e2e-bootstrap."""
    e2e_bootstrap_reindex_and_wait(e2e_runtime)
    wm_base = wiremock_public_base(e2e_runtime.wiremock_host, e2e_runtime.wiremock_port)

    all_entries = journal_entries_for_stub_tag_with_header(
        wm_base,
        stub_tag=THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
        header_name="X-Threlium-Thread-Root",
        header_value=E2E_BOOTSTRAP_THREAD_ROOT,
    )
    log.info(
        "bootstrap_embedding_direct_check",
        total_all_header_matched=len(all_entries),
    )

    entries = bootstrap_embedding_entries(wm_base)
    assert entries, (
        f"No embedding requests with X-Threlium-Thread-Root={E2E_BOOTSTRAP_THREAD_ROOT!r} "
        f"found in WireMock journal (stub_tag={THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG!r}, "
        f"url_contains=/embeddings). "
        f"All entries matching header (any url): {len(all_entries)}."
    )
    log.info("bootstrap_knowledge_wiremock_verified", count=len(entries))
    e2e_start_threlium_user_pipeline_services(e2e_runtime)


def test_bootstrap_idempotent_on_restart(e2e_runtime: E2EComposeRuntime) -> None:
    """B2: restart engine -> no new embedding requests with bootstrap thread-root."""
    e2e_bootstrap_reindex_and_wait(e2e_runtime)
    project = e2e_runtime.project_name
    wm_base = wiremock_public_base(e2e_runtime.wiremock_host, e2e_runtime.wiremock_port)

    ids_before = bootstrap_embedding_entry_ids(wm_base)
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
    e2e_wait_engine_active(project, timeout=90.0)
    time.sleep(8)

    ids_after = bootstrap_embedding_entry_ids(wm_base)
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
    e2e_start_threlium_user_pipeline_services(e2e_runtime)
