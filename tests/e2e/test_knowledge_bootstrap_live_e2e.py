"""E2E: детерминированная bootstrap-индексация knowledge + инфраструктурные проверки.

Детерминизм: probe-корпус ``e2e_bootstrap_probe.md`` ставится при cold reset
(:func:`tests.e2e.toolkit.knowledge.e2e_install_deterministic_knowledge_corpus` в ``wipe_sync`` /
``fsts_between_test_reset``), не в pytest-fixture.

Reindex — ОДИН раз за сессию в cold-reset лидера (``conftest._e2e_wiremock_journal_reset_once``): flushall
lightrag + рестарт engine (пере-эмбед probe) + второй рестарт без flushall (упражнение идемпотентности).
Поэтому модуль больше НЕ serial: тесты читают результат **read-only**, совместимы с ``-n N`` (skipif снят).

Проверки (все read-only):
- P1: на SUT в ``knowledge/`` — ровно один probe-документ;
- doc_status: probe в redis после bootstrap;
- B1: WM journal — embedding с ``X-Threlium-Thread-Root: e2e-bootstrap``;
- R4: промпты reasoning/memory_query, formal_reason и observation на месте;
- B2: идемпотентность — в журнале НЕТ дублей bootstrap-embedding тел (второй рестарт без flushall не
  пере-эмбедил → LightRAG dedup сработал).
"""
from __future__ import annotations

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import E2E_THRELIUM_USER

from .toolkit import (
    E2EComposeRuntime,
    E2E_BOOTSTRAP_THREAD_ROOT,
    E2E_KNOWLEDGE_PROBE_FILENAME,
    REPO_ROOT,
    bootstrap_embedding_entries,
    service_exec,
)
from .wiremock_client import (
    THRELIUM_WIREMOCK_COMPOSE_BOOTSTRAP_STUB_TAG,
    journal_entries_for_stub_tag_with_header,
    wiremock_public_base,
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
    # Read-only: bootstrap reindex сделан в session cold-reset (conftest), здесь только читаем redis.
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
    # Read-only: bootstrap embeddings произведены в session cold-reset (conftest reindex).
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


def test_bootstrap_idempotent_on_restart(e2e_runtime: E2EComposeRuntime) -> None:
    """B2 (read-only): рестарт engine БЕЗ flushall (cold-reset сделал) → LightRAG обнаружил probe как
    ДУБЛИКАТ и НЕ переиндексировал.

    Authoritative-сигнал дедупа — redis ``doc_status`` (НЕ счётчик embedding-запросов в журнале: LightRAG
    шлёт chunk-эмбеддинги ДО doc-level dup-проверки, поэтому журнал показывает повторные запросы даже когда
    документ дедуплицирован). После re-insert на рестарте в ``doc_status`` появляется запись
    ``[DUPLICATE:filename]`` (``status: failed``, ``chunks_count: 0``, ``Original doc_id: …, Status:
    processed``) — это и есть доказательство, что dedup сработал. Read-only → совместимо с ``-n N``."""
    project = e2e_runtime.project_name
    cmd = [
        "bash", "-lc",
        "for k in $(redis-cli --scan --pattern 'doc_status:*' 2>/dev/null); do "
        "redis-cli get \"$k\" 2>/dev/null; done",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
    text = (r.stdout or "") + (r.stderr or "")
    assert r.returncode == 0, f"redis doc_status unreadable: {text[:400]!r}"
    # probe проиндексирован (status processed) И его re-insert на рестарте дедуплицирован.
    assert E2E_KNOWLEDGE_PROBE_FILENAME in text, (
        f"probe {E2E_KNOWLEDGE_PROBE_FILENAME!r} not in doc_status; snippet={text[:400]!r}"
    )
    assert "[DUPLICATE:filename]" in text, (
        "expected a LightRAG duplicate-detected doc_status entry ([DUPLICATE:filename]) — proof that the "
        f"engine restart re-inserted the probe and dedup skipped re-indexing. snippet={text[:600]!r}"
    )
    log.info("bootstrap_idempotent_dedup_detected", probe=E2E_KNOWLEDGE_PROBE_FILENAME)
