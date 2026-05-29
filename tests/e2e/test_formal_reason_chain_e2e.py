"""E2E тест цепочки formal_reason → memory_query → response_finalize на **живом** стеке.

Сценарий: reasoning → formal_reason (conforms=true) → enrich_fast → reasoning →
memory_query (SHACL reference) → enrich_fast → reasoning → response_finalize.

Покрытие:
- formal_reason handler: pySHACL validate, observation-note с conforms=true
- memory_query handler: aquery к LightRAG, observation relay через enrich_fast
- enrich_fast: **аддитивное** накопление relay observation-частей с уникальными
  Content-ID (``<observation-note@mid>``) — observation от formal_reason НЕ
  затирается observation от memory_query; промпт 3-го reasoning содержит оба
  маркера (``conforms: True`` + memory_query reasoning), см. stub 102.
- Полный FSM цикл с 3 reasoning вызовами

Стабы используют фазовый автомат WireMock State Extension:
- phase_formal_reason_done: после первого reasoning → formal_reason
- phase_query_done: после второго reasoning → memory_query
- Третий reasoning видит phase_query_done → response_finalize

**Подготовка (вне этого модуля):** shared compose + baked SUT (``wipe_bake`` / уже поднятый
``threlium_e2e_*``). Синхронизация кода и шаблонов на SUT — ``pytest -n0 tests/e2e/wipe_sync.py``
(тег ``refresh``), без полного ``site.yml`` при каждом прогоне этого файла.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FORMAL_REASON_BODY_MARKER = "E2E-FORMAL-REASON-CHAIN-BODY"

FORMAL_REASON_CHAIN_SPEC = MailflowScenarioSpec(
    label="formal_reason_chain",
    raw_id_prefix="e2e-formal-reason-chain-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_chain_e2e",
    stub_tag="stub-formal-reason-chain-01",
    body_head=f"{E2E_FORMAL_REASON_BODY_MARKER}\ne2e formal_reason chain validation test body",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.FORMAL_REASON.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.MEMORY_QUERY.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-formal-reason-verified-answer",
)


@pytest.fixture()
def formal_reason_chain_processed_stack(live_e2e_stack_ready: str) -> object:
    """WireMock (formal_reason_chain) -> inject -> \\Seen -> FSM activity (live stack)."""
    with mailflow_inject_and_wait(FORMAL_REASON_CHAIN_SPEC, live_e2e_stack_ready) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_formal_reason_chain_full_pipeline(
    formal_reason_chain_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """Knowledge system: formal_reason(conforms) -> memory_query -> response_finalize."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        formal_reason_chain_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            FORMAL_REASON_CHAIN_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
