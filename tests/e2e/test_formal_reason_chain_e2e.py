"""E2E тест цепочки formal_reason → memory_query → response_finalize на **живом** стеке.

Сценарий: reasoning → formal_reason (conforms=true) → enrich_fast → reasoning →
memory_query (SHACL reference) → enrich_fast → reasoning → response_finalize.

Покрытие:
- formal_reason handler: pySHACL validate, ``<history>``-часть с conforms=true
- memory_query handler: aquery к LightRAG, ``<history>``-наблюдение через enrich_fast
- enrich_fast: сплайс сырых ``<history>``-частей окна-дельты (IRT-письма с прошлого
  ``To: reasoning``, дедуп по контент-CID) → единый поток в секции ``<conversation_delta>``
  промпта reasoning, каждая запись подписана ``[from: <stage>]``
- enrich_fast: **аддитивное** накопление ``<history>``-частей — наблюдение formal_reason НЕ
  затирается наблюдением memory_query; дельта 1-го цикла (``ex:PositiveAgeShape`` из входа
  formal_reason = ``<history>`` tool-call'а reasoning) видна в 3-м reasoning вместе с маркерами
  memory_query
- post-assert: WireMock journal (``ex:PositiveAgeShape``) + notmuch ``PositiveAgeShape`` в
  ``reasoning/Maildir`` (тело ``<history>``-части на диске; без ``:`` — notmuch phrase-tokenizer)
- Полный FSM цикл с 3 reasoning вызовами

Стабы используют фазовый автомат WireMock State Extension:
- phase_formal_reason_done: после первого reasoning → formal_reason
- phase_query_done: после второго reasoning → memory_query
- phase_query_done_ledger_done: tasks_upsert (fail-closed gate) после memory_query
- Четвёртый reasoning видит phase_query_done_ledger_done → response_finalize

**Подготовка (вне этого модуля):** shared compose + baked SUT (``wipe_bake`` / уже поднятый
``threlium_e2e_*``). Синхронизация кода и шаблонов на SUT — ``pytest -n0 tests/e2e/wipe_sync.py``
(тег ``refresh``), без полного ``site.yml`` при каждом прогоне этого файла.
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .formal_reason_assertions import (
    FULL_TOOL_NAMES,
    GATE_TOOL_NAMES,
    assert_all_reasoning_gate_absent,
    tool_names_from_chat_body,
)
from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    assert_notmuch_folder_contains_body_token,
    assert_notmuch_thread_stage_message_count_at_least,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
)
from .wiremock_client import (
    _wiremock_headers_get_ci,
    find_wiremock_requests_by_body_contains,
    wiremock_public_base,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FORMAL_REASON_BODY_MARKER = "E2E-FORMAL-REASON-CHAIN-BODY"
E2E_UNIFIED_DELTA_SHAPE_MARKER = "ex:PositiveAgeShape"
# notmuch фразовый поиск трактует ``:`` как разделитель префикса поля даже в кавычках —
# для on-disk проверки берём бесколоночный терм того же shape-маркера.
E2E_UNIFIED_DELTA_NOTMUCH_TOKEN = "PositiveAgeShape"
E2E_UNIFIED_DELTA_SECTION = "<conversation_delta>"
E2E_MEMORY_QUERY_REASONING_MARKER = "SHACL sh:sparql constraint SELECT variable binding"

FORMAL_REASON_CHAIN_SPEC = MailflowScenarioSpec(
    label="formal_reason_chain",
    raw_id_prefix="e2e-formal-reason-chain-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_chain_e2e",
    stub_tag="stub-formal-reason-chain-01",
    body_head=f"{E2E_FORMAL_REASON_BODY_MARKER}\ne2e formal_reason chain validation test body",
    min_chat_completion_posts=4,
    # reasoning #1 logic, #2 query; #3 tasks_upsert — позже, отдельный journal needle.
    min_reasoning_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.FORMAL_REASON.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.MEMORY_QUERY.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-formal-reason-verified-answer",
    # После tasks_upsert (ledger open_count=0) — короткое окно до finalize/egress в GreenMail poll.
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_phase_query_done_ledger_done",
)


def _assert_chat_journal_contains(
    wm_base: str, stub_tag: str, needle: str
) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, needle, stub_tag=stub_tag
    )
    chat_matches = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert chat_matches, (
        f"No chat/completions requests contain {needle!r} (stub_tag={stub_tag!r})"
    )


def _assert_unified_delta_in_reasoning_journal(project: str, stub_tag: str) -> None:
    """2-й/3-й reasoning: unified-delta relay дошёл до LLM-промпта (не только observation)."""
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    for needle in (E2E_UNIFIED_DELTA_SECTION, E2E_UNIFIED_DELTA_SHAPE_MARKER):
        _assert_chat_journal_contains(wm_base, stub_tag, needle)
    for needle in (E2E_UNIFIED_DELTA_SHAPE_MARKER, E2E_MEMORY_QUERY_REASONING_MARKER):
        _assert_chat_journal_contains(wm_base, stub_tag, needle)
    log.info("formal_reason_chain_unified_delta_journal_verified", stub_tag=stub_tag)


def test_formal_reason_chain_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """Knowledge system: formal_reason(conforms) -> memory_query -> response_finalize."""
    with mailflow_inject_and_wait(FORMAL_REASON_CHAIN_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            assert_full_mailflow_pipeline(
                FORMAL_REASON_CHAIN_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_unified_delta_in_reasoning_journal(project, stub_tag)
            assert_all_reasoning_gate_absent(wm_base, stub_tag)
            mq_matches = find_wiremock_requests_by_body_contains(
                wm_base, E2E_MEMORY_QUERY_REASONING_MARKER, stub_tag=stub_tag
            )
            # Маркер reasoning-контента попадает и в lightrag-индексацию (gleaning/extract) — его
            # текст индексируется как сущности. Поэтому фильтруем СТРОГО по call-site=reasoning
            # (а не берём [0] из всех chat-совпадений: на пустом индексе свежей сборки первым в
            # журнале оказывается gleaning-вызов, и тест ложно падал на его tool-наборе).
            mq_chat = [
                e
                for e in mq_matches
                if "/chat/completions" in (e.get("request", {}).get("url") or "")
                and _wiremock_headers_get_ci(
                    (e.get("request") or {}).get("headers"), "X-Threlium-Call-Site"
                ) == FsmStage.REASONING.value
            ]
            assert mq_chat, "expected memory_query reasoning journal entry (call-site=reasoning)"
            # Среди reasoning-хопов с маркером должен быть memory_query-фазовый: предлагает полный
            # набор тулов с memory_query + response_finalize, и это НЕ gate-only хоп.
            mq_tool_sets = [
                frozenset(tool_names_from_chat_body(str(e.get("request", {}).get("body") or "")))
                for e in mq_chat
            ]
            assert any(
                ts != GATE_TOOL_NAMES
                and FsmStage.MEMORY_QUERY.value in ts
                and FsmStage.RESPONSE_FINALIZE.value in ts
                and ts <= FULL_TOOL_NAMES
                for ts in mq_tool_sets
            ), (
                "expected a memory_query-phase reasoning hop offering "
                f"{{memory_query, response_finalize}} ⊆ FULL (got tool-sets {mq_tool_sets})"
            )
            assert_notmuch_folder_contains_body_token(
                project,
                stage_folder_id=FsmStage.REASONING.value,
                body_token=E2E_UNIFIED_DELTA_NOTMUCH_TOKEN,
                min_count=1,
                repo_root=REPO_ROOT,
            )
            assert_notmuch_thread_stage_message_count_at_least(
                project,
                anchor_message_id=canonical_id,
                stage_folder_id=FsmStage.ENRICH.value,
                min_count=1,
                repo_root=REPO_ROOT,
            )
            assert_notmuch_thread_stage_message_count_at_least(
                project,
                anchor_message_id=canonical_id,
                stage_folder_id=FsmStage.ENRICH_FAST.value,
                min_count=2,
                repo_root=REPO_ROOT,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
