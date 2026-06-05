"""E2E matrix: fatal parse → gate → memory_query → QUERY ERROR → gate → recovery → finalize.

Один inject, фазовый WireMock State (``stub-formal-reason-gate-matrix-01``).
Стабы: ``wiremock_stubs/test_formal_reason_gate_recovery_matrix_e2e/``.

Post-assert: несколько циклов gate ON; ``enrich_fast`` не затирает прошлые
observation/tool-call контексты ``formal_reason`` (PARSE + QUERY + memory_query relay).
"""
from __future__ import annotations

from pathlib import Path


from threlium.types import FsmStage

from .formal_reason_assertions import (
    assert_chat_request_contains_all,
    assert_gated_formal_reason_history_accumulated,
    assert_gated_reasoning_calls,
    assert_gated_reasoning_includes_memory_query,
    assert_journal_contains,
    assert_memory_query_tool_served,
    assert_ungated_reasoning_has_finalize,
)
from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
)
from .log import clip_log_body, log
from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    wiremock_public_base,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_FORMAL_REASON_GATE_MATRIX_BODY = "E2E-FORMAL-REASON-GATE-MATRIX-BODY"
# Маркеры reasoning/tool-call из matrix-стабов (должны дойти до позднего reasoning через enrich_fast).
E2E_MATRIX_FR_FATAL_REASONING = "e2e matrix: intentional invalid Turtle"
E2E_MATRIX_FR_QUERY_ERR_REASONING = (
    "e2e matrix: valid Turtle but broken SPARQL"
)
E2E_MATRIX_MEMORY_QUERY_TEXT = "turtle_syntax.md SHACL gate recovery"
E2E_MATRIX_MQ_TOOL_CALL_ID = "call_e2e_memory_query_matrix_gate"

FORMAL_REASON_GATE_MATRIX_SPEC = MailflowScenarioSpec(
    label="formal_reason_gate_recovery_matrix",
    raw_id_prefix="e2e-formal-reason-gate-matrix-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_formal_reason_gate_recovery_matrix_e2e",
    stub_tag="stub-formal-reason-gate-matrix-01",
    body_head=(
        f"{E2E_FORMAL_REASON_GATE_MATRIX_BODY}\n"
        "e2e formal_reason gate recovery matrix test body"
    ),
    min_chat_completion_posts=4,
    # gated hops: fatal → mq (до needle tasks_ledger); см. formal_reason_chain harness.
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
    reply_body_needle="e2e-formal-reason-gate-matrix-verified-answer",
    # Readiness-needle — на recovery formal_reason (надёжно <30s после сокращения матрицы:
    # fatal→mq→recovery). tasks_ledger был на грани 30s (флап из-за summarize-циклов
    # накопленной formal_reason-истории). tasks+finalize+egress дальше ловит GreenMail-poll
    # (reply_body_needle). Покрытие finalize сохранено: сматченные стабы 104/105 + ответ.
    wiremock_journal_ready_needle="call_e2e_formal_reason_matrix_recovery",
)


def _assert_at_least_two_gated_reasoning_calls(wm_base: str, stub_tag: str) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, "Gate retry counter:", stub_tag=stub_tag
    )
    chat = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert len(chat) >= 2, (
        f"expected at least 2 gated reasoning calls, got {len(chat)} "
        f"(stub_tag={stub_tag!r})"
    )



def test_formal_reason_gate_recovery_matrix_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    with mailflow_inject_and_wait(FORMAL_REASON_GATE_MATRIX_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FORMAL_REASON_GATE_MATRIX_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            assert_journal_contains(wm_base, stub_tag, "PARSE ERROR")
            assert_journal_contains(wm_base, stub_tag, "FSM locked")
            # QUERY ERROR hop убран из матрицы (покрыт technical_gate); матрица:
            # fatal(parse) → memory_query(gated) → recovery → tasks → finalize.
            _assert_at_least_two_gated_reasoning_calls(wm_base, stub_tag)
            assert_gated_reasoning_calls(wm_base, stub_tag)
            assert_gated_reasoning_includes_memory_query(wm_base, stub_tag)
            assert_memory_query_tool_served(
                wm_base,
                stub_tag,
                tool_call_id=E2E_MATRIX_MQ_TOOL_CALL_ID,
            )
            assert_journal_contains(
                wm_base, stub_tag, E2E_MATRIX_MEMORY_QUERY_TEXT
            )
            # Накопление контекста — post-assert по WireMock journal (не bodyPatterns стабов).
            # Первый gated hop (101, stub phase_matrix_fatal_done): после fatal formal_reason
            # enrich_fast relayed PARSE в дельту → gate ON. Текст memory_query ещё не в промпте
            # (ответ 101 только запрашивает MQ; relay query — на 103 после ungated 102).
            assert_chat_request_contains_all(
                wm_base,
                stub_tag,
                (
                    "PARSE ERROR",
                    "FSM locked",
                    "Gate retry counter:",
                    E2E_MATRIX_FR_FATAL_REASONING,
                    "<conversation_delta>",
                ),
                gate_only=True,
                exclude=(
                    "QUERY ERROR",
                    E2E_MATRIX_FR_QUERY_ERR_REASONING,
                    E2E_MATRIX_MEMORY_QUERY_TEXT,
                ),
            )
            # Поздний gated hop (recovery): PARSE (fatal) + formal_reason tool-call + MQ в одном промпте.
            assert_gated_formal_reason_history_accumulated(
                wm_base,
                stub_tag,
                prior_formal_reason_markers=(
                    E2E_MATRIX_FR_FATAL_REASONING,
                ),
                error_observation_markers=("PARSE ERROR", "FSM locked"),
                memory_query_marker=E2E_MATRIX_MEMORY_QUERY_TEXT,
            )
            # Финальный ungated reasoning: весь накопленный контур ошибок + успешный query_result.
            assert_chat_request_contains_all(
                wm_base,
                stub_tag,
                (
                    "PARSE ERROR",
                    E2E_MATRIX_MEMORY_QUERY_TEXT,
                    E2E_MATRIX_FR_FATAL_REASONING,
                    "query_result:",
                    "<conversation_delta>",
                ),
                gate_only=False,
            )
            assert_ungated_reasoning_has_finalize(
                wm_base, stub_tag, needle="query_result:"
            )
        except Exception:
            log.error(
                "formal_reason_gate_matrix_failed",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
