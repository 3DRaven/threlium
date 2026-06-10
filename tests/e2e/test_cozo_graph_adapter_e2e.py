"""CozoGraphStorage adapter STRUCTURE e2e — deterministic extract → known nodes/edges in the cozo graph →
query retrieves them → reasoning content-flags (state-only, no docker-exec). Confirms the graph adapter
actually stores AND serves the structure (upsert_node/upsert_edge → get/degree/get_node_edges/batch via the
query traversal), not just "the suite is green".

Mechanism (we own the LLM endpoints; graph is GLOBAL across threads; embeddings deterministic → retrieval
deterministic):
  1. INDEX turn (thread-root A): the extract stub returns entities ``ZZNODEALPHA``/``ZZNODEBETA`` + a
     relationship keyed ``ZZRELMARKER`` → lightrag indexes them into cozo (``upsert_node``/``upsert_edge``).
     Wait until the async drain processed it (``extract_knowledge_graph`` call-site appears on A).
  2. QUERY turn (thread-root B, a NEW thread over the same global graph): enrich queries the graph →
     retrieves the entities+relation → the ``generate_rag_answer`` stub ECHOES the present markers from its
     request into the answer → that answer reaches reasoning → reasoning recordState sets sticky content-flags
     ``saw_alpha``/``saw_beta``/``saw_rel``. Assert all == "1" → the cozo adapter stored and served the graph.

Stub dir ``wiremock_stubs/test_cozo_graph_adapter_e2e`` (cloned from summarize, retuned): 085 extract = the
rich entity/relation set; 082 generate_rag_answer = marker echo; 100* reasoning = the three content-flags.
"""
from __future__ import annotations

import time
from pathlib import Path

from tests.e2e.log import clip_log_body, log

from .test_reasoning_litellm_mock_live import REASONING_E2E_BODY_MARKER
from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    poll_until,
)
from .wiremock_client import (
    wiremock_public_base,
    wiremock_state_thread_root_call_sites,
    wiremock_state_thread_root_property,
)

_STUB_DIR = Path(__file__).resolve().parent / "wiremock_stubs" / "test_cozo_graph_adapter_e2e"
_STUB_TAG = "stub-cozo-graph-adapter-01"

INDEX_SPEC = MailflowScenarioSpec(
    label="cozo_graph_index",
    raw_id_prefix="cozo-graph-idx-",
    stub_dir=_STUB_DIR,
    stub_tag=_STUB_TAG,
    body_head=f"{REASONING_E2E_BODY_MARKER}\ncozo graph adapter index turn ZZNODEALPHA ZZNODEBETA ZZRELMARKER",
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_phase_tasks_ledger_done",
)

QUERY_SPEC = MailflowScenarioSpec(
    label="cozo_graph_query",
    raw_id_prefix="cozo-graph-qry-",
    stub_dir=_STUB_DIR,
    stub_tag=_STUB_TAG,
    body_head=f"{REASONING_E2E_BODY_MARKER}\ncozo graph adapter query turn",
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_phase_tasks_ledger_done",
)


def _wm(project: str) -> str:
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    return wiremock_public_base(rt.wiremock_host, rt.wiremock_port)


def test_cozo_graph_adapter_structure(e2e_runtime: E2EComposeRuntime) -> None:
    project = e2e_runtime.project_name

    # --- INDEX turn: deterministic extract → ZZNODEALPHA/ZZNODEBETA + ZZRELMARKER into the cozo graph ---
    with mailflow_inject_and_wait(INDEX_SPEC, project) as (
        project,
        raw_a,
        _canon_a,
        nm_a,
        stub_tag_a,
        corr_a,
    ):
        try:
            assert_full_mailflow_pipeline(
                INDEX_SPEC,
                project=project,
                raw_id=raw_a,
                nm_inner=nm_a,
                stub_tag=stub_tag_a,
                correlation_key=corr_a,
            )
            wm = _wm(project)

            # ``extract_knowledge_graph`` пишется в ГЛОБАЛЬНЫЙ контекст (batch-агрегация, см. integrity-тест),
            # не per-thread-root. Per-thread индексационный сигнал = ``lightrag_index`` (embeddings drain); в
            # lightrag-ainsert он идёт ПОСЛЕ extract(LLM)+graph-upsert → его наличие ⟹ узлы/рёбра уже в cozo.
            def _indexed() -> list[str] | None:
                cs = wiremock_state_thread_root_call_sites(wm, corr_a)
                return cs if "lightrag_index" in cs else None

            poll_until(
                _indexed,
                timeout=TIMEOUT_POLL_SHORT,
                interval=2.0,
                desc="lightrag_index drained (index turn A)",
            )
            time.sleep(3.0)  # settle: graph-upsert завершается сразу после entity-embeddings в том же ainsert
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise

    # --- QUERY turn (new thread over the same global graph): enrich traversal → reasoning sees the entities ---
    with mailflow_inject_and_wait(QUERY_SPEC, project) as (
        project,
        raw_b,
        _canon_b,
        nm_b,
        stub_tag_b,
        corr_b,
    ):
        try:
            assert_full_mailflow_pipeline(
                QUERY_SPEC,
                project=project,
                raw_id=raw_b,
                nm_inner=nm_b,
                stub_tag=stub_tag_b,
                correlation_key=corr_b,
            )
            wm = _wm(project)

            def _flag(name: str) -> str:
                return wiremock_state_thread_root_property(wm, corr_b, name)

            assert _flag("saw_alpha") == "1", (
                "ZZNODEALPHA node did not reach reasoning — cozo upsert_node/get/traversal broken"
            )
            assert _flag("saw_beta") == "1", (
                "ZZNODEBETA node did not reach reasoning — cozo upsert_node/get/traversal broken"
            )
            assert _flag("saw_rel") == "1", (
                "ZZRELMARKER edge did not reach reasoning — cozo upsert_edge/get_node_edges broken"
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
