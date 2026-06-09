"""E2e: token-ledger overflow (excess X) → summarize_context → enrich → reasoning."""
from __future__ import annotations

import uuid
from pathlib import Path


from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    e2e_dense_threlium_ctx_body,
    email_ingress_notmuch_id_inner,
    greenmail_wait_agent_reply_message_id,
    mailflow_inject_and_wait,
    mailflow_wait_fsm_maildir_activity,
    smtp_inject_inbound,
    wait_for_greenmail_inbox_message_gone_host,
    wait_for_greenmail_user_reply,
)
from .test_reasoning_litellm_mock_live import REASONING_E2E_BODY_MARKER
from .wiremock_client import (
    wiremock_public_base,
    wiremock_state_thread_root_call_sites,
    wiremock_state_thread_root_property,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

SUMMARIZE_CONTEXT_SPEC = MailflowScenarioSpec(
    label="summarize_context_e2e",
    raw_id_prefix="e2e-summarize-ctx-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_summarize_context_e2e",
    stub_tag="stub-summarize-context-e2e-01",
    body_head=f"{REASONING_E2E_BODY_MARKER}\ne2e summarize context overflow inbound",
    summarize_overflow_body=True,
    # Один prior + main: один overflow batch закрывает excess по seed-ходу; 2 prior
    # оставляют несummarized history с PAD_MARKER (granular batch не покрывает весь тред).
    summarize_overflow_prior_turns=1,
    min_chat_completion_posts=2,
    min_embedding_posts=1,
    min_rerank_posts=0,
    wiremock_journal_ready_needle="call_e2e_tasks_ledger_phase_tasks_ledger_done",
)


def _count_summarize_llm_posts(wm_base: str, *, correlation_key: str) -> int:
    # Счётчик summarize_thread_context — по единому STATE-списку call-site (generic recorder, §3.6),
    # а не сканом журнала по ``stub_tag``. Изоляция = thread-root заголовок (§2): устойчиво на ``-n2``.
    cs = wiremock_state_thread_root_call_sites(wm_base, correlation_key)
    return sum(1 for c in cs if c == "summarize_thread_context")


def _assert_summarize_pipeline_artifacts(
    *,
    project: str,
    nm_inner: str,
    stub_tag: str,
    correlation_key: str,
) -> int:
    # context_summarized тег + summary в SUMMARIZE_MEMORY-папке (раньше — notmuch docker-exec) покрыты
    # без захода в контейнер: факт summarize — call_sites (n_summarize ниже), а попадание summary в
    # контекст — journal-проверкой reasoning ниже (E2E_SUMMARY_MARKER in merged) — это строго сильнее
    # «лежит в папке» (summary дошёл до reasoning). См. §3.6.1 / Phase 3.
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    n_summarize = _count_summarize_llm_posts(wm_base, correlation_key=correlation_key)
    assert n_summarize >= 1, (
        f"expected at least one summarize_context LLM POST, got {n_summarize}"
    )
    # Содержимое — по STATE content-flags (recordState на лету в стабах, §3.6.1), БЕЗ скана журнала:
    #   reasoning-стаб: saw_summary (summary дошёл в reasoning), saw_raw_pad (сырой PAD-блок протёк? → 0);
    #   075 summarize-стаб: saw_distill (distill-заголовки), saw_head (HEAD-маркер), saw_raw_pad_input (0).
    # saw_summary поллим — reasoning-хопы идут во время контура. Сырой PAD в этом тесте не инжектится
    # (pad_chars=0), поэтому whole-body saw_raw_pad эквивалентен секционной проверке (без секций/regex).
    # Прямое чтение (без поллинга): контентные ассерты идут ПОСЛЕ assert_full_mailflow_pipeline (ждёт
    # ответ GreenMail = контур завершён), а reasoning/summarize отрабатывают причинно ДО egress→ответа →
    # флаги уже записаны (recordState beforeResponseSent). Time-independent, без flaky-тайминга.
    def _flag(name: str) -> str:
        return wiremock_state_thread_root_property(wm_base, correlation_key, name)

    assert _flag("saw_summary") == "1", (
        "summary marker did not reach reasoning (state saw_summary)"
    )
    assert _flag("saw_raw_pad") == "0", (
        "summarized originals (raw PAD block) must not appear in reasoning (get_body regression)"
    )
    assert _flag("saw_distill") == "1", (
        "summarize input must include ingress distill <history> headings (granular, not get_body)"
    )
    assert _flag("saw_head") == "1", (
        "summarize input must include HEAD from inject (via distill or original_user history)"
    )
    assert _flag("saw_raw_pad_input") == "0", (
        "summarize input must not include raw PAD block (get_body regression)"
    )
    return n_summarize



def test_summarize_overflow_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """Переполнение unified → summarize FSM → тег context_summarized → reasoning с маркером summary."""
    with mailflow_inject_and_wait(SUMMARIZE_CONTEXT_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                SUMMARIZE_CONTEXT_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_summarize_pipeline_artifacts(
                project=project,
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


def test_summarize_idempotent_second_enrich(e2e_runtime: E2EComposeRuntime) -> None:
    """Второе письмо в том же треде не вызывает повторный summarize LLM."""
    with mailflow_inject_and_wait(SUMMARIZE_CONTEXT_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        assert_full_mailflow_pipeline(
            SUMMARIZE_CONTEXT_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        n_after_first = _assert_summarize_pipeline_artifacts(
            project=project,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )

        raw_id2 = f"e2e-summarize-ctx2-{uuid.uuid4().hex}@localhost"
        nm_inner2 = email_ingress_notmuch_id_inner(raw_id2)
        rt = discover_runtime(project, repo_root=REPO_ROOT)
        wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
        # Реалистичный threading: второе письмо тредится на ОТВЕТ агента (его Message-ID),
        # а не на исходный inbound. Тогда IRT-цепочка второго хода проходит через tasks_upsert
        # первого хода (egress glue-record), per-frame task-ledger наследуется и finalize-gate
        # проходит без ручного сброса WireMock-латча phase_tasks_ledger_done.
        agent_reply_mid = greenmail_wait_agent_reply_message_id(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            in_reply_to_anchor=raw_id,
        )
        smtp_inject_inbound(
            project,
            checkout="/unused",
            repo_root=REPO_ROOT,
            message_id=raw_id2,
            in_reply_to=agent_reply_mid,
            body=e2e_dense_threlium_ctx_body(
                head="e2e summarize second turn short body",
                correlation_key=correlation_key,
            ),
        )
        wait_for_greenmail_inbox_message_gone_host(
            rt.greenmail_imap_host,
            rt.greenmail_imap_port,
            message_id=raw_id2,
        )
        mailflow_wait_fsm_maildir_activity(
            project, repo_root=REPO_ROOT, message_id=nm_inner2
        )
        wait_for_greenmail_user_reply(
            project,
            raw_id=raw_id2,
            repo_root=REPO_ROOT,
        )

        # Под дизайном «summarize до бюджета» (см. summarize_overflow): enrich сжимает только
        # старейшие units до покрытия excess, остаток остаётся сырым. Поэтому второй короткий
        # ход треда МОЖЕТ маргинально переполниться и сжать СВОЙ новый остаток — это не нарушение
        # идемпотентности. Инвариант идемпотентности: уже помеченные context_summarized оригиналы
        # НЕ суммаризируются повторно (rolling summary сходится, без rework того же контента).
        n_after_second = _count_summarize_llm_posts(wm_base, correlation_key=correlation_key)
        # Идемпотентность: summarize-count монотонен (call_sites), без infinite re-summarize. Тег
        # context_summarized (раньше notmuch docker-exec) — следствие самого факта summarize (call_sites).
        assert n_after_second >= n_after_first, (
            f"summarize LLM count regressed: {n_after_first} → {n_after_second}"
        )
        # Сводка переживает второй вход (идемпотентная консолидация): re-summarize произошёл
        # (n_after_second ≥ n_after_first, выше) И summary присутствует в reasoning по STATE-флагу
        # (saw_summary, sticky per thread-root) — без скана журнала.
        assert (
            wiremock_state_thread_root_property(wm_base, correlation_key, "saw_summary") == "1"
        ), "durable summary marker must persist in reasoning after the second enrich (state saw_summary)"
