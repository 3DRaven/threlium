"""Mailflow scenario DSL: inject, assert (state-only, без notmuch/docker-exec во внутренней части)."""
from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


from .bridges.email import (
    canonical_external_msgid,
    email_ingress_notmuch_id_inner,
    e2e_thread_root_mid_for_message_id,
)
from .constants import REPO_ROOT, TIMEOUT_POLL_SHORT
from .diag import (
    mailflow_fsm_maildir_systemd_snapshot,
    mailflow_wait_fsm_maildir_activity,
    reset_maildrop_debug_log,
)
from .wiremock_assert import (
    assert_wiremock_mailflow_zero_unmatched,
)
from .fixtures import (
    e2e_dense_threlium_ctx_body,
    e2e_oversized_context_trim_body,
    e2e_oversized_context_trim_current_turn_body,
    e2e_oversized_context_trim_prior_turn_body,
    e2e_summarize_overflow_inject_body,
)
from .greenmail import (
    greenmail_wait_agent_reply_message_id,
    wait_for_greenmail_inbox_message_gone_host,
    wait_for_greenmail_user_reply,
)
from .poll import mailflow_diag_block, mailflow_log_phase, poll_until
from .runtime import discover_runtime
from .smtp_ingress import smtp_inject_inbound

@dataclass(frozen=True)
class MailflowScenarioSpec:
    """Declarative config for a full email-mailflow e2e scenario.

    Encapsulates the variable parts so that the fixture (arrange) and assertion
    (act+assert) code can be shared across tests with different WireMock stubs.
    """

    label: str
    raw_id_prefix: str
    stub_dir: Path
    stub_tag: str
    body_head: str
    body_override: str | None = None
    oversized_trim_body: bool = False
    summarize_overflow_body: bool = False
    # Сколько старых ходов треда инжектить ПЕРЕД основным, чтобы их distill-брифы
    # (каждый под cap distill) накопились в unified до переполнения → summarize.
    summarize_overflow_prior_turns: int = 1
    min_chat_completion_posts: int = 1
    # Cold-reset SUT: один probe в knowledge/ → меньше drain/bootstrap embeddings на тред.
    min_embedding_posts: int = 5
    min_rerank_posts: int = 1
    warmup_body_extra: str = ""
    reply_subject_needle: str | None = None
    reply_body_needle: str | None = None
    # Длинные multi-hop: poll только reasoning POST (не все chat/LightRAG) до needle/GreenMail.
    min_reasoning_chat_completion_posts: int | None = None
    # Poll журнала WireMock (request/response) до GreenMail после reasoning-порога выше.
    wiremock_journal_ready_needle: str | None = None
    assert_thread_no_unread: bool = False
    length_recovery_e2e: bool = False


@contextlib.contextmanager
def mailflow_inject_and_wait(
    spec: MailflowScenarioSpec,
    project_name: str,
) -> Iterator[tuple[str, str, str, str, str, str]]:
    """Arrange phase: prepare WireMock → inject email → wait bridge pickup (gone from INBOX) + FSM activity.

    Yields ``(project_name, raw_id, canonical_id, nm_inner, stub_tag, correlation_key)``.
    Teardown не чистит журнал WireMock (оставлен для ручной отладки).
    """
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        prepare_wiremock_scenario,
        teardown_wiremock_scenario,
        wiremock_public_base,
    )

    needs_prior_thread_turn = spec.summarize_overflow_body or spec.oversized_trim_body
    seed_id: str | None = None
    main_in_reply_to: str | None = None
    if needs_prior_thread_turn:
        seed_id = f"{spec.raw_id_prefix}seed-{uuid.uuid4().hex}@localhost"
        correlation_key = e2e_thread_root_mid_for_message_id(seed_id)
    raw_id = f"{spec.raw_id_prefix}{uuid.uuid4().hex}@localhost"
    if not needs_prior_thread_turn:
        correlation_key = e2e_thread_root_mid_for_message_id(raw_id)
    nm_inner = email_ingress_notmuch_id_inner(raw_id)
    canonical_id = canonical_external_msgid(raw_id)
    t0 = time.monotonic()
    mailflow_log_phase(
        f"{spec.label}: start (project={project_name}) "
        f"message_id={raw_id!r} correlation_key={correlation_key!r}"
    )
    rt = discover_runtime(project_name, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    prepare_wiremock_scenario(
        wm_base,
        stub_dir=spec.stub_dir,
        stub_tag=spec.stub_tag,
        correlation_key=correlation_key,
    )
    if spec.length_recovery_e2e:
        from tests.e2e.wiremock_client import (  # noqa: PLC0415
            composite_context_key,
            wiremock_state_length_recovery_enable,
        )

        wiremock_state_length_recovery_enable(
            wm_base,
            composite_context_key(spec.stub_tag, correlation_key),
        )
    elif spec.stub_tag == "stub-reasoning-litellm-live-01":
        from tests.e2e.wiremock_client import (  # noqa: PLC0415
            composite_context_key,
            wiremock_state_standard_tasks_ledger_enable,
        )

        wiremock_state_standard_tasks_ledger_enable(
            wm_base,
            composite_context_key(spec.stub_tag, correlation_key),
        )

    # RAG-warmup убран: тесты больше НЕ зависят от тёплой vdb (rerank не ассертится в mailflow;
    # покрытие корреляторов lightrag — в выделенном тесте test_lightrag_correlator_integrity).
    # Индексация идёт async background, контур гейтится GreenMail-ответом, не drain'ом.
    reset_maildrop_debug_log(project_name, repo_root=REPO_ROOT)

    if seed_id is not None:
        # summarize overflow: несколько старых ходов одного треда, каждый distill-бриф под
        # cap (distill_max_chars), накапливаются в history tokens до excess X (token ledger) →
        # summarize. Каждый ход тредится на ОТВЕТ агента предыдущего (см. комментарий ниже).
        prior_turns_count = (
            max(1, spec.summarize_overflow_prior_turns)
            if (spec.summarize_overflow_body or spec.oversized_trim_body)
            else 1
        )
        chain_in_reply_to: str | None = None
        for turn_idx in range(prior_turns_count):
            cur_seed_id = (
                seed_id
                if turn_idx == 0
                else f"{spec.raw_id_prefix}seed{turn_idx}-{uuid.uuid4().hex}@localhost"
            )
            if spec.summarize_overflow_body:
                # Маленькое сырое тело (HEAD/PAD-маркеры для проверки «raw не протёк»);
                # размер unified задаёт templated distill-бриф (per-turn Message-ID → разный CID),
                # а не это тело.
                seed_body = e2e_summarize_overflow_inject_body(
                    head=f"{spec.body_head} (prior thread turn seed {turn_idx})",
                    correlation_key=correlation_key,
                    # Токены overflow — из distill-брифа (wiremock accumulation-filler), не из
                    # сырого P-блока в ## Original user message (иначе один CID закрывает excess).
                    pad_chars=0,
                )
            elif spec.oversized_trim_body:
                # HEAD-маркер без сырого pad: overflow гонится из distill-брифа (accumulation-
                # filler в wiremock), не из сырого X-блока. Иначе один большой prior-CID
                # закрывает excess, остальные prior-ходы остаются несуммаризированными
                # (summarize редуцирует до бюджета, не «всё») и сырой X протекает в reasoning.
                seed_body = e2e_oversized_context_trim_prior_turn_body(
                    head=f"{spec.body_head} (prior thread turn seed {turn_idx})",
                    correlation_key=correlation_key,
                    pad_chars=0,
                )
            else:
                seed_body = e2e_dense_threlium_ctx_body(
                    head=f"{spec.body_head} (prior thread turn seed)",
                    correlation_key=correlation_key,
                )
            smtp_inject_inbound(
                project_name,
                checkout="/unused",
                repo_root=REPO_ROOT,
                message_id=cur_seed_id,
                body=seed_body,
                **(
                    {"in_reply_to": chain_in_reply_to}
                    if chain_in_reply_to is not None
                    else {}
                ),
            )
            mailflow_log_phase(
                f"{spec.label}: prior-turn seed[{turn_idx}] injected mid={cur_seed_id!r} "
                f"(+{time.monotonic() - t0:.1f}s)"
            )
            wait_for_greenmail_inbox_message_gone_host(
                rt.greenmail_imap_host,
                rt.greenmail_imap_port,
                message_id=cur_seed_id,
            )
            _mailflow_wait_wiremock_journal_ready_if_configured(
                spec,
                project=project_name,
                stub_tag=spec.stub_tag,
                correlation_key=correlation_key,
            )
            # Барьер прошлого хода = ОТВЕТ агента в GreenMail (единственный внешний выход:
            # ingress→enrich→reasoning→…→egress_email пройдены). Внутренние notmuch/maildir
            # docker-exec защёлки (FSM-activity, notmuch-indexed, fully_in_stages) сняты —
            # ответное письмо доказывает прохождение сильнее folder-присутствия (§3.6.1,
            # time-independent, без docker-exec). Реалистичный threading: следующий ход
            # тредится на ответ агента (egress glue-record) → IRT-цепочка проходит через
            # ``tasks_upsert`` прошлого хода → per-frame task-ledger наследуется, finalize-gate
            # проходит без ручного сброса латча ``phase_tasks_ledger_done``.
            chain_in_reply_to = greenmail_wait_agent_reply_message_id(
                rt.greenmail_imap_host,
                rt.greenmail_imap_port,
                in_reply_to_anchor=cur_seed_id,
            )
            mailflow_log_phase(
                f"{spec.label}: prior-turn seed[{turn_idx}] agent reply mid={chain_in_reply_to!r} "
                f"(+{time.monotonic() - t0:.1f}s)"
            )
        main_in_reply_to = chain_in_reply_to

    if spec.body_override is not None:
        inject_body = spec.body_override
    elif spec.oversized_trim_body:
        if seed_id is not None:
            inject_body = e2e_oversized_context_trim_current_turn_body(
                head=spec.body_head, correlation_key=correlation_key
            )
        else:
            inject_body = e2e_oversized_context_trim_body(
                head=spec.body_head, correlation_key=correlation_key
            )
    elif spec.summarize_overflow_body:
        inject_body = e2e_summarize_overflow_inject_body(
            head=spec.body_head,
            correlation_key=correlation_key,
            pad_chars=0,
        )
    else:
        inject_body = e2e_dense_threlium_ctx_body(
            head=spec.body_head, correlation_key=correlation_key
        )
    smtp_inject_inbound(
        project_name,
        checkout="/unused",
        repo_root=REPO_ROOT,
        message_id=raw_id,
        body=inject_body,
        **({"in_reply_to": main_in_reply_to} if main_in_reply_to is not None else {}),
    )
    mailflow_log_phase(f"{spec.label}: after smtp_inject_inbound (+{time.monotonic() - t0:.1f}s)")
    wait_for_greenmail_inbox_message_gone_host(
        rt.greenmail_imap_host,
        rt.greenmail_imap_port,
        message_id=raw_id,
        timeout=TIMEOUT_POLL_SHORT,
    )
    mailflow_log_phase(
        f"{spec.label}: after wait_for_greenmail_inbox_message_gone_host (+{time.monotonic() - t0:.1f}s)"
    )
    snap = mailflow_fsm_maildir_systemd_snapshot(project_name, repo_root=REPO_ROOT)
    mailflow_diag_block(
        f"{spec.label}: fsm maildir + systemd snapshot after IMAP IDLE pickup",
        snap,
        max_chars=30000,
    )
    mailflow_wait_fsm_maildir_activity(
        project_name,
        repo_root=REPO_ROOT,
        message_id=nm_inner,
    )
    try:
        yield project_name, raw_id, canonical_id, nm_inner, spec.stub_tag, correlation_key
    finally:
        teardown_wiremock_scenario(
            wm_base, correlation_key=correlation_key, stub_tag=spec.stub_tag
        )


def _mailflow_wait_reasoning_chat_posts_if_configured(
    spec: MailflowScenarioSpec,
    *,
    project: str,
    stub_tag: str,
    correlation_key: str,
) -> None:
    min_r = spec.min_reasoning_chat_completion_posts
    if min_r is None:
        return
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        wait_for_wiremock_reasoning_chat_posts_for_stub,
        wiremock_public_base,
    )

    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    mailflow_log_phase(
        f"{spec.label}: wait reasoning chat posts>={min_r} (call-site=reasoning)"
    )
    wait_for_wiremock_reasoning_chat_posts_for_stub(
        wm,
        stub_tag=stub_tag,
        anchor_needle=correlation_key,
        min_posts=min_r,
    )


def _mailflow_wait_wiremock_journal_ready_if_configured(
    spec: MailflowScenarioSpec,
    *,
    project: str,
    stub_tag: str,
    correlation_key: str,
) -> None:
    needle = spec.wiremock_journal_ready_needle
    if not needle:
        return
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        wait_for_wiremock_stub_journal_contains,
        wiremock_public_base,
    )

    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    mailflow_log_phase(f"{spec.label}: wait wiremock journal needle={needle!r}")
    wait_for_wiremock_stub_journal_contains(
        wm,
        stub_tag=stub_tag,
        needle=needle,
        anchor_needle=correlation_key,
    )


def assert_full_mailflow_pipeline(
    spec: MailflowScenarioSpec,
    *,
    project: str,
    raw_id: str,
    nm_inner: str,
    stub_tag: str,
    correlation_key: str,
) -> None:
    """Assert phase (state+greenmail, без docker-exec): GreenMail reply → call_sites (жизненный цикл из
    state, §3.6.1) → zero unmatched.

    Маршрутизация по notmuch-Maildir стадиям больше **не** проверяется напрямую (`docker exec`): LLM-стадии
    подтверждаются call-site списком (`ingress_distill`/`enrich_*`/`reasoning`/`lightrag_index`/`…rerank`),
    терминальные без LLM (`egress_router`/`egress_email`/`archive`) — **ответным письмом GreenMail**.
    Наружу ходим только в GreenMail; всё остальное — WireMock state. Изоляция = thread-root (§2)."""
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        wiremock_public_base,
        wiremock_state_thread_root_call_sites,
    )

    t0 = time.monotonic()
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)

    # 1. Ответное письмо = контур дошёл до egress (ingress→enrich→reasoning→egress_router→egress_email→
    #    archive пройдены). Единственный внешний выход.
    wait_for_greenmail_user_reply(
        project,
        raw_id=raw_id,
        repo_root=REPO_ROOT,
        **({"subject_substring": spec.reply_subject_needle} if spec.reply_subject_needle is not None else {}),
        **({"body_substring": spec.reply_body_needle} if spec.reply_body_needle is not None else {}),
    )
    mailflow_log_phase(f"{spec.label}: greenmail reply OK (+{time.monotonic() - t0:.1f}s)")

    # 2. Жизненный цикл — из единого call-site списка state (recordState на лету, §3.6.1). Поллим: часть
    #    индексации (lightrag_index drain) может отставать от письма. embed = lightrag_index/lightrag_query;
    #    rerank (lightrag_query_rerank) — НЕ per-message инвариант (LightRAG-rerank опционален на query и под
    #    thread-root тестового сообщения может не сработать — он отрабатывает в RAG-warmup), поэтому в gate
    #    не входит; остальное (chat/completions) = всё прочее.
    _EMBED = {"lightrag_index", "lightrag_query"}
    _RERANK = "lightrag_query_rerank"

    def _probe() -> list[str] | None:
        cs = wiremock_state_thread_root_call_sites(wm, correlation_key)
        chat = [c for c in cs if c not in _EMBED and c != _RERANK]
        # Развязка per-message assert от LightRAG-drain (главный -n4 флак rag pending == 0):
        # gate ТОЛЬКО на chat-completion count (контурные LLM-вызовы), БЕЗ ожидания индексации
        # (embed/lightrag_index сняты) и БЕЗ привязки к конкретному call-site enrich (он не
        # универсален: enrich-flow зовёт enrich_task_plan, а response-table-flow — generate_rag_answer).
        # GreenMail reply (шаг 1) уже доказал, что контур дошёл до egress → chat-вызовы завершены.
        ok = len(chat) >= spec.min_chat_completion_posts
        return cs if ok else None

    call_sites = poll_until(
        _probe,
        timeout=TIMEOUT_POLL_SHORT,
        interval=2.0,
        desc=f"call_sites: chat>={spec.min_chat_completion_posts}",
    )
    mailflow_log_phase(
        f"{spec.label}: lifecycle OK via state ({len(call_sites)} call-sites, +{time.monotonic() - t0:.1f}s)"
    )

    # 3. Контроль целостности WireMock (unmatched-guard, §5) — не lifecycle-assert, остаётся.
    assert_wiremock_mailflow_zero_unmatched(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    mailflow_log_phase(f"{spec.label}: pipeline checks OK (+{time.monotonic() - t0:.1f}s)")
