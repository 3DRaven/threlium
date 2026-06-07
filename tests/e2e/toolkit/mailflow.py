"""Mailflow scenario DSL: inject, assert, RAG warmup."""
from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from threlium.lightrag_drain_query import lightrag_drain_pending_search

from .bridges.email import (
    canonical_external_msgid,
    email_ingress_notmuch_id_inner,
    e2e_thread_root_mid_for_message_id,
)
from .constants import E2E_SUT_NOTMUCH_BASH_EXPORT, REPO_ROOT, TIMEOUT_POLL_SHORT
from .diag import (
    mailflow_fsm_maildir_systemd_snapshot,
    mailflow_pipeline_diag,
    mailflow_wait_fsm_maildir_activity,
    reset_maildrop_debug_log,
)
from .lightrag_assert import assert_notmuch_mailflow_thread_has_lightrag_indexed
from .notmuch_assert import (
    assert_notmuch_thread_fully_in_stages,
    assert_notmuch_thread_has_messages_in_folders,
    assert_notmuch_thread_has_no_unread,
    poll_lightrag_indexed_positive,
    wait_for_notmuch_message,
)
from .wiremock_assert import (
    assert_wiremock_mailflow_min_embedding_posts,
    assert_wiremock_mailflow_min_rerank_posts,
    assert_wiremock_mailflow_received_chat_completion_posts,
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
from .runtime import E2EComposeRuntime, discover_runtime, service_exec
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
    expect_notmuch_stage_folders: tuple[str, ...] | None = None
    reply_subject_needle: str | None = None
    reply_body_needle: str | None = None
    # Длинные multi-hop: poll только reasoning POST (не все chat/LightRAG) до needle/GreenMail.
    min_reasoning_chat_completion_posts: int | None = None
    # Poll журнала WireMock (request/response) до GreenMail после reasoning-порога выше.
    wiremock_journal_ready_needle: str | None = None
    assert_thread_no_unread: bool = False
    length_recovery_e2e: bool = False


def _wait_rag_drain_idle(project_name: str, *, label: str) -> None:
    """Poll until the LightRAG pending selector returns empty (drain finished)."""
    selector = lightrag_drain_pending_search()
    cmd = [
        "bash", "-lc",
        f"{E2E_SUT_NOTMUCH_BASH_EXPORT}; "
        f"notmuch count '{selector}' 2>/dev/null || echo 99",
    ]

    def _probe() -> str | None:
        r = service_exec(project_name, "sut", cmd, repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT))
        if r.returncode != 0:
            return None
        try:
            n = int((r.stdout or "").strip())
        except ValueError:
            return None
        return "0" if n == 0 else None

    poll_until(_probe, timeout=TIMEOUT_POLL_SHORT, interval=2.0, desc="rag pending count == 0")
    mailflow_log_phase(f"{label}: rag drain idle (no pending messages)")


def _inject_rag_warmup(
    project_name: str,
    *,
    rt: E2EComposeRuntime,
    wm_base: str,
    stub_tag: str,
    body_head: str,
    body_extra: str,
    label: str,
) -> None:
    """Ensure vectordb has data for rerank; inject warm-up only if needed.

    If vectordb already contains data (from a previous test in the same session),
    skip injection entirely. Otherwise inject a warm-up message through the agent
    mailbox and wait for LightRAG drain to populate the vectordb.
    """
    from tests.e2e.wiremock_client import (  # noqa: PLC0415
        composite_context_key,
        wiremock_state_seed_context,
        wiremock_state_standard_tasks_ledger_enable,
    )

    cmd = [
        "bash", "-lc",
        "stat --printf='%s' /home/threlium/threlium/data/lightrag/faiss_index_chunks.index.meta.json 2>/dev/null || echo 0",
    ]
    r = service_exec(project_name, "sut", cmd, repo_root=REPO_ROOT, timeout=int(TIMEOUT_POLL_SHORT))
    try:
        sz = int((r.stdout or "").strip())
    except ValueError:
        sz = 0
    if sz > 10:
        _wait_rag_drain_idle(project_name, label=label)
        # Cold session bootstrap knowledge leaves a small vdb (~8KiB) without rerank-ready
        # scenario vectors; do not skip warmup on that footprint (see enrich_task_hypotheses briefing).
        _RAG_WARMUP_SKIP_MIN_BYTES = 32_768
        if sz >= _RAG_WARMUP_SKIP_MIN_BYTES:
            mailflow_log_phase(
                f"{label}: vectordb already has data ({sz} bytes), skip warmup"
            )
            return
        mailflow_log_phase(
            f"{label}: vdb {sz} bytes (< {_RAG_WARMUP_SKIP_MIN_BYTES}), run warmup after bootstrap"
        )

    warmup_id = f"e2e-rag-warmup-{uuid.uuid4().hex[:12]}@localhost"
    warmup_corr = e2e_thread_root_mid_for_message_id(warmup_id)
    warmup_ctx = composite_context_key(stub_tag, warmup_corr)
    wiremock_state_seed_context(wm_base, warmup_ctx)
    # Стандартный reasoning-путь (100_tasks → tasks_upsert → 100_egress): без
    # phase_standard_tasks_ledger ни один reasoning-стаб не матчится → unmatched.
    wiremock_state_standard_tasks_ledger_enable(wm_base, warmup_ctx)

    warmup_body = e2e_dense_threlium_ctx_body(
        head=body_head, correlation_key=warmup_corr
    )
    if body_extra:
        warmup_body = warmup_body.rstrip("\n") + "\n" + body_extra + "\n"
    smtp_inject_inbound(
        project_name,
        checkout="/unused",
        repo_root=REPO_ROOT,
        message_id=warmup_id,
        body=warmup_body,
    )
    mailflow_log_phase(f"{label}: rag warmup injected mid={warmup_id!r}")

    wait_for_greenmail_inbox_message_gone_host(
        rt.greenmail_imap_host,
        rt.greenmail_imap_port,
        message_id=warmup_id,
    )
    mailflow_log_phase(f"{label}: rag warmup picked up (gone from INBOX, pipeline complete)")

    poll_lightrag_indexed_positive(
        project_name, correlation_key=warmup_corr, repo_root=REPO_ROOT
    )
    _wait_rag_drain_idle(project_name, label=label)
    mailflow_log_phase(f"{label}: rag warmup indexed in vectordb")


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

    if spec.min_rerank_posts > 0:
        _inject_rag_warmup(
            project_name,
            rt=rt,
            wm_base=wm_base,
            stub_tag=spec.stub_tag,
            body_head=spec.body_head,
            body_extra=spec.warmup_body_extra,
            label=spec.label,
        )
        mailflow_log_phase(
            f"{spec.label}: lightrag vectordb has indexed data (+{time.monotonic() - t0:.1f}s)"
        )

    reset_maildrop_debug_log(project_name, repo_root=REPO_ROOT)

    if seed_id is not None and spec.min_rerank_posts == 0:
        # Cold-reset сносит lightrag; цепочка prior-turn seed (summarize/trim) гоняет
        # полный ingress→egress на каждый ход. Без прогретого vdb первый seed часто не
        # успевает в TIMEOUT_POLL_SHORT на solo/cold (в batch vdb уже тёплый от соседей).
        _inject_rag_warmup(
            project_name,
            rt=rt,
            wm_base=wm_base,
            stub_tag=spec.stub_tag,
            body_head=spec.body_head,
            body_extra=spec.warmup_body_extra,
            label=spec.label,
        )
        mailflow_log_phase(
            f"{spec.label}: lightrag ready for prior-turn seeds (+{time.monotonic() - t0:.1f}s)"
        )

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
            seed_nm_inner = email_ingress_notmuch_id_inner(cur_seed_id)
            mailflow_wait_fsm_maildir_activity(
                project_name,
                repo_root=REPO_ROOT,
                message_id=seed_nm_inner,
            )
            wait_for_notmuch_message(
                project_name, message_id=seed_nm_inner, repo_root=REPO_ROOT
            )
            mailflow_log_phase(
                f"{spec.label}: prior-turn seed[{turn_idx}] indexed mid={cur_seed_id!r} "
                f"(+{time.monotonic() - t0:.1f}s)"
            )
            _mailflow_wait_wiremock_journal_ready_if_configured(
                spec,
                project=project_name,
                stub_tag=spec.stub_tag,
                correlation_key=correlation_key,
            )
            wait_for_greenmail_user_reply(
                project_name,
                raw_id=cur_seed_id,
                repo_root=REPO_ROOT,
            )
            assert_notmuch_thread_fully_in_stages(
                project_name,
                anchor_message_id=seed_nm_inner,
                repo_root=REPO_ROOT,
            )
            mailflow_log_phase(
                f"{spec.label}: prior-turn seed[{turn_idx}] pipeline settled mid={cur_seed_id!r} "
                f"(+{time.monotonic() - t0:.1f}s)"
            )
            # Реалистичный threading: следующий ход (seed или основной) тредится на ОТВЕТ
            # агента (egress glue-record), а не на собственную инъекцию. Тогда IRT-цепочка
            # проходит через ``tasks_upsert`` предыдущего хода → per-frame task-ledger
            # наследуется и finalize-gate проходит без ручного сброса латча
            # ``phase_tasks_ledger_done``.
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
    """Assert phase: notmuch indexed → WireMock coverage → FSM stages → user reply → zero unmatched."""
    t0 = time.monotonic()
    mailflow_log_phase(
        f"{spec.label}: wait_for_notmuch_message nm_inner={nm_inner!r} "
        f"correlation_key_tail={correlation_key[-24:]!r}"
    )
    wait_for_notmuch_message(project, message_id=nm_inner, repo_root=REPO_ROOT)
    mailflow_log_phase(f"{spec.label}: notmuch OK (+{time.monotonic() - t0:.1f}s)")
    mailflow_pipeline_diag(project, anchor_message_id=nm_inner, repo_root=REPO_ROOT)
    _mailflow_wait_reasoning_chat_posts_if_configured(
        spec, project=project, stub_tag=stub_tag, correlation_key=correlation_key
    )
    _mailflow_wait_wiremock_journal_ready_if_configured(
        spec, project=project, stub_tag=stub_tag, correlation_key=correlation_key
    )
    assert_wiremock_mailflow_received_chat_completion_posts(
        project,
        stub_tag=stub_tag,
        anchor_message_id=correlation_key,
        repo_root=REPO_ROOT,
        min_posts=spec.min_chat_completion_posts,
    )
    assert_wiremock_mailflow_min_embedding_posts(
        project,
        anchor_message_id=correlation_key,
        min_posts=spec.min_embedding_posts,
        repo_root=REPO_ROOT,
    )
    if spec.min_rerank_posts > 0:
        assert_wiremock_mailflow_min_rerank_posts(
            project,
            anchor_message_id=correlation_key,
            min_posts=spec.min_rerank_posts,
            repo_root=REPO_ROOT,
        )
    wait_for_greenmail_user_reply(
        project,
        raw_id=raw_id,
        repo_root=REPO_ROOT,
        **({"subject_substring": spec.reply_subject_needle} if spec.reply_subject_needle is not None else {}),
        **({"body_substring": spec.reply_body_needle} if spec.reply_body_needle is not None else {}),
    )
    assert_notmuch_thread_fully_in_stages(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    assert_notmuch_mailflow_thread_has_lightrag_indexed(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    if spec.expect_notmuch_stage_folders:
        assert_notmuch_thread_has_messages_in_folders(
            project,
            anchor_message_id=nm_inner,
            stage_folder_ids=spec.expect_notmuch_stage_folders,
            repo_root=REPO_ROOT,
        )
    if spec.assert_thread_no_unread:
        assert_notmuch_thread_has_no_unread(
            project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
        )
    assert_wiremock_mailflow_zero_unmatched(
        project, anchor_message_id=nm_inner, repo_root=REPO_ROOT
    )
    mailflow_log_phase(f"{spec.label}: pipeline checks OK (+{time.monotonic() - t0:.1f}s)")
