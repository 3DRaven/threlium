"""Селективная индексация LightRAG на **уже поднятом** e2e-стеке — проверка по WireMock-state (§3.6.3).

**Уникальный инвариант теста:** drain ПРОПУСКАЕТ письмо без ``<history>`` (внешнее письмо моста
``to:ingress`` — голый ``text/plain``) и НЕ кладёт его в граф. Позитив «письмо с ``<history>``
индексируется» здесь НЕ проверяется — он покрыт ``test_lightrag_correlator_integrity`` (тот проверяет,
что wrapper ``lightrag_index`` в принципе срабатывает). Дублировать покрытие незачем.

**Как проверяем exclusion без docker-exec/notmuch** (docs/E2E.md §3.6.3, concurrency-safe forbidden-флаг):
generic embeddings-index стаб (``compose_bootstrap/011``) несёт СТАТИЧЕСКИЙ forbidden-маркер
``To: ingress@localhost`` и на каждом indexing-вызове **append-only** пишет hit в ВЫДЕЛЕННЫЙ контекст
``forbidden-index-<body-corr>`` ТОЛЬКО когда тело содержит маркер (есть в индексируемом MIME-чанке с
``To:``-заголовком, нет в query-embed'ах). В этот контекст пишет лишь маркер-embed → единственный писатель,
без read-modify-write гонки. Тест читает ``list_size("forbidden-index-" + correlation_key)`` → ``0`` =
no-history письмо НЕ проиндексировано (PASS). Прежний «динамический seed + sticky saw_match» развалился под
параллельным lightrag-drain'ом (seed-гонка + потеря записей + ненадёжный thread-root header) — см. §3.6.3.
Контур/idle подтверждает ``assert_full_mailflow_pipeline`` (GreenMail+state).
"""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .toolkit import (
    E2EComposeRuntime,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    poll_until,
)
from .wiremock_client import (
    wiremock_public_base,
    wiremock_state_thread_root_call_sites,
    wiremock_state_thread_root_list_size,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

LIGHTRAG_FILTER_SPEC = MailflowScenarioSpec(
    label="lightrag_index_filter_e2e",
    raw_id_prefix="lrf-ing-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_mailflow_e2e",
    stub_tag="stub-mailflow-e2e-01",
    body_head="e2e index filter body",
    min_chat_completion_posts=2,
    min_embedding_posts=5,
)

#: Forbidden-index маркер: ни один индексируемый документ не должен нести ``To: ingress@localhost``
#: (drain пропускает no-history ingress-письмо). Рендер индексируемого документа несёт ``To: <stage>``.
_FORBIDDEN_INDEX_MARKER = f"To: {FsmStage.INGRESS.rfc822_mailbox}"


def test_lightrag_selective_indexing(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """Drain ПРОПУСКАЕТ no-history ingress-письмо (НЕ индексирует) — по state, без docker-exec/notmuch."""
    with mailflow_inject_and_wait(LIGHTRAG_FILTER_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            rt = discover_runtime(project, repo_root=REPO_ROOT)
            wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            # Никакого динамического seed: forbidden-маркер (``To: ingress@localhost``) СТАТИЧЕН в
            # generic index-стабе 011 (docs/E2E.md §3.6.3). Динамический seed гонится с конкурентными
            # lightrag-embed'ами (доказано: seed коммитится ПОЗЖЕ embed'ов → маркер не виден) — поэтому
            # его нет.
            assert_full_mailflow_pipeline(
                LIGHTRAG_FILTER_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            # Drain async: дождаться, что embedding вообще сработал (lightrag_index в call-site списке) —
            # иначе forbidden-проверка читала бы состояние до индексации.
            poll_until(
                lambda: True
                if "lightrag_index"
                in set(wiremock_state_thread_root_call_sites(wm_base, correlation_key))
                else None,
                timeout=30.0,
                desc="lightrag_index drained (011 fired)",
            )
            # Concurrency-safe forbidden-index проверка (docs/E2E.md §3.6.3): 011 APPEND'ит hit в контекст
            # ``forbidden-index-<body-corr>`` ТОЛЬКО когда тело embed'а несёт ``To: ingress@localhost``
            # (есть в ИНДЕКСИРУЕМОМ чанке, нет в query-embed'ах). Append-only в контекст, куда пишет лишь
            # маркер-embed → единственный писатель, без read-modify-write гонки (в отличие от sticky-флага,
            # который терял запись под конкуренцией). ``body-corr`` == ``correlation_key`` (Message-ID
            # no-history письма в его собственном чанке). list_size 0 = письмо НЕ проиндексировано (PASS).
            hits = wiremock_state_thread_root_list_size(
                wm_base, f"forbidden-index-{correlation_key}"
            )
            assert hits == 0, (
                f"drain indexed a no-history ingress message: {_FORBIDDEN_INDEX_MARKER!r} reached the "
                f"embeddings index ({hits} forbidden-index hit(s) recorded for thread {correlation_key!r})."
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
