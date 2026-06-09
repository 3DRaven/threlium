"""Селективная индексация LightRAG на **уже поднятом** e2e-стеке — проверка по WireMock-state (§3.6.3).

**Уникальный инвариант теста:** drain ПРОПУСКАЕТ письмо без ``<history>`` (внешнее письмо моста
``to:ingress`` — голый ``text/plain``) и НЕ кладёт его в граф. Позитив «письмо с ``<history>``
индексируется» здесь НЕ проверяется — он покрыт ``test_lightrag_correlator_integrity`` (тот проверяет,
что wrapper ``lightrag_index`` в принципе срабатывает). Дублировать покрытие незачем.

**Как проверяем exclusion без docker-exec/notmuch** (docs/E2E.md §3.6.3, seeded-marker на generic-стабе):
тест сидит forbidden-маркер ``To: ingress@localhost`` в СВОЙ thread-root контекст; generic
embeddings-index стаб (``compose_bootstrap/011``) на каждом indexing-вызове берёт «что искать» ИЗ
контекста запроса, ``contains`` тело и sticky-пишет ``saw_match`` В ТОТ ЖЕ per-test контекст. Drain
рендерит индексируемый документ с ``To: <stage>@localhost`` в теле; пропущенный ingress не рендерится
вовсе → ``saw_match == "0"``. Контур/idle подтверждает ``assert_full_mailflow_pipeline`` (GreenMail+state).
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
    wiremock_state_seed_context,
    wiremock_state_thread_root_property,
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
            # Сид forbidden-маркера в СВОЙ thread-root контекст ДО drain (drain async, отстаёт от контура).
            wiremock_state_seed_context(
                wm_base, correlation_key, search_for=_FORBIDDEN_INDEX_MARKER
            )
            assert_full_mailflow_pipeline(
                LIGHTRAG_FILTER_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            # Drain async: дождаться, что indexing вообще сработал (lightrag_index в call-site списке) —
            # иначе saw_match ещё не записан (читали бы пусто). 011 пишет cs-список и saw_match одним
            # recordState под одним body-flag контекстом → присутствие cs гарантирует записанный saw_match.
            poll_until(
                lambda: True
                if "lightrag_index"
                in set(wiremock_state_thread_root_call_sites(wm_base, correlation_key))
                else None,
                timeout=30.0,
                desc="lightrag_index drained (011 fired)",
            )
            saw = wiremock_state_thread_root_property(wm_base, correlation_key, "saw_match")
            # Трёхзначный сигнал (StateHandlerbarHelper.getProperty + probe-default 'error'): "0"=стаб
            # сработал, маркера нет (PASS); "1"=forbidden-маркер сматчен (drain проиндексировал ingress);
            # "error"=property не записана в этот контекст → recordState не сработал ИЛИ context-ключ
            # разошёлся (wiring-баг, НЕ регрессия продукта — отличаем явно, не молчим пустой строкой).
            assert saw == "0", (
                f"drain indexed a no-history ingress message ({_FORBIDDEN_INDEX_MARKER!r} reached "
                f"the embeddings index); saw_match={saw!r}"
                if saw == "1"
                else f"saw_match not recorded under the thread-root context — recordState did not fire "
                f"here (wiring/context-key, not a product regression); saw_match={saw!r}"
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
