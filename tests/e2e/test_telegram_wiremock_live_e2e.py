"""Telegram и LLM против WireMock в живом стеке (compose уже поднят вне этого модуля).

Сквозной контур: подменный Bot API (WireMock) + тот же WireMock как OpenAI-совместимый API.
Два сценария — личка и forum topic (разные каталоги стабов и ``stub_tag``).
"""
from __future__ import annotations

import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from tests.e2e.log import log
from threlium.types import FsmStage

from .toolkit import (
    TIMEOUT_POLL_SHORT,
    E2EComposeRuntime,
    e2e_telegram_generate_update_bundle,
    e2e_telegram_thread_root_mid_for_message,
    e2e_threlium_user_unit_journalctl_bash,
    poll_until,
    REPO_ROOT,
    service_exec,
)
from .wiremock_client import (
    WiremockCorrelation,
    assert_wiremock_transport_egress_via_state,
    wiremock_state_thread_root_property,
    wiremock_state_thread_root_reply_targets,
    composite_context_key,
    find_wiremock_requests_by_body_contains,
    journal_has_compose_bootstrap_request,
    log_wiremock_correlation_journal,
    prepare_wiremock_scenario,
    wiremock_journal_request_body,
    wiremock_public_base,
    wiremock_state_reasoning_gate_release,
    wiremock_telegram_register_update,
    wiremock_telegram_unregister_update,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"

TELEGRAM_WIREMOCK_STUB_TAG_PRIVATE = "stub-telegram-wiremock-live-e2e-private"
TELEGRAM_WIREMOCK_STUB_DIR_PRIVATE = (
    _WIREMOCK_STUBS_ROOT / "test_telegram_wiremock_live_e2e_private"
)
TELEGRAM_AGENT_REPLY_BODY_PRIVATE = "ok telegram wiremock live e2e private"

TELEGRAM_WIREMOCK_STUB_TAG_FORUM = "stub-telegram-wiremock-live-e2e-forum-topic"
TELEGRAM_WIREMOCK_STUB_DIR_FORUM = (
    _WIREMOCK_STUBS_ROOT / "test_telegram_wiremock_live_e2e_forum_topic"
)
TELEGRAM_AGENT_REPLY_BODY_FORUM = "ok telegram wiremock live e2e forum topic"

TELEGRAM_WIREMOCK_STUB_TAG_TAIL_307 = "stub-telegram-wiremock-live-e2e-private-tail-307"
TELEGRAM_WIREMOCK_STUB_DIR_TAIL_307 = (
    _WIREMOCK_STUBS_ROOT / "test_telegram_wiremock_live_e2e_private_tail_307"
)
TELEGRAM_AGENT_REPLY_BODY_TAIL_307 = "ok telegram wiremock live e2e private tail 307"


@contextmanager
def wiremock_correlation_scope(
    e2e_runtime: E2EComposeRuntime,
    tag: str,
    nodeid: str,
) -> Generator[WiremockCorrelation, None, None]:
    base = wiremock_public_base(e2e_runtime.wiremock_host, e2e_runtime.wiremock_port)
    wc = WiremockCorrelation(test_id=tag, public_base=base)
    try:
        yield wc
    finally:
        try:
            log_wiremock_correlation_journal(wc, pytest_nodeid=nodeid)
        except Exception as e:  # noqa: BLE001
            log.warning(
                "wiremock_journal_dump_failed",
                nodeid=nodeid,
                error=repr(e),
            )






def test_live_telegram_wiremock_full_contour_private(
    e2e_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> None:
    """Стабы → register_update → журнал POST sendMessage (placeholder) + editMessageText (reply) + LLM coverage (без restart bridge в SUT)."""
    with wiremock_correlation_scope(
        e2e_runtime, TELEGRAM_WIREMOCK_STUB_TAG_PRIVATE, request.node.nodeid
    ) as wc:
        rt = e2e_runtime
        test_id = wc.test_id
        base = wc.public_base
        chat_id, message_id, update_id, mtid = e2e_telegram_generate_update_bundle(
            with_forum_topic=False,
        )
        correlation_key = e2e_telegram_thread_root_mid_for_message(
            chat_id=chat_id,
            message_id=message_id,
            message_thread_id=mtid,
        )
        log.debug(
            "telegram_e2e_private_setup",
            chat_id=chat_id,
            message_id=message_id,
            update_id=update_id,
            correlation_key_tail=correlation_key[-30:],
        )

        prepare_wiremock_scenario(
            base,
            stub_dir=TELEGRAM_WIREMOCK_STUB_DIR_PRIVATE,
            stub_tag=test_id,
            correlation_key=correlation_key,
        )

        wiremock_telegram_register_update(
            base,
            update_id=update_id,
            chat_id=chat_id,
            message_id=message_id,
            text=f"e2e telegram user text ({test_id}) (dynamic private)",
            thread_kind="",
            chat_title="",
        )

        # Контур telegram по STATE (без journal): egress editMessageText записал ответ агента
        # ('ok telegram wiremock live e2e private') в content-flag saw_egress_edit (по thread-root),
        # sendMessage-placeholder — saw_egress_send; LLM-фазы — call_sites. Egress асинхронный → поллим
        # флаг. Без stub_tag/журнала. Транспортный egress = тоже WireMock-вызов с X-Threlium-Thread-Root.
        try:
            assert_wiremock_transport_egress_via_state(
                base, correlation_key=correlation_key
            )
        finally:
            try:
                wiremock_telegram_unregister_update(base, update_id=update_id)
            except Exception:  # noqa: BLE001
                pass


def test_live_telegram_wiremock_full_contour_forum_topic(
    e2e_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> None:
    """Forum topic: ``message_thread_id`` в update и в теле ``sendMessage``/``editMessageText``."""
    with wiremock_correlation_scope(
        e2e_runtime, TELEGRAM_WIREMOCK_STUB_TAG_FORUM, request.node.nodeid
    ) as wc:
        rt = e2e_runtime
        test_id = wc.test_id
        base = wc.public_base
        chat_id, message_id, update_id, mtid = e2e_telegram_generate_update_bundle(
            with_forum_topic=True,
        )
        assert mtid is not None
        correlation_key = e2e_telegram_thread_root_mid_for_message(
            chat_id=chat_id,
            message_id=message_id,
            message_thread_id=mtid,
        )
        log.debug(
            "telegram_e2e_forum_setup",
            chat_id=chat_id,
            message_id=message_id,
            update_id=update_id,
            message_thread_id=mtid,
            correlation_key_tail=correlation_key[-30:],
        )

        prepare_wiremock_scenario(
            base,
            stub_dir=TELEGRAM_WIREMOCK_STUB_DIR_FORUM,
            stub_tag=test_id,
            correlation_key=correlation_key,
        )

        wiremock_telegram_register_update(
            base,
            update_id=update_id,
            chat_id=chat_id,
            message_id=message_id,
            text=f"e2e telegram user text ({test_id}) (dynamic forum)",
            thread_kind="forum",
            message_thread_id=mtid,
            chat_title="E2E Telegram Forum",
        )

        # Контур по STATE (forum topic): egress editMessageText → saw_egress_edit (ответ агента дошёл),
        # sendMessage → saw_egress_send, LLM → call_sites. Поллим асинхронный egress-флаг. Без журнала.
        try:
            assert_wiremock_transport_egress_via_state(
                base, correlation_key=correlation_key
            )
        finally:
            try:
                wiremock_telegram_unregister_update(base, update_id=update_id)
            except Exception:  # noqa: BLE001
                pass


def test_live_telegram_wiremock_private_tail_307_second_message(
    e2e_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> None:
    """Два входящих сообщения в один чат без reply: первое «держим» на 307 reasoning, второе — хвост.

    Отчёт (ожидаемое при одном notmuch-треде): второе сообщение не доходит до reasoning LiteLLM,
    пока не завершится первое (mutex ``threlium-work@…`` по ``thread_id``).     После ``reasoning_release`` оба проходят; два POST ``editMessageText`` с текстом агента из стаба и
    ``reply_parameters.message_id`` на ``message_id_1`` / ``message_id_2`` в ``sendMessage`` (placeholder). В промпте reasoning для
    второго — текст первого (общий notmuch-тред / хвост). См. ``docs/E2E.md`` §8.6.
    """
    with wiremock_correlation_scope(
        e2e_runtime, TELEGRAM_WIREMOCK_STUB_TAG_TAIL_307, request.node.nodeid
    ) as wc:
        rt = e2e_runtime
        test_id = wc.test_id
        base = wc.public_base
        chat_id, message_id_1, update_id_1, mtid = e2e_telegram_generate_update_bundle(
            with_forum_topic=False,
        )
        assert mtid is None
        message_id_2 = int(uuid.uuid4().int % 90_000) + 10_000
        while message_id_2 == message_id_1:
            message_id_2 = int(uuid.uuid4().int % 90_000) + 10_000
        update_id_2 = int(uuid.uuid4().int % 900_000_000) + 100_000_000
        while update_id_2 == update_id_1:
            update_id_2 = int(uuid.uuid4().int % 900_000_000) + 100_000_000

        # ФИКСИРОВАННЫЕ маркеры (не uuid): изоляция между тестами — по thread-root (correlation_key),
        # поэтому токены могут быть стабильными. Это позволяет статичному reasoning-стабу проверять
        # tail-attach content-flag'ом (contains body TOK1 AND TOK2 = текст msg1 дошёл до reasoning msg2),
        # без скана журнала по рантайм-значению. tok1 — текст msg1, tok2 — текст msg2 в общем треде.
        tok1 = "tg-tail307-msg1-marker"
        tok2 = "tg-tail307-msg2-marker"
        correlation_key = e2e_telegram_thread_root_mid_for_message(
            chat_id=chat_id,
            message_id=message_id_1,
            message_thread_id=mtid,
        )
        # msg1_mid_angle не нужен в ассерте: FSM перезаписывает IRT на каждой стадии,
        # оригинальный bridge MID не попадает в reasoning prompt verbatim.
        # Проверка tail attachment — tok1 в unified mail context msg2's reasoning.

        def _reasoning_chat_completion_seen(token: str) -> bool | None:
            for e in find_wiremock_requests_by_body_contains(
                base, token, stub_tag=test_id, timeout=2.0
            ):
                req = e.get("request")
                if not isinstance(req, dict):
                    continue
                url = str(req.get("url") or "")
                if "chat/completions" not in url:
                    continue
                b = wiremock_journal_request_body(e)
                if "<envelope>" in b and '"tools"' in b:
                    return True
            return None

        log.debug(
            "telegram_tail_307_setup",
            chat_id=chat_id,
            message_id_1=message_id_1,
            message_id_2=message_id_2,
            update_id_1=update_id_1,
            update_id_2=update_id_2,
            correlation_key_tail=correlation_key[-36:],
        )

        prepare_wiremock_scenario(
            base,
            stub_dir=TELEGRAM_WIREMOCK_STUB_DIR_TAIL_307,
            stub_tag=test_id,
            correlation_key=correlation_key,
        )

        registered_update_ids: list[int] = []
        ctx_key = composite_context_key(test_id, correlation_key)
        try:
            wiremock_telegram_register_update(
                base,
                update_id=update_id_1,
                chat_id=chat_id,
                message_id=message_id_1,
                text=f"e2e telegram ({test_id}) msg1 {tok1}",
                thread_kind="",
                chat_title="",
            )
            registered_update_ids.append(update_id_1)

            poll_until(
                lambda: _reasoning_chat_completion_seen(tok1),
                timeout=TIMEOUT_POLL_SHORT,
                interval=2.0,
                desc="WireMock: reasoning POST chat/completions с текстом msg1 (307 gate)",
            )

            # Детерминизм tail-attachment: msg2 привяжется к треду msg1 только если bridge-сообщение
            # msg1 уже проиндексировано в notmuch с тегом ``route`` (anchor-запрос
            # ``resolve_bridge_tail_mid_for_space``: ``tag:route AND from:telegram AND Threliumspace``).
            # Видимость reasoning msg1 в журнале WM этого НЕ гарантирует (индекс notmuch коммитится
            # асинхронно от стадии) → без явного ожидания msg2 иногда стартует свой тред и
            # ``tok1 in msg2.reasoning`` флапает. Ждём индексацию msg1 до регистрации msg2.
            _wait_telegram_correlation_indexed(
                rt.project_name, correlation_key=correlation_key
            )

            wiremock_telegram_register_update(
                base,
                update_id=update_id_2,
                chat_id=chat_id,
                message_id=message_id_2,
                text=f"e2e telegram ({test_id}) msg2 {tok2}",
                thread_kind="",
                chat_title="",
            )
            registered_update_ids.append(update_id_2)

            # До release: msg2-reasoning НЕ должно произойти (307-mutex по thread_id держит msg2, пока
            # msg1 в обработке). Проверяем по STATE-флагу saw_msg2_reasoning (contains tok2 в reasoning),
            # без скана журнала. Ожидаем таймаут (флаг остаётся "0").
            msg2_reasoning_before_release = False
            try:
                poll_until(
                    lambda: True
                    if wiremock_state_thread_root_property(
                        base, correlation_key, "saw_msg2_reasoning"
                    ) == "1"
                    else None,
                    timeout=5.0,
                    interval=1.0,
                    desc="state: msg2 reasoning до release (ожидаем таймаут — gated 307-mutex)",
                )
                msg2_reasoning_before_release = True
            except TimeoutError:
                msg2_reasoning_before_release = False

            log.debug(
                "telegram_tail_307_reasoning_before_release",
                msg2_reasoning_before_release=msg2_reasoning_before_release,
            )

            wiremock_state_reasoning_gate_release(base, ctx_key)

            # Контур обоих сообщений по STATE (без журнала). Egress асинхронный → поллим.
            # 1) Ответы на ОБА входящих: reply-target message_id placeholder-sendMessage (записаны
            #    list.addLast {rt: …} по thread-root) → множество == {msg1, msg2}. Это и есть «ровно два
            #    разных ответа» (count==2 = два разных таргета), без journal-скана по stub_tag.
            def _both_reply_targets() -> set[int] | None:
                rts = set(wiremock_state_thread_root_reply_targets(base, correlation_key))
                return rts if {message_id_1, message_id_2} <= rts else None

            reply_targets = poll_until(
                _both_reply_targets,
                timeout=TIMEOUT_POLL_SHORT,
                interval=3.0,
                desc="state: reply_parameters → reply-targets == {msg1, msg2}",
            )
            assert reply_targets == {message_id_1, message_id_2}, (
                "Ответы должны быть reply ровно на msg1 и msg2 (разные routing.message_id); "
                f"ожидалось {{{message_id_1}, {message_id_2}}}, получено {reply_targets!r}"
            )

            # 2) LLM-фазы + egress-ответ агента дошли (saw_egress_edit content-flag по thread-root).
            assert_wiremock_transport_egress_via_state(base, correlation_key=correlation_key)

            # 3) Tail-attach: текст msg1 (tok1) попал в reasoning-промпт msg2 (tok2) — общий notmuch-тред.
            #    Статичный content-flag saw_tail_attach = (contains body tok1) AND (contains body tok2) на
            #    reasoning-стабе msg2 (токены фиксированы; изоляция — thread-root). Прямое чтение после
            #    барьера reply-targets выше (контур обоих сообщений завершён).
            assert (
                wiremock_state_thread_root_property(base, correlation_key, "saw_tail_attach") == "1"
            ), (
                "tail-attach: текст msg1 должен попасть в reasoning-промпт msg2 (общий notmuch-тред) — "
                "state saw_tail_attach"
            )


            log.info(
                "telegram_tail_307_report",
                tail_attachment_confirmed=True,
                tok1=tok1,
                msg2_reasoning_before_release=msg2_reasoning_before_release,
                agent_reply_targets_n=len(reply_targets),
                reply_targets=sorted(reply_targets),
            )
        finally:
            for uid in reversed(registered_update_ids):
                try:
                    wiremock_telegram_unregister_update(base, update_id=uid)
                except Exception:  # noqa: BLE001
                    pass


def _wait_telegram_correlation_indexed(project: str, *, correlation_key: str) -> None:
    """Дождаться, что первое сообщение telegram-бриджа дошло до ingress (= вставлено в notmuch).

    Сигнал — ``ingress_distill`` в call_sites треда (ingress-LLM вызывается ПОСЛЕ вставки письма в notmuch),
    через WireMock state, БЕЗ ``docker exec`` ``notmuch search``. Предусловие tail-attach msg2: msg1 уже в
    общем notmuch-треде. Изоляция = thread-root (correlation_key == X-Threlium-Thread-Root ingress-запроса).
    """
    from .toolkit import discover_runtime  # noqa: PLC0415
    from .wiremock_client import wiremock_state_thread_root_call_sites  # noqa: PLC0415

    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)

    def _probe() -> bool | None:
        return (
            True
            if "ingress_distill" in wiremock_state_thread_root_call_sites(wm, correlation_key)
            else None
        )

    poll_until(
        _probe,
        timeout=TIMEOUT_POLL_SHORT,
        desc=f"telegram first delivery ingress (state) for {correlation_key!r}",
    )


def _wait_bridge_telegram_duplicate_skip(project: str, *, message_id: int) -> None:
    journal_cmd = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@telegram.service", 400, transport_journal=False
    )
    needle = f'"message_id": {int(message_id)}'

    def _probe() -> bool | None:
        r = service_exec(
            project,
            "sut",
            ["bash", "-lc", journal_cmd],
            repo_root=REPO_ROOT,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        text = (r.stdout or "") + (r.stderr or "")
        for line in text.splitlines():
            if "duplicate_skip" in line and needle in line:
                return True
        return None

    poll_until(
        _probe,
        timeout=TIMEOUT_POLL_SHORT,
        desc=f"telegram bridge duplicate_skip for message_id={message_id}",
    )


def test_live_telegram_bridge_duplicate_skip_on_running_stack(
    e2e_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> None:
    """Повторная регистрация того же Telegram update → ``duplicate_skip`` в journal telegram-бриджа."""
    with wiremock_correlation_scope(
        e2e_runtime, TELEGRAM_WIREMOCK_STUB_TAG_PRIVATE, request.node.nodeid
    ) as wc:
        rt = e2e_runtime
        test_id = wc.test_id
        base = wc.public_base
        chat_id, message_id, update_id, mtid = e2e_telegram_generate_update_bundle(with_forum_topic=False)
        correlation_key = e2e_telegram_thread_root_mid_for_message(
            chat_id=chat_id,
            message_id=message_id,
            message_thread_id=mtid,
        )
        try:
            prepare_wiremock_scenario(
                base,
                stub_dir=TELEGRAM_WIREMOCK_STUB_DIR_PRIVATE,
                stub_tag=test_id,
                correlation_key=correlation_key,
            )
            wiremock_telegram_register_update(
                base,
                update_id=update_id,
                chat_id=chat_id,
                message_id=message_id,
                text="e2e telegram duplicate_skip probe",
                message_thread_id=mtid,
            )
            _wait_telegram_correlation_indexed(rt.project_name, correlation_key=correlation_key)
            wiremock_telegram_register_update(
                base,
                update_id=update_id,
                chat_id=chat_id,
                message_id=message_id,
                text="e2e telegram duplicate_skip probe",
                message_thread_id=mtid,
            )
            _wait_bridge_telegram_duplicate_skip(rt.project_name, message_id=message_id)
        finally:
            try:
                wiremock_telegram_unregister_update(base, update_id=update_id)
            except Exception:  # noqa: BLE001
                pass
