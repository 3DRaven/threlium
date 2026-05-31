"""Telegram и LLM против WireMock в живом стеке (compose уже поднят вне этого модуля).

Сквозной контур: подменный Bot API (WireMock) + тот же WireMock как OpenAI-совместимый API.
Два сценария — личка и forum topic (разные каталоги стабов и ``stub_tag``).
"""
from __future__ import annotations

import uuid
from collections.abc import Generator
from pathlib import Path

import pytest

from tests.e2e.log import log
from threlium.types import FsmStage

from .helpers import (
    TIMEOUT_POLL_SHORT,
    E2EComposeRuntime,
    assert_notmuch_folder_contains_body_token,
    discover_live_e2e_project_name,
    discover_runtime,
    e2e_telegram_generate_update_bundle,
    e2e_telegram_thread_root_mid_for_message,
    e2e_threlium_user_unit_journalctl_bash,
    poll_until,
    REPO_ROOT,
    service_exec,
)
from .wiremock_client import (
    WiremockCorrelation,
    assert_wiremock_telegram_e2e_openai_coverage,
    composite_context_key,
    find_wiremock_requests_by_body_contains,
    journal_has_request,
    log_wiremock_correlation_journal,
    prepare_wiremock_scenario,
    wiremock_journal_request_body,
    wiremock_journal_telegram_sendmessage_bodies_matching_agent_reply,
    wiremock_journal_telegram_sendmessage_placeholder_bodies,
    wiremock_public_base,
    wiremock_state_reasoning_gate_release,
    wiremock_telegram_register_update,
    wiremock_telegram_sendmessage_body_reply_target_message_id,
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


def _telegram_bridge_journal_suggests_missing_env(project_name: str) -> bool:
    """По journal user unit telegram-бриджа: типичная ошибка деплоя без THRELIUM_TELEGRAM_* в unit."""
    jc = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@telegram.service",
        80,
        shell_redirect="2>/dev/null",
    )
    inner = (
        "if "
        + jc
        + " | grep -qE 'required via systemd EnvironmentFile|THRELIUM_TELEGRAM_'; then echo MISCONFIG; fi"
    )
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", inner],
    )
    return r.returncode == 0 and "MISCONFIG" in (r.stdout or "")


@pytest.fixture(scope="module")
def live_telegram_wiremock_runtime() -> E2EComposeRuntime:
    """Host-порты WireMock с хоста pytest (как ``live_matrix_wiremock_runtime``)."""
    pn = discover_live_e2e_project_name()
    if not pn:
        pytest.skip(
            "No live e2e stack: start compose (pytest tests/e2e / wipe_bake)."
        )
    try:
        return discover_runtime(pn)
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"live e2e stack not reachable: {e}")


@pytest.fixture
def wiremock_correlation_private(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> Generator[WiremockCorrelation, None, None]:
    base = wiremock_public_base(
        live_telegram_wiremock_runtime.wiremock_host,
        live_telegram_wiremock_runtime.wiremock_port,
    )
    wc = WiremockCorrelation(
        test_id=TELEGRAM_WIREMOCK_STUB_TAG_PRIVATE,
        public_base=base,
    )
    yield wc
    try:
        log_wiremock_correlation_journal(wc, pytest_nodeid=request.node.nodeid)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "wiremock_journal_dump_failed",
            nodeid=request.node.nodeid,
            error=repr(e),
        )


@pytest.fixture
def wiremock_correlation_forum(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> Generator[WiremockCorrelation, None, None]:
    base = wiremock_public_base(
        live_telegram_wiremock_runtime.wiremock_host,
        live_telegram_wiremock_runtime.wiremock_port,
    )
    wc = WiremockCorrelation(
        test_id=TELEGRAM_WIREMOCK_STUB_TAG_FORUM,
        public_base=base,
    )
    yield wc
    try:
        log_wiremock_correlation_journal(wc, pytest_nodeid=request.node.nodeid)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "wiremock_journal_dump_failed",
            nodeid=request.node.nodeid,
            error=repr(e),
        )


@pytest.fixture
def wiremock_correlation_tail_307(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> Generator[WiremockCorrelation, None, None]:
    base = wiremock_public_base(
        live_telegram_wiremock_runtime.wiremock_host,
        live_telegram_wiremock_runtime.wiremock_port,
    )
    wc = WiremockCorrelation(
        test_id=TELEGRAM_WIREMOCK_STUB_TAG_TAIL_307,
        public_base=base,
    )
    yield wc
    try:
        log_wiremock_correlation_journal(wc, pytest_nodeid=request.node.nodeid)
    except Exception as e:  # noqa: BLE001
        log.warning(
            "wiremock_journal_dump_failed",
            nodeid=request.node.nodeid,
            error=repr(e),
        )


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_live_telegram_wiremock_full_contour_private(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    wiremock_correlation_private: WiremockCorrelation,
) -> None:
    """Стабы → register_update → журнал POST sendMessage (placeholder) + editMessageText (reply) + LLM coverage (без restart bridge в SUT)."""
    rt = live_telegram_wiremock_runtime
    test_id = wiremock_correlation_private.test_id
    base = wiremock_correlation_private.public_base

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

    def _send_seen() -> bool | None:
        if journal_has_request(
            base,
            stub_tag=test_id,
            method="POST",
            url_contains="editMessageText",
        ):
            return True
        return None

    try:
        poll_until(
            _send_seen,
            timeout=TIMEOUT_POLL_SHORT,
            interval=3.0,
            desc="WireMock journal: POST editMessageText (private)",
        )
    except TimeoutError:
        if _telegram_bridge_journal_suggests_missing_env(rt.project_name):
            pytest.skip(
                "Нет POST editMessageText в WireMock: telegram-бридж без THRELIUM_TELEGRAM_* "
                "(по journal user unit)."
            )
        raise
    finally:
        try:
            wiremock_telegram_unregister_update(base, update_id=update_id)
        except Exception:  # noqa: BLE001
            pass

    assert_wiremock_telegram_e2e_openai_coverage(
        base,
        test_id=test_id,
        chat_id=chat_id,
        reply_body=TELEGRAM_AGENT_REPLY_BODY_PRIVATE,
        message_thread_id=mtid,
    )
    assert_notmuch_folder_contains_body_token(
        rt.project_name,
        stage_folder_id=FsmStage.ARCHIVE.value,
        body_token=str(chat_id),
        repo_root=REPO_ROOT,
    )


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_live_telegram_wiremock_full_contour_forum_topic(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    wiremock_correlation_forum: WiremockCorrelation,
) -> None:
    """Forum topic: ``message_thread_id`` в update и в теле ``sendMessage``/``editMessageText``."""
    rt = live_telegram_wiremock_runtime
    test_id = wiremock_correlation_forum.test_id
    base = wiremock_correlation_forum.public_base

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

    def _send_seen() -> bool | None:
        if journal_has_request(
            base,
            stub_tag=test_id,
            method="POST",
            url_contains="editMessageText",
        ):
            return True
        return None

    try:
        poll_until(
            _send_seen,
            timeout=TIMEOUT_POLL_SHORT,
            interval=3.0,
            desc="WireMock journal: POST editMessageText (forum topic)",
        )
    except TimeoutError:
        if _telegram_bridge_journal_suggests_missing_env(rt.project_name):
            pytest.skip(
                "Нет POST editMessageText в WireMock: telegram-бридж без THRELIUM_TELEGRAM_* "
                "(по journal user unit)."
            )
        raise
    finally:
        try:
            wiremock_telegram_unregister_update(base, update_id=update_id)
        except Exception:  # noqa: BLE001
            pass

    assert_wiremock_telegram_e2e_openai_coverage(
        base,
        test_id=test_id,
        chat_id=chat_id,
        reply_body=TELEGRAM_AGENT_REPLY_BODY_FORUM,
        message_thread_id=mtid,
    )
    assert_notmuch_folder_contains_body_token(
        rt.project_name,
        stage_folder_id=FsmStage.ARCHIVE.value,
        body_token=str(chat_id),
        repo_root=REPO_ROOT,
    )


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_live_telegram_wiremock_private_tail_307_second_message(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
    wiremock_correlation_tail_307: WiremockCorrelation,
) -> None:
    """Два входящих сообщения в один чат без reply: первое «держим» на 307 reasoning, второе — хвост.

    Отчёт (ожидаемое при одном notmuch-треде): второе сообщение не доходит до reasoning LiteLLM,
    пока не завершится первое (mutex ``threlium-work@…`` по ``thread_id``).     После ``reasoning_release`` оба проходят; два POST ``editMessageText`` с текстом агента из стаба и
    ``reply_parameters.message_id`` на ``message_id_1`` / ``message_id_2`` в ``sendMessage`` (placeholder). В промпте reasoning для
    второго — текст первого (общий notmuch-тред / хвост). См. ``docs/E2E_ISOLATION.md`` §8.6.
    """
    rt = live_telegram_wiremock_runtime
    test_id = wiremock_correlation_tail_307.test_id
    base = wiremock_correlation_tail_307.public_base

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

    tok1 = f"tg_tail307_a_{uuid.uuid4().hex[:12]}"
    tok2 = f"tg_tail307_b_{uuid.uuid4().hex[:12]}"
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

    wiremock_telegram_register_update(
        base,
        update_id=update_id_1,
        chat_id=chat_id,
        message_id=message_id_1,
        text=f"e2e telegram ({test_id}) msg1 {tok1}",
        thread_kind="",
        chat_title="",
    )
    ctx_key = composite_context_key(test_id, correlation_key)

    try:
        poll_until(
            lambda: _reasoning_chat_completion_seen(tok1),
            timeout=TIMEOUT_POLL_SHORT,
            interval=2.0,
            desc="WireMock: reasoning POST chat/completions с текстом msg1 (307 gate)",
        )
    except TimeoutError:
        if _telegram_bridge_journal_suggests_missing_env(rt.project_name):
            pytest.skip(
                "Нет reasoning для msg1: telegram-бридж без THRELIUM_TELEGRAM_* "
                "(по journal user unit)."
            )
        raise

    wiremock_telegram_register_update(
        base,
        update_id=update_id_2,
        chat_id=chat_id,
        message_id=message_id_2,
        text=f"e2e telegram ({test_id}) msg2 {tok2}",
        thread_kind="",
        chat_title="",
    )

    msg2_reasoning_before_release = False
    try:
        poll_until(
            lambda: True if _reasoning_chat_completion_seen(tok2) else None,
            timeout=5.0,
            interval=1.0,
            desc="WireMock: проба — reasoning для второго сообщения до release (ожидаем таймаут)",
        )
        msg2_reasoning_before_release = True
    except TimeoutError:
        msg2_reasoning_before_release = False

    log.debug(
        "telegram_tail_307_reasoning_before_release",
        msg2_reasoning_before_release=msg2_reasoning_before_release,
    )

    wiremock_state_reasoning_gate_release(base, ctx_key)

    def _two_agent_sendmessages_seen() -> bool | None:
        bs = wiremock_journal_telegram_sendmessage_bodies_matching_agent_reply(
            base,
            stub_tag=test_id,
            chat_id=chat_id,
            reply_body=TELEGRAM_AGENT_REPLY_BODY_TAIL_307,
            message_thread_id=mtid,
        )
        return True if len(bs) >= 2 else None

    try:
        poll_until(
            _two_agent_sendmessages_seen,
            timeout=TIMEOUT_POLL_SHORT,
            interval=3.0,
            desc=(
                "WireMock journal: ≥2 POST editMessageText с chat_id и текстом ответа агента "
                f"({TELEGRAM_AGENT_REPLY_BODY_TAIL_307!r})"
            ),
        )
    except TimeoutError:
        if _telegram_bridge_journal_suggests_missing_env(rt.project_name):
            pytest.skip(
                "Нет двух editMessageText с ответом агента: telegram-бридж без THRELIUM_TELEGRAM_* "
                "(по journal user unit)."
            )
        raise

    assert_wiremock_telegram_e2e_openai_coverage(
        base,
        test_id=test_id,
        chat_id=chat_id,
        reply_body=TELEGRAM_AGENT_REPLY_BODY_TAIL_307,
        message_thread_id=mtid,
    )

    reply_bodies = wiremock_journal_telegram_sendmessage_bodies_matching_agent_reply(
        base,
        stub_tag=test_id,
        chat_id=chat_id,
        reply_body=TELEGRAM_AGENT_REPLY_BODY_TAIL_307,
        message_thread_id=mtid,
    )
    assert len(reply_bodies) == 2, (
        "Ожидались ровно два исходящих ответа агента (POST editMessageText с текстом из reasoning-стаба); "
        f"получено {len(reply_bodies)}. Превью тел: {[b[:900] for b in reply_bodies]!r}"
    )
    placeholder_bodies = wiremock_journal_telegram_sendmessage_placeholder_bodies(
        base,
        stub_tag=test_id,
        chat_id=chat_id,
    )
    reply_targets = [
        wiremock_telegram_sendmessage_body_reply_target_message_id(b) for b in placeholder_bodies
    ]
    reply_targets = [t for t in reply_targets if t is not None]
    assert len(reply_targets) >= 2, (
        "В каждом sendMessage (placeholder) ожидался JSON ``reply_parameters`` с ``message_id`` входящего сообщения; "
        f"targets={reply_targets!r}, превью тел: {[b[:1200] for b in placeholder_bodies]!r}"
    )
    assert set(reply_targets) == {message_id_1, message_id_2}, (
        "Ответы должны быть reply на msg1 и msg2 (разные ``routing.message_id``); "
        f"ожидалось множество {{{message_id_1}, {message_id_2}}}, получено {set(reply_targets)!r}"
    )

    msg2_reasoning_bodies: list[str] = []
    for e in find_wiremock_requests_by_body_contains(
        base, tok2, stub_tag=test_id, timeout=2.0
    ):
        req = e.get("request")
        if not isinstance(req, dict):
            continue
        url = str(req.get("url") or "")
        if "chat/completions" not in url:
            continue
        b = wiremock_journal_request_body(e)
        if "<envelope>" in b and '"tools"' in b:
            msg2_reasoning_bodies.append(b)

    assert msg2_reasoning_bodies, (
        "После завершения контура ожидался хотя бы один POST reasoning с телом msg2 "
        f"(needle {tok2!r})."
    )
    joined = "\n".join(msg2_reasoning_bodies)
    assert tok1 in joined, (
        "В reasoning для второго сообщения ожидался текст msg1 в unified mail context "
        "(доказательство общего notmuch-треда через tail attachment): "
        f"tok1={tok1!r}, превью тел={joined[:4000]!r}"
    )

    assert_notmuch_folder_contains_body_token(
        rt.project_name,
        stage_folder_id=FsmStage.ARCHIVE.value,
        body_token=str(chat_id),
        repo_root=REPO_ROOT,
    )

    log.info(
        "telegram_tail_307_report",
        tail_attachment_confirmed=True,
        tok1=tok1,
        msg2_reasoning_before_release=msg2_reasoning_before_release,
        agent_send_message_n=len(reply_bodies),
        reply_targets=sorted(reply_targets),
    )

    for uid in (update_id_2, update_id_1):
        try:
            wiremock_telegram_unregister_update(base, update_id=uid)
        except Exception:  # noqa: BLE001
            pass


def _wait_bridge_telegram_duplicate_skip(project: str, *, message_id: int) -> None:
    jc = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@telegram.service", 120, shell_redirect="2>/dev/null"
    )
    inner = (
        "if "
        + jc
        + f" | grep -q 'duplicate_skip' && {jc} | grep -q 'message_id={message_id}'; then echo OK; fi"
    )

    def _probe() -> bool | None:
        r = service_exec(project, "sut", ["bash", "-lc", inner], repo_root=REPO_ROOT, timeout=30)
        return True if r.returncode == 0 and "OK" in (r.stdout or "") else None

    poll_until(
        _probe,
        timeout=TIMEOUT_POLL_SHORT,
        desc=f"telegram bridge duplicate_skip for message_id={message_id}",
    )


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_live_telegram_bridge_duplicate_skip_on_running_stack(
    live_telegram_wiremock_runtime: E2EComposeRuntime,
) -> None:
    """Повторная регистрация того же Telegram update → ``duplicate_skip`` в journal telegram-бриджа."""
    rt = live_telegram_wiremock_runtime
    base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    chat_id, message_id, update_id, mtid = e2e_telegram_generate_update_bundle(with_forum_topic=False)
    try:
        wiremock_telegram_register_update(
            base,
            update_id=update_id,
            chat_id=chat_id,
            message_id=message_id,
            text="e2e telegram duplicate_skip probe",
            message_thread_id=mtid,
        )
        poll_until(
            lambda: True
            if journal_has_request(base, method="GET", url_contains="getUpdates")
            else None,
            timeout=TIMEOUT_POLL_SHORT,
            interval=2.0,
            desc="telegram getUpdates after first register",
        )
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
