"""Matrix и LLM против WireMock в живом стеке (compose уже поднят вне этого модуля).

**Тест-кейс: сквозной контур «подменный Matrix homeserver + подменный OpenAI-совместимый API».**

**Цель.** Убедиться, что бридж Matrix ходит в застабленный сервер (WireMock), а подсистема
рассуждений и обогащения ходит в тот же WireMock как к базовому URL нейросети: в журнале подмены
видно и исходящую отправку сообщения в комнату Matrix, и цепочку исходящих запросов к HTTP-API
вида completion и embeddings, согласованных со сценарием стабов (включая вспомогательные фазы RAG).

**Предусловие.** В развёрнутой системе базовый URL Matrix и базовый URL API LLM должны указывать на
WireMock (настраивается при деплое вне этого теста). Если система настроена только на заглушку LLM
без WireMock, тест будет ждать события до таймаута.

**Шаги.**

1. Определить запущенный compose-проект (переменная окружения или автообнаружение) и адрес WireMock
   **с хоста** прогона (проброшенный порт). Иначе тест пропускается.
2. Сгенерировать уникальные ``room_id`` / ``event_id``, вычислить ``correlation_key`` (MID корня треда
   как ``RfcMessageIdWire.from_native(MatrixNativeId(v=1, room_id, event_id))``) и зарегистрировать
   комнату в shared list ``matrix_rooms`` (WireMock State Extension). Засидировать State контекст
   LiteLLM по ``correlation_key``. Загрузить стабы и очистить журнал.
3. До таймаута ждать в журнале WireMock записи об **исходящем** HTTP-запросе Matrix на отправку
   текстового сообщения в комнату (клиентский API отправки события в комнату). Бридж не перезапускается:
   подхват стабов обеспечивается общим cold-reset при старте сессии pytest / поднятием стека.
4. Отдельно проверить, что из системы под тестом на WireMock ушли ожидаемые **POST** к
   OpenAI-совместимым путям (чат, эмбеддинги, сопутствующие вызовы по сценарию), с тем же ``stub_tag`` в metadata
   и узнаваемым содержимым (модель и маркеры сценария, фразы из цепочки RAG). Успех только на шаге 3
   без шага 4 — провал.
5. В ``finally`` — удалить свою комнату из shared list (``unregister_room``); при ошибке — подавить.

**Поведение стабов.** Первый ``/sync`` собирается response-template из **shared list** ``matrix_rooms``
(State Extension): ``{{#each}}`` по всем зарегистрированным комнатам. Каждый тест добавляет свою
комнату при setup и удаляет при teardown — параллельные pytest-workers не мешают друг другу.
"""
from __future__ import annotations

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
    e2e_matrix_generate_room_ids,
    e2e_matrix_thread_root_mid_for_sync_event,
    e2e_threlium_user_unit_journalctl_bash,
    poll_until,
    REPO_ROOT,
    service_exec,
)
from .wiremock_client import (
    WiremockCorrelation,
    assert_wiremock_matrix_e2e_openai_coverage,
    journal_has_request,
    log_wiremock_correlation_journal,
    prepare_wiremock_scenario,
    wiremock_matrix_register_room,
    wiremock_matrix_unregister_room,
    wiremock_public_base,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
MATRIX_WIREMOCK_STUB_TAG = "stub-matrix-wiremock-live-e2e-01"
MATRIX_WIREMOCK_STUB_DIR = _WIREMOCK_STUBS_ROOT / "test_matrix_wiremock_live_e2e"


def _matrix_bridge_journal_suggests_missing_env(project_name: str) -> bool:
    """По journal user unit matrix-бриджа: типичная ошибка деплоя без THRELIUM_MATRIX_* в unit."""
    jc = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@matrix.service",
        80,
        shell_redirect="2>/dev/null",
    )
    inner = (
        "if "
        + jc
        + " | grep -qE 'required via systemd EnvironmentFile|THRELIUM_MATRIX_'; then echo MISCONFIG; fi"
    )
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", inner],
    )
    return r.returncode == 0 and "MISCONFIG" in (r.stdout or "")


@pytest.fixture(scope="module")
def live_matrix_wiremock_runtime() -> E2EComposeRuntime:
    """Host-порты GreenMail / WireMock с хоста pytest (как ``live_mailflow_runtime``)."""
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
def wiremock_correlation(
    live_matrix_wiremock_runtime: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> Generator[WiremockCorrelation, None, None]:
    base = wiremock_public_base(
        live_matrix_wiremock_runtime.wiremock_host,
        live_matrix_wiremock_runtime.wiremock_port,
    )
    wc = WiremockCorrelation(
        test_id=MATRIX_WIREMOCK_STUB_TAG,
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
def test_live_matrix_wiremock_full_contour_on_running_stack(
    live_matrix_wiremock_runtime: E2EComposeRuntime,
    wiremock_correlation: WiremockCorrelation,
) -> None:
    """Стабы → register_room → журнал WireMock PUT send + LLM coverage (без restart bridge в SUT)."""
    rt = live_matrix_wiremock_runtime
    test_id = wiremock_correlation.test_id
    base = wiremock_correlation.public_base

    room_id, event_id = e2e_matrix_generate_room_ids()
    correlation_key = e2e_matrix_thread_root_mid_for_sync_event(
        room_id=room_id, event_id=event_id,
    )
    log.debug(
        "matrix_e2e_setup",
        room_id=room_id,
        event_id=event_id,
        correlation_key_tail=correlation_key[-30:],
    )

    prepare_wiremock_scenario(
        base,
        stub_dir=MATRIX_WIREMOCK_STUB_DIR,
        stub_tag=test_id,
        correlation_key=correlation_key,
    )

    wiremock_matrix_register_room(
        base,
        room_id=room_id,
        event_id=event_id,
        event_body=f"e2e matrix user text ({test_id}) (dynamic root event)",
        room_name="E2E Matrix Live Room",
    )

    def _matrix_send_put_seen() -> bool | None:
        if journal_has_request(
            base,
            stub_tag=test_id,
            method="PUT",
            url_contains="send/m.room.message",
        ):
            return True
        return None

    try:
        poll_until(
            _matrix_send_put_seen,
            timeout=TIMEOUT_POLL_SHORT,
            interval=3.0,
            desc="WireMock journal: PUT Matrix m.room.message send",
        )
    except TimeoutError:
        if _matrix_bridge_journal_suggests_missing_env(rt.project_name):
            pytest.skip(
                "Нет PUT send/m.room.message в WireMock: matrix-бридж без THRELIUM_MATRIX_* "
                "(по journal user unit)."
            )
        raise
    finally:
        try:
            wiremock_matrix_unregister_room(base, room_id=room_id)
        except Exception:  # noqa: BLE001
            pass

    assert_wiremock_matrix_e2e_openai_coverage(base, test_id=test_id)
    matrix_room_token = room_id.removeprefix("!").split(":", 1)[0]
    assert_notmuch_folder_contains_body_token(
        rt.project_name,
        stage_folder_id=FsmStage.ARCHIVE.value,
        body_token=matrix_room_token,
        repo_root=REPO_ROOT,
    )


def _wait_bridge_matrix_duplicate_skip(project: str, *, event_id: str) -> None:
    jc = e2e_threlium_user_unit_journalctl_bash(
        "threlium-bridge@matrix.service", 120, shell_redirect="2>/dev/null"
    )
    inner = (
        "if "
        + jc
        + f" | grep -q 'duplicate_skip' && {jc} | grep -q {event_id!r}; then echo OK; fi"
    )
    def _probe() -> bool | None:
        r = service_exec(project, "sut", ["bash", "-lc", inner], repo_root=REPO_ROOT, timeout=30)
        return True if r.returncode == 0 and "OK" in (r.stdout or "") else None

    poll_until(_probe, timeout=TIMEOUT_POLL_SHORT, desc=f"matrix bridge duplicate_skip for {event_id!r}")


@pytest.mark.e2e
@pytest.mark.e2e_live
def test_live_matrix_bridge_duplicate_skip_on_running_stack(
    live_matrix_wiremock_runtime: E2EComposeRuntime,
) -> None:
    """Повторная доставка того же Matrix event → ``duplicate_skip`` в journal matrix-бриджа."""
    rt = live_matrix_wiremock_runtime
    base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    room_id, event_id = e2e_matrix_generate_room_ids()
    try:
        wiremock_matrix_register_room(
            base,
            room_id=room_id,
            event_id=event_id,
            event_body="e2e matrix duplicate_skip probe",
            room_name="E2E Matrix Dup Room",
        )
        poll_until(
            lambda: True
            if journal_has_request(base, method="GET", url_contains="/sync")
            else None,
            timeout=TIMEOUT_POLL_SHORT,
            interval=2.0,
            desc="matrix /sync activity after first register",
        )
        wiremock_matrix_register_room(
            base,
            room_id=room_id,
            event_id=event_id,
            event_body="e2e matrix duplicate_skip probe",
            room_name="E2E Matrix Dup Room",
        )
        _wait_bridge_matrix_duplicate_skip(rt.project_name, event_id=str(event_id))
    finally:
        try:
            wiremock_matrix_unregister_room(base, room_id=room_id)
        except Exception:  # noqa: BLE001
            pass
