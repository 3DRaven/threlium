"""E2E: общий Docker Compose стек через filelock (shared compose, hintgrid-модель).

Единственный compose-проект на всю pytest(-xdist) сессию: первый воркер (или
единственный процесс при однопоточном ``pytest``) — лидер. Лидер поднимает стек
под ``FileLock``, записывает координационные файлы (``ready.flag`` +
``runtime.json``) в стабильный каталог под ``/tmp`` (см.
:func:`~tests.e2e.helpers.e2e_compose_coord_paths`); остальные воркеры читают
``project_name`` и ``discover_runtime``.

**Дефолт — однопоточный** ``pytest`` / ``pytest tests/e2e``. Shared compose и
filelock работают и при ``-n 1`` (единственный ``gw0`` всегда лидер). Явный
``-n 8`` — стресс / контракт параллельности, отдельная команда в CI или локально.
**Не** указывать ``addopts = -n 8`` в ``pyproject.toml``.

**Attach-only.** ``compose_stack`` НИКОГДА не пекёт образ и не поднимает compose сам —
он только **присоединяется** к уже healthy shared-стеку
(:func:`~tests.e2e.helpers.discover_live_e2e_project_name`). Подъём + bake образа —
исключительно ``tests/e2e/wipe_bake.py``; синхронизация репозитория/среды без запекания —
``tests/e2e/wipe_sync.py`` (см. ``docs/FSTS_SYNC.md``). Если healthy-стека нет, лидер делает
``pytest.fail`` с подсказкой запустить ``wipe_bake`` / ``wipe_sync`` — без скрытого подъёма.

**Политика teardown:** ``pytest_sessionfinish`` **не** вызывает ``compose down`` —
стек остаётся поднятым для повторных прогонов и ручной отладки. Явный
``compose down`` — по opt-in env ``THRELIUM_E2E_COMPOSE_DOWN=1``.

Prereq — **hard failure, не skip**. Отсутствие Linux / Docker daemon / extras ``[e2e]``
даёт ``pytest.fail`` — см. ``docs/TESTING.md`` §4.2.

**Журнал unmatched** (``GET /__admin/requests/unmatched``): один раз за инвокацию pytest
(при ``-n N`` — один раз на все воркеры через маркер в session-unique ``tmp_path_factory`` dir)
выполняется сброс журнала WireMock (``DELETE /__admin/requests``) и idempotent upsert
``compose_bootstrap/`` стабов. Стабы, State-контексты и pipeline **не трогаются** —
изоляция обеспечивается ``state-matcher`` + ``composite_context_key`` (см.
``docs/E2E_ISOLATION.md``): каждый тест/запуск генерирует уникальный ``correlation_key``
(UUID в ``Message-ID``), поэтому стабы, контексты и сообщения **не пересекаются** между
тестами и между запусками. Стале сообщения из прошлых запусков тихо обрабатываются
существующими стабами/контекстами и не создают unmatched. Тяжёлая чистка
(Maildir/notmuch/LightRAG/GreenMail) — только при провижининге стека
(``wipe_bake.py`` / ``wipe_sync.py``), не при каждом запуске pytest.
Дальше журнал **не** чистится автоматически — при любых
несматченных записях падает guard в :func:`pytest_runtest_call` до и после **тела** каждого
``tests/e2e/test_*.py`` (глобально по инстансу; без локов на сам хук и без повторного
``DELETE /__admin/requests``; межпроцессный ``FileLock`` для WireMock Admin API —
:func:`~tests.e2e.wiremock_client._wiremock_admin_api_exclusive`, в т.ч. вокруг
:func:`~tests.e2e.wiremock_client.wiremock_unmatched_request_entries` для ``GET …/unmatched``).
При истинном параллельном прогоне на общем WireMock каждый HTTP должен матчиться стабами (State +
``X-Threlium-Route`` и узкие ``matches``), иначе журнал unmatched перестаёт быть пустым и любой воркер
упадёт на assert — так и задумано.

"""
from __future__ import annotations

import json
import os
from collections.abc import Generator
from dataclasses import replace
import subprocess
import sys
import time
from pathlib import Path

import pytest
from filelock import FileLock

from threlium.logutil import setup_logging, shutdown_logging

from tests.e2e.log import clip_log_body, log

try:
    import testcontainers.compose  # noqa: F401, PLC0415
    from testcontainers.compose import DockerCompose  # noqa: PLC0415
except ImportError:
    DockerCompose = None  # type: ignore[misc, assignment]

from .helpers import (
    E2E_PROJECT,
    E2E_SUT_IMAGE_ENV,
    TIMEOUT_POLL_SHORT,
    E2EComposeRuntime,
    compose_down_project,
    discover_live_e2e_project_name,
    discover_runtime,
    dump_failure_artifacts,
    e2e_compose_coord_paths,
    e2e_controller_hint_cleanup,
    e2e_controller_hint_read,
    e2e_controller_hint_write,
    e2e_flush_greenmail_inboxes,
    e2e_flush_sut_fsm_maildirs,
    e2e_install_deterministic_knowledge_corpus,
    e2e_shared_compose_stack_is_healthy,
    e2e_start_threlium_user_pipeline_services,
    e2e_stop_threlium_user_pipeline_services,
    resolve_e2e_sut_image,
    run_greenmail_host_readiness_probe,
    wait_for_sut_threlium_user_workers_idle,
    wait_for_wiremock_ready,
)
from .wiremock_client import (
    assert_wiremock_unmatched_journal_empty,
    assert_wiremock_zero_unmatched_requests,
    reset_non_bootstrap_wiremock_mappings,
    reset_request_journal,
    upsert_wiremock_compose_bootstrap_stubs,
    wiremock_public_base,
    wiremock_state_reset_all_contexts,
)

_ACTIVE_E2E_PROJECT: str | None = None

E2E_COMPOSE_DOWN_ENV = "THRELIUM_E2E_COMPOSE_DOWN"

# Оставить SUT-pipeline (engine + bridges) поднятым после ``pytest_sessionfinish`` вместо штатной
# остановки. Ставит ``wipe_sync.py``: после harness ``refresh`` (чистка Maildir/notmuch/lightrag +
# рестарт user-units на уровне playbook) стек должен остаться чистым и работающим для последующих
# прогонов тестов (которые делают attach-only + session cold reset, но сам стек не поднимают).
E2E_LEAVE_STACK_RUNNING_ENV = "THRELIUM_E2E_LEAVE_STACK_RUNNING"
# После FAIL тела теста sessionfinish всё равно ждёт idle work@/sweep@ и unmatched;
# без укороченного лимита выглядит как «зависание» (до 120 с). См. run_individual_e2e.sh.
E2E_SESSIONFINISH_DRAIN_SEC_ENV = "THRELIUM_E2E_SESSIONFINISH_DRAIN_SEC"
E2E_SESSIONFINISH_FAIL_DRAIN_SEC_ENV = "THRELIUM_E2E_SESSIONFINISH_FAIL_DRAIN_SEC"

_E2E_TESTS_ROOT = Path(__file__).resolve().parent

_THRELIUM_E2E_WM_JOURNAL_RESET_STASH = pytest.StashKey[bool]()
_THRELIUM_E2E_SESSION_TMP_DIR = pytest.StashKey[Path]()


def pytest_configure(config: pytest.Config) -> None:
    # e2e harness: INFO по умолчанию (DEBUG — только явно через THRELIUM_LOG_LEVEL).
    setup_logging(os.environ.get("THRELIUM_LOG_LEVEL", "INFO"))


def _sessionfinish_drain_sec(exitstatus: int) -> float:
    """Лимит ожидания idle work@/sweep@ и unmatched в ``pytest_sessionfinish``."""
    try:
        default = float(os.environ.get(E2E_SESSIONFINISH_DRAIN_SEC_ENV, "120"))
    except ValueError:
        default = 120.0
    if exitstatus == 0:
        return default
    try:
        fail_cap = float(os.environ.get(E2E_SESSIONFINISH_FAIL_DRAIN_SEC_ENV, "30"))
    except ValueError:
        fail_cap = 30.0
    return min(default, fail_cap)


def pytest_unconfigure(config: pytest.Config) -> None:
    shutdown_logging()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Auto-mark ``tests/e2e/test_*.py``; флаг sessionfinish для e2e harness."""
    will = False
    for it in items:
        p = getattr(it, "path", None)
        if p is None:
            continue
        try:
            Path(p).resolve().relative_to(_E2E_TESTS_ROOT)
        except ValueError:
            continue
        will = True
        if _e2e_py_file_is_scenario_module(Path(p)) and not it.get_closest_marker("e2e"):
            it.add_marker(pytest.mark.e2e)
    config._threlium_e2e_will_run = will  # type: ignore[attr-defined]


def _e2e_wiremock_journal_reset_once(
    session: pytest.Session,
    session_tmp: Path,
) -> None:
    """Один раз за **инвокацию pytest** (все xdist-процессы): session cold reset SUT + WireMock.

    Под IPC-``FileLock`` лидер: stop user pipeline → полный flush Maildir/notmuch/LightRAG
    и GreenMail → ``reset_request_journal`` + ``wiremock_state_reset_all_contexts`` +
    ``reset_non_bootstrap_wiremock_mappings`` → bootstrap stubs → start pipeline → idle →
    повторный сброс журнала. Между тестами pipeline **не** перезапускается; per-test
    очистка «своих» прошлых писем — :func:`~tests.e2e.helpers.e2e_clean_sut_messages_for_test`
    из :func:`~tests.e2e.wiremock_client.prepare_wiremock_scenario` (см. ``docs/E2E_ISOLATION.md`` §7).

  Изоляция WireMock между тестами: ``state-matcher`` + ``composite_context_key``
    (уникальный ``correlation_key`` на запуск). В конце сессии pipeline **не** останавливается
    (post-mortem); ``pytest_sessionfinish`` ждёт idle и проверяет пустой unmatched.

    *session_tmp* уникален на каждый запуск pytest (xdist: ``getbasetemp().parent``).
    Маркер ``e2e_wm_journal_reset.done`` — только внутри одной инвокации pytest.
    """
    if session.config.stash.get(_THRELIUM_E2E_WM_JOURNAL_RESET_STASH, False):
        return

    pn = discover_live_e2e_project_name()
    if not pn or not e2e_shared_compose_stack_is_healthy(pn):
        log.info("journal_reset_skip", project_name=pn)
        return

    marker = session_tmp / "e2e_wm_journal_reset.done"
    ipc_lock = session_tmp / "e2e_wm_journal_reset.lock"
    log.debug(
        "journal_reset_marker",
        marker_exists=marker.is_file(),
        session_tmp=str(session_tmp),
    )

    with FileLock(str(ipc_lock)):
        if marker.is_file():
            session.config.stash[_THRELIUM_E2E_WM_JOURNAL_RESET_STASH] = True
            return
        try:
            rt = discover_runtime(pn)
            e2e_stop_threlium_user_pipeline_services(rt)
            e2e_flush_sut_fsm_maildirs(rt)
            e2e_flush_greenmail_inboxes(rt)
            wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            reset_request_journal(wm)
            wiremock_state_reset_all_contexts(wm)
            reset_non_bootstrap_wiremock_mappings(wm)
            upsert_wiremock_compose_bootstrap_stubs(wm)
            e2e_start_threlium_user_pipeline_services(rt)
            wait_for_sut_threlium_user_workers_idle(pn, timeout=120.0)
            reset_request_journal(wm)
        except Exception as e:
            log.warning(
                "journal_reset_skipped",
                error=repr(e),
                detail="will retry after compose if needed",
            )
            return
        try:
            marker.touch()
        except OSError:  # pragma: no cover
            pass

    session.config.stash[_THRELIUM_E2E_WM_JOURNAL_RESET_STASH] = True
    log.info("journal_reset_done")


def _e2e_py_file_is_scenario_module(path: Path) -> bool:
    parts = path.parts
    return (
        len(parts) >= 3
        and parts[-2] == "e2e"
        and parts[-3] == "tests"
        and path.name.startswith("test_")
        and path.suffix == ".py"
    )


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_call(item: pytest.Item) -> Generator[None, None, None]:
    """Глобально пустой журнал ``GET /__admin/requests/unmatched`` до и после тела сценарного теста.

    Выполняется после setup фикстур (в т.ч. ``prepare_wiremock_scenario``) и до teardown. Сам хук не оборачивают
    локами; опрос Admin ``GET …/unmatched`` идёт через
    :func:`~tests.e2e.wiremock_client.wiremock_unmatched_request_entries`, где ``GET`` сериализуется
    :func:`~tests.e2e.wiremock_client._wiremock_admin_api_exclusive` (иначе 500 WM при ``pytest -n N``).
    При ``pytest -n N`` параллельные воркеры делят один WireMock — инвариант «unmatched пуст» держится
    за счёт изоляции стабов (State, корреляторы), а не за счёт сериализации всего хука. Любой unmatched —
    жёсткий fail на любом воркере; повторная полная очистка WM (журнал, маппинги, глобальный State)
    кроме :func:`_e2e_wiremock_journal_reset_once` запрещена.
    """
    path = getattr(item, "path", None)
    if path is None:
        yield
        return
    p = Path(path)
    if not _e2e_py_file_is_scenario_module(p):
        yield
        return

    pn = _ACTIVE_E2E_PROJECT
    if not pn:
        yield
        return

    try:
        rt = discover_runtime(pn)
    except Exception:
        yield
        return

    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    nid = item.nodeid
    assert_wiremock_unmatched_journal_empty(wm, phase=f"{nid} (before test body)")
    yield
    assert_wiremock_unmatched_journal_empty(wm, phase=f"{nid} (after test body)")


_RUNTIME_JSON_GREENMAIL_PROBE_MID_KEY = "greenmail_readiness_probe_inner_mid"


def _persist_greenmail_probe_mid(runtime_json: Path, probe_mid: str) -> None:
    """Дописать inner Message-ID readiness-письма в ``runtime.json`` (диагностика / аудит)."""
    data = json.loads(runtime_json.read_text(encoding="utf-8"))
    data[_RUNTIME_JSON_GREENMAIL_PROBE_MID_KEY] = probe_mid
    runtime_json.write_text(json.dumps(data), encoding="utf-8")


def _compose_prereq_failure_message() -> str | None:
    """Если e2e недоступен, вернуть текст ошибки; иначе None."""
    if sys.platform != "linux":
        return "e2e harness is Linux-only"
    if DockerCompose is None:
        return "install extras: pip install -e '.[e2e]'"
    try:
        subprocess.run(
            ["docker", "info"],
            check=True,
            capture_output=True,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
    except (FileNotFoundError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return "docker not available (need a running Docker daemon)"
    return None


# Bootstrap-модуль: ``e2e_runtime`` только discover (reindex — helper из тела теста).
_E2E_BOOTSTRAP_MODULE = "test_knowledge_bootstrap_live_e2e.py"


def _e2e_request_is_bootstrap_module(request: pytest.FixtureRequest) -> bool:
    p = getattr(request.node, "path", None)
    return p is not None and Path(p).name == _E2E_BOOTSTRAP_MODULE


@pytest.fixture(scope="function")
def e2e_runtime(
    compose_stack: E2EComposeRuntime,
    request: pytest.FixtureRequest,
) -> Generator[E2EComposeRuntime, None, None]:
    """Per-test runtime: mailflow prep (pipeline + drain) или тихий discover для bootstrap-модуля."""
    global _ACTIVE_E2E_PROJECT
    project_name = compose_stack.project_name
    _ACTIVE_E2E_PROJECT = project_name
    os.environ["COMPOSE_PROJECT_NAME"] = project_name

    if _e2e_request_is_bootstrap_module(request):
        rt = discover_runtime(project_name)
        e2e_install_deterministic_knowledge_corpus(rt)
        yield rt
        return

    rt = discover_runtime(project_name)
    yield rt


@pytest.fixture(autouse=True, scope="function")
def _e2e_autouse_runtime(request: pytest.FixtureRequest) -> None:
    """Для ``tests/e2e/test_*.py``: session ``compose_stack`` + per-test ``e2e_runtime``."""
    p = getattr(request.node, "path", None)
    if p is None:
        return
    if not _e2e_py_file_is_scenario_module(Path(p)):
        return
    request.getfixturevalue("e2e_runtime")


@pytest.fixture(scope="session")
def compose_stack(request: pytest.FixtureRequest, tmp_path_factory: pytest.TempPathFactory) -> E2EComposeRuntime:
    """Shared compose stack: leader/follower via filelock, single project for all xdist workers."""
    msg = _compose_prereq_failure_message()
    if msg:
        pytest.fail(
            "e2e prerequisites not met: "
            f"{msg}. "
            "E2E harness requires Linux + running Docker daemon + extras `[e2e]` "
            "(pip install -e '.[e2e]'). Missing prerequisites are a hard failure: "
            "all e2e tests depending on `compose_stack` will be reported as ERROR, "
            "not SKIPPED. Fix the environment (or deselect e2e tests explicitly) "
            "instead of expecting them to silently pass.",
            pytrace=False,
        )

    lock_path, ready_flag, runtime_json = e2e_compose_coord_paths()

    setup_started = time.monotonic()
    log.info("compose_stack_setup_start")

    greenmail_probe_inner_mid: str | None = None

    with FileLock(str(lock_path)):
        if ready_flag.exists() and runtime_json.exists():
            try:
                _coord = json.loads(runtime_json.read_text())
                _pn = _coord["project_name"]
                if not isinstance(_pn, str) or not _pn:
                    raise ValueError("missing project_name")
                if not e2e_shared_compose_stack_is_healthy(_pn):
                    raise ValueError("compose stack not healthy")
            except (json.JSONDecodeError, OSError, KeyError, TypeError, ValueError):
                log.warning("compose_coordinator_stale", detail="resetting shared flags")
                ready_flag.unlink(missing_ok=True)
                runtime_json.unlink(missing_ok=True)

        if ready_flag.exists():
            # Follower: stack already up, read shared state
            data = json.loads(runtime_json.read_text())
            project_name: str = data["project_name"]
            sut_fresh_bake: bool = data.get("sut_fresh_bake", False)
            _is_leader = False
            log.info(
                "compose_follower_reuse",
                project_name=project_name,
                elapsed_sec=round(time.monotonic() - setup_started, 1),
            )
        else:
            # Attach-only: тесты работают ТОЛЬКО на уже поднятом shared-стеке и никогда не
            # пекут образ и не поднимают compose сами. Подъём + bake — это wipe_bake.py;
            # синхронизация репозитория/среды без запекания — wipe_sync.py (см. docs/FSTS_SYNC.md).
            _is_leader = False
            live_pn = discover_live_e2e_project_name()
            if not (live_pn and e2e_shared_compose_stack_is_healthy(live_pn)):
                pytest.fail(
                    "No healthy e2e stack is up. Tests run only on an already-running shared "
                    "stack and never bake/bring it up themselves. Prepare the environment first:\n"
                    "  pytest -n0 tests/e2e/wipe_bake.py   # bake SUT image + compose up\n"
                    "  pytest -n0 tests/e2e/wipe_sync.py   # sync repo/env onto a running stack\n"
                    "then re-run the tests.",
                    pytrace=False,
                )
            log.info(
                "compose_leader_attach",
                project_name=live_pn,
                elapsed_sec=round(time.monotonic() - setup_started, 1),
            )
            project_name = live_pn
            sut_fresh_bake = False
            os.environ[E2E_SUT_IMAGE_ENV] = resolve_e2e_sut_image()
            os.environ["COMPOSE_PROJECT_NAME"] = project_name
            runtime_json.write_text(
                json.dumps({"project_name": project_name, "sut_fresh_bake": sut_fresh_bake})
            )
            ready_flag.touch()
            _is_leader = True
            try:
                greenmail_probe_inner_mid = _leader_post_up(project_name)
            except Exception:
                _leader_rollback(project_name, runtime_json, ready_flag)
                raise
            _persist_greenmail_probe_mid(runtime_json, greenmail_probe_inner_mid)

    # Common path for leader and followers.
    # If anything below fails for the leader, we still need to roll back
    # coordination files so followers don't enter an inconsistent state.
    try:
        os.environ["COMPOSE_PROJECT_NAME"] = project_name
        global _ACTIVE_E2E_PROJECT
        _ACTIVE_E2E_PROJECT = project_name

        runtime0 = discover_runtime(project_name)
        wait_for_wiremock_ready(project_name, timeout=TIMEOUT_POLL_SHORT)
        log.info(
            "wiremock_ready",
            url=f"http://{runtime0.wiremock_host}:{runtime0.wiremock_port}",
        )

        # Полный pre-run reset (DELETE всех маппингов, журнал, State, Maildir, bootstrap, рестарт engine).
        # session_tmp уникален на каждый запуск pytest — стухшие маркеры от прерванных сессий невозможны.
        # xdist workers: getbasetemp() = .../pytest-NNN/worker-gwM/ → .parent = .../pytest-NNN/ (unique).
        # Non-xdist:     getbasetemp() = .../pytest-NNN/           → use directly (unique).
        if hasattr(request.session.config, "workerinput"):
            session_tmp = tmp_path_factory.getbasetemp().parent
        else:
            session_tmp = tmp_path_factory.getbasetemp()
        request.session.config.stash[_THRELIUM_E2E_SESSION_TMP_DIR] = session_tmp
        _e2e_wiremock_journal_reset_once(request.session, session_tmp)

        runtime = replace(runtime0, sut_fresh_bake=sut_fresh_bake)

        e2e_controller_hint_write(project_name, runtime_json_path=runtime_json)
    except Exception:
        if _is_leader:
            _leader_rollback(project_name, runtime_json, ready_flag)
        raise

    yield runtime
    # No compose down: stack stays up for reuse (policy).


def _leader_post_up(project_name: str) -> str:
    """Post-compose-up readiness checks run by the leader under FileLock.

    WireMock Admin и общие стабы (включая ``POST /embeddings`` с ``state-matcher``)
    поднимаются **до** SMTP readiness-письма. Readiness-письмо уходит на отдельный ящик
    ``pytest@localhost`` (не ``test@`` агента): проверяется только SMTP→IMAP доставка, без
    fetchmail/SUT и без сида State под probe.

    Возвращает inner ``Message-ID`` проверочного письма (пишется в ``runtime.json``).
    """
    wait_for_wiremock_ready(project_name, timeout=TIMEOUT_POLL_SHORT)
    rt = discover_runtime(project_name)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    upsert_wiremock_compose_bootstrap_stubs(wm_base)
    return run_greenmail_host_readiness_probe(project_name)


def _leader_rollback(
    project_name: str,
    runtime_json: Path,
    ready_flag: Path,
) -> None:
    """Снять только координаторы; compose **не** гасим — остаётся для docker exec / логов.

    Следующий прогон: preflight :func:`~tests.e2e.helpers.stop_compose_projects_for_e2e_compose_dir`
    уберёт контейнеры из каталога compose (включая этот проект).
    """
    log.warning("compose_leader_rollback", project_name=project_name)
    for p in (ready_flag, runtime_json):
        try:
            p.unlink(missing_ok=True)
        except OSError:
            pass


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Diagnostics on failure; compose NOT down by default (reuse policy).

    Workers dump failure artifacts. Controller handles opt-in compose down
    (after all workers finish) via ``THRELIUM_E2E_COMPOSE_DOWN=1``.
    """
    is_controller = not hasattr(session.config, "workerinput")

    if not is_controller:
        # Worker: dump artifacts on failure
        if exitstatus != 0:
            project_name = _ACTIVE_E2E_PROJECT
            if project_name:
                try:
                    log.debug(
                        "failure_artifacts",
                        phase="sessionfinish_worker",
                        body=clip_log_body(dump_failure_artifacts(project_name)),
                    )
                except Exception as e:  # pragma: no cover
                    log.warning("sessionfinish_diagnostics_failed", error=repr(e))
        return

    # Controller: runs after all workers finish
    project_name = e2e_controller_hint_read()

    if exitstatus != 0 and project_name:
        try:
            log.debug(
                "failure_artifacts",
                phase="sessionfinish_controller",
                body=clip_log_body(dump_failure_artifacts(project_name)),
            )
        except Exception as e:  # pragma: no cover
            log.warning("sessionfinish_diagnostics_failed", error=repr(e))

    if os.environ.get(E2E_COMPOSE_DOWN_ENV, "").strip().lower() in ("1", "true", "yes", "on"):
        if project_name:
            log.info("compose_down_requested", project_name=project_name, env=E2E_COMPOSE_DOWN_ENV)
            try:
                compose_down_project(project_name)
            except Exception as e:  # pragma: no cover
                log.warning("compose_down_failed", error=repr(e))
    elif getattr(session.config, "_threlium_e2e_will_run", False) and not getattr(
        session.config.option, "collectonly", False
    ):
        pn_fin = _ACTIVE_E2E_PROJECT or project_name or discover_live_e2e_project_name()
        if pn_fin and e2e_shared_compose_stack_is_healthy(pn_fin):
            try:
                rt = discover_runtime(pn_fin)
                wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
                drain_sec = _sessionfinish_drain_sec(exitstatus)
                if exitstatus != 0:
                    log.info(
                        "sessionfinish_fail_fast_drain",
                        drain_sec=drain_sec,
                        exitstatus=exitstatus,
                    )
                wait_for_sut_threlium_user_workers_idle(pn_fin, timeout=drain_sec)
                assert_wiremock_zero_unmatched_requests(wm, wait_timeout_sec=drain_sec)
                # Изоляцию обеспечивает state-matcher + composite_context_key
                # (docs/E2E_ISOLATION.md). После прогона WireMock-журнал, State и
                # pipeline НЕ трогаем — контексты нужны для обработки стале сообщений
                # и для пост-mortem отладки.
                log.info("wiremock_sessionfinish_left_running")
            except Exception as e:  # pragma: no cover
                log.warning("wiremock_sessionfinish_reset_skipped", error=repr(e))

    e2e_controller_hint_cleanup()
