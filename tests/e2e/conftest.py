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

**Политика teardown:** ``pytest_sessionfinish`` **не** вызывает ``compose down`` —
стек остаётся поднятым для повторных прогонов и ручной отладки. Явный
``compose down`` — по opt-in env ``THRELIUM_E2E_COMPOSE_DOWN=1``.
При ошибке лидера после ``compose up`` rollback **не** делает ``compose down`` —
контейнеры остаются для расследования; следующий прогон: preflight
``stop_stale_compose_projects`` снимает все проекты из ``tests/e2e/compose``
(любой ``docker compose -p``, включая ``threlium_dbg``, ``threlium_e2e_bake``).
Если остановлен текущий
проект из ``runtime.json``, лидер сбрасывает координаторы и поднимает новый
стек (``e2e_shared_compose_stack_is_healthy``).

Лидер при отсутствии валидных координаторов сначала пытается **присоединиться**
к уже healthy стеку (:func:`~tests.e2e.helpers.discover_live_e2e_project_name`).
При успешном attach **не** выполняется полный preflight ``compose down`` по каталогу
compose — сохраняется уже поднятый shared-проект (например после ``wipe_bake``).
Если attach невозможен, preflight
:func:`~tests.e2e.helpers.stop_compose_projects_for_e2e_compose_dir` снимает все
проекты из ``tests/e2e/compose`` (любой ``-p``, включая ``threlium_dbg``), затем
лидер поднимает новый стек.

Prereq — **hard failure, не skip**. Отсутствие Linux / Docker daemon / extras ``[e2e]``
даёт ``pytest.fail`` — см. ``docs/TESTING.md`` §4.2.

**Журнал unmatched** (``GET /__admin/requests/unmatched``): один раз за инвокацию pytest
(при ``-n N`` — один раз на все воркеры через маркер в session-unique ``tmp_path_factory`` dir)
выполняется «холодный старт»: остановка user-scope pipeline на SUT,
:func:`~tests.e2e.helpers.e2e_sut_threlium_user_journal_rotate_and_vacuum` (ротация + vacuum user journald),
``DELETE /__admin/mappings`` и ``DELETE /__admin/requests``, сброс Store State Extension, очистка Maildir
(:func:`~tests.e2e.helpers.e2e_flush_sut_fsm_maildirs`), upsert только ``compose_bootstrap/`` и снова
запуск engine. Триггер — :func:`_e2e_wiremock_journal_reset_once` при первом разрешении session-фикстуры
``compose_stack`` (после ``wait_for_wiremock_ready``). ``tmp_path_factory.getbasetemp().parent`` уникален
для каждого запуска pytest (``/tmp/pytest-of-user/pytest-42/`` → ``pytest-43/``), поэтому маркер от
прерванной/упавшей предыдущей сессии не может «протухнуть» и заблокировать cold reset.
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
import secrets
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
    COMPOSE_DIR,
    E2E_AUTO_BAKE_IF_MISSING_ENV,
    E2E_COMPOSE_FILE_NAME,
    E2E_PROJECT,
    E2E_REBUILD_BAKED_IMAGE_ENV,
    E2E_SUT_IMAGE_ENV,
    TIMEOUT_POLL_SHORT,
    E2EComposeRuntime,
    cleanup_stale_bundle_archives,
    compose_down_project,
    discover_live_e2e_project_name,
    discover_runtime,
    dump_failure_artifacts,
    e2e_compose_coord_paths,
    e2e_controller_hint_cleanup,
    e2e_controller_hint_read,
    e2e_controller_hint_write,
    e2e_flush_sut_fsm_maildirs,
    e2e_flush_greenmail_inboxes,
    e2e_sut_threlium_user_journal_rotate_and_vacuum,
    e2e_start_threlium_user_pipeline_services,
    e2e_stop_threlium_user_pipeline_services,
    e2e_rebuild_baked_image_requested,
    e2e_shared_compose_stack_is_healthy,
    ensure_e2e_sut_image_exists,
    run_greenmail_host_readiness_probe,
    stop_stale_compose_projects,
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

_E2E_TESTS_ROOT = Path(__file__).resolve().parent

_THRELIUM_E2E_WM_JOURNAL_RESET_STASH = pytest.StashKey[bool]()
_THRELIUM_E2E_SESSION_TMP_DIR = pytest.StashKey[Path]()


def pytest_configure(config: pytest.Config) -> None:
    setup_logging(os.environ.get("THRELIUM_LOG_LEVEL", "DEBUG"))


def pytest_unconfigure(config: pytest.Config) -> None:
    shutdown_logging()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Флаг: в прогон попали тесты из ``tests/e2e`` (для session-start WM и sessionfinish State)."""
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
        break
    config._threlium_e2e_will_run = will  # type: ignore[attr-defined]


def _e2e_wiremock_journal_reset_once(
    session: pytest.Session,
    session_tmp: Path,
) -> None:
    """Один раз за **инвокацию pytest** (все xdist-процессы): холодный старт SUT + WireMock.

    *session_tmp* уникален на каждый запуск pytest:

    - **xdist worker:** ``getbasetemp().parent`` = ``/tmp/pytest-of-user/pytest-42/``
      (``getbasetemp()`` = ``…/pytest-42/worker-gwN/``).
    - **non-xdist:** ``getbasetemp()`` = ``/tmp/pytest-of-user/pytest-42/`` напрямую.

    Маркер ``e2e_wm_cold_reset.done`` в этой директории физически не может
    существовать от предыдущей сессии — новый запуск получает ``pytest-43/``.
    Если предыдущая сессия была прервана (Ctrl+C, OOM), стухший маркер остаётся
    в старой директории и не мешает новому запуску.

    При ``pytest -n N`` xdist-воркеры разделяют один ``session_tmp``
    (``pytest-42/``), поэтому первый воркер, захвативший ``FileLock``, выполняет
    cold reset и создаёт маркер; остальные видят маркер и пропускают.

    Порядок: остановка user-scope pipeline на SUT →
    :func:`~tests.e2e.helpers.e2e_sut_threlium_user_journal_rotate_and_vacuum` (ротация + vacuum user journal) →
    :func:`~tests.e2e.wiremock_client.reset_non_bootstrap_wiremock_mappings` (bootstrap стабы остаются) →
    ``DELETE /__admin/requests`` → :func:`~tests.e2e.wiremock_client.wiremock_state_reset_all_contexts` →
    flush Maildir → flush GreenMail IMAP inboxes (EXPUNGE ``test@`` + ``pytest@``) →
    :func:`~tests.e2e.wiremock_client.upsert_wiremock_compose_bootstrap_stubs` (idempotent) →
    запуск engine + bridges → assert unmatched пуст.
    """
    if session.config.stash.get(_THRELIUM_E2E_WM_JOURNAL_RESET_STASH, False):
        return

    pn = discover_live_e2e_project_name()
    if not pn or not e2e_shared_compose_stack_is_healthy(pn):
        log.info("cold_reset_skip", project_name=pn)
        return

    marker = session_tmp / "e2e_wm_cold_reset.done"
    ipc_lock = session_tmp / "e2e_wm_cold_reset.lock"
    log.debug(
        "cold_reset_marker",
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
            e2e_sut_threlium_user_journal_rotate_and_vacuum(rt)
            wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
            reset_non_bootstrap_wiremock_mappings(wm)
            reset_request_journal(wm)
            wiremock_state_reset_all_contexts(wm)
            e2e_flush_sut_fsm_maildirs(rt)
            e2e_flush_greenmail_inboxes(rt)
            upsert_wiremock_compose_bootstrap_stubs(wm)
        except Exception as e:
            log.warning(
                "cold_reset_skipped",
                error=repr(e),
                detail="will retry after compose if needed",
            )
            return
        e2e_start_threlium_user_pipeline_services(rt)
        # GreenMail inboxes expunged above; bridges should start clean.
        # Wait for any residual workers to settle, then reset WM journal.
        try:
            wait_for_sut_threlium_user_workers_idle(
                rt.project_name, timeout=TIMEOUT_POLL_SHORT,
            )
        except Exception as e:
            # Часто n>0 (остаются threlium-work@* / sweep) при гонке с xdist; до 30 с backoff
            # без успеха — не блокируем cold reset, но фиксируем в логе (см. journald / list-units на SUT).
            log.warning("cold_reset_workers_idle_skipped", error=repr(e))
        reset_request_journal(wm)
        assert_wiremock_unmatched_journal_empty(
            wm,
            phase="e2e pre-run after cold reset (mappings+journal+state+maildir+bootstrap+engine)",
        )
        try:
            marker.touch()
        except OSError:  # pragma: no cover
            pass

    session.config.stash[_THRELIUM_E2E_WM_JOURNAL_RESET_STASH] = True
    log.info("cold_reset_done")


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


# Модули только на уже поднятом стеке (``e2e_live``): без session ``compose_stack`` / bake.
# Подготовка: ``wipe_bake`` / compose up; синхронизация кода — ``wipe_sync.py`` (``--tags refresh``).
_E2E_COMPOSE_AUTOUSE_SKIP_MODULES = frozenset({
    "test_knowledge_bootstrap_live_e2e.py",
    "test_logic_validate_chain_e2e.py",
    "test_mailflow_live_only_e2e.py",
    "test_greenmail_delivery_e2e.py",
    "test_matrix_wiremock_live_e2e.py",
    "test_telegram_wiremock_live_e2e.py",
})


@pytest.fixture(scope="module")
def live_e2e_project_name() -> str:
    """Имя healthy compose-проекта; skip, если стека нет (без ``compose_stack`` / Ansible)."""
    pn = discover_live_e2e_project_name()
    if not pn:
        pytest.skip(
            "No live e2e stack: start compose (wipe_bake / shared stack). "
            "Sync code to SUT: pytest -n0 tests/e2e/wipe_sync.py"
        )
    if not e2e_shared_compose_stack_is_healthy(pn):
        pytest.skip(f"Live e2e stack not healthy: {pn!r}")
    return pn


@pytest.fixture(scope="module")
def live_e2e_stack_ready(live_e2e_project_name: str) -> str:
    """Live stack с запущенными engine + bridges (после ``sessionfinish`` они могут быть остановлены)."""
    global _ACTIVE_E2E_PROJECT
    _ACTIVE_E2E_PROJECT = live_e2e_project_name
    os.environ["COMPOSE_PROJECT_NAME"] = live_e2e_project_name
    rt = discover_runtime(live_e2e_project_name)
    wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    reset_request_journal(wm)
    e2e_start_threlium_user_pipeline_services(rt)
    wait_for_sut_threlium_user_workers_idle(live_e2e_project_name, timeout=120.0)
    reset_request_journal(wm)
    return live_e2e_project_name


@pytest.fixture(autouse=True, scope="function")
def _e2e_autouse_compose_stack(request: pytest.FixtureRequest) -> None:
    """Для сценарных модулей ``tests/e2e/test_*.py`` поднимает/присоединяет shared compose один раз на сессию."""
    p = getattr(request.node, "path", None)
    if p is None:
        return
    path = Path(p)
    parts = path.parts
    if len(parts) < 3 or parts[-2] != "e2e" or parts[-3] != "tests":
        return
    if not (path.name.startswith("test_") and path.suffix == ".py"):
        return
    if path.name in _E2E_COMPOSE_AUTOUSE_SKIP_MODULES:
        return
    request.getfixturevalue("compose_stack")


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

    force_rebuild = e2e_rebuild_baked_image_requested()

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
            # Leader: attach to live healthy stack, or preflight + bring up
            _is_leader = False
            live_pn = discover_live_e2e_project_name()
            if live_pn and e2e_shared_compose_stack_is_healthy(live_pn):
                log.info(
                    "compose_leader_attach",
                    project_name=live_pn,
                    elapsed_sec=round(time.monotonic() - setup_started, 1),
                )
                project_name = live_pn
                sut_fresh_bake = False
                sut_image, _ = ensure_e2e_sut_image_exists(force_rebuild=False)
                os.environ[E2E_SUT_IMAGE_ENV] = sut_image
                log.info("sut_image_resolved", image=sut_image, fresh_bake=False)
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
            else:
                cleaned = stop_stale_compose_projects(project_prefix=E2E_PROJECT)
                if cleaned:
                    log.info("preflight_cleanup_projects", removed=",".join(cleaned))
                removed_bundles = cleanup_stale_bundle_archives()
                if removed_bundles:
                    log.info("preflight_cleanup_bundles", removed=removed_bundles)

                sut_image, sut_fresh_bake = ensure_e2e_sut_image_exists(force_rebuild=force_rebuild)
                os.environ[E2E_SUT_IMAGE_ENV] = sut_image
                log.info(
                    "sut_image_resolved",
                    image=sut_image,
                    fresh_bake=sut_fresh_bake,
                    force_rebuild=force_rebuild,
                )

                project_name = f"{E2E_PROJECT}_shared_{secrets.token_hex(3)}"
                os.environ["COMPOSE_PROJECT_NAME"] = project_name

                log.info("compose_leader_up_start", project_name=project_name)
                dc = DockerCompose(
                    str(COMPOSE_DIR),
                    compose_file_name=E2E_COMPOSE_FILE_NAME,
                    pull=False,
                    build=False,
                )
                try:
                    dc.start()
                except Exception:
                    # Стек не снимаем — отладка; следующий pytest preflight уберёт проект.
                    raise

                log.info(
                    "compose_leader_up_done",
                    project_name=project_name,
                    elapsed_sec=round(time.monotonic() - setup_started, 1),
                )

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


@pytest.fixture(scope="module")
def deployed_stack(compose_stack: E2EComposeRuntime) -> str:
    """Project name after compose_stack is healthy and WireMock poll passed."""
    return compose_stack.project_name


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
    elif getattr(session.config, "_threlium_e2e_will_run", False):
        pn_fin = _ACTIVE_E2E_PROJECT or project_name or discover_live_e2e_project_name()
        if pn_fin and e2e_shared_compose_stack_is_healthy(pn_fin):
            try:
                rt = discover_runtime(pn_fin)
                wm = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
                drain_sec = float(os.environ.get("THRELIUM_E2E_SESSIONFINISH_DRAIN_SEC", "120"))
                wait_for_sut_threlium_user_workers_idle(pn_fin, timeout=drain_sec)
                assert_wiremock_zero_unmatched_requests(wm, wait_timeout_sec=drain_sec)
                e2e_stop_threlium_user_pipeline_services(rt)
                wiremock_state_reset_all_contexts(wm)
                log.info("wiremock_sessionfinish_reset_done")
            except Exception as e:  # pragma: no cover
                log.warning("wiremock_sessionfinish_reset_skipped", error=repr(e))

    e2e_controller_hint_cleanup()
