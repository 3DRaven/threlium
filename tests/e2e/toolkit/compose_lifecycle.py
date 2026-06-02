"""Compose project discovery, bake, teardown."""
from __future__ import annotations

import contextlib
import fcntl
import os
import subprocess
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

from tests.e2e.log import log

from .constants import (
    COMPOSE_DIR,
    E2E_AUTO_BAKE_IF_MISSING_ENV,
    E2E_BAKE_SCRIPT,
    E2E_BAKED_SUT_IMAGE,
    E2E_COMPOSE_FILE,
    E2E_DEFAULT_SUT_IMAGE,
    E2E_PROJECT,
    E2E_WIREMOCK_CONTAINER_PORT,
    E2E_REBUILD_BAKED_IMAGE_ENV,
    E2E_SHARED_COMPOSE_SERVICES,
    E2E_SUT_IMAGE_ENV,
    REPO_ROOT,
    TIMEOUT_ANSIBLE_PLAYBOOK,
    TIMEOUT_POLL_SHORT,
)
from .poll import _diag, poll_until
from .runtime import (
    E2EComposeRuntime,
    _compose_container,
    _compose_project_containers,
    _docker_client,
    _mapped_port,
    discover_runtime,
)

def resolve_e2e_sut_image() -> str:
    """Тег образа `sut` для compose: явный THRELIUM_E2E_SUT_IMAGE или предсобранный по умолчанию."""
    return os.environ.get(E2E_SUT_IMAGE_ENV, E2E_BAKED_SUT_IMAGE).strip()


def e2e_rebuild_baked_image_requested() -> bool:
    """Принудительный полный bake перед тестами (THRELIUM_E2E_REBUILD_BAKED_IMAGE)."""
    raw = os.environ.get(E2E_REBUILD_BAKED_IMAGE_ENV)
    if raw is None or not str(raw).strip():
        return False
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


def _e2e_auto_bake_if_missing() -> bool:
    raw = os.environ.get(E2E_AUTO_BAKE_IF_MISSING_ENV)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() not in ("0", "false", "no", "off")


@contextlib.contextmanager
def _e2e_bake_image_lock() -> Iterator[None]:
    lock_path = Path(tempfile.gettempdir()) / "threlium_e2e_bake_image.lock"
    fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _docker_image_exists_locally(image: str) -> bool:
    try:
        r = subprocess.run(
            ["docker", "image", "inspect", image],
            check=False,
            capture_output=True,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _run_bake_for_e2e_sut_image(image_tag: str) -> None:
    env = os.environ.copy()
    env["THRELIUM_E2E_BAKE_IMAGE"] = image_tag
    subprocess.run(
        [str(E2E_BAKE_SCRIPT)],
        check=True,
        cwd=str(REPO_ROOT),
        env=env,
    )


def ensure_e2e_sut_image_exists(*, force_rebuild: bool = False) -> tuple[str, bool]:
    """Гарантирует наличие локального образа SUT для e2e; при необходимости запускает bake.

    Returns ``(image_tag, did_bake)`` — *did_bake* is ``True`` when this call actually
    executed the bake script (forced or auto), ``False`` when the image already existed.
    """
    image = resolve_e2e_sut_image()
    if force_rebuild:
        with _e2e_bake_image_lock():
            _diag(
                f"SUT image rebuild ({E2E_REBUILD_BAKED_IMAGE_ENV}=1 or wipe_bake): {image} "
                f"(fresh upstream + full site.yml + docker commit)"
            )
            _run_bake_for_e2e_sut_image(image)
        return image, True
    if _docker_image_exists_locally(image):
        return image, False
    if image != E2E_BAKED_SUT_IMAGE:
        return image, False
    if not _e2e_auto_bake_if_missing():
        raise RuntimeError(
            f"e2e SUT image {image!r} not found locally. Either run "
            f"`pytest -n0 tests/e2e/wipe_bake.py` (or set {E2E_REBUILD_BAKED_IMAGE_ENV}=1 in CI) "
            f"to bake it from upstream, run {E2E_BAKE_SCRIPT} manually with "
            f"THRELIUM_E2E_BAKE_IMAGE={image}, or allow auto-bake "
            f"(unset {E2E_AUTO_BAKE_IF_MISSING_ENV} or set to 1)."
        )
    with _e2e_bake_image_lock():
        if _docker_image_exists_locally(image):
            return image, False
        _diag(
            f"SUT image {image!r} missing; running bake ({E2E_AUTO_BAKE_IF_MISSING_ENV} defaults to enabled)"
        )
        _run_bake_for_e2e_sut_image(image)
    return image, True


def e2e_shared_compose_stack_is_healthy(project_name: str) -> bool:
    """True, если для ``project_name`` все сервисы из ``E2E_SHARED_COMPOSE_SERVICES`` имеют running-контейнер."""
    try:
        containers = _compose_project_containers(project_name)
    except Exception:
        return False
    by_service: dict[str, list[Any]] = {}
    for c in containers:
        labels = c.labels or {}
        svc = labels.get("com.docker.compose.service") or ""
        if not svc:
            continue
        by_service.setdefault(svc, []).append(c)
    for required in E2E_SHARED_COMPOSE_SERVICES:
        running = [c for c in by_service.get(required, []) if getattr(c, "status", None) == "running"]
        if not running:
            return False
    return True


def discover_live_e2e_project_name() -> str | None:
    """Имя уже поднятого e2e compose-проекта **без** фикстуры ``compose_stack`` / bake.

    Используется сценариями «только проверки на живом стеке» (см. ``test_mailflow_live_only_e2e``).

    Первый *healthy* проект среди *running* контейнеров ``service=sut``, чей
    ``com.docker.compose.project`` начинается с ``{E2E_PROJECT}_`` (лексикографически первый).

    Политика: один shared-стек после ``wipe_bake`` / ``compose_stack``.

    ``None`` — Docker недоступен или нет ни одного healthy стека с нужным префиксом.
    """
    try:
        client = _docker_client()
        containers = client.containers.list(filters={"status": "running"})
    except Exception:
        return None
    candidates: set[str] = set()
    prefix = f"{E2E_PROJECT}_"
    for c in containers:
        labels = c.labels or {}
        if labels.get("com.docker.compose.service") != "sut":
            continue
        pn = labels.get("com.docker.compose.project") or ""
        if isinstance(pn, str) and pn.startswith(prefix):
            candidates.add(pn)
    for pn in sorted(candidates):
        if e2e_shared_compose_stack_is_healthy(pn):
            return pn
    return None


def discover_compose_projects_for_e2e_compose_dir() -> list[str]:
    """Уникальные ``com.docker.compose.project`` для контейнеров из каталога ``COMPOSE_DIR``.

    Совпадение по ``com.docker.compose.project.working_dir`` (Compose v2) или по
    ``com.docker.compose.project.config_files``, если рабочая директория в лейблах пуста.
    Так снимаются стеки с любым ``docker compose -p`` (включая ``threlium_dbg``,
    ``threlium_e2e_bake``), а не только ``{E2E_PROJECT}_*``.
    """
    compose_dir = COMPOSE_DIR.resolve()
    compose_file = E2E_COMPOSE_FILE.resolve()
    client = _docker_client()
    try:
        containers = client.containers.list(
            all=True,
            filters={"label": ["com.docker.compose.project"]},
        )
    except Exception:
        return []
    projects: set[str] = set()
    for c in containers:
        labels = c.labels or {}
        pn = (labels.get("com.docker.compose.project") or "").strip()
        if not pn:
            continue
        wd_raw = (labels.get("com.docker.compose.project.working_dir") or "").strip()
        if wd_raw:
            try:
                if Path(wd_raw).resolve() == compose_dir:
                    projects.add(pn)
                    continue
            except OSError:
                pass
        cfg = (labels.get("com.docker.compose.project.config_files") or "").strip()
        if not cfg:
            continue
        for part in (p.strip() for p in cfg.split(",") if p.strip()):
            try:
                if Path(part).resolve() == compose_file:
                    projects.add(pn)
                    break
            except OSError:
                tail = "tests/e2e/compose/docker-compose.yml"
                if part.replace("\\", "/").endswith(tail):
                    projects.add(pn)
                    break
    return sorted(projects)


def discover_stale_compose_projects(*, project_prefix: str = E2E_PROJECT) -> list[str]:
    """Совместимость API: *project_prefix* игнорируется.

    См. :func:`discover_compose_projects_for_e2e_compose_dir`.
    """
    _ = project_prefix
    return discover_compose_projects_for_e2e_compose_dir()


def stop_compose_projects_for_e2e_compose_dir(
    *, timeout: int = int(TIMEOUT_POLL_SHORT)
) -> list[str]:
    """``docker compose down`` для всех проектов из ``COMPOSE_DIR`` (любой ``-p``)."""
    stale_projects = discover_compose_projects_for_e2e_compose_dir()
    if not stale_projects:
        return []

    cleaned: list[str] = []
    warnings: list[str] = []
    for project_name in stale_projects:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(E2E_COMPOSE_FILE),
                "-p",
                project_name,
                "down",
                "--remove-orphans",
                "--volumes",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(COMPOSE_DIR),
        )
        remaining = _compose_project_containers(project_name)
        if remaining:
            names = ", ".join(sorted(c.name for c in remaining))
            raise RuntimeError(
                "failed to cleanup stale e2e compose project "
                f"{project_name!r}; remaining containers: {names}\n"
                f"docker compose down exit={result.returncode}\n"
                f"stdout:\n{result.stdout}\n"
                f"stderr:\n{result.stderr}"
            )
        if result.returncode != 0:
            warnings.append(
                f"cleanup warning for {project_name!r}: "
                f"docker compose down exit={result.returncode}"
            )
        cleaned.append(project_name)

    for warning in warnings:
        log.warning("compose_cleanup_warning", detail=warning)
    return cleaned


def stop_stale_compose_projects(
    *, project_prefix: str = E2E_PROJECT, timeout: int = int(TIMEOUT_POLL_SHORT)
) -> list[str]:
    """Останавливает все compose-проекты из каталога ``COMPOSE_DIR`` до нового прогона.

    Имя сохранено по истории; *project_prefix* игнорируется — см.
    :func:`stop_compose_projects_for_e2e_compose_dir`.
    """
    _ = project_prefix
    return stop_compose_projects_for_e2e_compose_dir(timeout=timeout)


def compose_down_project(project_name: str, *, timeout: int = int(TIMEOUT_POLL_SHORT)) -> None:
    """``docker compose down --remove-orphans --volumes`` for a single project."""
    subprocess.run(
        [
            "docker", "compose",
            "-f", str(E2E_COMPOSE_FILE),
            "-p", project_name,
            "down", "--remove-orphans", "--volumes",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(COMPOSE_DIR),
    )


def cleanup_stale_bundle_archives(*, artifacts_root: Path | None = None) -> int:
    """Удаляет старые post-deploy bundle архивы из ansible/artifacts."""
    root = artifacts_root or (REPO_ROOT / "ansible" / "artifacts")
    if not root.exists():
        _diag(f"bundle cleanup skipped: {root} does not exist")
        return 0

    removed = 0
    for archive_path in root.rglob("threlium-bundle-*.tar.gz"):
        if not archive_path.is_file():
            continue
        try:
            archive_path.unlink()
            removed += 1
        except OSError as e:
            _diag(f"bundle cleanup warning: failed to remove {archive_path}: {e!r}")

    _diag(f"bundle cleanup done: removed={removed} root={root}")
    return removed


def wait_for_wiremock_ready(project_name: str, *, timeout: float = TIMEOUT_POLL_SHORT) -> tuple[str, int]:
    """Ждём, пока WireMock Admin API отвечает на ``GET /__admin/mappings``."""
    host, port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    import urllib.error
    import urllib.request

    def _probe() -> tuple[str, int] | None:
        try:
            with urllib.request.urlopen(
                f"http://{host}:{port}/__admin/mappings",
                timeout=float(TIMEOUT_POLL_SHORT),
            ) as r:
                if r.status == 200:
                    return (host, port)
        except (urllib.error.URLError, OSError, TimeoutError):
            return None
        return None

    return poll_until(_probe, timeout=timeout, desc=f"wiremock admin ready http://{host}:{port}")
