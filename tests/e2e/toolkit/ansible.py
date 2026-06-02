"""Ansible site playbook runner for e2e."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from tests.e2e.log import log
from tests.e2e.sut_user_systemd import (
    e2e_patch_hop_budget_in_threlium_yaml_bash,
    e2e_restart_threlium_engine_bash,
)

from .constants import (
    E2E_ANSIBLE_CONFIG_NAME,
    E2E_ANSIBLE_INVENTORY_PATH,
    E2E_REMOTE_REPO_PATH,
    E2E_REMOTE_THRELIUM_HOME,
    REPO_ROOT,
    TIMEOUT_ANSIBLE_PLAYBOOK,
    TIMEOUT_POLL_SHORT,
    _E2E_DEFAULT_HOP_BUDGET,
)
from .diag import dump_failure_artifacts
from .poll import _diag
from .runtime import _compose_container, service_exec

def ensure_e2e_ansible_collections(*, repo_root: Path | None = None) -> None:
    """Ставит Galaxy-коллекции для e2e.

    Нужны ``community.docker`` (inventory), ``community.general``
    (``archive`` в site.yml) и ``ansible.posix`` (``authorized_key`` при SSH-hardening).
    """
    root = repo_root or REPO_ROOT
    ansible_dir = root / "ansible"
    coll_install_root = ansible_dir / "collections"
    requirements = coll_install_root / "requirements.yml"
    ac = coll_install_root / "ansible_collections"
    marker_docker = ac / "community" / "docker" / "plugins" / "connection" / "docker.py"
    marker_general = ac / "community" / "general" / "plugins" / "modules" / "archive.py"
    marker_posix = ac / "ansible" / "posix" / "plugins" / "modules" / "authorized_key.py"
    if marker_docker.is_file() and marker_general.is_file() and marker_posix.is_file():
        return
    if not requirements.is_file():
        raise RuntimeError(
            f"e2e Ansible collections requirements missing: {requirements} "
            "(need community.docker + community.general + ansible.posix; see file contents)"
        )
    coll_install_root.mkdir(parents=True, exist_ok=True)
    if not shutil.which("ansible-galaxy"):
        raise RuntimeError("ansible-galaxy not on PATH (install ansible-core extras e2e)")
    t0 = time.monotonic()
    _diag("ansible-galaxy collection install (e2e) start")
    gal_env = {
        **os.environ,
        "ANSIBLE_CONFIG": str((ansible_dir / E2E_ANSIBLE_CONFIG_NAME).resolve()),
    }
    r = subprocess.run(
        [
            "ansible-galaxy",
            "collection",
            "install",
            "-r",
            str(requirements),
            "-p",
            str(coll_install_root),
            "--force",
        ],
        cwd=str(ansible_dir),
        check=False,
        text=True,
        timeout=int(TIMEOUT_POLL_SHORT),
        stdout=sys.stderr,
        stderr=sys.stderr,
        env=gal_env,
    )
    if r.returncode != 0:
        raise RuntimeError(
            "ansible-galaxy collection install failed (e2e needs collections from requirements.yml). "
            f"command: ansible-galaxy collection install -r {requirements} -p {coll_install_root}"
        )
    if not marker_docker.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but docker connection plugin missing: " + str(marker_docker)
        )
    if not marker_general.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but community.general.archive missing: " + str(marker_general)
        )
    if not marker_posix.is_file():
        raise RuntimeError(
            "ansible-galaxy succeeded but ansible.posix.authorized_key missing: " + str(marker_posix)
        )
    _diag(f"ansible-galaxy collection install (e2e) done (elapsed={time.monotonic() - t0:.1f}s)")


def run_e2e_site_playbook(
    project_name: str,
    *,
    checkout: str,
    repo_root: Path | None = None,
    ansible_tags: str | None = None,
    ansible_extra_vars: dict[str, Any] | None = None,
) -> None:
    """``site.yml`` в контейнер ``sut`` (e2e inventory).

    По умолчанию прогоняет полный плейбук: задачи ``deploy`` и ``deploy``+``refresh``; блоки с ``never``+``refresh`` (чистка harness) без явного ``--tags refresh`` не выполняются.
    При ``ansible_tags="refresh"`` (``wipe_sync``): цепочка файлов/env/шаблонов (``deploy``+``refresh`` в ``site.yml``, **без** ``pip``) + harness (``never``+``refresh``); без apt и без полного acceptance.

    * ``THRELIUM_E2E_ANSIBLE_TAGS``      — ``--tags`` (например ``refresh`` для sync кода/env/шаблонов + harness e2e);
    * ``THRELIUM_E2E_ANSIBLE_SKIP_TAGS`` — ``--skip-tags`` (например ``refresh`` при необходимости).

    Пустые / не заданные — полный ``site.yml`` без фильтрации по тегам.

    Явный аргумент ``ansible_tags`` переопределяет env.

    ``ansible_extra_vars`` — дополнительный JSON-файл ``-e @…`` **после**
    переменных инвентаря ``inventory/e2e/group_vars/threlium_hosts.yml`` (см. symlink на
    ``group_vars/e2e.yml``) и ``e2e_sut_container_id`` (перекрывает переменные для одного прогона).

    Вывод ``ansible-playbook`` наследует stdio pytest (без перенаправления всего в ``stderr``);
    при наличии ``stdbuf(1)`` — построчная буферизация. Уровень Ansible: env
    ``THRELIUM_E2E_ANSIBLE_VERBOSITY`` — ``0`` без ``-v``, ``1``…``4`` → ``-v``…``-vvvv``
    (по умолчанию ``1``, чтобы в ``pytest -s`` были видны ход задач между длинными ``apt``).
    """
    del checkout
    root = repo_root or REPO_ROOT
    started = time.monotonic()
    _diag("ansible deploy start")
    container_id = _compose_container(project_name, "sut").id
    if not container_id:
        raise RuntimeError(f"sut container id is empty for compose project {project_name!r}")
    cmd: list[str] = ["ansible-playbook"]
    verb_raw = os.environ.get("THRELIUM_E2E_ANSIBLE_VERBOSITY", "1").strip()
    if verb_raw and verb_raw != "0":
        try:
            vn = min(max(int(verb_raw), 1), 4)
        except ValueError:
            vn = 1
        cmd.append("-" + ("v" * vn))
    cmd.extend(
        [
            "playbooks/site.yml",
            "-i",
            E2E_ANSIBLE_INVENTORY_PATH,
            "-e",
            f"e2e_sut_container_id={container_id}",
        ]
    )
    extra_file: Path | None = None
    if ansible_extra_vars:
        tf = tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            prefix="ansible-e2e-extra-",
            delete=False,
            encoding="utf-8",
        )
        with tf:
            json.dump(ansible_extra_vars, tf)
        extra_file = Path(tf.name)
        cmd.extend(["-e", f"@{extra_file}"])
    if ansible_tags is not None:
        ansible_tags_val = ansible_tags.strip()
    else:
        ansible_tags_val = os.environ.get("THRELIUM_E2E_ANSIBLE_TAGS", "").strip()
    ansible_skip_tags = os.environ.get("THRELIUM_E2E_ANSIBLE_SKIP_TAGS", "").strip()
    if ansible_tags_val:
        cmd += ["--tags", ansible_tags_val]
    if ansible_skip_tags:
        cmd += ["--skip-tags", ansible_skip_tags]
    if not shutil.which("ansible-playbook"):
        raise RuntimeError(
            "ansible-playbook not found on host; "
            "install it in test environment (e.g. pip install ansible-core)."
        )
    ansible_dir = root / "ansible"
    e2e_cfg = (ansible_dir / E2E_ANSIBLE_CONFIG_NAME).resolve()
    if not e2e_cfg.is_file():
        raise RuntimeError(f"missing e2e ansible config: {e2e_cfg}")
    ensure_e2e_ansible_collections(repo_root=root)
    run_env = {
        **os.environ,
        "ANSIBLE_CONFIG": str(e2e_cfg),
        "PYTHONUNBUFFERED": "1",
    }
    exec_cmd = list(cmd)
    if shutil.which("stdbuf"):
        exec_cmd = ["stdbuf", "-oL", "-eL", *exec_cmd]
    try:
        r = subprocess.run(
            exec_cmd,
            check=False,
            text=True,
            timeout=TIMEOUT_ANSIBLE_PLAYBOOK,
            cwd=str(ansible_dir),
            env=run_env,
        )
    finally:
        if extra_file is not None:
            extra_file.unlink(missing_ok=True)
    if r.returncode != 0:
        raise RuntimeError(
            "ansible-playbook failed (see streamed logs above).\ncommand: "
            + " ".join(exec_cmd)
            + "\n"
            + dump_failure_artifacts(project_name, repo_root=root)
        )
    _diag(f"ansible deploy done (elapsed={time.monotonic() - started:.1f}s)")


def _e2e_apply_hop_budget_on_sut(
    project_name: str,
    *,
    budget_root: int,
    budget_sub: int,
    repo_root: Path | None = None,
) -> None:
    """Патч ``hop`` в ``threlium.yaml`` на SUT и перезапуск ``threlium-engine`` (без ansible)."""
    root = repo_root or REPO_ROOT
    patch = service_exec(
        project_name,
        "sut",
        [
            "bash",
            "-lc",
            e2e_patch_hop_budget_in_threlium_yaml_bash(
                budget_root=budget_root,
                budget_sub=budget_sub,
            ),
        ],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if patch.returncode != 0:
        raise RuntimeError(
            "e2e: failed to patch hop budget in threlium.yaml on SUT; "
            f"rc={patch.returncode} stdout={(patch.stdout or '')[-800:]!r}"
        )
    restart = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", e2e_restart_threlium_engine_bash()],
        repo_root=root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    if restart.returncode != 0:
        raise RuntimeError(
            "e2e: failed to restart threlium-engine after hop budget patch; "
            f"rc={restart.returncode} stdout={(restart.stdout or '')[-800:]!r}"
        )
    log.info(
        "sut_hop_budget_applied",
        budget_root=budget_root,
        budget_sub=budget_sub,
        patch_tail=(patch.stdout or "").strip()[-200:],
    )


def e2e_refresh_hop_budget_sub(
    project_name: str,
    *,
    budget_sub: int,
    repo_root: Path | None = None,
) -> None:
    """Установить ``hop.budget_sub`` на SUT и перезапустить engine (e2e live budget tests)."""
    hop = dict(_E2E_DEFAULT_HOP_BUDGET)
    hop["budget_sub"] = budget_sub
    _e2e_apply_hop_budget_on_sut(
        project_name,
        budget_root=int(hop["budget_root"]),
        budget_sub=int(hop["budget_sub"]),
        repo_root=repo_root,
    )


def e2e_refresh_hop_budget_default(
    project_name: str,
    *,
    repo_root: Path | None = None,
) -> None:
    """Вернуть e2e-дефолты ``hop`` (``budget_root/sub=256``) на SUT и перезапустить engine."""
    _e2e_apply_hop_budget_on_sut(
        project_name,
        budget_root=int(_E2E_DEFAULT_HOP_BUDGET["budget_root"]),
        budget_sub=int(_E2E_DEFAULT_HOP_BUDGET["budget_sub"]),
        repo_root=repo_root,
    )


def copy_repo_and_run_ansible(
    project_name: str,
    *,
    checkout: str,
    repo_root: Path | None = None,
) -> None:
    """Устаревшее имя; используйте ``run_e2e_site_playbook``."""
    run_e2e_site_playbook(project_name, checkout=checkout, repo_root=repo_root)
