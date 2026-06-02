"""SUT worker idle waits."""
from __future__ import annotations

from pathlib import Path

from tests.e2e.log import clip_log_body, log
from tests.e2e.sut_user_systemd import (
    e2e_sut_threlium_user_workers_idle_probe_bash,
    e2e_sut_threlium_user_workers_stall_diag_bash,
)

from .constants import REPO_ROOT, TIMEOUT_POLL_SHORT
from .poll import poll_until_backoff
from .runtime import service_exec

def _e2e_log_sut_workers_stall_diag(project_name: str, *, repo_root: Path, banner: str) -> None:
    """Снимок SUT при таймауте ``wait_for_sut_threlium_user_workers_idle`` (list-units + journal)."""
    r = service_exec(
        project_name,
        "sut",
        ["bash", "-lc", e2e_sut_threlium_user_workers_stall_diag_bash()],
        repo_root=repo_root,
        timeout=int(TIMEOUT_POLL_SHORT),
    )
    body = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
    cap = 25_000
    if len(body) > cap:
        body = body[:cap] + "\n… (truncated)"
    log.debug(
        "sut_workers_stall_diag",
        banner=banner,
        body=clip_log_body(body, max_len=cap),
    )


def wait_for_sut_threlium_user_workers_idle(
    project_name: str,
    *,
    repo_root: Path | None = None,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """Дождаться отсутствия активных user-unit ``threlium-work@*`` и ``threlium-sweep@*``.

    Нужно перед mailflow на живом SUT: иначе долетающие LiteLLM со старым ``X-Threlium-Route``
    дают unmatched после холодного прогона или до ``wiremock_state_reset_all_contexts`` в ``pytest_sessionfinish``.

    При ``TimeoutError`` в лог уходит :func:`~tests.e2e.sut_user_systemd.e2e_sut_threlium_user_workers_stall_diag_bash`.
    """
    root = repo_root or REPO_ROOT
    script = e2e_sut_threlium_user_workers_idle_probe_bash()

    def _probe() -> bool | None:
        r = service_exec(
            project_name,
            "sut",
            ["bash", "-lc", script],
            repo_root=root,
            timeout=int(TIMEOUT_POLL_SHORT),
        )
        if r.returncode != 0:
            return None
        try:
            line = (r.stdout or "").strip().splitlines()[-1]
            n = int(line)
        except (ValueError, IndexError):
            return None
        return True if n == 0 else None

    try:
        poll_until_backoff(
            _probe,
            timeout=timeout,
            desc="sut: threlium-work@ / threlium-sweep@ idle (user systemd)",
        )
    except TimeoutError as e:
        _e2e_log_sut_workers_stall_diag(
            project_name,
            repo_root=root,
            banner=f"sut workers idle TIMEOUT diag (timeout={timeout}s): {e}",
        )
        raise
