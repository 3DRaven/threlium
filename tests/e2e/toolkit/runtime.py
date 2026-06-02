"""Docker compose runtime: exec, ports, E2EComposeRuntime."""
from __future__ import annotations

import subprocess
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import docker  # type: ignore[import-not-found]

from .constants import (
    E2E_WIREMOCK_CONTAINER_PORT,
    REPO_ROOT,
    TIMEOUT_POLL_SHORT,
)
from .poll import _diag


@dataclass
class E2EComposeRuntime:
    """Нормализованный runtime-контекст e2e-стека, поднятого через Testcontainers."""

    project_name: str
    repo_root: Path
    greenmail_smtp_host: str
    greenmail_smtp_port: int
    greenmail_imap_host: str
    greenmail_imap_port: int
    wiremock_host: str
    wiremock_port: int
    sut_fresh_bake: bool = False


def _docker_client() -> Any:
    return docker.from_env()


def _compose_container(project_name: str, service: str) -> Any:
    client = _docker_client()
    containers = client.containers.list(
        all=True,
        filters={
            "label": [
                f"com.docker.compose.project={project_name}",
                f"com.docker.compose.service={service}",
            ]
        },
    )
    if not containers:
        raise RuntimeError(f"container not found for compose service={service!r}, project={project_name!r}")
    running = [c for c in containers if c.status == "running"]
    return running[0] if running else containers[0]


def _compose_project_containers(project_name: str) -> list[Any]:
    client = _docker_client()
    return client.containers.list(
        all=True,
        filters={"label": [f"com.docker.compose.project={project_name}"]},
    )


def _mapped_port(project_name: str, service: str, container_port: int) -> tuple[str, int]:
    c = _compose_container(project_name, service)
    c.reload()
    key = f"{container_port}/tcp"
    binding = (c.attrs.get("NetworkSettings", {}).get("Ports", {}) or {}).get(key)
    if not binding:
        raise RuntimeError(
            f"port mapping not found for service={service!r}, port={container_port}, project={project_name!r}"
        )
    host_ip = binding[0].get("HostIp") or "127.0.0.1"
    if host_ip in ("0.0.0.0", "::"):
        host_ip = "127.0.0.1"
    return host_ip, int(binding[0]["HostPort"])


def discover_runtime(project_name: str, *, repo_root: Path | None = None) -> E2EComposeRuntime:
    smtp_host, smtp_port = _mapped_port(project_name, "greenmail", 3025)
    imap_host, imap_port = _mapped_port(project_name, "greenmail", 3143)
    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    return E2EComposeRuntime(
        project_name=project_name,
        repo_root=repo_root or REPO_ROOT,
        greenmail_smtp_host=smtp_host,
        greenmail_smtp_port=smtp_port,
        greenmail_imap_host=imap_host,
        greenmail_imap_port=imap_port,
        wiremock_host=wm_host,
        wiremock_port=wm_port,
    )


def service_exec(
    project_name: str,
    service: str,
    argv: list[str],
    *,
    repo_root: Path | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    del repo_root, timeout
    _diag(f"exec start: service={service} argv={argv[:3]}...")
    container = _compose_container(project_name, service)
    result = container.exec_run(cmd=argv, stdout=True, stderr=True, tty=False, demux=False)
    output = result.output or b""
    if isinstance(output, bytes):
        text_out = output.decode("utf-8", errors="replace")
    else:
        text_out = str(output)
    completed = subprocess.CompletedProcess(args=argv, returncode=int(result.exit_code), stdout=text_out, stderr="")
    _diag(f"exec done: service={service} rc={completed.returncode}")
    return completed


def compose_logs(project_name: str, *, repo_root: Path | None = None) -> str:
    del repo_root
    client = _docker_client()
    containers = client.containers.list(
        all=True,
        filters={"label": [f"com.docker.compose.project={project_name}"]},
    )
    if not containers:
        return f"(no containers found for compose project {project_name})\n"
    parts: list[str] = []
    for c in sorted(containers, key=lambda it: it.name):
        parts.append(f"--- {c.name} ({c.status}) ---\n")
        try:
            parts.append(c.logs(stdout=True, stderr=True, tail=500).decode("utf-8", errors="replace"))
        except Exception as e:  # pragma: no cover
            parts.append(f"(failed to fetch logs: {e!r})\n")
    return "".join(parts)


def tcp_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=float(TIMEOUT_POLL_SHORT)):
            return True
    except OSError:
        return False
