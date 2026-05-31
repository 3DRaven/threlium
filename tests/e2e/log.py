"""E2e harness logging — тот же structlog-контракт, что в threlium.logutil."""
from __future__ import annotations

from threlium.logutil import logger

# setup_logging — только в conftest.py::pytest_configure (не при import: иначе root=DEBUG
# до pytest и шум urllib3/docker/httpcore ломает collect-only / run_individual_e2e.sh).

log = logger.bind(stage="e2e")

E2E_LOG_BODY_MAX = 8000


def clip_log_body(text: str, *, max_len: int = E2E_LOG_BODY_MAX) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."
