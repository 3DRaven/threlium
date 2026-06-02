"""Thin WireMock journal asserts for mailflow."""
from __future__ import annotations

from pathlib import Path

from .bridges.email import notmuch_id_search_term
from .constants import E2E_WIREMOCK_CONTAINER_PORT, REPO_ROOT, TIMEOUT_POLL_SHORT
from .diag import mailflow_pipeline_diag
from .poll import poll_until_backoff
from .runtime import _mapped_port

def wait_for_wiremock_global_unmatched_zero(
    project_name: str,
    *,
    timeout: float = TIMEOUT_POLL_SHORT,
) -> None:
    """``GET /__admin/requests/unmatched`` пуст (глобально по инстансу)."""
    from tests.e2e.wiremock_client import wiremock_public_base, wiremock_unmatched_requests_count

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = wiremock_public_base(wm_host, wm_port)

    def _probe() -> bool | None:
        try:
            if wiremock_unmatched_requests_count(public_base) == 0:
                return True
        except Exception:
            return None
        return None

    poll_until_backoff(
        _probe,
        timeout=timeout,
        desc="wiremock: zero global unmatched requests",
    )


def assert_wiremock_mailflow_received_chat_completion_posts(
    project_name: str,
    *,
    stub_tag: str,
    anchor_message_id: str = "e2e-inbound@localhost",
    repo_root: Path | None = None,
    min_posts: int = 1,
) -> None:
    """Проверка журнала WireMock: POST ``/chat/completions`` с ``stub_tag`` и якорем в теле/headers.

    Источник истины — Admin API ``GET /__admin/requests`` (записи с ``metadata.threlium_e2e_stub_tag``).
    ``anchor_message_id`` — canonical thread-root MID (``X-Threlium-Thread-Root`` у запроса к LiteLLM
    и сид State), см. :func:`e2e_thread_root_mid_for_message_id`.
    ``diag_callback`` перед ошибкой — :func:`mailflow_pipeline_diag`.
    """
    from tests.e2e.wiremock_client import assert_wiremock_stub_received_min_chat_completions

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_stub_received_min_chat_completions(
        public_base,
        stub_tag=stub_tag,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )


def assert_wiremock_mailflow_zero_unmatched(
    project_name: str,
    *,
    anchor_message_id: str,
    correlation_route_wire: str | None = None,
    repo_root: Path | None = None,
) -> None:
    """Журнал ``GET /__admin/requests/unmatched`` пуст (с опросом до ``TIMEOUT_POLL_SHORT``).

    Нормативно — **глобально** по инстансу (``correlation_route_wire`` не передаётся); параметр
    оставлен для совместимости и особых случаев (например узкий фильтр при отладке ``pytest -n>1``).
    """
    from tests.e2e.wiremock_client import assert_wiremock_zero_unmatched_requests

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_zero_unmatched_requests(
        public_base,
        diag_callback=_diag,
        x_threlium_route_wire=correlation_route_wire,
    )


def assert_wiremock_mailflow_min_embedding_posts(
    project_name: str,
    *,
    anchor_message_id: str,
    min_posts: int,
    repo_root: Path | None = None,
) -> None:
    """≥ ``min_posts`` успешных POST ``/embeddings`` с якорем ``X-Threlium-Thread-Root`` (полный журнал)."""
    from tests.e2e.wiremock_client import assert_wiremock_min_embedding_posts_matching_anchor

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_min_embedding_posts_matching_anchor(
        public_base,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )


def assert_wiremock_mailflow_min_rerank_posts(
    project_name: str,
    *,
    anchor_message_id: str,
    min_posts: int,
    repo_root: Path | None = None,
) -> None:
    """>=``min_posts`` POST ``/rerank`` (200) with anchor ``X-Threlium-Thread-Root``."""
    from tests.e2e.wiremock_client import assert_wiremock_min_rerank_posts_matching_anchor

    wm_host, wm_port = _mapped_port(project_name, "wiremock", E2E_WIREMOCK_CONTAINER_PORT)
    public_base = f"http://{wm_host}:{wm_port}"

    def _diag() -> None:
        mailflow_pipeline_diag(project_name, anchor_message_id=anchor_message_id, repo_root=repo_root)

    assert_wiremock_min_rerank_posts_matching_anchor(
        public_base,
        anchor_needle=anchor_message_id,
        min_posts=min_posts,
        diag_callback=_diag,
    )
