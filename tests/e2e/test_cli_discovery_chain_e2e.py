"""E2E: ``cli_intent`` shell chain (``sh -c`` + pipe in ``grep -E``) → sandbox ``cli_exec`` → finalize.

Стабы: ``wiremock_stubs/test_cli_discovery_chain_e2e/`` (``stub-cli-discovery-chain-01``).
"""
from __future__ import annotations

from pathlib import Path


from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2E_REMOTE_REPO_PATH,
    MailflowScenarioSpec,
    REPO_ROOT,
    assert_full_mailflow_pipeline,
    discover_runtime,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    service_exec,
)
from .wiremock_client import (
    wiremock_public_base,
    wiremock_state_thread_root_property,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs"
E2E_CLI_DISCOVERY_CHAIN_BODY = "E2E-CLI-DISCOVERY-CHAIN-BODY"
E2E_CLI_DISCOVERY_MARKER = "E2E-CLI-DISCOVERY-MARKER"
E2E_CLI_DISCOVERY_STDOUT = "E2E-CLI-DISCOVERY-STDOUT-MARKER"
_MARKER_FILE = "e2e-cli-discovery-marker.txt"


def _ensure_discovery_marker_file(project: str) -> None:
    path = f"{E2E_REMOTE_REPO_PATH}/{_MARKER_FILE}"
    cmd = [
        "bash",
        "-lc",
        f"printf '%s\\n' '{E2E_CLI_DISCOVERY_MARKER}' > {path}",
    ]
    r = service_exec(project, "sut", cmd, repo_root=REPO_ROOT, timeout=30)
    assert r.returncode == 0, f"failed to create discovery marker file: {(r.stderr or r.stdout)!r}"


CLI_DISCOVERY_CHAIN_SPEC = MailflowScenarioSpec(
    label="cli_discovery_chain",
    raw_id_prefix="e2e-cli-disc-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_cli_discovery_chain_e2e",
    stub_tag="stub-cli-discovery-chain-01",
    body_head=f"{E2E_CLI_DISCOVERY_CHAIN_BODY}\ne2e cli discovery chain test",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    reply_body_needle="e2e-cli-discovery-chain-verified",
    wiremock_journal_ready_needle="call_threlium_e2e_egress_after_allow",
)


def _assert_cli_stdout_in_reasoning_state(
    project: str, correlation_key: str
) -> None:
    """cli_exec stdout must reach reasoning via enrich_fast relay (LLM prompt) — по STATE.

    «На диске в ENRICH_FAST-папке» (notmuch docker-exec) избыточно; вместо journal-скана читаем
    content-flag ``saw_cli_stdout``, записанный на лету post-cli reasoning-стабом, когда stdout-маркер
    попал в его промпт — строго сильнее (дошёл до LLM, а не только лёг в папку), дёшево из state, без
    захода в контейнер и без скана журнала. Прямое чтение после барьера ответа GreenMail
    (``assert_full_mailflow_pipeline``) — time-independent. См. §3.6.1 / §3.6.2."""
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    assert (
        wiremock_state_thread_root_property(wm_base, correlation_key, "saw_cli_stdout") == "1"
    ), f"cli_exec stdout marker {E2E_CLI_DISCOVERY_STDOUT!r} must reach reasoning (state saw_cli_stdout)"
    log.info("cli_discovery_stdout_verified", correlation_key=correlation_key)



def test_cli_discovery_chain_full_pipeline(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """``sh -c 'rg … && echo …'`` → cli_exec → reasoning sees stdout marker → finalize."""
    _ensure_discovery_marker_file(e2e_runtime.project_name)
    with mailflow_inject_and_wait(CLI_DISCOVERY_CHAIN_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                CLI_DISCOVERY_CHAIN_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_cli_stdout_in_reasoning_state(project, correlation_key)
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


E2E_CLI_ROUTE_COLLISION_BODY = "E2E-CLI-ROUTE-COLLISION-BODY"
E2E_ROUTE_COLLISION_OBSERVATION = "FSM reasoning route exposed as a separate tool"

CLI_ROUTE_COLLISION_SPEC = MailflowScenarioSpec(
    label="cli_route_collision",
    raw_id_prefix="e2e-cli-route-coll-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "test_cli_route_collision_e2e",
    stub_tag="stub-cli-route-collision-01",
    body_head=f"{E2E_CLI_ROUTE_COLLISION_BODY}\ne2e cli route collision test",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    min_rerank_posts=0,
    reply_body_needle="e2e-cli-route-collision-verified",
    # Multi-hop (cli_intent → enrich_fast ×2 → tasks → finalize): ранние LightRAG chat
    # не должны открывать окно GreenMail до стаба egress finalize.
    wiremock_journal_ready_needle="call_threlium_e2e_egress_after_allow",
)


def _assert_route_collision_observation_in_state(
    project: str, correlation_key: str
) -> None:
    # ENRICH_FAST-папка (notmuch docker-exec) избыточна; вместо journal-скана читаем content-flag
    # saw_route_collision_observation, записанный post-collision reasoning-стабом при попадании observation
    # в его промпт (сильнее, дёшево из state, без контейнера/журнала). §3.6.1 / §3.6.2.
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    assert (
        wiremock_state_thread_root_property(
            wm_base, correlation_key, "saw_route_collision_observation"
        )
        == "1"
    ), (
        f"route-collision observation {E2E_ROUTE_COLLISION_OBSERVATION!r} must reach reasoning "
        "(state saw_route_collision_observation)"
    )
    log.info("cli_route_collision_observation_verified", correlation_key=correlation_key)



def test_cli_route_collision_enrich_fast_not_cli_exec(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """``argv[0]=memory_query`` → ``CliRouteCollision`` → enrich_fast observation, not cli_exec."""
    with mailflow_inject_and_wait(CLI_ROUTE_COLLISION_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                CLI_ROUTE_COLLISION_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
            _assert_route_collision_observation_in_state(project, correlation_key)
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
