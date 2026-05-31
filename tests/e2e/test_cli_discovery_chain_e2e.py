"""E2E: ``cli_intent`` shell chain (``sh -c`` + pipe in ``grep -E``) → sandbox ``cli_exec`` → finalize.

Стабы: ``wiremock_stubs/test_cli_discovery_chain_e2e/`` (``stub-cli-discovery-chain-01``).
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.log import clip_log_body, log
from threlium.types import FsmStage

from .helpers import (
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
    find_wiremock_requests_by_body_contains,
    wiremock_public_base,
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
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.CLI_INTENT.value,
        FsmStage.CLI_EXEC.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-cli-discovery-chain-verified",
)


def _assert_cli_stdout_in_reasoning_journal(project: str, stub_tag: str) -> None:
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    matches = find_wiremock_requests_by_body_contains(
        wm_base, E2E_CLI_DISCOVERY_STDOUT, stub_tag=stub_tag
    )
    chat = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert chat, (
        f"expected cli stdout marker {E2E_CLI_DISCOVERY_STDOUT!r} in reasoning journal"
    )
    log.info("cli_discovery_stdout_verified", hits=len(chat))


@pytest.fixture()
def cli_discovery_chain_processed_stack(deployed_stack: str) -> object:
    _ensure_discovery_marker_file(deployed_stack)
    with mailflow_inject_and_wait(CLI_DISCOVERY_CHAIN_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_cli_discovery_chain_full_pipeline(
    cli_discovery_chain_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """``sh -c 'rg … && echo …'`` → cli_exec → reasoning sees stdout marker → finalize."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        cli_discovery_chain_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            CLI_DISCOVERY_CHAIN_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        _assert_cli_stdout_in_reasoning_journal(project, stub_tag)
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
    expect_notmuch_stage_folders=(
        FsmStage.INGRESS.value,
        FsmStage.ENRICH.value,
        FsmStage.REASONING.value,
        FsmStage.CLI_INTENT.value,
        FsmStage.ENRICH_FAST.value,
        FsmStage.TASKS_UPSERT.value,
        FsmStage.RESPONSE_FINALIZE.value,
        FsmStage.EGRESS_ROUTER.value,
        FsmStage.EGRESS_EMAIL.value,
        FsmStage.ARCHIVE.value,
    ),
    reply_body_needle="e2e-cli-route-collision-verified",
)


def _assert_route_collision_observation_in_journal(project: str, stub_tag: str) -> None:
    rt = discover_runtime(project, repo_root=REPO_ROOT)
    wm_base = wiremock_public_base(rt.wiremock_host, rt.wiremock_port)
    matches = find_wiremock_requests_by_body_contains(
        wm_base, E2E_ROUTE_COLLISION_OBSERVATION, stub_tag=stub_tag
    )
    chat = [
        e
        for e in matches
        if "/chat/completions" in (e.get("request", {}).get("url") or "")
    ]
    assert chat, (
        f"expected route-collision observation {E2E_ROUTE_COLLISION_OBSERVATION!r} "
        "in reasoning journal after enrich_fast relay"
    )
    log.info("cli_route_collision_observation_verified", hits=len(chat))


@pytest.fixture()
def cli_route_collision_processed_stack(deployed_stack: str) -> object:
    with mailflow_inject_and_wait(CLI_ROUTE_COLLISION_SPEC, deployed_stack) as ids:
        yield ids


@pytest.mark.e2e
@pytest.mark.e2e_live
@pytest.mark.mailflow
def test_cli_route_collision_enrich_fast_not_cli_exec(
    cli_route_collision_processed_stack: tuple[str, str, str, str, str, str],
) -> None:
    """``argv[0]=memory_query`` → ``CliRouteCollision`` → enrich_fast observation, not cli_exec."""
    project, raw_id, _canonical_id, nm_inner, stub_tag, correlation_key = (
        cli_route_collision_processed_stack
    )
    try:
        assert_full_mailflow_pipeline(
            CLI_ROUTE_COLLISION_SPEC,
            project=project,
            raw_id=raw_id,
            nm_inner=nm_inner,
            stub_tag=stub_tag,
            correlation_key=correlation_key,
        )
        _assert_route_collision_observation_in_journal(project, stub_tag)
    except Exception:
        log.debug(
            "failure_artifacts",
            body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
        )
        raise
