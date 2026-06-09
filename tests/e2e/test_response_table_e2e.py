"""E2E тесты RESPONSE_TABLE: response_edit / response_observe / response_finalize modes.

Покрывает сценарии, не вошедшие в test_response_buffer_e2e.py (который тестирует
только Mode 2: append×2 → finalize buffer-only).

Сценарии:
- finalize_mode3: buffer + inline content → egress
- observe: append → observe → finalize (buffer)
- edit_replace: append → edit(replace) → finalize
- edit_delete: append×2 → edit(delete) → finalize
- finalize_mode4: пустой finalize → enrich loop → recovery finalize
- edit_invalid_position: append → edit(position=999) → enrich → recovery finalize
"""
from __future__ import annotations

from pathlib import Path


from tests.e2e.log import clip_log_body, log

from .toolkit import (
    E2EComposeRuntime,
    E2EComposeRuntime,
    E2EComposeRuntime,
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    dump_failure_artifacts,
    mailflow_inject_and_wait,
    REPO_ROOT,
)

_WIREMOCK_STUBS_ROOT = Path(__file__).resolve().parent / "wiremock_stubs" / "test_response_table_e2e"

# ---------------------------------------------------------------------------
# Scenario: response_finalize Mode 3 (buffer + inline content)
# ---------------------------------------------------------------------------

E2E_FIN_MODE3_BODY_MARKER = "E2E-RESP-FIN-MODE3-MARKER"

FINALIZE_MODE3_SPEC = MailflowScenarioSpec(
    label="finalize_mode3",
    raw_id_prefix="e2e-fin-mode3-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "finalize_mode3",
    stub_tag="stub-resp-fin-mode3-01",
    body_head=f"{E2E_FIN_MODE3_BODY_MARKER}\ne2e response finalize mode3 test body",
    min_chat_completion_posts=3,
    min_embedding_posts=1,
    reply_body_needle="E2E-FIN-MODE3-BUFFER-PART",
)



def test_response_finalize_mode3_buffer_plus_content(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: response_append → response_finalize(Mode 3: buffer + inline content)."""
    with mailflow_inject_and_wait(FINALIZE_MODE3_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FINALIZE_MODE3_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


# ---------------------------------------------------------------------------
# Scenario: response_observe
# ---------------------------------------------------------------------------

E2E_OBSERVE_BODY_MARKER = "E2E-RESP-OBSERVE-MARKER"

OBSERVE_SPEC = MailflowScenarioSpec(
    label="observe",
    raw_id_prefix="e2e-observe-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "observe",
    stub_tag="stub-resp-observe-01",
    body_head=f"{E2E_OBSERVE_BODY_MARKER}\ne2e response observe test body",
    min_chat_completion_posts=4,
    min_embedding_posts=1,
    wiremock_journal_ready_needle="call_e2e_observe_finalize",
    reply_body_needle="E2E-OBSERVED-CHUNK",
)



def test_response_observe_cycle(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: response_append → response_observe → response_finalize (buffer)."""
    with mailflow_inject_and_wait(OBSERVE_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                OBSERVE_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


# ---------------------------------------------------------------------------
# Scenario: response_edit (replace)
# ---------------------------------------------------------------------------

E2E_EDIT_REPLACE_BODY_MARKER = "E2E-RESP-EDIT-REPLACE-MARKER"

EDIT_REPLACE_SPEC = MailflowScenarioSpec(
    label="edit_replace",
    raw_id_prefix="e2e-edit-repl-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "edit_replace",
    stub_tag="stub-resp-edit-replace-01",
    body_head=f"{E2E_EDIT_REPLACE_BODY_MARKER}\ne2e response edit replace test body",
    min_chat_completion_posts=4,
    min_embedding_posts=1,
    wiremock_journal_ready_needle="call_e2e_edit_replace_finalize",
    reply_body_needle="E2E-EDIT-REPLACED-TEXT",
)



def test_response_edit_replace_chunk(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: response_append → response_edit(replace) → response_finalize."""
    with mailflow_inject_and_wait(EDIT_REPLACE_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                EDIT_REPLACE_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


# ---------------------------------------------------------------------------
# Scenario: response_edit (delete)
# ---------------------------------------------------------------------------

E2E_EDIT_DELETE_BODY_MARKER = "E2E-RESP-EDIT-DELETE-MARKER"

EDIT_DELETE_SPEC = MailflowScenarioSpec(
    label="edit_delete",
    raw_id_prefix="e2e-edit-del-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "edit_delete",
    stub_tag="stub-resp-edit-delete-01",
    body_head=f"{E2E_EDIT_DELETE_BODY_MARKER}\ne2e response edit delete test body",
    min_chat_completion_posts=5,
    min_embedding_posts=1,
    wiremock_journal_ready_needle="call_e2e_edit_delete_finalize",
    reply_body_needle="E2E-CHUNK-KEEP",
)



def test_response_edit_delete_chunk(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: 2×response_append → response_edit(delete pos=1) → response_finalize."""
    with mailflow_inject_and_wait(EDIT_DELETE_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                EDIT_DELETE_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


# ---------------------------------------------------------------------------
# Scenario: response_finalize Mode 4 (empty → enrich recovery loop)
# ---------------------------------------------------------------------------

E2E_FIN_MODE4_BODY_MARKER = "E2E-RESP-FIN-MODE4-MARKER"

FINALIZE_MODE4_SPEC = MailflowScenarioSpec(
    label="finalize_mode4",
    raw_id_prefix="e2e-fin-mode4-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "finalize_mode4",
    stub_tag="stub-resp-fin-mode4-01",
    body_head=f"{E2E_FIN_MODE4_BODY_MARKER}\ne2e response finalize mode4 test body",
    min_chat_completion_posts=4,
    min_embedding_posts=1,
    reply_body_needle="E2E-MODE4-RECOVERY",
)



def test_response_finalize_mode4_ingress_recovery(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: response_finalize(Mode 4: empty) → enrich → recovery finalize."""
    with mailflow_inject_and_wait(FINALIZE_MODE4_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                FINALIZE_MODE4_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise


# ---------------------------------------------------------------------------
# Scenario: response_edit (invalid position → enrich recovery)
# ---------------------------------------------------------------------------

E2E_EDIT_INVALID_BODY_MARKER = "E2E-RESP-EDIT-INVALID-MARKER"

EDIT_INVALID_SPEC = MailflowScenarioSpec(
    label="edit_invalid_position",
    raw_id_prefix="e2e-edit-inv-",
    stub_dir=_WIREMOCK_STUBS_ROOT / "edit_invalid_position",
    stub_tag="stub-resp-edit-invalid-01",
    body_head=f"{E2E_EDIT_INVALID_BODY_MARKER}\ne2e response edit invalid position test body",
    min_chat_completion_posts=5,
    min_embedding_posts=1,
    wiremock_journal_ready_needle="call_e2e_edit_invalid_recovery",
    reply_body_needle="E2E-EDIT-INVALID-RECOVERY",
)



def test_response_edit_invalid_position_recovery(
    e2e_runtime: E2EComposeRuntime,
) -> None:
    """RESPONSE_TABLE: response_edit(position=999) → enrich (graceful) → recovery finalize."""
    with mailflow_inject_and_wait(EDIT_INVALID_SPEC, e2e_runtime.project_name) as (
        project,
        raw_id,
        _canonical_id,
        nm_inner,
        stub_tag,
        correlation_key,
    ):
        try:
            assert_full_mailflow_pipeline(
                EDIT_INVALID_SPEC,
                project=project,
                raw_id=raw_id,
                nm_inner=nm_inner,
                stub_tag=stub_tag,
                correlation_key=correlation_key,
            )
        except Exception:
            log.debug(
                "failure_artifacts",
                body=clip_log_body(dump_failure_artifacts(project, repo_root=REPO_ROOT)),
            )
            raise
