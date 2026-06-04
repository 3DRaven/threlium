"""E2e test toolkit — granular harness for e2e tests."""
from __future__ import annotations

from tests.e2e.sut_user_systemd import e2e_threlium_user_unit_journalctl_bash

from .ansible import (
    copy_repo_and_run_ansible,
    ensure_e2e_ansible_collections,
    e2e_refresh_hop_budget_default,
    e2e_refresh_hop_budget_sub,
    run_e2e_site_playbook,
)
from .bridges.email import (
    canonical_external_msgid,
    e2e_smtp_inject_ingress_route_wire,
    e2e_smtp_inject_ingress_route_wire_for_message_id,
    e2e_thread_root_mid_for_message_id,
    email_ingress_notmuch_id_inner,
    notmuch_id_search_term,
    rfc_first_message_id_in_in_reply_to_header,
)
from .bridges.matrix import (
    e2e_matrix_generate_room_ids,
    e2e_matrix_thread_root_mid_for_sync_event,
)
from .bridges.telegram import (
    e2e_telegram_generate_update_bundle,
    e2e_telegram_thread_root_mid_for_message,
)
from .cleanup import (
    e2e_clean_sut_messages_for_test,
    e2e_flush_greenmail_inboxes,
    e2e_flush_sut_fsm_maildirs,
)
from .compose_lifecycle import (
    cleanup_stale_bundle_archives,
    compose_down_project,
    discover_compose_projects_for_e2e_compose_dir,
    discover_live_e2e_project_name,
    discover_stale_compose_projects,
    ensure_e2e_sut_image_exists,
    e2e_shared_compose_stack_is_healthy,
    resolve_e2e_sut_image,
    stop_compose_projects_for_e2e_compose_dir,
    stop_stale_compose_projects,
    wait_for_wiremock_ready,
)
from .constants import *  # noqa: F403
from .coord import (
    e2e_compose_coord_dir,
    e2e_compose_coord_paths,
    e2e_controller_hint_cleanup,
    e2e_controller_hint_read,
    e2e_controller_hint_write,
)
from .diag import (
    dump_failure_artifacts,
    mailflow_fsm_maildir_systemd_snapshot,
    mailflow_pipeline_diag,
    mailflow_wait_fsm_maildir_activity,
    reset_maildrop_debug_log,
    reset_mda_pipeline_diag,
)
from .fixtures import (
    e2e_dense_threlium_ctx_body,
    e2e_oversized_context_trim_body,
    e2e_oversized_context_trim_current_turn_body,
    e2e_oversized_context_trim_prior_turn_body,
    e2e_summarize_overflow_inject_body,
)
from .greenmail import (
    assert_imap_inner_mid_in_folder,
    assert_imap_inner_mid_not_in_inbox,
    e2e_greenmail_mailbox_address,
    greenmail_wait_agent_reply_message_id,
    run_greenmail_host_readiness_probe,
    wait_for_greenmail_inbox_message_gone_host,
    wait_for_greenmail_inbox_message_host,
    wait_for_greenmail_inbox_message_seen_host,
    wait_for_greenmail_ready,
    wait_for_greenmail_user_reply,
)
from .imap_checkpoint import (
    email_ingress_imap_checkpoint_from_notmuch,
    restart_email_bridge_service,
)
from .knowledge import (
    bootstrap_embedding_entries,
    bootstrap_embedding_entry_ids,
    e2e_bootstrap_reindex_and_wait,
    e2e_bootstrap_scenario,
    e2e_install_deterministic_knowledge_corpus,
    e2e_wait_engine_active,
)
from .lightrag_assert import (
    assert_notmuch_mailflow_thread_has_lightrag_indexed,
    assert_notmuch_thread_lightrag_index_filter,
)
from .mailflow import (
    MailflowScenarioSpec,
    assert_full_mailflow_pipeline,
    mailflow_inject_and_wait,
)
from .notmuch_assert import (
    assert_notmuch_folder_contains_body_token,
    assert_notmuch_thread_fully_in_stages,
    assert_notmuch_thread_has_messages_in_folders,
    assert_notmuch_thread_has_no_unread,
    assert_notmuch_thread_stage_message_count_at_least,
    assert_notmuch_thread_tag_count,
    poll_notmuch_positive,
    poll_notmuch_thread_in_stage_folder,
    poll_lightrag_indexed_positive,
    wait_for_notmuch_message,
)
from .pipeline import (
    e2e_start_threlium_user_pipeline_services,
    e2e_stop_threlium_user_pipeline_services,
    e2e_sut_threlium_user_journal_rotate_and_vacuum,
)
from .poll import mailflow_diag_block, mailflow_log_phase, poll_until, poll_until_backoff
from .runtime import E2EComposeRuntime, compose_logs, discover_runtime, service_exec, tcp_open
from .smtp_ingress import smtp_inject_inbound
from .wiremock_assert import (
    assert_wiremock_mailflow_min_embedding_posts,
    assert_wiremock_mailflow_min_rerank_posts,
    assert_wiremock_mailflow_received_chat_completion_posts,
    assert_wiremock_mailflow_zero_unmatched,
    wait_for_wiremock_global_unmatched_zero,
)
from .workers import wait_for_sut_threlium_user_workers_idle

__all__ = [n for n in globals() if not n.startswith("_")]
