"""Shared WireMock journal assertions for formal_reason gate e2e scenarios."""
from __future__ import annotations

import json

from threlium.types import FsmStage
from threlium.types.reasoning_routes import REASONING_TARGET_STAGES

from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    journal_entries_for_stub_tag,
)

GATE_NOTICE = "FORMAL REASON GATE"
GATE_TOOL_NAMES = frozenset(
    {FsmStage.FORMAL_REASON.value, FsmStage.MEMORY_QUERY.value}
)
FULL_TOOL_NAMES = frozenset(s.value for s in REASONING_TARGET_STAGES)

def _journal_request_body(entry: dict) -> str:
    req = entry.get("request")
    if not isinstance(req, dict):
        return ""
    body = req.get("body")
    return body if isinstance(body, str) else ""


def _is_chat_completions_entry(entry: dict) -> bool:
    req = entry.get("request")
    if not isinstance(req, dict):
        return False
    if str(req.get("method") or "").upper() != "POST":
        return False
    url = str(req.get("url") or "")
    return "/chat/completions" in url


def fsm_reasoning_chat_entries(wm_base: str, stub_tag: str) -> list[dict]:
    """FSM chat/completions requests that include reasoning tools + envelope."""
    out: list[dict] = []
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not _is_chat_completions_entry(entry):
            continue
        body = _journal_request_body(entry)
        if '"tools"' not in body or "<envelope>" not in body:
            continue
        out.append(entry)
    return out


def tool_names_from_chat_body(body: str) -> list[str]:
    data = json.loads(body) if isinstance(body, str) else body
    tools = data.get("tools") or []
    names: list[str] = []
    for tool in tools:
        fn = tool.get("function") or {}
        name = fn.get("name")
        if name:
            names.append(str(name))
    return names


def assert_reasoning_tools_set(
    body: str,
    expected_names: frozenset[str],
    *,
    msg: str = "",
) -> None:
    names = frozenset(tool_names_from_chat_body(body))
    prefix = f"{msg}: " if msg else ""
    assert names == expected_names, (
        f"{prefix}expected tools {sorted(expected_names)!r}, got {sorted(names)!r}"
    )


def assert_gate_active_in_body(body: str) -> None:
    assert GATE_NOTICE in body, f"expected {GATE_NOTICE!r} in reasoning request body"


def assert_gate_absent_in_body(body: str) -> None:
    assert GATE_NOTICE not in body, f"unexpected {GATE_NOTICE!r} in reasoning request body"


def assert_gated_reasoning_calls(wm_base: str, stub_tag: str) -> None:
    """Every gated FSM reasoning request exposes only formal_reason + memory_query tools."""
    matches = find_wiremock_requests_by_body_contains(
        wm_base, GATE_NOTICE, stub_tag=stub_tag
    )
    chat_matches = [e for e in matches if _is_chat_completions_entry(e)]
    assert chat_matches, (
        f"No gated reasoning chat/completions with {GATE_NOTICE!r} "
        f"(stub_tag={stub_tag!r})"
    )
    for entry in chat_matches:
        body = _journal_request_body(entry)
        assert_gate_active_in_body(body)
        names = frozenset(tool_names_from_chat_body(body))
        assert names == GATE_TOOL_NAMES, (
            "Gated reasoning must expose exactly formal_reason and memory_query; "
            f"got {sorted(names)!r}"
        )


def assert_gated_reasoning_includes_memory_query(
    wm_base: str, stub_tag: str
) -> None:
    """At least one gated reasoning request lists memory_query in tools."""
    matches = find_wiremock_requests_by_body_contains(
        wm_base, GATE_NOTICE, stub_tag=stub_tag
    )
    for entry in matches:
        if not _is_chat_completions_entry(entry):
            continue
        body = _journal_request_body(entry)
        names = tool_names_from_chat_body(body)
        if FsmStage.MEMORY_QUERY.value in names:
            return
    raise AssertionError(
        f"No gated reasoning chat/completions includes memory_query in tools "
        f"(stub_tag={stub_tag!r})"
    )


def assert_ungated_reasoning_has_finalize(
    wm_base: str,
    stub_tag: str,
    *,
    needle: str | None = None,
) -> None:
    """At least one ungated reasoning request offers response_finalize."""
    finalize = FsmStage.RESPONSE_FINALIZE.value
    for entry in fsm_reasoning_chat_entries(wm_base, stub_tag):
        body = _journal_request_body(entry)
        if GATE_NOTICE in body:
            continue
        if needle is not None and needle not in body:
            continue
        names = tool_names_from_chat_body(body)
        if finalize in names:
            return
    detail = f" needle={needle!r}" if needle else ""
    raise AssertionError(
        f"No ungated reasoning chat/completions with {finalize!r} in tools"
        f" (stub_tag={stub_tag!r}{detail})"
    )


def assert_all_reasoning_gate_absent(wm_base: str, stub_tag: str) -> None:
    for entry in fsm_reasoning_chat_entries(wm_base, stub_tag):
        assert_gate_absent_in_body(_journal_request_body(entry))


def assert_gate_absent_with_body_marker(
    wm_base: str, stub_tag: str, body_marker: str
) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, body_marker, stub_tag=stub_tag
    )
    chat_matches = [e for e in matches if _is_chat_completions_entry(e)]
    assert chat_matches, (
        f"No chat/completions with body marker {body_marker!r} "
        f"(stub_tag={stub_tag!r})"
    )
    for entry in chat_matches:
        assert_gate_absent_in_body(_journal_request_body(entry))


def assert_journal_contains(
    wm_base: str,
    stub_tag: str,
    needle: str,
    *,
    chat_only: bool = True,
) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, needle, stub_tag=stub_tag
    )
    if chat_only:
        matches = [e for e in matches if _is_chat_completions_entry(e)]
    assert matches, (
        f"No journal requests contain {needle!r} (stub_tag={stub_tag!r}, "
        f"chat_only={chat_only})"
    )


def assert_violation_reasoning_without_gate(
    wm_base: str, stub_tag: str, *, violation_needle: str = "conforms: False"
) -> None:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, violation_needle, stub_tag=stub_tag
    )
    chat_matches = [e for e in matches if _is_chat_completions_entry(e)]
    assert chat_matches, (
        f"No violation reasoning journal for {violation_needle!r} "
        f"(stub_tag={stub_tag!r})"
    )
    for entry in chat_matches:
        assert_gate_absent_in_body(_journal_request_body(entry))
