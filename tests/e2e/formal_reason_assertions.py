"""Shared WireMock journal assertions for formal_reason gate e2e scenarios."""
from __future__ import annotations

import json
import re

from threlium.types import FsmStage
from threlium.types.reasoning_routes import REASONING_TARGET_STAGES

from .wiremock_client import (
    find_wiremock_requests_by_body_contains,
    journal_entries_for_stub_tag,
)

# Rendered only when ``formal_reason_gate.j2`` is appended (gate ON). ``system.j2`` describes
# the same policy in static strategy text — do not match on ``FORMAL REASON GATE`` substrings.
GATE_ACTIVE_BODY_MARKER = "Gate retry counter:"
GATE_NOTICE = GATE_ACTIVE_BODY_MARKER
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
    """Every FSM reasoning request carrying ``body_marker`` must be ungated."""
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


def assert_first_fsm_reasoning_gate_absent(
    wm_base: str, stub_tag: str, body_marker: str
) -> None:
    """First FSM reasoning hop with ``body_marker`` (pre-gate) must not render gate notice."""
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not _is_chat_completions_entry(entry):
            continue
        body = _journal_request_body(entry)
        if '"tools"' not in body or "<envelope>" not in body:
            continue
        if body_marker not in body:
            continue
        assert_gate_absent_in_body(body)
        return
    raise AssertionError(
        f"No FSM reasoning chat/completions with body marker {body_marker!r} "
        f"(stub_tag={stub_tag!r})"
    )


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


def _gated_reasoning_chat_bodies(wm_base: str, stub_tag: str) -> list[str]:
    matches = find_wiremock_requests_by_body_contains(
        wm_base, GATE_NOTICE, stub_tag=stub_tag
    )
    return [
        _journal_request_body(e)
        for e in matches
        if _is_chat_completions_entry(e)
    ]


def assert_chat_request_contains_all(
    wm_base: str,
    stub_tag: str,
    needles: tuple[str, ...],
    *,
    gate_only: bool = False,
    exclude: tuple[str, ...] = (),
) -> None:
    """At least one chat/completions **request** body contains every ``needle``.

    With ``exclude``, matching bodies must not contain any excluded substring
    (proves staged accumulation: later hops not yet visible in an earlier prompt).
    """
    if gate_only:
        bodies = _gated_reasoning_chat_bodies(wm_base, stub_tag)
    else:
        bodies = [
            _journal_request_body(e)
            for e in fsm_reasoning_chat_entries(wm_base, stub_tag)
        ]
    for body in bodies:
        if not all(needle in body for needle in needles):
            continue
        if exclude and any(ex in body for ex in exclude):
            continue
        return
    scope = "gated reasoning" if gate_only else "reasoning"
    ex_msg = f", excluding any body containing {exclude!r}" if exclude else ""
    raise AssertionError(
        f"No {scope} chat/completions request contains all of {needles!r}"
        f"{ex_msg} (stub_tag={stub_tag!r})"
    )


def assert_gated_formal_reason_history_accumulated(
    wm_base: str,
    stub_tag: str,
    *,
    prior_formal_reason_markers: tuple[str, ...],
    error_observation_markers: tuple[str, ...] = (
        "PARSE ERROR",
        "QUERY ERROR",
        "FSM locked",
    ),
    memory_query_marker: str | None = None,
    require_conversation_delta: bool = True,
) -> None:
    """Gated LLM prompt carries prior formal_reason tool I/O and error observations.

    Verifies enrich_fast relay: multiple ``formal_reason`` hops leave both fatal/supplemental
    observations and earlier tool-call context in one later reasoning request under gate.
    """
    needles: list[str] = list(error_observation_markers)
    needles.extend(prior_formal_reason_markers)
    if memory_query_marker:
        needles.append(memory_query_marker)
    if require_conversation_delta:
        needles.append("<conversation_delta>")
    assert_chat_request_contains_all(
        wm_base, stub_tag, tuple(needles), gate_only=True
    )


def _journal_served_response_text(entry: dict) -> str:
    """Тело ответа из журнала WM: ``body`` или сериализованный ``jsonBody``."""
    for key in ("response", "responseDefinition"):
        block = entry.get(key)
        if not isinstance(block, dict):
            continue
        body = block.get("body")
        if isinstance(body, str) and body.strip():
            return body
        jb = block.get("jsonBody")
        if jb is not None:
            return json.dumps(jb, ensure_ascii=False)
    return ""


def assert_memory_query_tool_served(
    wm_base: str,
    stub_tag: str,
    *,
    tool_call_id: str,
    tool_name: str = FsmStage.MEMORY_QUERY.value,
) -> None:
    """WireMock served a reasoning response with ``memory_query`` tool_calls (FSM executed)."""
    name_pat = re.compile(
        rf'"name"\s*:\s*"{re.escape(tool_name)}"',
    )
    for entry in journal_entries_for_stub_tag(wm_base, stub_tag=stub_tag):
        if not _is_chat_completions_entry(entry):
            continue
        text = _journal_served_response_text(entry)
        if tool_call_id in text and name_pat.search(text):
            return
    raise AssertionError(
        f"No journal entry served {tool_name!r} tool_call {tool_call_id!r} "
        f"(stub_tag={stub_tag!r})"
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
