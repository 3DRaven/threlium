#!/usr/bin/env python3
"""Verify knowledge/*.md formal_reason JSON snippets through the real FSM stage.

Every executable example in ``knowledge/`` is a JSON body for the ``formal_reason``
tool. This script feeds each block to :func:`threlium.states.formal_reason.main`
exactly as the FSM worker would (stub ``EmailMessage`` text/plain body →
multipart observation-note to ``enrich_fast@localhost``), extracts the
observation text, and asserts it against the ``<!-- expect: ... -->`` comment
that precedes the block.

It also lints the whole corpus: no ```python fences are allowed in
``knowledge/**/*.md`` — the reasoning model calls only the ``formal_reason``
tool, so REPL/``rdflib``/``pyshacl`` API snippets would only mislead retrieval.

The stage and its dependencies are NOT re-implemented here; the script only
calls ``formal_reason.main`` and reads the rendered observation. A mismatch
between an example and the observation is therefore a real signal — first fix
the markdown JSON/expect, and if it persists, escalate (possible engine bug).

Usage::

    .venv/bin/python ansible/roles/threlium/files/scripts/verify_knowledge_snippets.py [--strict] [--verbose]

``--strict`` fails on any ``formal_reason`` JSON block without an ``expect``
comment. ``--verbose`` prints the full observation for each block so the
markdown ``Expected observation`` can be aligned with the real output.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from email.message import EmailMessage
from pathlib import Path

# Run from scripts/ with threlium on PYTHONPATH (pip install -e .).
from threlium.mime_reform import EnrichPartId, extract_part_by_content_id
from threlium.prompts import init_prompts_root
from threlium.settings import ThreliumSettings
from threlium.states import formal_reason
from threlium.types import FsmStage

FILES_ROOT = Path(__file__).resolve().parents[1]
KNOWLEDGE_DIR = FILES_ROOT / "knowledge"

PYTHON_FENCE_RE = re.compile(r"^[ \t]*```py(?:thon)?\b", re.MULTILINE)
JSON_BLOCK_RE = re.compile(r"```json\s*\n(?P<body>.*?)```", re.DOTALL)
EXPECT_RE = re.compile(r"<!--\s*expect:(?P<expect>.*?)-->", re.DOTALL)
EXPECT_KV_RE = re.compile(r"(\w+)=(\S+)")


def _parse_expect_before(text: str, block_start: int) -> dict[str, str] | None:
    """Last ``<!-- expect: ... -->`` in the 400 chars before a json block, if any."""
    window = text[max(0, block_start - 400) : block_start]
    matches = list(EXPECT_RE.finditer(window))
    if not matches:
        return None
    return dict(EXPECT_KV_RE.findall(matches[-1].group("expect")))


def _build_stub_incoming(payload_json: str) -> EmailMessage:
    """Minimal text/plain email carrying the tool JSON, as reasoning would emit."""
    msg = EmailMessage()
    msg["Subject"] = "verify formal_reason snippet"
    msg.set_content(payload_json, subtype="plain", charset="utf-8")
    return msg


def _run_observation(payload_json: str, config: ThreliumSettings) -> str:
    """Call the real stage and return the observation-note text."""
    incoming = _build_stub_incoming(payload_json)
    out = formal_reason.main(incoming, FsmStage.FORMAL_REASON, config=config)
    if out is None:
        raise RuntimeError("formal_reason.main returned None")
    to_addr = out["To"]
    if to_addr != FsmStage.ENRICH_FAST.rfc822_mailbox:
        raise RuntimeError(f"unexpected To: {to_addr!r}")
    note = extract_part_by_content_id(out, EnrichPartId.OBSERVATION_NOTE)
    if note is None:
        raise RuntimeError("no observation-note part in stage output")
    return note


_ERROR_KIND_MARKERS = {
    "parse": "PARSE ERROR",
    "shape": "SHAPE LOAD ERROR",
    "runtime": "RUNTIME ERROR (validation)",
    "query": "QUERY ERROR (SPARQL)",
    "derived": "DERIVED ERROR (inference)",
    "none": "conforms:",
}


def _is_true(raw: str) -> bool:
    return raw.lower() in ("true", "1", "yes")


def _check_expect(observation: str, expect: dict[str, str], label: str) -> list[str]:
    errors: list[str] = []
    for key, raw in expect.items():
        if key == "conforms":
            want = _is_true(raw)
            marker = f"conforms: {want}"
            if marker not in observation:
                errors.append(f"{label}: expected '{marker}' in observation")
        elif key == "violations":
            m = re.search(r"^violations: (\d+)\s*$", observation, re.MULTILINE)
            got = int(m.group(1)) if m else None
            if got != int(raw):
                errors.append(f"{label}: violations want {raw} got {got}")
        elif key == "error_kind":
            marker = _ERROR_KIND_MARKERS.get(raw.lower())
            if marker is None:
                errors.append(f"{label}: unknown error_kind={raw!r}")
            elif marker not in observation:
                errors.append(f"{label}: error_kind={raw} marker '{marker}' missing")
        elif key == "derived_nonempty":
            present = "derived_triples (inference delta):" in observation
            if present != _is_true(raw):
                errors.append(f"{label}: derived_nonempty want {raw} got {present}")
        elif key == "query_nonempty":
            present = "query_result:" in observation
            if present != _is_true(raw):
                errors.append(f"{label}: query_nonempty want {raw} got {present}")
        elif key == "observation_contains":
            if raw not in observation:
                errors.append(f"{label}: observation_contains {raw!r} missing")
        else:
            errors.append(f"{label}: unknown expect key {key!r}")
    return errors


def verify_file(
    path: Path, config: ThreliumSettings, *, strict: bool, verbose: bool
) -> list[str]:
    text = path.read_text(encoding="utf-8")
    errors: list[str] = []

    for m in PYTHON_FENCE_RE.finditer(text):
        line = text.count("\n", 0, m.start()) + 1
        errors.append(
            f"{path.name}:{line}: python fence not allowed in knowledge/ "
            "(model uses only the formal_reason tool)"
        )

    for idx, match in enumerate(JSON_BLOCK_RE.finditer(text)):
        label = f"{path.name}#{idx}"
        try:
            data = json.loads(match.group("body"))
        except json.JSONDecodeError as e:
            errors.append(f"{label}: invalid JSON: {e}")
            continue
        if not isinstance(data, dict) or "facts_ttl" not in data or "shapes_ttl" not in data:
            continue
        expect = _parse_expect_before(text, match.start())
        if expect is None:
            if strict:
                errors.append(f"{label}: formal_reason JSON without <!-- expect: ... -->")
            continue
        try:
            observation = _run_observation(match.group("body"), config)
        except Exception as e:
            errors.append(f"{label}: stage failed: {e}")
            continue
        if verbose:
            print(f"\n===== {label} observation =====\n{observation}\n", file=sys.stderr)
        errors.extend(_check_expect(observation, expect, label))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="fail on JSON blocks without expect")
    parser.add_argument("--verbose", action="store_true", help="print full observation per block")
    args = parser.parse_args(argv)

    init_prompts_root(FILES_ROOT)
    config = ThreliumSettings()

    all_errors: list[str] = []
    for path in sorted(KNOWLEDGE_DIR.rglob("*.md")):
        if path.name.startswith("_"):
            continue
        all_errors.extend(
            verify_file(path, config, strict=args.strict, verbose=args.verbose)
        )

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        print(f"FAILED: {len(all_errors)} error(s)", file=sys.stderr)
        return 1
    print(f"OK: verified formal_reason JSON snippets under {KNOWLEDGE_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
