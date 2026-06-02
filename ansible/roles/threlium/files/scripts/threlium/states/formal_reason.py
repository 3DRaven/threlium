"""formal_reason@localhost → enrich_fast@localhost.

Formal reasoning over RDF: SHACL validation, optional RDFS/OWL inference (entailed
triples), and optional SPARQL query against the model's graph. Idempotent, safe,
no HITL. Returns observation-note for the reasoning loop.
"""
from __future__ import annotations

import msgspec
from email.message import EmailMessage

from threlium.formal_reason_engine import run_formal_reason_engine
from threlium.formal_reason_gate import compute_formal_reason_outcome
from threlium.fsm_emit_semantic import emit_to_enrich_fast
from threlium.knowledge_fsm import parse_formal_reason_payload
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import (
    EnrichObservationNoteText,
    FormalReasonDerivedErrorText,
    FormalReasonDerivedTtlText,
    FormalReasonErrorKind,
    FormalReasonOutcome,
    FormalReasonQueryErrorText,
    FormalReasonQueryResultText,
    FormalReasonResultPayload,
    FsmStage,
    PromptPath,
)


def _observation_prompt_path(
    outcome: FormalReasonOutcome, error_kind: FormalReasonErrorKind
) -> PromptPath:
    if outcome is FormalReasonOutcome.PASSED:
        return PromptPath.FORMAL_REASON_OBSERVATION_PASSED
    if outcome is FormalReasonOutcome.SHACL_NEGATIVE:
        return PromptPath.FORMAL_REASON_OBSERVATION_SHACL_NEGATIVE
    if error_kind is not FormalReasonErrorKind.NONE:
        return PromptPath.FORMAL_REASON_OBSERVATION_FATAL
    return PromptPath.FORMAL_REASON_OBSERVATION_SUPPLEMENTAL_ERROR


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    request_payload = system_part_text(msg)
    payload = parse_formal_reason_payload(request_payload)
    if payload is None:
        raise RuntimeError("formal_reason: invalid payload")

    engine = run_formal_reason_engine(payload, config=config)

    derived_err_vo = FormalReasonDerivedErrorText.parse_present_optional(
        engine.derived_error
    )
    query_err_vo = FormalReasonQueryErrorText.parse_present_optional(engine.query_error)
    derived_vo = FormalReasonDerivedTtlText.parse(engine.derived_ttl)
    query_vo = FormalReasonQueryResultText.parse(engine.query_result)

    has_query_error = query_err_vo is not None
    has_derived_error = derived_err_vo is not None
    outcome = compute_formal_reason_outcome(
        error_kind=engine.error_kind,
        conforms=engine.conforms,
        violations=engine.violations,
        has_query_error=has_query_error,
        has_derived_error=has_derived_error,
    )
    result = FormalReasonResultPayload(
        outcome=outcome,
        error_kind=engine.error_kind,
        conforms=engine.conforms,
        violations=engine.violations,
        has_query_error=has_query_error,
        has_derived_error=has_derived_error,
    )
    system_json = msgspec.json.encode(result).decode()

    observation = render_prompt(
        _observation_prompt_path(outcome, engine.error_kind),
        reasoning=payload.reasoning,
        conforms=engine.conforms,
        error_kind=engine.error_kind.value,
        error_message=engine.fatal_message.value,
        report_text=engine.report_text,
        violations=engine.violations,
        return_derived_ignored=engine.return_derived_ignored,
        derived_ttl=derived_vo.value,
        query_result=query_vo.value,
        has_derived_error=derived_err_vo is not None,
        derived_error=derived_err_vo.value if derived_err_vo is not None else "",
        has_query_error=query_err_vo is not None,
        query_error=query_err_vo.value if query_err_vo is not None else "",
    )
    note = EnrichObservationNoteText.parse(observation).value

    return emit_to_enrich_fast(
        msg,
        stage,
        history=note,
        request_echo=request_payload,
        system=system_json,
        settings=config,
    )
