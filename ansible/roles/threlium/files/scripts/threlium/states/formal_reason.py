"""formal_reason@localhost → enrich_fast@localhost.

Formal reasoning over RDF: SHACL validation, optional RDFS/OWL inference (entailed
triples), and optional SPARQL query against the model's graph. Idempotent, safe,
no HITL. Returns observation-note for the reasoning loop.
"""
from email.message import EmailMessage

from rdflib import Graph
from rdflib.namespace import RDF, SH
from pyshacl import validate
from pyshacl.errors import ConstraintLoadError, RuleLoadError, ShapeLoadError

from threlium.fsm_emit import build_fsm_step_to_stage
from threlium.knowledge_fsm import parse_formal_reason_payload
from threlium.mime_reform import system_part_text
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.states.rdf_graphs import (
    combined_graph,
    delta_ttl_from_expanded,
    parse_formal_reason_graphs,
    query_graph_ttl,
)
from threlium.types import (
    EnrichObservationNoteText,
    FormalReasonDerivedErrorText,
    FormalReasonDerivedTtlText,
    FormalReasonErrorKind,
    FormalReasonFatalErrorText,
    FormalReasonQueryErrorText,
    FormalReasonQueryResultText,
    FormalReasonReportText,
    FsmStage,
    PromptPath,
)


def _count_violations(results_graph: Graph) -> int:
    return sum(1 for _ in results_graph.subjects(RDF.type, SH.ValidationResult))


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    request_payload = system_part_text(msg)
    payload = parse_formal_reason_payload(request_payload)
    if payload is None:
        raise RuntimeError("formal_reason: invalid payload")

    error_kind = FormalReasonErrorKind.NONE
    fatal_message = ""
    conforms = False
    report_text = ""
    violations = 0
    derived_ttl = ""
    query_result = ""
    derived_error = ""
    query_error = ""

    data_graph: Graph | None = None
    shapes_graph: Graph | None = None
    ont_graph: Graph | None = None

    derived_cap = config.knowledge.formal_derived_max_chars
    query_cap = config.knowledge.formal_query_max_chars

    try:
        parsed = parse_formal_reason_graphs(
            payload.facts_ttl, payload.shapes_ttl, payload.ontology_ttl
        )
        data_graph = parsed.data
        shapes_graph = parsed.shapes
        ont_graph = parsed.ontology
    except Exception as e:
        error_kind = FormalReasonErrorKind.PARSE
        fatal_message = str(e)

    inference_py = (
        payload.inference.to_pyshacl() if payload.inference is not None else None
    )

    if (
        error_kind is FormalReasonErrorKind.NONE
        and data_graph is not None
        and shapes_graph is not None
    ):
        # Single validate covers conforms + inference closure: when inference is on
        # and we need derived/query, validate a baseline copy in place so the same
        # expanded graph feeds the delta and the SPARQL query (no extra runs).
        use_expanded = bool(inference_py) and (
            payload.return_derived or bool(payload.query)
        )
        baseline: Graph | None = None
        check_graph: Graph
        try:
            if use_expanded:
                baseline = combined_graph(data_graph, ont_graph)
                work = Graph()
                for triple in baseline:
                    work.add(triple)
                conforms, results_graph, raw_report = validate(
                    work,
                    shacl_graph=shapes_graph,
                    ont_graph=ont_graph,
                    advanced=True,
                    inference=inference_py,
                    inplace=True,
                )
                check_graph = work
            else:
                conforms, results_graph, raw_report = validate(
                    data_graph,
                    shacl_graph=shapes_graph,
                    ont_graph=ont_graph,
                    advanced=True,
                    inference=inference_py,
                )
                check_graph = combined_graph(data_graph, ont_graph)
            violations = _count_violations(results_graph)
            report_text = FormalReasonReportText.parse(
                raw_report[: config.knowledge.formal_report_max_chars]
            ).value
        except (ConstraintLoadError, RuleLoadError, ShapeLoadError) as e:
            error_kind = FormalReasonErrorKind.SHAPE
            fatal_message = str(e)
            conforms = False
        except Exception as e:
            error_kind = FormalReasonErrorKind.RUNTIME
            fatal_message = str(e)
            conforms = False

        # Derived/query are supplemental: a failure here is reported as its own
        # section and never overwrites a successful validation result.
        if error_kind is FormalReasonErrorKind.NONE and payload.return_derived and baseline is not None:
            try:
                derived_ttl = delta_ttl_from_expanded(
                    baseline, check_graph, max_chars=derived_cap
                )
            except Exception as e:
                derived_error = str(e)

        if error_kind is FormalReasonErrorKind.NONE and payload.query:
            try:
                query_result = query_graph_ttl(
                    check_graph, payload.query, max_chars=query_cap
                )
            except Exception as e:
                query_error = str(e)

    fatal_vo = FormalReasonFatalErrorText.parse(fatal_message)
    derived_vo = FormalReasonDerivedTtlText.parse(derived_ttl)
    query_vo = FormalReasonQueryResultText.parse(query_result)
    derived_err_vo = FormalReasonDerivedErrorText.parse_present_optional(derived_error)
    query_err_vo = FormalReasonQueryErrorText.parse_present_optional(query_error)

    return_derived_ignored = bool(payload.return_derived) and inference_py is None

    observation = render_prompt(
        PromptPath.FORMAL_REASON_OBSERVATION,
        reasoning=payload.reasoning,
        conforms=conforms,
        error_kind=error_kind.value,
        error_message=fatal_vo.value,
        report_text=report_text,
        violations=violations,
        return_derived_ignored=return_derived_ignored,
        derived_ttl=derived_vo.value,
        query_result=query_vo.value,
        has_derived_error=derived_err_vo is not None,
        derived_error=derived_err_vo.value if derived_err_vo is not None else "",
        has_query_error=query_err_vo is not None,
        query_error=query_err_vo.value if query_err_vo is not None else "",
    )
    note = EnrichObservationNoteText.parse(observation).value

    # Модель «callee владеет историей»: в память едут ОБЕ стороны диалога с инструментом —
    # ОТВЕТ (observation: conforms/violations/derived/query) как <history> origin=formal_reason,
    # и ЗАПРОС (что отдали на проверку: reasoning + shapes/facts/ontology/query) как
    # request_echo с предштампом origin=reasoning. Без эха формулировка задачи терялась бы из
    # истории (reasoning теперь шлёт payload только в <system>) — ровно тот регресс, с которого
    # начался рефакторинг. Разные тела → разные <hash@history>, дедуп их не схлопывает.
    return build_fsm_step_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        history=note,
        request_echo=request_payload,
        settings=config,
    )
