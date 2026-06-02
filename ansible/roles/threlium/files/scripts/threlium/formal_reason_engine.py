"""pySHACL / RDF engine for formal_reason stage (no FSM emit, no gate policy)."""
from __future__ import annotations

from dataclasses import dataclass

from rdflib import Graph
from rdflib.namespace import RDF, SH
from pyshacl import validate
from pyshacl.errors import ConstraintLoadError, RuleLoadError, ShapeLoadError

from threlium.settings import ThreliumSettings
from threlium.states.rdf_graphs import (
    combined_graph,
    delta_ttl_from_expanded,
    parse_formal_reason_graphs,
    query_graph_ttl,
)
from threlium.types import (
    FormalReasonErrorKind,
    FormalReasonFatalErrorText,
    FormalReasonReportText,
    FormalReasonStagePayload,
)


def _count_violations(results_graph: Graph) -> int:
    return sum(1 for _ in results_graph.subjects(RDF.type, SH.ValidationResult))


@dataclass(frozen=True)
class FormalReasonEngineResult:
    error_kind: FormalReasonErrorKind
    fatal_message: FormalReasonFatalErrorText
    conforms: bool
    report_text: str
    violations: int
    derived_ttl: str
    query_result: str
    derived_error: str
    query_error: str
    return_derived_ignored: bool


def run_formal_reason_engine(
    payload: FormalReasonStagePayload, *, config: ThreliumSettings
) -> FormalReasonEngineResult:
    """Validate SHACL, optional inference delta, optional SPARQL — pure engine, no outcome policy."""
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

    return FormalReasonEngineResult(
        error_kind=error_kind,
        fatal_message=FormalReasonFatalErrorText.parse(fatal_message),
        conforms=conforms,
        report_text=report_text,
        violations=violations,
        derived_ttl=derived_ttl,
        query_result=query_result,
        derived_error=derived_error,
        query_error=query_error,
        return_derived_ignored=bool(payload.return_derived) and inference_py is None,
    )


__all__ = ["FormalReasonEngineResult", "run_formal_reason_engine"]
