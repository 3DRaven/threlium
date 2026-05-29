"""Shared RDF graph build, SHACL helpers, inference delta, and SPARQL result formatting."""
from __future__ import annotations

from dataclasses import dataclass

from rdflib import Graph
from rdflib.query import Result


@dataclass(frozen=True)
class ParsedFormalReasonGraphs:
    """Turtle graphs parsed from formal_reason payload fields."""

    data: Graph
    shapes: Graph
    ontology: Graph | None


def _parse_ttl_field(field_name: str, ttl: str) -> Graph:
    """Parse one Turtle field, prefixing any error with the field name.

    rdflib parse errors (e.g. ``string index out of range``) do not say which
    graph failed; with three TTL fields the model must know whether to fix
    facts_ttl, shapes_ttl, or ontology_ttl.
    """
    try:
        return Graph().parse(data=ttl, format="turtle")
    except Exception as e:
        raise RuntimeError(f"{field_name}: {e}") from e


def parse_formal_reason_graphs(
    facts_ttl: str, shapes_ttl: str, ontology_ttl: str | None
) -> ParsedFormalReasonGraphs:
    data_graph = _parse_ttl_field("facts_ttl", facts_ttl)
    shapes_graph = _parse_ttl_field("shapes_ttl", shapes_ttl)
    ont_graph = (
        _parse_ttl_field("ontology_ttl", ontology_ttl) if ontology_ttl else None
    )
    return ParsedFormalReasonGraphs(
        data=data_graph, shapes=shapes_graph, ontology=ont_graph
    )


def combined_graph(data_graph: Graph, ont_graph: Graph | None) -> Graph:
    """data + ont in one graph — pyshacl merges them the same way during validation."""
    combined = Graph()
    for triple in data_graph:
        combined.add(triple)
    if ont_graph is not None:
        for triple in ont_graph:
            combined.add(triple)
    return combined


def delta_ttl_from_expanded(baseline: Graph, expanded: Graph, *, max_chars: int) -> str:
    """Turtle of triples present in ``expanded`` but not ``baseline`` (no re-validate).

    ``expanded`` is the in-place RDFS/OWL closure produced by the single
    ``validate(..., inplace=True, inference=...)`` run in :mod:`formal_reason`.
    """
    if expanded is baseline:
        return ""
    before = set(baseline)
    delta = Graph()
    for triple in expanded:
        if triple not in before:
            delta.add(triple)
    if len(delta) == 0:
        return ""
    text = delta.serialize(format="turtle")
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    return text[:max_chars]


def query_graph_ttl(graph: Graph, sparql: str, *, max_chars: int) -> str:
    """Run SPARQL against graph and return human-readable result text."""
    result = graph.query(sparql)
    return format_sparql_result(result, max_chars=max_chars)


def format_sparql_result(result: Result, *, max_chars: int) -> str:
    if result.type == "ASK":
        value = result.askAnswer
        text = f"ask: {value}"
    elif result.type == "CONSTRUCT" or result.type == "DESCRIBE":
        g = result.graph
        if g is None or len(g) == 0:
            text = "(empty graph)"
        else:
            text = g.serialize(format="turtle")
            if isinstance(text, bytes):
                text = text.decode("utf-8")
    else:
        vars_ = result.vars
        lines: list[str] = []
        row_count = 0
        truncated = False
        for row in result:
            row_count += 1
            parts = []
            for v in vars_:
                val = row[v]
                parts.append(f"{v}={_format_term(val)}")
            lines.append(" | ".join(parts))
            if sum(len(ln) + 1 for ln in lines) > max_chars:
                truncated = True
                break
        if not lines:
            text = "(no bindings)"
        else:
            header = f"({row_count}+ rows, truncated)" if truncated else f"({row_count} rows)"
            text = "\n".join([header, *lines])
    return text[:max_chars]


def _format_term(term: object) -> str:
    if term is None:
        return "?"
    s = str(term)
    return s if len(s) <= 120 else s[:117] + "..."
