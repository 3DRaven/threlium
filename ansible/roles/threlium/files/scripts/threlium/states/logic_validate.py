"""logic_validate@localhost → enrich_fast@localhost.

Валидация произвольной предикатной логики над RDF-графом через pySHACL
(SHACL + SHACL-SPARQL). Идемпотентная, безопасная, без HITL.
Возвращает отчёт (conforms + violations) как observation-note.
"""
from email.message import EmailMessage

from rdflib import Graph
from rdflib.namespace import RDF, RDFS, SH
from pyshacl import validate
from pyshacl.errors import ConstraintLoadError, RuleLoadError, ShapeLoadError

from threlium.fsm_emit import build_fsm_multipart_to_stage
from threlium.knowledge_fsm import parse_logic_validate_payload
from threlium.mime_reform import EnrichPartId, extract_plain_body
from threlium.prompts import render_prompt
from threlium.settings import ThreliumSettings
from threlium.types import FsmStage, PromptPath, LogicValidateReportText


def _count_violations(results_graph: Graph) -> int:
    return sum(1 for _ in results_graph.subjects(RDF.type, SH.ValidationResult))


def _combined_graph(data_graph: Graph, ont_graph: Graph | None) -> Graph:
    """data + ont в одном графе — pyshacl при валидации тоже их смешивает."""
    combined = Graph()
    for triple in data_graph:
        combined.add(triple)
    if ont_graph is not None:
        for triple in ont_graph:
            combined.add(triple)
    return combined


def _standard_target_focus(shapes_graph: Graph, check_graph: Graph) -> tuple[int, int]:
    """Anti-vacuous эвристика: ``(target_declarations, focus_nodes)``.

    Считает focus-узлы стандартных таргетов SHACL (``sh:targetClass`` с учётом
    ``rdfs:subClassOf``, ``sh:targetNode``, ``sh:targetSubjectsOf``,
    ``sh:targetObjectsOf``) против данных. SPARQL-таргеты и неявные class-таргеты
    не учитываются. ``conforms=True`` при ``declarations>0 и focus==0`` означает,
    что shape ни на чём не сработал (ложноположительное «нарушений нет»).
    """
    focus: set[object] = set()
    declarations = 0

    for cls in shapes_graph.objects(None, SH.targetClass):
        declarations += 1
        classes: set[object] = {cls}
        frontier = [cls]
        while frontier:
            current = frontier.pop()
            for sub in check_graph.subjects(RDFS.subClassOf, current):
                if sub not in classes:
                    classes.add(sub)
                    frontier.append(sub)
        for resolved in classes:
            for node in check_graph.subjects(RDF.type, resolved):
                focus.add(node)

    for node in shapes_graph.objects(None, SH.targetNode):
        declarations += 1
        focus.add(node)

    for prop in shapes_graph.objects(None, SH.targetSubjectsOf):
        declarations += 1
        for subj in check_graph.subjects(prop, None):
            focus.add(subj)

    for prop in shapes_graph.objects(None, SH.targetObjectsOf):
        declarations += 1
        for obj in check_graph.objects(None, prop):
            focus.add(obj)

    return declarations, len(focus)


def main(
    msg: EmailMessage, stage: FsmStage, *, config: ThreliumSettings
) -> EmailMessage | None:
    payload = parse_logic_validate_payload(extract_plain_body(msg))
    if payload is None:
        raise RuntimeError("logic_validate: invalid payload")

    error_kind = ""
    error_message = ""
    conforms = False
    report_text = ""
    violations = 0
    vacuous = False

    data_graph: Graph | None = None
    shapes_graph: Graph | None = None
    ont_graph: Graph | None = None

    try:
        data_graph = Graph().parse(data=payload.facts_ttl, format="turtle")
        shapes_graph = Graph().parse(data=payload.shapes_ttl, format="turtle")
        ont_graph = (
            Graph().parse(data=payload.ontology_ttl, format="turtle")
            if payload.ontology_ttl
            else None
        )
    except Exception as e:
        error_kind = "parse"
        error_message = str(e)

    if not error_kind and data_graph is not None and shapes_graph is not None:
        inference = (
            payload.inference.to_pyshacl() if payload.inference is not None else None
        )
        try:
            conforms, results_graph, raw_report = validate(
                data_graph,
                shacl_graph=shapes_graph,
                ont_graph=ont_graph,
                advanced=True,
                inference=inference,
            )
            violations = _count_violations(results_graph)
            declarations, focus_count = _standard_target_focus(
                shapes_graph, _combined_graph(data_graph, ont_graph)
            )
            vacuous = bool(conforms and declarations > 0 and focus_count == 0)
            report_text = LogicValidateReportText.parse(
                raw_report[: config.knowledge.logic_report_max_chars]
            ).value
        except (ConstraintLoadError, RuleLoadError, ShapeLoadError) as e:
            error_kind = "shape"
            error_message = str(e)
            conforms = False
        except Exception as e:
            error_kind = "runtime"
            error_message = str(e)
            conforms = False

    observation = render_prompt(
        PromptPath.LOGIC_VALIDATE_OBSERVATION,
        reasoning=payload.reasoning,
        conforms=conforms,
        error_kind=error_kind,
        error_message=error_message,
        report_text=report_text,
        violations=violations,
        vacuous=vacuous,
    ).strip()

    return build_fsm_multipart_to_stage(
        msg,
        to_addr=FsmStage.ENRICH_FAST,
        from_stage=stage,
        parts=[(EnrichPartId.OBSERVATION_NOTE, observation)],
        settings=config,
    )
