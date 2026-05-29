# Merging graphs

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


Graphs share blank nodes only if they are derived from graphs described by documents or other structures (such as an RDF dataset) that explicitly provide for the sharing of blank nodes between different RDF graphs. Simply downloading a web document does not mean that the blank nodes in a resulting RDF graph are the same as the blank nodes coming from other downloads of the same document or from the same RDF source.

RDF applications which manipulate concrete syntaxes for RDF which use blank node identifiers should take care to keep track of the identity of the blank nodes they identify. Blank node identifiers often have a local scope, so when RDF from different sources is combined, identifiers may have to be changed in order to avoid accidental conflation of distinct blank nodes.

For example, two documents may both use the blank node identifier "_:x" to identify a blank node, but unless these documents are in a shared identifier scope or are derived from a common source, the occurrences of "_:x" in one document will identify a different blank node than the one in the graph described by the other document. When graphs are formed by combining RDF from multiple sources, it may be necessary to standardize apart the blank node identifiers by replacing them by others which do not occur in the other document(s).

_(copied directly from <https://www.w3.org/TR/rdf11-mt/#shared-blank-nodes-unions-and-merges>_

In RDFLib, blank nodes are given unique IDs when parsing, so merging happens by
parsing several sources into one graph. In Threlium you achieve the same by
**concatenating Turtle** within a single field: put all premises in `facts_ttl`
(and any RDFS/OWL axioms in `ontology_ttl`). The engine parses the combined text
into one graph before validation, assigning fresh IDs to blank nodes.

!!! warning "Blank Node Collision"
    Set-theoretic graph operations assume shared blank node IDs and therefore do
    NOT perform a *correct* merge: naively unioning two graphs that both use, say,
    `_:x` can conflate distinct blank nodes. Because you author one combined
    `facts_ttl`, give blank nodes you mean to keep distinct their own labels
    (`_:a`, `_:b`) or use the `[ ... ]` inline form, which is always a fresh node.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
