# Navigating Graphs

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


An RDF Graph is a set of RDF triples, and we try to mirror exactly this in RDFLib. The Python [`Graph`][rdflib.graph.Graph] tries to emulate a container type.

## Graphs as sets of triples

A graph is an unordered set of *Subject Predicate Object* statements. The engine
iterates it for you during validation and SPARQL; you only need to author the
triples as Turtle.

## Contains check

To ask whether a fact is present, use a SPARQL `ASK` in the `query` field rather
than a Python `in` check. "Is Bob a person?":

```sparql
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
ASK { <http://example.org/people/bob> rdf:type foaf:Person }
```

A pattern can be partially bound — use a variable as a wildcard. "Are there any
triples about Bob?":

```sparql
ASK { <http://example.org/people/bob> ?p ?o }
```

The observation reports `ask: true` / `ask: false` under `query_result`.

## Set Operations on RDFLib Graphs

Graphs override several pythons operators: [`__iadd__()`][rdflib.graph.Graph.__iadd__], [`__isub__()`][rdflib.graph.Graph.__isub__], etc. This supports addition, subtraction and other set-operations on Graphs:

| operation | effect |
|-----------|--------|
| `G1 + G2` | return new graph with union (triples on both) |
| `G1 += G2` | in place union / addition |
| `G1 - G2` | return new graph with difference (triples in G1, not in G2) |
| `G1 -= G2` | in place difference / subtraction |
| `G1 & G2` | intersection (triples in both graphs) |
| `G1 ^ G2` | xor (triples in either G1 or G2, but not in both) |

!!! warning
    Set-operations on graphs assume Blank Nodes are shared between graphs. This may or may not be what you want. See [merging](rdflib_merging.md) for details.

## Basic Triple Matching

Triple pattern matching — restricting subject, predicate and/or object, with the
rest left as wildcards — is expressed in SPARQL. A variable is the wildcard.

Find all people:

```sparql
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?s WHERE { ?s rdf:type foaf:Person }
```

Find every subject's type (predicate fixed, subject and object free):

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s ?o WHERE { ?s rdf:type ?o }
```

For a *functional* property where only one value is expected (e.g. a single
`foaf:name`), select it with `LIMIT 1`, or enforce single-valuedness with a
`sh:maxCount 1` shape (see [shacl_sparql.md](shacl_sparql.md)):

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?name WHERE { <http://example.org/people/bob> foaf:name ?name } LIMIT 1
```

## Querying the graph

All access patterns — by subject, predicate, object, or combinations — reduce to
SPARQL `SELECT` / `ASK` / `CONSTRUCT` in the `query` field. There is no separate
`triples()` / `subjects()` / `objects()` accessor on the agent path.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
