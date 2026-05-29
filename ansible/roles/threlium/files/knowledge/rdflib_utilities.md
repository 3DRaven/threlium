# Utilities & convenience functions

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


For RDF programming, RDFLib and Python may not be the fastest tools, but we try hard to make them the easiest and most convenient to use and thus the *fastest* overall!

This is a collection of hints and pointers for hassle-free RDF coding.

## Functional properties

A *functional property* may occur only once per resource (e.g. one `foaf:age`).
To read the single value, `SELECT` it with `LIMIT 1`; to **enforce** that only one
exists, validate with a `sh:maxCount 1` shape via `formal_reason`:

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?age WHERE { <http://example.org/people/Bob> foaf:age ?age } LIMIT 1
```

```turtle
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

[] a sh:NodeShape ;
  sh:targetClass foaf:Person ;
  sh:property [ sh:path foaf:age ; sh:maxCount 1 ] .
```

## Querying by pattern

RDFLib offers a Python "slice" shortcut over a graph, but on the agent path every
access pattern is a SPARQL pattern in the `query` field. The mapping:

| Goal | SPARQL pattern |
|------|----------------|
| all triples | `SELECT ?s ?p ?o WHERE { ?s ?p ?o }` |
| everything about Bob | `SELECT ?p ?o WHERE { ex:Bob ?p ?o }` |
| Bob's `foaf:knows` objects | `SELECT ?o WHERE { ex:Bob foaf:knows ?o }` |
| does `ex:Bob foaf:knows ex:Bill` hold? | `ASK { ex:Bob foaf:knows ex:Bill }` |
| all subject–object pairs of `foaf:knows` | `SELECT ?s ?o WHERE { ?s foaf:knows ?o }` |

## SPARQL Paths

[SPARQL property paths](http://www.w3.org/TR/sparql11-property-paths/) are possible using overridden operators on URIRefs. See [`examples.foafpaths`][examples.foafpaths] and [`rdflib.paths`][rdflib.paths].

## Readable term form (N3/Turtle)

The readable representation of a term is its Turtle/N3 form. A URI is written in
angle brackets, or compactly with a declared prefix; a typed literal carries its
datatype after `^^`:

| Term | Without prefix | With prefix |
|------|----------------|-------------|
| `http://xmlns.com/foaf/0.1/Person` | `<http://xmlns.com/foaf/0.1/Person>` | `foaf:Person` |
| the integer 2 | `"2"^^<http://www.w3.org/2001/XMLSchema#integer>` | `"2"^^xsd:integer` |

This is exactly what you write inside `facts_ttl` / `shapes_ttl` and what the
`report_text` / `query_result` sections show back.

## Providing data as a string

You provide all data as a Turtle string in `facts_ttl` — there is no separate
`parse(data=...)` step. Even a single triple is just text:

```turtle
<a:> <p:> <p:> .
```

## Command Line tools

RDFLib's command-line tools are not exposed on the agent path.

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
