# Querying with SPARQL

> **Threlium:** Use `memory_query` to retrieve this doc from the knowledge graph. Use `formal_reason` with `facts_ttl`/`shapes_ttl` to validate or run SPARQL on *your* graph (`query` is not LightRAG). See also `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — https://github.com/RDFLib/rdflib/tree/0fdac395078b81313f30eeab74b1a18eb7bb4db6/docs
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0


## Run a Query

The engine implements [SPARQL 1.1 Query](http://www.w3.org/TR/sparql11-query/).
Put your premises in `facts_ttl` and the query in the `formal_reason` `query`
field; the bindings come back under `query_result` in the observation.

For `SELECT` you read variable bindings; for `CONSTRUCT`/`DESCRIBE` you read
triples; for `ASK` you read a single `ask: true/false`. Always declare your
prefixes with `PREFIX` at the top of the query. For example, "who knows whom":

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT DISTINCT ?aname ?bname
WHERE {
  ?a foaf:knows ?b .
  ?a foaf:name ?aname .
  ?b foaf:name ?bname .
}
```

The `query_result` excerpt would then list pairs such as
`aname=Timothy Berners-Lee | bname=Edd Dumbill`. Prefer SPARQL `PREFIX`
directives over any host-side namespace binding — see
[namespaces_and_bindings](rdflib_namespaces.md).

## No UPDATE / DELETE on the agent path

SPARQL Update (`INSERT DATA`, `DELETE`/`INSERT … WHERE`) mutates a store in place.
`formal_reason` does **not** mutate: you author the final intended graph in
`facts_ttl`. To compute a *transformed view* of your data, use a read-only
`CONSTRUCT` in the `query` field and read the constructed triples from the
observation. For example, re-typing instances of `<c:>` as `<d:>`:

```sparql
CONSTRUCT { ?s a <d:> }
WHERE     { ?s a <c:> }
```

## No remote SPARQL service

SPARQL 1.1's `SERVICE` keyword federates to a remote endpoint. Threlium does not
reach remote endpoints — every query runs on the in-memory graph you supplied in
`facts_ttl` (optionally expanded by `inference`). Author the data locally instead
of querying DBPedia or another service.

## Variable binding

There is no `prepareQuery` / `initBindings` on the tool. Bind a focus value by
writing it directly into the query pattern (e.g. a concrete IRI instead of a
variable), or filter with `FILTER`/`VALUES`:

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?s
WHERE { <http://www.w3.org/People/Berners-Lee/card#i> foaf:knows ?s }
```

## In Threlium

The model does **not** call `rdflib.Graph()` directly in production. Author Turtle in the `formal_reason` tool fields (`facts_ttl`, `shapes_ttl`, optional `ontology_ttl`) and read the observation (`conforms`, `violations`, `query_result`, `derived_triples`). End-to-end tool-call patterns: `formal_reason_workflows.md`. SPARQL language reference for the `query` field: `sparql_functions.md`.
