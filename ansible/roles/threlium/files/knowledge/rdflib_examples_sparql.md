# RDFLib SPARQL examples → formal_reason

> **Threlium:** Express graphs as `facts_ttl` / `shapes_ttl` and use `query` for SELECT/ASK/CONSTRUCT — not `g.query()` in Python. Workflows: `formal_reason_workflows.md`. SPARQL syntax: `sparql_functions.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — examples/sparql_query_example.py, sparql_update_example.py, prepared_query.py

## SELECT bindings (foaf:knows)

**formal_reason use:** explore (query)  
**Source:** rdflib/examples/sparql_query_example.py + foaf.n3

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "List people who know someone (FOAF knows chain)",
  "facts_ttl": "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix : <http://example.org/people#> .\n\n:alice a foaf:Person ; foaf:name \"Alice\" ; foaf:knows :bob .\n:bob a foaf:Person ; foaf:name \"Bob\" ; foaf:knows :carol .\n:carol a foaf:Person ; foaf:name \"Carol\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/shapes#> .\n\nex:Permissive a sh:NodeShape ;\n  sh:targetSubjectsOf <http://xmlns.com/foaf/0.1/knows> ;\n  sh:property [ sh:path <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ; sh:minCount 0 ] .",
  "query": "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT ?s ?name WHERE { ?s foaf:knows [] . ?s foaf:name ?name . }"
}
```

**query_result:** rows with `s=` and `name=` bindings (engine formats as `var=value | ...`).

## ASK pattern

**formal_reason use:** explore (query)

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "ASK whether :alice knows :bob",
  "facts_ttl": "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix : <http://example.org/people#> .\n\n:alice foaf:knows :bob .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix ex: <http://example.org/shapes#> .\n@prefix : <http://example.org/people#> .\n\nex:Trivial a sh:NodeShape ; sh:targetNode :alice ; sh:property [ sh:path foaf:knows ; sh:minCount 0 ] .",
  "query": "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nPREFIX : <http://example.org/people#>\nASK { :alice foaf:knows :bob . }"
}
```

**query_result:** `ask: True`

## CONSTRUCT (derive triples in observation)

**formal_reason use:** explore (query) — alternative to `return_derived` for custom projections

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "CONSTRUCT fullName from first and last",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice ex:firstName \"Alice\" ; ex:lastName \"Example\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:S a sh:NodeShape ; sh:targetClass ex:Person ; sh:property [ sh:path ex:firstName ; sh:minCount 0 ] .",
  "query": "PREFIX ex: <http://example.org/>\nCONSTRUCT { ?p ex:fullName ?full }\nWHERE {\n  ?p ex:firstName ?fn ; ex:lastName ?ln .\n  BIND(CONCAT(?fn, \" \", ?ln) AS ?full)\n}"
}
```

**Note:** SPARQL UPDATE (`INSERT`/`DELETE`) is **not** exposed on the tool — mutate `facts_ttl` between calls instead (rdflib `g.update` is engine-internal only).

## In Threlium

Prepared queries (`initBindings`) are applied by inlining values into your `query` string or binding via repeated `formal_reason` calls with different `facts_ttl`. See `rdflib_namespaces.md` for PREFIX/`initNs` concepts.
