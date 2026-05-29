# SPARQL query patterns (derwen ex4) → formal_reason

> **Threlium:** Put the RDF model in `facts_ttl`, SPARQL in `query`, permissive or real shapes in `shapes_ttl`. Read bindings from `query_result` in the observation — not from a Python loop over `g.query()`.
>
> **Source:** https://derwen.ai/docs/kgl/ex4_0/ (adapted; kglab/pandas/pyvis removed)  
> **Verified stack:** rdflib 7.6.0

## FOAF directory (SELECT)

**formal_reason use:** explore (query)  
**Source:** derwen ex4_0 — FOAF Turtle + ORDER BY surname

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "List surnames and mailboxes sorted descending by surname",
  "facts_ttl": "@prefix : <http://www.w3.org/2012/12/rdf-val/SOTA-ex#> .\n@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n\n:peep0 a foaf:Person ;\n  foaf:givenName \"Alice\" ;\n  foaf:familyName \"Nakamoto\" ;\n  foaf:mbox <mailto:alice@example.org> .\n\n:peep1 a foaf:Person ;\n  foaf:givenName \"Bob\" ;\n  foaf:familyName \"Patel\" ;\n  foaf:mbox <mailto:bob@example.org> .\n\n:peep2 a foaf:Person ;\n  foaf:givenName \"Dhanya\" ;\n  foaf:familyName \"O'Neill\" ;\n  foaf:mbox <mailto:dhanya@example.org> .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix ex: <http://example.org/shapes#> .\n\nex:PersonShape a sh:NodeShape ;\n  sh:targetClass foaf:Person ;\n  sh:property [ sh:path foaf:familyName ; sh:minCount 0 ] .",
  "query": "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT ?person ?surname ?email\nWHERE {\n  ?person foaf:familyName ?surname .\n  ?person foaf:mbox ?email .\n}\nORDER BY DESC(?surname)"
}
```

**query_result:** three rows; `Patel` before `O'Neill` before `Nakamoto`.

## SPARQL + post-processing (reasoning loop)

Upstream ex4 annotates a recipe KG in Python after SELECT. In Threlium:

1. `formal_reason` with `query` to list candidate entities.
2. Reasoning model reads `query_result` bindings.
3. Update `facts_ttl` with new triples (e.g. `ex:alice a ex:Noodle .`).
4. Call `formal_reason` again with shapes that validate annotations.

Do **not** use `memory_query` for entities you authored in the same proof — only for project docs.

## Recipe-style multi-pattern SELECT (simplified)

**formal_reason use:** explore

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "Find ex:Recipe nodes with ex:hasIngredient ex:Egg",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n@prefix wtm: <http://purl.org/heals/food/> .\n@prefix ind: <http://purl.org/heals/ingredient/> .\n\nex:r1 a wtm:Recipe ;\n  wtm:hasIngredient ind:ChickenEgg ;\n  wtm:hasCookTime \"PT1H\"^^<http://www.w3.org/2001/XMLSchema#duration> .\n\nex:r2 a wtm:Recipe ;\n  wtm:hasIngredient ind:Flour .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix wtm: <http://purl.org/heals/food/> .\n@prefix ex: <http://example.org/shapes#> .\n\nex:R a sh:NodeShape ; sh:targetClass wtm:Recipe ; sh:property [ sh:path wtm:hasIngredient ; sh:minCount 0 ] .",
  "query": "PREFIX wtm: <http://purl.org/heals/food/>\nPREFIX ind: <http://purl.org/heals/ingredient/>\nSELECT ?recipe ?time\nWHERE {\n  ?recipe a wtm:Recipe .\n  ?recipe wtm:hasIngredient ind:ChickenEgg .\n  ?recipe wtm:hasCookTime ?time .\n}"
}
```

## In Threlium

SPARQL language reference: `sparql_functions.md`. Tool workflows: `formal_reason_workflows.md`.
