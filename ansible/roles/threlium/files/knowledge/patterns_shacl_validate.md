# SHACL validation patterns (derwen ex5) → formal_reason

> **Threlium:** Validate with `facts_ttl` + `shapes_ttl`; inspect violations in observation `report_text`, or run a follow-up call with `query` over the validation report graph (engine does not return report graph separately — use `report_text` or query your facts).
>
> **Source:** https://derwen.ai/docs/kgl/ex5_0/ (adapted; kglab removed)  
> **Verified stack:** pyshacl 0.31.0

## schema.org Person + Address (violations)

**formal_reason use:** refute  
**Source:** derwen ex5_0 / pySHACL two_file_example

<!-- expect: conforms=false violations=4 -->

```json
{
  "reasoning": "Validate Person and nested Address constraints",
  "facts_ttl": "@prefix schema: <http://schema.org/> .\n\n<http://example.org/ns#Bob> a schema:Person ;\n  schema:givenName \"Robert\" ;\n  schema:birthDate \"1971-07-07\" ;\n  schema:deathDate \"1968-09-10\" ;\n  schema:address <http://example.org/ns#BobsAddress> .\n\n<http://example.org/ns#BobsAddress> schema:streetAddress \"1600 Amphitheatre Pkway\" ;\n  schema:postalCode 9404 .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix schema: <http://schema.org/> .\n\nschema:PersonShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [\n    sh:path schema:birthDate ;\n    sh:lessThan schema:deathDate ;\n  ] ;\n  sh:property [\n    sh:path schema:address ;\n    sh:node schema:AddressShape ;\n  ] .\n\nschema:AddressShape a sh:NodeShape ;\n  sh:closed true ;\n  sh:property [\n    sh:path schema:postalCode ;\n    sh:datatype xsd:integer ;\n    sh:minInclusive 10000 ;\n    sh:maxInclusive 99999 ;\n  ] .",
  "inference": "rdfs"
}
```

**report_text:** multiple violations (birthDate ordering, address node, postal code range, etc.) — `violations: 4` typical with `inference: rdfs`.

## After fixing data

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "Fixed dates and postal code",
  "facts_ttl": "@prefix schema: <http://schema.org/> .\n\n<http://example.org/ns#Bob> a schema:Person ;\n  schema:birthDate \"1968-09-10\" ;\n  schema:deathDate \"1971-07-07\" ;\n  schema:address <http://example.org/ns#BobsAddress> .\n\n<http://example.org/ns#BobsAddress> schema:postalCode 94040 .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix schema: <http://schema.org/> .\n\nschema:PersonShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [ sh:path schema:birthDate ; sh:lessThan schema:deathDate ] ;\n  sh:property [ sh:path schema:address ; sh:node schema:AddressShape ] .\n\nschema:AddressShape a sh:NodeShape ;\n  sh:property [ sh:path schema:postalCode ; sh:datatype xsd:integer ; sh:minInclusive 10000 ; sh:maxInclusive 99999 ] .",
  "inference": "rdfs"
}
```

## Query validation results (pattern)

Upstream ex5 queries `sh:ValidationResult` from the report graph. In Threlium the first call's `report_text` is usually enough; for structured extraction, add triples to `facts_ttl` that mirror results you care about, or use `query` on an expanded graph after fixing data.

**formal_reason use:** explore — ASK whether any ex:Person lacks ex:verified

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "ASK if any Person lacks ex:verified flag",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person ; ex:verified true .\nex:bob a ex:Person .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:Permissive a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:property [ sh:path ex:verified ; sh:minCount 0 ] .",
  "query": "PREFIX ex: <http://example.org/>\nASK { ?p a ex:Person . FILTER NOT EXISTS { ?p ex:verified true } }"
}
```

**query_result:** `ask: True` (bob unverified)

## Recipe KG shapes (conceptual)

ex5 also validates a large recipe KG with `sh:minCount` / `sh:maxLength` — same tool pattern: encode `wtm:Recipe` targets in `shapes_ttl`, facts in `facts_ttl`, read violation count. No `kglab.validate()` — only `formal_reason`.

## In Threlium

Layer stack reminder (ex5): SKOS classification, SHACL requirements, OWL concepts, RDF facts — all can live across `ontology_ttl` + `facts_ttl` + `shapes_ttl`. See `formal_reason_workflows.md`.
