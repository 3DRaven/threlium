# pySHACL examples → formal_reason

> **Threlium:** Each scenario is a `formal_reason` JSON payload (not a standalone Python script). Upstream `validate(file_path)` becomes inline Turtle in `facts_ttl` / `shapes_ttl`.
>
> **Source:** pySHACL @ 5b46638cadde2e32efaed0ee53fc2545d5c0a179 — examples/example.py, two_file_example.py, sparql_assert_datatype.py

## schema.org Person (violations)

**formal_reason use:** refute / data quality  
**Source:** pySHACL/examples/two_file_example.py (Turtle shapes + data adapted)

<!-- expect: conforms=false violations=1 -->

```json
{
  "reasoning": "birthDate must be less than deathDate",
  "facts_ttl": "@prefix schema: <http://schema.org/> .\n\n<http://example.org/ns#Bob> a schema:Person ;\n  schema:givenName \"Robert\" ;\n  schema:birthDate \"1971-07-07\" ;\n  schema:deathDate \"1968-09-10\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix schema: <http://schema.org/> .\n\nschema:PersonDateShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [\n    sh:path schema:birthDate ;\n    sh:lessThan schema:deathDate ;\n  ] .",
  "inference": "none"
}
```

## Fixed data (conforms)

**formal_reason use:** invariant after fix

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "Correct date ordering",
  "facts_ttl": "@prefix schema: <http://schema.org/> .\n\n<http://example.org/ns#Bob> a schema:Person ;\n  schema:birthDate \"1968-09-10\" ;\n  schema:deathDate \"1971-07-07\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix schema: <http://schema.org/> .\n\nschema:PersonDateShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [ sh:path schema:birthDate ; sh:lessThan schema:deathDate ] ."
}
```

## SHACL-SPARQL datatype assert

**formal_reason use:** invariant  
**Source:** pySHACL/examples/sparql_assert_datatype.py (pattern)

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "ex:age must be xsd:integer",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\nex:alice a ex:Person ; ex:age 30 .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix ex: <http://example.org/> .\n\nex:AgeInteger a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:sparql [\n    sh:message \"age must be integer\" ;\n    sh:select \"\"\"\n      SELECT $this\n      WHERE {\n        $this ex:age ?v .\n        FILTER (!isLiteral(?v) || datatype(?v) != xsd:integer)\n      }\n    \"\"\" ;\n  ] ."
}
```

## Rules / advanced (upstream only)

`rules_inference.py` and `advanced.py` use pySHACL features (SHACL rules, advanced) beyond what `formal_reason` exposes. Model rule-like checks as standard `sh:NodeShape` / `sh:sparql` in `shapes_ttl` instead.

## Remote shapes (not in tool)

`remote_sparql.py` loads shapes from HTTP — **not** supported. Inline all shapes in `shapes_ttl`; use `memory_query` to fetch project docs, not remote SHACL URLs inside validation.

## In Threlium

See `patterns_shacl_validate.md` for validation-report SPARQL via a second `formal_reason` call with `query`.
