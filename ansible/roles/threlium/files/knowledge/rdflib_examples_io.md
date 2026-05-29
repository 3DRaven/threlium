# RDFLib serialization / parsing → formal_reason

> **Threlium:** The tool accepts **Turtle strings** in `facts_ttl`, `shapes_ttl`, `ontology_ttl` only (not file paths). Author Turtle directly; parsing happens inside `parse_formal_reason_graphs`. See `rdflib_parsing.md` for format notes.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — examples/jsonld_serialization.py, resource_example.py

## Turtle facts (preferred)

**formal_reason use:** all scenarios use inline Turtle in JSON fields.

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "Minimal valid Turtle round-trip through engine parse",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:item a ex:Thing ; ex:label \"ok\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:ThingShape a sh:NodeShape ;\n  sh:targetClass ex:Thing ;\n  sh:property [ sh:path ex:label ; sh:minCount 1 ] ."
}
```

## JSON-LD in upstream examples

pySHACL examples often use JSON-LD strings; **formal_reason expects Turtle**. Convert to Turtle mentally or via a one-off parse — do not pass JSON-LD in tool fields unless you add a conversion step outside the FSM.

## Security note

Do not use `g.parse("http://...")` patterns from upstream docs. The engine only parses strings you supply; no network fetch.

## In Threlium

`<!-- verify:skip -->` — engine-internal serialize output is not returned in observations except `derived_triples` and CONSTRUCT `query_result`.
