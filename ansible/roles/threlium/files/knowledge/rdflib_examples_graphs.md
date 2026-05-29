# RDFLib graph patterns → formal_reason

> **Threlium:** Merge ontologies into `ontology_ttl`, facts into `facts_ttl`. The engine's `combined_graph` matches `data + ont` before validation. Workflows: `formal_reason_workflows.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — examples/simple_example.py, datasets.py (conceptual)

## Basic typed nodes (FOAF Person)

**formal_reason use:** invariant  
**Source:** rdflib/examples/simple_example.py

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "Every foaf:Person must have foaf:name",
  "facts_ttl": "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix : <http://example.org/people#> .\n\n:donna a foaf:Person ;\n  foaf:nick \"donna\" ;\n  foaf:name \"Donna Fales\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix ex: <http://example.org/shapes#> .\n\nex:PersonName a sh:NodeShape ;\n  sh:targetClass foaf:Person ;\n  sh:property [ sh:path foaf:name ; sh:minCount 1 ; sh:datatype <http://www.w3.org/2001/XMLSchema#string> ] ."
}
```

## Merging ontology + facts (subClassOf)

**formal_reason use:** prove with `inference: rdfs`  
**Source:** rdflib/examples — same pattern as `formal_reason_workflows.md` inference scenario

<!-- expect: conforms=true violations=0 -->

```json
{
  "reasoning": "Dog is subclass of Animal; instance typing should validate against Animal shape when inference on",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:spot a ex:Dog .",
  "ontology_ttl": "@prefix ex: <http://example.org/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\nex:Dog rdfs:subClassOf ex:Animal .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:AnimalShape a sh:NodeShape ;\n  sh:targetClass ex:Animal ;\n  sh:property [ sh:path <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ; sh:minCount 1 ] .",
  "inference": "rdfs"
}
```

## Named graphs / Dataset

RDFLib `Dataset` with multiple named graphs is **not** mirrored in `formal_reason` (single merged graph). Model separate contexts as distinct IRIs or separate `formal_reason` invocations per scenario.

## In Threlium

See `rdflib_merging.md` for how `+=` relates to splitting `facts_ttl` vs `ontology_ttl`.
