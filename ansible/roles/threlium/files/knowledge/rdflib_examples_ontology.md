# RDFLib OWL/RDFS examples → formal_reason

> **Threlium:** Put `rdfs:subClassOf`, `owl:equivalentClass`, etc. in `ontology_ttl`; use `inference: rdfs` or `owlrl` on the tool. Workflows: `formal_reason_workflows.md`.
>
> **Source:** RDFLib @ 0fdac395078b81313f30eeab74b1a18eb7bb4db6 — examples/infixowl_ontology_creation.py, transitive.py (patterns)

## Subclass entailment (RDFS)

**formal_reason use:** inference delta  
**Source:** OWL-RL / pySHACL inference

<!-- expect: conforms=true violations=0 derived_nonempty=true -->

```json
{
  "reasoning": "Infer type Animal for ex:spot via Dog subclass",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:spot a ex:Dog .",
  "ontology_ttl": "@prefix ex: <http://example.org/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\nex:Dog rdfs:subClassOf ex:Animal .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:MustBeAnimal a sh:NodeShape ;\n  sh:targetClass ex:Animal ;\n  sh:property [ sh:path <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ; sh:minCount 1 ] .",
  "inference": "rdfs",
  "return_derived": true
}
```

## Transitive property (model + query)

**formal_reason use:** explore — check reachability with SPARQL property path  
**Source:** rdflib/examples/transitive.py (pattern)

<!-- expect: conforms=true violations=0 query_nonempty=true -->

```json
{
  "reasoning": "ex:ancestor+ path reaches ex:grandparent from ex:child",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:child ex:ancestor ex:parent .\nex:parent ex:ancestor ex:grandparent .",
  "ontology_ttl": "@prefix ex: <http://example.org/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\nex:ancestor rdfs:subPropertyOf ex:ancestor .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:T a sh:NodeShape ; sh:targetNode ex:child ; sh:property [ sh:path ex:ancestor ; sh:minCount 1 ] .",
  "query": "PREFIX ex: <http://example.org/>\nASK { ex:child ex:ancestor+ ex:grandparent . }"
}
```

## In Threlium

Heavy OWL constructs may need `inference: owlrl` or `both`. The tool does not warn about vacuous passes: after `conforms=true`, confirm your `sh:target*` matched nodes (count the target class with a `query`) — see the vacuous scenario in `formal_reason_workflows.md`.
