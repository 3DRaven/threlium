# formal_reason workflows (Threlium tool contract)

> **Threlium:** This is the primary guide for the `formal_reason` tool. Retrieve it via `memory_query` when authoring shapes, debugging observations, or planning a proof loop. For Turtle/SPARQL syntax see `turtle_syntax.md`, `sparql_functions.md`, `shacl_sparql.md`. For RDFLib internals see `rdflib_*.md` — the engine runs rdflib/pySHACL for you.
>
> **Verified stack:** rdflib 7.6.0, pyshacl 0.31.0 — same pipeline as [`formal_reason.py`](../scripts/threlium/states/formal_reason.py).

## Tool fields (JSON body)

| Field | Required | Role |
|-------|----------|------|
| `reasoning` | yes | What you are proving/checking (shown in observation) |
| `facts_ttl` | yes | Data graph (premises, entities under test) |
| `shapes_ttl` | yes | SHACL shapes (constraints, refutation shapes) |
| `ontology_ttl` | no | RDFS/OWL axioms merged with facts before validation |
| `inference` | no | `none` (default) \| `rdfs` \| `owlrl` \| `both` — expands graph before validate/query |
| `return_derived` | no | If true and inference set, observation includes `derived_triples` TTL delta |
| `query` | no | SPARQL SELECT/ASK/CONSTRUCT on facts+ontology (expanded if inference set) |

**Do not** put project documentation lookup in `query` — use `memory_query` for the LightRAG graph.

## Observation shape (what you read back)

After the stage runs, enrich_fast relays an observation-note:

- `conforms: true|false`, `violations: N`
- A reminder on `conforms=true` that you must confirm your `sh:target*` actually matched nodes (the tool does NOT detect vacuous passes — count your target class with a `query`)
- `report_text` — pySHACL human report when non-conformant
- `derived_triples` — optional inference delta
- `query_result` — bindings or `ask: true/false`
- Errors: `PARSE ERROR`, `SHAPE LOAD ERROR`, `QUERY ERROR`, `RUNTIME ERROR` — then `memory_query` the named reference doc and retry

---

### Scenario: Property constraint (conforms)

**formal_reason use:** invariant  
**Source:** e2e formal_reason_chain stub

<!-- expect: conforms=true violations=0 -->

#### Tool call

```json
{
  "reasoning": "Every Person must have a non-negative ex:age",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person ;\n  ex:age 30 .\n\nex:bob a ex:Person ;\n  ex:age 25 .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:PositiveAgeShape a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:property [\n    sh:path ex:age ;\n    sh:minInclusive 0 ;\n    sh:message \"Age must be non-negative\" ;\n  ] .",
  "inference": "none"
}
```

#### Expected observation (abbreviated)

```
conforms: True
violations: 0
```

---

### Scenario: Proof by refutation (sh:sparql)

**formal_reason use:** prove  
**Source:** `shacl_sparql.md` — encode negation as violating SELECT

<!-- expect: conforms=true violations=0 -->

#### Tool call

```json
{
  "reasoning": "Prove: if ex:parentOf holds, child age is less than parent age",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person ; ex:age 40 .\nex:bob a ex:Person ; ex:age 30 ; ex:parent ex:alice .\nex:alice ex:parentOf ex:bob .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n@prefix ex: <http://example.org/> .\n\nex:ChildYoungerThanParent a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:sparql [\n    sh:message \"Child must be younger than parent\" ;\n    sh:prefixes [\n      sh:declare [ sh:prefix \"ex\" ; sh:namespace \"http://example.org/\"^^xsd:anyURI ]\n    ] ;\n    sh:select \"\"\"\n      SELECT $this\n      WHERE {\n        $this ex:parent ?p .\n        $this ex:age ?childAge .\n        ?p ex:age ?parentAge .\n        FILTER (?childAge >= ?parentAge)\n      }\n    \"\"\" ;\n  ] .",
  "inference": "none"
}
```

#### Expected observation

```
conforms: True
violations: 0
```

Refutation shape matches only when the claim **fails**; zero matches ⇒ conforms.

---

### Scenario: Counterexample (conforms false)

**formal_reason use:** refute  
**Source:** adapted from pySHACL schema.org example

<!-- expect: conforms=false violations=1 -->

#### Tool call

```json
{
  "reasoning": "Detect birthDate after deathDate (schema.org ordering)",
  "facts_ttl": "@prefix schema: <http://schema.org/> .\n\n<http://example.org/ns#Bob> a schema:Person ;\n  schema:givenName \"Robert\" ;\n  schema:birthDate \"1971-07-07\" ;\n  schema:deathDate \"1968-09-10\" .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix schema: <http://schema.org/> .\n\nschema:PersonDateShape a sh:NodeShape ;\n  sh:targetClass schema:Person ;\n  sh:property [\n    sh:path schema:birthDate ;\n    sh:lessThan schema:deathDate ;\n  ] .",
  "inference": "none"
}
```

#### Expected observation

```
conforms: False
violations: 1
```

Read `report_text` for `sh:LessThanConstraintComponent` focus node.

---

### Scenario: Vacuous validation (you must check coverage)

**formal_reason use:** pitfall — not a proof  
**Source:** target/facts mismatch — the tool does NOT warn, you verify

The shape targets `ex:Widget`, but the facts only mention `ex:Person`. No focus
node matches, so SHACL reports `conforms: True` — yet nothing was actually checked.
The tool does not flag this; you must confirm coverage. Add a `query` counting
instances of your target class: `0 rows` means the pass was vacuous.

<!-- expect: conforms=true violations=0 query_nonempty=true -->

#### Tool call

```json
{
  "reasoning": "Targets ex:Widget but facts only mention ex:Person — count targets to detect a vacuous pass",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:WidgetShape a sh:NodeShape ;\n  sh:targetClass ex:Widget ;\n  sh:property [ sh:path ex:name ; sh:minCount 1 ] .",
  "query": "PREFIX ex: <http://example.org/>\nSELECT (COUNT(?n) AS ?c) WHERE { ?n a ex:Widget }",
  "inference": "none"
}
```

#### Expected observation

```
conforms: True
violations: 0
conforms=true means no targeted node violated the shapes. ... confirm coverage yourself ...
---
query_result:
(1 rows)
c=0
```

`c=0` proves the pass was vacuous — nothing matched `ex:Widget`. Fix
`sh:targetClass` / prefixes, or enable `inference` if subclasses should match,
before treating `conforms=true` as a result.

---

### Scenario: SPARQL query on your graph

**formal_reason use:** explore (query)  
**Source:** derwen ex4_0 (FOAF), via `query` field

<!-- expect: conforms=true violations=0 query_nonempty=true -->

#### Tool call

```json
{
  "reasoning": "List family names and mailboxes from the authored FOAF graph",
  "facts_ttl": "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n@prefix : <http://example.org/people#> .\n\n:alice a foaf:Person ;\n  foaf:familyName \"Nakamoto\" ;\n  foaf:mbox <mailto:alice@example.org> .\n\n:bob a foaf:Person ;\n  foaf:familyName \"Patel\" ;\n  foaf:mbox <mailto:bob@example.org> .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/shapes#> .\n\nex:NoOp a sh:NodeShape ;\n  sh:targetClass <http://www.w3.org/2002/07/owl#Thing> ;\n  sh:property [ sh:path <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ; sh:minCount 0 ] .",
  "query": "PREFIX foaf: <http://xmlns.com/foaf/0.1/>\nSELECT ?person ?surname ?email\nWHERE {\n  ?person foaf:familyName ?surname .\n  ?person foaf:mbox ?email .\n}\nORDER BY DESC(?surname)"
}
```

#### Expected observation (query_result excerpt)

```
query_result:
person=... | surname=Patel | email=...
person=... | surname=Nakamoto | email=...
```

Use a minimal permissive shape so validation does not block; focus on `query_result`.

---

### Scenario: Inference + derived triples

**formal_reason use:** inference delta  
**Source:** RDFS subclass pattern

<!-- expect: conforms=true violations=0 derived_nonempty=true -->

#### Tool call

```json
{
  "reasoning": "Infer ex:alice rdf:type ex:Animal via rdfs:subClassOf",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\nex:alice a ex:Dog .",
  "ontology_ttl": "@prefix ex: <http://example.org/> .\n@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\nex:Dog rdfs:subClassOf ex:Animal .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:AnimalShape a sh:NodeShape ;\n  sh:targetClass ex:Animal ;\n  sh:property [ sh:path <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ; sh:minCount 1 ] .",
  "inference": "rdfs",
  "return_derived": true
}
```

#### Expected observation

```
conforms: True
---
derived_triples (inference delta):
... ex:alice ... ex:Animal ...
```

---

### Scenario: Parse error (bad Turtle)

**formal_reason use:** error recovery — `error_kind=parse`  
**Source:** missing terminating `.` in `facts_ttl`

<!-- expect: error_kind=parse -->

#### Tool call

```json
{
  "reasoning": "facts_ttl is missing the terminating dot",
  "facts_ttl": "@prefix ex: <http://example.org/> .\nex:alice a ex:Person",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:PersonShape a sh:NodeShape ;\n  sh:targetClass ex:Person .",
  "inference": "none"
}
```

#### Expected observation

```
PARSE ERROR (Turtle): ...
```

Fix the Turtle syntax (close every statement with `.`, declare every `@prefix`) and retry.

---

### Scenario: Shape load error (malformed constraint)

**formal_reason use:** error recovery — `error_kind=shape`  
**Source:** `sh:minCount` given a non-integer literal

<!-- expect: error_kind=shape -->

#### Tool call

```json
{
  "reasoning": "sh:minCount must be an xsd:integer, not a string",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:PersonShape a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:property [ sh:path ex:age ; sh:minCount \"abc\" ] .",
  "inference": "none"
}
```

#### Expected observation

```
SHAPE LOAD ERROR (SHACL): ... sh:minCount must be a literal with datatype xsd:integer ...
```

The shape itself is malformed — fix the constraint definition (here use `sh:minCount 1`) and retry.

---

### Scenario: Runtime error (undeclared prefix in sh:select)

**formal_reason use:** error recovery — `error_kind=runtime`  
**Source:** `sh:sparql` query references a prefix bound nowhere

<!-- expect: error_kind=runtime -->

#### Tool call

```json
{
  "reasoning": "sh:select uses prefix foo: that is not declared anywhere",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:PersonShape a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:sparql [ sh:select \"SELECT $this WHERE { $this foo:bar ?x }\" ] .",
  "inference": "none"
}
```

#### Expected observation

```
RUNTIME ERROR (validation): Unknown namespace prefix : foo
```

Declare the prefix via `sh:prefixes`/`sh:declare` (see `shacl_sparql.md`), or bind it in the data graph, then retry.

---

### Scenario: Query error (validation holds, query fails)

**formal_reason use:** supplemental error — `error_kind=query`  
**Source:** `query` uses a prefix that rdflib does not auto-bind

<!-- expect: conforms=true error_kind=query -->

#### Tool call

```json
{
  "reasoning": "SELECT uses an undeclared prefix zz: — validation still holds",
  "facts_ttl": "@prefix ex: <http://example.org/> .\n\nex:alice a ex:Person .",
  "shapes_ttl": "@prefix sh: <http://www.w3.org/ns/shacl#> .\n@prefix ex: <http://example.org/> .\n\nex:PersonShape a sh:NodeShape ;\n  sh:targetClass ex:Person ;\n  sh:property [ sh:path ex:age ; sh:minCount 0 ] .",
  "query": "SELECT ?p WHERE { ?p a zz:Person }"
}
```

#### Expected observation

```
conforms: True
violations: 0
...
QUERY ERROR (SPARQL): Unknown namespace prefix : zz
```

A `QUERY ERROR` is supplemental — the SHACL result above still holds. Add the missing `PREFIX` to the query and retry.

---

### Scenario: When NOT to use formal_reason

| Need | Route | Why |
|------|-------|-----|
| Project file path, FSM route, config | `memory_query` | LightRAG graph, not your TTL |
| Turtle/SHACL syntax help | `memory_query` → `turtle_syntax.md` / `shacl_sparql.md` | Reference docs |
| Trivial answer already in `<knowledge_graph>` | `response_finalize` | No proof required |
| Remote SPARQL endpoint | Not supported in tool | `formal_reason` only runs on in-memory graphs you author |

## Engine mapping

1. `parse_formal_reason_payload` → `FormalReasonStagePayload`
2. `parse_formal_reason_graphs(facts_ttl, shapes_ttl, ontology_ttl)`
3. `pyshacl.validate(..., advanced=True, inference=...)` (single run; when inference feeds derived/query it runs in place on a baseline copy)
4. Optional `delta_ttl_from_expanded` when `return_derived`
5. Optional `query_graph_ttl` on the validated graph when `query` set

See also: `pyshacl_overview.md`, `pyshacl_examples.md`, `patterns_sparql_query.md`, `patterns_shacl_validate.md`.
