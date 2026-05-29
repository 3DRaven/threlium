# pySHACL overview (Threlium / formal_reason)

> **Threlium:** Production validation runs inside `formal_reason` via `pyshacl.validate(..., advanced=True)` — you supply TTL in tool fields. Workflows: `formal_reason_workflows.md`. SHACL-SPARQL shapes: `shacl_sparql.md`.
>
> **Source:** pySHACL @ 5b46638cadde2e32efaed0ee53fc2545d5c0a179 — https://github.com/RDFLib/pySHACL  
> **Verified stack:** pyshacl 0.31.0, rdflib 7.6.0

## What pySHACL does

Validates an RDF **data graph** against a SHACL **shapes graph**. Optional **ontology graph** supplies RDFS/OWL axioms. Returns:

- `conforms` (bool)
- validation report graph (rdflib `Graph`)
- human-readable `report_text`

Threlium maps these to the observation-note (`conforms`, `violations`, `report_text`).

## Inference modes (`inference` tool field)

| Tool value | pySHACL `inference=` | Use |
|------------|----------------------|-----|
| omitted / `none` | `None` | No expansion before validate |
| `rdfs` | `"rdfs"` | `rdfs:subClassOf`, domain/range |
| `owlrl` | `"owlrl"` | OWL 2 RL profile expansion |
| `both` | `"both"` | RDFS + OWL RL |

Set `return_derived: true` to read new triples in `derived_triples` (engine uses `delta_ttl_from_expanded`).

## How tool fields map to the engine

You never call the validator directly — the `formal_reason` stage runs it for you.
Each tool field maps to one validation input:

| Tool field | Engine input | Notes |
|------------|--------------|-------|
| `facts_ttl` | data graph | premises / entities under test |
| `shapes_ttl` | shapes graph | constraints, refutation shapes |
| `ontology_ttl` | ontology graph | RDFS/OWL axioms merged before validate |
| `inference` | `inference=` | `None` \| `rdfs` \| `owlrl` \| `both` |
| (always on) | `advanced=True` | SHACL-SPARQL (`sh:sparql`) enabled — required for refutation proofs |

CLI options (`pyshacl -m -j`, metashacl, SHACL-JS) are **not** exposed on the tool.

## Report interpretation

- `sh:ValidationResult` — each violation; count appears as `violations: N` in observation.
- Severity `sh:Violation` — must fix before treating claim as proven.
- `sh:conforms false` in report graph matches `conforms: False` in observation.

## Related packages (RDFLib family)

- [OWL-RL](https://github.com/RDFLib/OWL-RL) — inference backend
- [pySHACL](https://github.com/RDFLib/pySHACL) — validator

For executable tool-call examples see `pyshacl_examples.md`.

## In Threlium

Command-line `pyshacl` is not used on the agent path — only the Python API inside `formal_reason.py`.
