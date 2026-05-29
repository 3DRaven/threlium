# FSM routes available from reasoning

The `reasoning@localhost` stage chooses exactly one tool call per turn. Each tool maps to a mailbox stage (`ROUTE_TO_ADDRESS` in `states/reasoning.py`).

## Knowledge and memory (same LightRAG graph)

| Route | Stage chain | Use |
|-------|-------------|-----|
| `memory_query` | `memory_query@` → `enrich_fast@` → `reasoning@` | Targeted graph lookup: facts, relations, docs (`knowledge/*.md`), past notes. Cheap. Use when context blocks lack a fact but it may exist in the graph. |
| `reflect` | `reflect@` → `ingress@` → `enrich@` → `reasoning@` | Broad re-enrich with new formulation. Use when several targeted `memory_query` calls cannot connect entities. |
| `thread_memory` | `thread_memory@` → `ingress@` → … | Store a fact for this dialog thread (indexed into graph). |
| `global_memory` | `global_memory@` → `ingress@` → … | Store a cross-thread fact (indexed into graph). |

Context blocks `<knowledge_graph>`, `<thread_memory>`, `<global_memory>` are one enrich sample — absence there does not mean absence in the graph.

## Discovery and execution

| Route | Stage chain | Use |
|-------|-------------|-----|
| `cli_intent` | `cli_intent@` → `cli_exec@` / HITL / deny → `ingress@` | Shell on agent host: read-only discovery (`rg`, `find`, `cat`, `head`, `git grep`/`log`/`show`) or implementation commands. One argv array per call. |
| `subagent_intent` | subagent frame → `subagent_end` | Isolated multi-step work; use inventory-only tasks to survey repo without polluting parent thread. |

Discovery order: in-context → `memory_query` → `cli_intent` (files) → `subagent_intent` (broad survey) → `reflect` (graph refresh). Do not skip to new code without checking existing implementations.

## Response buffer

| Route | Notes |
|-------|--------|
| `response_append` / `response_edit` / `response_observe` | Build long replies incrementally; `enrich_fast` relays buffer state. `response_observe` also reviews the task ledger. |
| `tasks_upsert` | `tasks_upsert@` → `enrich_fast@` → `reasoning@` | Maintain the durable task ledger (plan): add new subtasks and/or change statuses of existing ones in one call. See `agent_task_ledger.md`. |
| `response_finalize` | Required to deliver reply; never call `egress_router` directly from reasoning. **Hard-gated** on the task ledger: refused while any subtask is `pending`/`in_progress`, or if all are `cancelled` with none `done`. |

## Verification

| Route | Use |
|-------|-----|
| `formal_reason` | RDF/SPARQL reasoning: SHACL validation, optional inference (derived triples), optional SPARQL query on your graph — not for fetching project facts (`memory_query`). |

## Related bootstrap docs

- **Core:** `turtle_syntax.md`, `shacl_sparql.md`, `sparql_functions.md` — SHACL/Turtle/SPARQL for `formal_reason`.
- **Workflows:** `formal_reason_workflows.md` — tool-call JSON examples and observation patterns.
- **Task ledger:** `agent_task_ledger.md` — `tasks_upsert` tool contract, content-addressed dedupe, monotonic status lattice, the `response_finalize` gate, batch patches.
- **RDFLib:** `rdflib_overview.md`, `rdflib_getting_started.md`, `rdflib_graphs.md`, `rdflib_sparql.md`, `rdflib_parsing.md`, … — API reference; `rdflib_examples_*.md` — scenarios as `formal_reason` payloads.
- **pySHACL:** `pyshacl_overview.md`, `pyshacl_examples.md`.
- **Patterns:** `patterns_sparql_query.md`, `patterns_shacl_validate.md` (derwen ex4/ex5 adapted).
