# Task ledger (Threlium anti-drift plan)

> **Threlium:** This is the guide for the `tasks_upsert` tool and the durable task
> ledger. Retrieve it via `memory_query` when planning multi-step work, before
> closing out a thread, or when `response_finalize` is refused. For the full route
> map see `fsm_routes.md`.

The task ledger is the agent's **plan for the current thread**, made durable. It is
reconstructed from the mail thread on every reasoning cycle (not from prompt memory),
so it does not drift: whatever you record stays until you close it. The current ledger
is shown to you each turn in the `<task_state>` block, with one line per subtask:
its `content_id` and its `status`.

## Lifecycle

1. **enrich seeds it.** When a thread starts, `enrich` decomposes the user request
   into an initial set of subtasks (status `pending`). They appear in `<task_state>`.
2. **You refine and progress it** with `tasks_upsert`: add concrete subtasks, mark the
   one you start as `in_progress`, mark finished ones `done`, drop dead ones `cancelled`.
3. **`response_finalize` is hard-gated on it.** It will refuse to send the reply while
   any subtask is `pending`/`in_progress`, or if every subtask is `cancelled` with none
   `done`. The gate checks the reconstructed ledger, not your prose — you cannot finalize
   by claiming you are done.

## Status lattice (monotonic, never moves backwards)

```
pending (0)  ->  in_progress (1)  ->  done (2)
                                  ->  cancelled (2)
```

Merge keeps the **highest** rank; a tie at rank 2 resolves to `done`. So once a subtask
is `done` it stays `done` — a later `pending`/`in_progress` on the same id is a no-op.
Re-running `enrich` (e.g. after `cli_exec → ingress → enrich`) never resets your
progress: enrich seeding is *ensure-exists* (adds missing subtasks, never downgrades).

- `done` = finished and verified.
- `cancelled` = no longer needed (scope narrowed, duplicate, user dropped that part).
  Prefer `cancelled` over leaving stale `pending` work — but cancelling *everything*
  with nothing `done` still fails the gate (no escape hatch).

## Content-addressed identity (dedupe)

A subtask's `content_id` is derived from its **normalized text** (whitespace-collapsed
hash). Two consequences:

- **Reuse the EXACT existing text** when you mean the same subtask — paraphrasing
  creates a *new* subtask (new `content_id`) instead of updating the old one.
- **Updates target `content_id`, not text.** Read the id from `<task_state>` and pass it
  in `subtask_updates`. An unknown `content_id` is rejected (the call is bounced to
  `ingress` with an error notice) — never invent ids.
- **Changing the wording** of a subtask = add the new text as a `new_subtask` **and**
  `cancel` the old `content_id`.

## Tool call shape

One call may BOTH add new subtasks AND update existing statuses. `reasoning` is required;
everything else is optional.

```json
{
  "reasoning": "decomposed plan into 2 concrete subtasks; closed the discovery one",
  "new_subtasks": [
    { "text": "Add tasks_upsert handler in states/", "status": "in_progress" },
    { "text": "Wire FsmStage.TASKS_UPSERT into the registry", "status": "pending" }
  ],
  "subtask_updates": [
    { "content_id": "a1b2c3d4", "status": "done" }
  ],
  "next_action": "implement the handler"
}
```

Keep at most one subtask `in_progress`. Batch several `done` in a single call after a
discovery hop rather than one call per subtask.

---

### Scenario: seed → batch-close → finalize

1. enrich seeds three `pending` subtasks (`id1`, `id2`, `id3`).
2. You do the work, then close two and start the third:

```json
{
  "reasoning": "finished the first two; starting the last",
  "subtask_updates": [
    { "content_id": "id1", "status": "done" },
    { "content_id": "id2", "status": "done" },
    { "content_id": "id3", "status": "in_progress" }
  ]
}
```

3. `response_finalize` is still refused: `id3` is `in_progress`. Close it:

```json
{ "reasoning": "done", "subtask_updates": [ { "content_id": "id3", "status": "done" } ] }
```

4. All subtasks `done` → gate passes → reply is delivered.

---

### Scenario: discovery adds work

A `memory_query` / `cli_intent` hop reveals a follow-up. Add it (it now blocks finalize
until closed) and close what you just resolved:

```json
{
  "reasoning": "cli_exec showed the config also needs vars/main.yml; the registry edit is done",
  "new_subtasks": [ { "text": "Register stage in vars/main.yml fsm stages", "status": "pending" } ],
  "subtask_updates": [ { "content_id": "id_registry", "status": "done" } ]
}
```

---

### Scenario: scope narrowed (cancel, but not all)

User drops one requirement. Cancel that subtask, keep the rest:

```json
{
  "reasoning": "user said the telegram path is out of scope",
  "subtask_updates": [ { "content_id": "id_telegram", "status": "cancelled" } ]
}
```

Gate still requires the remaining subtasks to reach `done`. If you cancel **all** of them
with none `done`, finalize is refused — that is the all-cancelled guard.

---

### When NOT to use `tasks_upsert`

| Need | Route | Why |
|------|-------|-----|
| Store a durable fact / note | `thread_memory` / `global_memory` | The ledger tracks work to do, not knowledge. |
| Review the buffer + plan | `response_observe` | observe only *reads* the ledger; it writes no ops. |
| Continue reasoning with fresh context | `reflect` | The ledger persists across reflect; no upsert needed just to think. |

## Engine mapping

1. `reasoning` emits `tasks_upsert` tool args → durable mail to `tasks_upsert@`.
2. `tasks_upsert` handler validates `subtask_updates` content_ids against the collected
   ledger, builds a `TasksUpsertOp`, forwards via `enrich_fast` (recomputes `<task-state>`).
3. `collect_task_ops` walks the IRT chain (whole subagent frame, isolated by hop depth)
   gathering `TaskInitOp` (from enrich `<task-init>`) and `TasksUpsertOp` (from
   `tasks_upsert@` bodies); `reduce_task_ops` merges by `content_id` via the lattice.
4. `response_finalize` reduces the ledger and runs `ledger_has_open_work` as the gate.
