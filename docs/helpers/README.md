# Knowledge Base (docs/helpers/)

Persistent knowledge base maintained by `worker`/`coder` (implementation)
and `review` (code review) subagents so lessons learned carry over to
future tasks. Part of the workflow described in
[`workflow.md`](../../workflow.md).

## Files

- [`faq.md`](faq.md) — frequently asked questions, recurring pitfalls and
  their solutions
- [`decisions.md`](decisions.md) — important project decisions with rationale

## Rules

1. **Read before starting** — implementation and review subagents must read
   `faq.md` and `decisions.md` before beginning work.
2. **Append after finishing** — add short entries for non-obvious learnings:
   one topic per entry (the problem, the solution/decision, optionally an
   issue/commit reference). Do not rewrite or delete existing entries.
3. **Commit as part of regular commits** — KB entries go into the normal
   fix/feat commits of the branch, never a separate PR.
4. **When in doubt** — extend [`docs/troubleshooting.md`](../troubleshooting.md)
   instead, or ask the user before adding a new entry.
5. Entries must be verifiable: only document facts observed in this
   repository (commands, configs, CI behavior), not guesses.
