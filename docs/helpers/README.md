# Knowledge Base (docs/helpers/)

Persistent knowledge base maintained by `worker`/`coder` (implementation)
and `review` (code review) subagents so lessons learned carry over to
future tasks. Part of the workflow described in
[`workflow.md`](../workflow.md).

## Files

- [`faq.md`](faq.md) — frequently asked questions, recurring pitfalls and
  their solutions
- [`decisions.md`](decisions.md) — important project decisions with rationale

## Rules

1. **Read before starting** — implementation and review subagents must read
   `faq.md` and `decisions.md` before beginning work.
2. **Insert after the index, never at the end** — add short entries for
   non-obvious learnings: one topic per entry (the problem, the
   solution/decision, optionally an issue/commit reference). A new `##`
   section goes directly below the index at the top of the file, newest
   first, and its link goes at the top of the index list. The fixed
   insertion point is what keeps two branches that learn something at the
   same time from conflicting at the end of the file; `HelpersIndexGuardTest`
   fails when the index and the headings drift apart or the file loses its
   trailing newline. Do not rewrite or delete existing entries.
3. **Commit as part of regular commits** — KB entries go into the normal
   fix/feat commits of the branch, never a separate PR.
4. **When in doubt** — extend [`docs/troubleshooting.md`](../troubleshooting.md)
   instead, or ask the user before adding a new entry.
5. Entries must be verifiable: only document facts observed in this
   repository (commands, configs, CI behavior), not guesses.
