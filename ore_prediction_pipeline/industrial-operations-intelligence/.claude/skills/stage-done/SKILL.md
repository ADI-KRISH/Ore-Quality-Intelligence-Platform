---
name: stage-done
description: Definition-of-done checklist before closing a stage and committing. Use when a stage's code is written.
---

Close out the stage: $ARGUMENTS

1. Run `make lint` and `make test`; show the output. Fix failures before continuing.
2. Check the stage's acceptance check from docs/spec.md §14 and show evidence it holds (row counts, test names, command output).
3. Confirm: no hardcoded paths/thresholds, no new unapproved dependencies, no typed metric values in docs.
4. List any design decisions made in this stage that are NOT yet in docs/decisions.md (I'll write them myself).
5. Propose a commit message (conventional commits style). Do not commit until I say so.
