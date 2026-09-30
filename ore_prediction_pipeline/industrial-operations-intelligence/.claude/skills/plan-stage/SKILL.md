---
name: plan-stage
description: Plan one pipeline stage and its tests without writing code. Use when starting any new stage of the IOP build.
---

Plan the stage: $ARGUMENTS

Do NOT write or edit any code in this turn.

1. Read CLAUDE.md, docs/decisions.md, docs/findings.md, and the relevant sections of docs/spec.md.
2. Produce a plan with these headings:
   - **Goal & acceptance check** (quote the matching row from spec §14)
   - **Inputs → outputs** (tables/files, grain, key columns)
   - **Files to create/change** (paths only)
   - **Functions** (signatures + one-line purpose each)
   - **Config keys** needed (which yaml, key names, proposed defaults)
   - **Tests** (list each test name and what it asserts; DQ rules need a passing AND failing case)
   - **Open design decisions for the owner** — list them as questions with 2–3 options each. Do not pick for me.
   - **Risks** (leakage, idempotency, local-vs-Databricks differences)
3. End by asking me to approve or correct the plan.
