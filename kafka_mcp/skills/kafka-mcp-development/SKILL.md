---
name: kafka-mcp-development
skill_type: skill
description: >-
  Develop, test, gate, and release the kafka-mcp connector. Use when changing
  Kafka or Kafka Connect API calls, mappings, MCP tools, packaging, docs, or
  repository gates.
license: MIT
tags: [kafka-mcp, development, connector, kafka, gates]
tier: repository
metadata:
  author: Knuckles-Team
  version: '0.1.0'
---

# kafka-mcp development

This repository owns Kafka and Kafka Connect calls, mappings, MCP tools, and
agent entry points. Shared connector contracts and transports belong to
`scion-sdk`; serving and durable graph state belong to their owning
repositories. Read `AGENTS.md` before changing code, and keep cluster details,
credentials, and customer data out of source and skill content.

## Steps

1. Find the owning spec row with `git grep -n "<term>" -- specs`. Verify one row owns the change.
2. Bootstrap the pinned environment with `scripts/bootstrap.sh`. Verify it exits 0.
3. Make the change in the existing client or tool module and add a bound test. Verify it passes with `uv run --all-extras pytest tests/<relevant-test>.py -q`.
4. Run the repository gate with `uvx pre-commit run --all-files`. Verify every hook reports Passed.
5. Stage reviewed paths with `git add -- <path>` and inspect `git diff --cached --name-status`. Verify only intended files are listed.
6. Commit with the `Spec:` trailer directly above the `Co-Authored-By:` trailer. Verify with `git log -1` that both lines are adjacent.

## Failure recovery

| Failure | Cause | Recovery |
|---------|-------|----------|
| Test reaches a live broker | Kafka or Connect call not mocked | Patch the client call in the test and run the single test file again |
| Pre-commit hook rewrites files | Formatter or spelling hook applied fixes | Stage the rewritten paths with `git add -- <path>` and commit again |
| `status.json` shows as modified | Generated file edited by hand | Restore it with `git checkout -- <path>` and regenerate with `pipelines-hook spec-status` |

## Rapid delivery: the 5-minute contract

Specs are already designed. Deliver small, reviewable slices and cite the
owning spec row in the commit trailer.

- Find the owning row first with `git grep -n "<term>" -- specs`; extend that
  spec before adding requirement text.
- Reuse the existing Kafka and Kafka Connect clients, auth policy, and tool
  factories. Keep provider calls and mappings inside this connector.
- Bind each delivered requirement to a test. Generate `status.json` with
  `pipelines-hook spec-status`; never edit generated status by hand.
- Keep external Kafka and Connect calls mocked in local tests. Never put live
  endpoints, credentials, or raw customer data in code, logs, or fixtures.
- Keep skill files under `kafka_mcp/skills/`; the package includes
  `skills/**` as package data.
- Make one logical change per commit with its `Spec:` trailer and the assisting
  agent's `Co-Authored-By:` trailer.

## Setup and gates

Bootstrap the pinned environment with `scripts/bootstrap.sh`. Before committing,
run the complete repository gate:

```bash
uvx pre-commit run --all-files
```

For focused behavior checks, run the relevant tests with the pinned environment:

```bash
uv run --all-extras pytest tests/<relevant-test>.py -q
```

Do not bypass a failing hook. If a required gate cannot pass, report the exact
failure and leave the change uncommitted.

## Delivery discipline

Work in a real Git worktree based on `origin/main`. Review the complete diff,
stage only reviewed paths, and confirm the final `git status --short`. Do not use
`git stash`, `git add -A`, or `git add .`. Claim Pages publication only after a
successful workflow run for the exact source commit and confirmation of the
public page.
