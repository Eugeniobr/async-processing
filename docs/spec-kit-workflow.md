# Spec Kit Workflow

## Tooling

This repository was initialized using the official
[GitHub Spec Kit](https://github.com/github/spec-kit) **v1.2.3**, with the Codex
integration and Bash scripts. The version is recorded in .specify/init-options.json.

To install the same CLI version in a development environment:

```bash
uv tool install specify-cli --from git+https://github.com/github/spec-kit.git@v1.2.3
```

Initialization has already been performed. Do not rerun it simply to use the existing
project. For reference, the initialization command was:

```bash
specify init --here --force --non-interactive --integration codex \
  --integration-options='--skills --events false' --script sh --ignore-agent-tools
```

The agent executable check was skipped because setup ran in a hosted environment.
Automatic runtime event handlers were disabled to keep stages explicit. Standard Spec Kit
skills, templates, and scripts remain installed. No optional extensions were installed.

## Stage Status

| Stage | Status | Artifact or next outcome |
| --- | --- | --- |
| Initialize Spec Kit | Complete | .specify/ and .agents/skills/ |
| Establish constitution | Complete | .specify/memory/constitution.md, version 1.0.0 |
| Specify the feature | Next | specs/001-async-transactions/spec.md and its requirements checklist |
| Clarify and plan | Pending | Resolve remaining behavior and document design decisions |
| Generate and analyze tasks | Pending | Small tasks traceable to requirements |
| Implement | Pending | Verified application increments |
| Converge | Pending | Check implemented behavior against the specification |

## Agent Workflow

The following are **Codex chat skill invocations**, not shell commands. Invoke one stage
at a time and review its output before proceeding:

1. `$speckit-constitution` — establish or amend project principles. The initial constitution
   has already been written using this workflow.
2. `$speckit-specify` — adapt docs/specification-v0.1.md into prioritized user scenarios,
   requirements, acceptance scenarios, and measurable outcomes. Preserve its scope and
   keep technical choices in the constitution and later planning artifacts.
3. `$speckit-clarify` — resolve ambiguous behavior, including the exchange-rate reference
   date and treatment of dates without a published rate.
4. `$speckit-plan` — research and select the exchange-rate provider, define precision and
   rounding, describe classes and interfaces, and document durable retries.
5. `$speckit-tasks` — derive small implementation tasks from the specification and plan.
6. `$speckit-analyze` — identify inconsistencies, missing coverage, and unresolved decisions.
7. `$speckit-implement` — implement the next bounded increment and run its relevant checks.
8. `$speckit-converge` — identify remaining gaps; repeat implementation and verification
   until the feature satisfies the specification.

Do not treat a generated checklist as evidence that application tests have passed.

## Decisions Carried Forward

Confirmed choices are RabbitMQ, decimal arithmetic, and exponential backoff capped at
60 seconds. PostgreSQL, object-oriented components, the Repository pattern, and constructor
injection remain the agreed design basis.

The earlier approval did not name an exchange-rate provider, numeric storage precision,
or rounding mode. Those details remain open and must be resolved before affected code is
implemented. Historical-rate lookup was proposed in v0.1 and still requires explicit
resolution during clarification.

The original specification is a historical record. Active feature documents will carry
future revisions, with links back to the baseline and the constitution.
