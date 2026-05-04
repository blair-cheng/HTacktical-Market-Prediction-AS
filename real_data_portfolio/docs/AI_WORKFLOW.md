# AI Workflow

This rebuild used AI as a software-engineering multiplier rather than a one-shot code generator.

## Setup

- Codex was the primary coding agent.
- Claude Code was used as a reviewer and second-opinion agent.
- A local maintenance log tracked decisions, TODOs, review findings, and context handoffs.

## Workflow

1. Define the target behavior in plain language:
   convert a Kaggle prototype on anonymized data into a real-data research pipeline.
2. Let Codex inspect the existing repository and implement narrow code changes.
3. Use Claude Code to review assumptions around feature semantics, leakage, risk-free-rate handling, and evaluation windows.
4. Feed review findings back into Codex for implementation.
5. Rebuild fragments and rerun walk-forward tests after each material data-processing change.
6. Keep a decision log so later AI turns do not repeat stale conclusions or lose context.

## Why This Was Faster

The work required code migration, data-source debugging, feature semantics review, repeated backtests, and documentation.
Using AI agents let me stay at the architecture and judgment layer while the agents handled implementation, consistency checks, and repeatable execution.

## Important Lessons

- Full-period metrics can be misleading when the historical library is still thin.
- Competition artifacts and real-data systems need separate evaluation standards.
- Data-source coverage matters as much as model design.
- A review agent is useful only when its conclusions are recorded and reconciled with implementation state.
