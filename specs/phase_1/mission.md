# Mission

## Purpose

The VAM LLM project exists to reduce the most time-consuming, low-judgment parts of a VAM officer's workflow — data retrieval, cleaning, and report drafting — so officers can spend their time on analysis and decisions instead of mechanical steps.

The current alpha ships four wizard-style pipelines (MFI Validator, Price Validator, MFI Drafter, Market Monitor). They prove the underlying capabilities work end-to-end but lock officers into rigid, non-interactive flows.

The next version is a **chat-based agent** that exposes those same capabilities as tools. Officers interact in natural language; the agent retrieves WFP data (via DataBridges), performs analysis, and composes fine-grained tools — in pre-established sequences when asked — to produce the same reports the current wizards produce today, only driven by conversation rather than forms.

## Vision for a Successful Session

A VAM officer opens the chat, asks a question in natural language about price data for a country, iterates with the agent (follow-ups, comparisons, alternative framings), and when ready asks for a full Market Monitor or MFI report. The agent orchestrates the necessary tool calls and produces the deliverable, grounded in retrieved data and external context, with a human sign-off at the end.

The agent is **exploratory + report-producing**: it must be as useful for "show me what's in DataBridges for Ethiopia over the last 6 months" as for "draft the March Market Monitor for Somalia."

## Primary Users

- **VAM officers in Country Offices** — the primary day-to-day users, doing data pulls, analysis, and drafting.
- **VAM officers at HQ** — consume and synthesise output across countries; need cross-cutting queries.

Both groups are **mixed in LLM comfort, trending comfortable** — many have used ChatGPT/Claude. The UI can assume basic chat literacy but should offer scaffolding (prompt templates, suggested next actions) rather than expect prompt-engineering skill.

## Core Problem to Solve

The single biggest gap in the current alpha is **rigid wizards**. Officers cannot:
- Ask follow-ups or variations without restarting a pipeline.
- Compose steps from different pipelines.
- Explore the underlying data before committing to a full report.
- Iterate on individual sections of a draft.

The agent must close this gap. Flexibility is the point.

## Scope (v1)

**In scope**
- Chat with the agent in **English, French, and Spanish**.
- Retrieve and analyse **price data** from DataBridges (currently our only available data source) with full analytical freedom over it.
- Use **Seerist, ReliefWeb, Trading Economics**, and **open web search** for contextual retrieval.
- Produce **MFI reports** and **Market Monitors** as DOCX, in the user's chosen language.
- Run **MFI and price validation** via tool calls when a user provides a file.
- Per-user login via **corporate email** with a **monthly credit budget** governing usage.

## Non-Goals (v1)

- **No writes to DataBridges.** The agent is strictly read-only against WFP data sources.
- **No beneficiary-level or PII data.** Only aggregated market/price data.
- **No fully autonomous publishing.** Every final report requires a human VAM officer to review and approve before it leaves the system.
- **No other data domains yet.** MFI survey data, household-level food security, etc. are out of scope until access is available.

## Success Metrics

We will judge the agent version by three signals:

1. **Officer adoption** — weekly/monthly active VAM officers using the agent. If they do not choose to use it over the wizards, it is not working.
2. **Time-to-draft reduction** — measurable drop in hours from "start of task" to "draft ready for review" compared to the current wizard flow, on matched tasks.
3. **Qualitative feedback** — structured interviews and surveys with pilot officers on quality, trust, and friction.

Hard thresholds will be set once the pilot is running and we have a baseline.

## Top Risk to Manage

**Hallucinations in reports.** A single fabricated number or misattributed trend in an officer's report destroys trust for the whole initiative. The roadmap front-loads grounding, citation, and human-in-the-loop checks before breadth of features.
