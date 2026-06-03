# Roadmap — From Wizard Alpha to Agent v1

This roadmap transforms the current four-wizard Streamlit app into a chat-based agent that exposes the same capabilities as fine-grained tools. Timeline target: **weeks, not months** — a friendly-officer pilot within ~4–8 weeks, then iterate.

## Guiding Principles

1. **Front-load hallucination de-risking.** Hallucinations in reports are the top identified risk; grounding, citation, and human-in-the-loop checks come before breadth of features.
2. **Keep the alpha working.** Each phase ships behind a flag or on a separate route so officers who use the current wizards are not blocked.
3. **No dead ends on AWS.** Every component added during GCP dev must sit behind an interface that makes the eventual AWS/Bedrock move a swap, not a rewrite.
4. **Small vertical slices.** Each micro-phase produces something a user (or we, in dogfood) can actually try.

## Phase Overview

| Phase | Theme | Rough duration | Exit signal |
|---|---|---|---|
| 0 | LLM switch & tool decomposition | ~1 week | Claude-on-Vertex live; pipelines refactored into fine-grained tools with typed schemas |
| 1 | Minimal chat agent | ~1 week | Streamlit chat page talking to a LangGraph agent with 2–3 working tools |
| 2 | Full tool library + pipeline shortcuts | ~1–2 weeks | Agent can run any of the four original pipelines end-to-end via chat |
| 3 | Grounding & hallucination guardrails | ~1 week | Turn-level citations, value-sanity checks, Red-Team QA wired into the agent loop |
| 4 | Auth + credit ledger | ~1 week | Corporate-email login, per-user monthly credits enforced |
| 5 | Multilingual (EN/FR/ES) | ~3–5 days | Officers can chat and receive DOCX reports in FR/ES |
| 6 | Agentic eval suite + logging extensions | ~1 week | Tool-selection and grounding evals running; agent traces in logs |
| 7 | Friendly-officer pilot | open-ended | At least one VAM officer using the agent on a real report; feedback loop active |
| 8 | AWS portability audit | deferred | No blockers identified for a Bedrock/AWS move |

Durations are nominal for a small team; they will compress or expand based on who is available.

---

## Phase 0 — LLM switch & tool decomposition

**Goal:** Replace Gemini with Claude on Vertex and break the monolithic pipelines into callable tools.

### Micro-phases
- **0.1 — LLM abstraction.** Refactor `app/shared/llm.py` into a provider-agnostic interface; implement the Claude-on-Vertex backend; delete Gemini-specific prompt formatting.
- **0.2 — Tool inventory.** Walk the four existing LangGraph pipelines and list every distinct step (retrieval, validation layer, analysis, drafting, QA, export). Finalise the tool list in `tech-stack.md`.
- **0.3 — Extract tools.** For each step, extract a standalone Python callable with a typed input/output schema (Pydantic) and an LLM-readable docstring. Keep them under `app/tools/`.
- **0.4 — Adapter layer for existing wizards.** Have the current wizards call the new tools under the hood. This proves the refactor without breaking the alpha.
- **0.5 — Smoke tests.** Unit tests per tool + one integration test per pipeline, running through the new tool layer.

**Exit signal:** All four existing wizards produce identical output to before, but now by composing fine-grained tools.

---

## Phase 1 — Minimal chat agent

**Goal:** Put a working chat agent in front of ourselves.

### Micro-phases
- **1.1 — LangGraph agent skeleton.** Build a LangGraph agent node that takes user messages, calls Claude with tool descriptions, handles tool calls, and returns replies.
- **1.2 — Streamlit chat page.** New page (`pages/0_Chat.py` or similar) with `st.chat_message` + streaming. Keep existing wizard pages intact.
- **1.3 — Wire 2–3 tools.** Start with `fetch_prices_databridges`, `generate_price_chart`, `web_search`. Enough to prove the chat-driven analytical loop.
- **1.4 — Turn tracing in logs.** Every tool call, argument, and result logged with a session id.

**Exit signal:** We can ask the agent "show me wheat prices in Somalia over the last 6 months" and get a grounded answer with a chart.

---

## Phase 2 — Full tool library + pipeline shortcuts

**Goal:** The agent can do everything the wizards do, plus more.

### Micro-phases
- **2.1 — Expose all fine-grained tools.** Register every tool from Phase 0 with the agent. Test each individually via chat.
- **2.2 — Pipeline-shortcut tools.** Implement `compose_mfi_report` and `compose_market_monitor` as meta-tools that run the pre-established sequence of fine-grained tools. Users say "draft the Market Monitor for Somalia" and the agent calls one tool that internally orchestrates the chain.
- **2.3 — DOCX download from chat.** Agent surfaces the generated DOCX as a download in the chat thread.
- **2.4 — File upload from chat.** Users drop a CSV/XLSX into chat to trigger `validate_mfi_csv` / `validate_price_xlsx`.
- **2.5 — Prompt scaffolding.** Suggested-prompt chips and example queries on the chat page (officers are mixed-comfort with LLM chat).

**Exit signal:** Every capability of the alpha is reachable from chat, plus ad-hoc exploration of price data.

---

## Phase 3 — Grounding & hallucination guardrails

**Goal:** Front-load the top identified risk.

### Micro-phases
- **3.1 — Inline citations.** Every factual claim in an agent reply links back to the tool call and retrieved data that produced it. Simple footnote-style markers in chat.
- **3.2 — Value-sanity checks.** Post-tool guard that flags statistically implausible numbers (e.g. month-on-month price jumps outside a tolerance) and forces the agent to re-examine before quoting.
- **3.3 — Red-Team QA in the loop.** The existing end-of-pipeline QA becomes a tool the agent invokes on any drafted section; corrections loop back into the agent.
- **3.4 — Human sign-off UI.** DOCX export from chat routes through a confirmation step ("review the draft — approve to download") that makes human sign-off explicit and loggable.
- **3.5 — Grounding eval fixtures.** Capture failures found during this phase as eval cases for Phase 6.

**Exit signal:** On a set of seeded adversarial prompts, the agent either answers with citations or declines with a clear reason; no silent fabrication.

---

## Phase 4 — Auth + credit ledger

**Goal:** Controlled access and cost control before widening the pilot.

### Micro-phases
- **4.1 — Corporate-email login.** Google Workspace OAuth in Streamlit; restrict to WFP domains.
- **4.2 — User records.** Minimal user model in Firestore (email, display name, country, monthly credit allowance).
- **4.3 — Credit ledger.** Write every LLM call and heavy tool call to a per-user ledger; token-weighted for LLM, flat-rate for report generation.
- **4.4 — Enforcement middleware.** Block new turns when a user is out of credits; show remaining balance in the UI.
- **4.5 — Admin top-up view.** Tiny Streamlit page gated to admins for granting / resetting credits.

**Exit signal:** We can onboard a VAM officer by email, give them credits, and have usage deducted correctly.

---

## Phase 5 — Multilingual (EN / FR / ES)

**Goal:** Support the three languages VAM officers actually work in.

### Micro-phases
- **5.1 — Chat language detection + override.** Detect per-session; expose a language picker.
- **5.2 — Translate section prompts.** FR and ES variants of every drafting prompt, reviewed by a fluent speaker.
- **5.3 — DOCX templates per language.** Headings, captions, boilerplate translated; numeric/date formatting localised.
- **5.4 — Tool outputs localisable.** Where tools produce human-readable strings (chart labels, section titles), they accept a language param.

**Exit signal:** An officer can run the full flow in French or Spanish and receive a clean DOCX.

---

## Phase 6 — Agentic eval suite + logging extensions

**Goal:** Make quality measurable before broadening adoption.

### Micro-phases
- **6.1 — Tool-selection eval.** Curated prompts with expected tool sequences; measure match rate.
- **6.2 — Grounding eval.** Prompts where the correct answer is in retrieved data; measure whether the agent cites correctly.
- **6.3 — Report-quality rubric.** LLM-judge scoring of drafted sections against a rubric (factuality, completeness, tone).
- **6.4 — Eval runner + dashboards.** Simple CLI runner; results surfaced in the extended logging system.
- **6.5 — Regression gate.** Evals run on every merge to the agent branch; flag regressions before pilot sessions.

**Exit signal:** We have hard numbers for tool-selection accuracy and grounding, and a rubric-based quality baseline.

---

## Phase 7 — Friendly-officer pilot

**Goal:** Real usage, real feedback.

### Micro-phases
- **7.1 — Onboarding one officer.** Hand-pick a friendly VAM officer (ideally CO, one with a near-term report due). Walk through setup.
- **7.2 — Shadowing.** Observe a session end-to-end; record friction.
- **7.3 — Rapid iteration.** Turn observed friction into issues; fix the top ones within days, not weeks.
- **7.4 — Expand to 2–3 more officers.** Once the first loop is smooth, bring in HQ and a second CO.
- **7.5 — Adoption metric baseline.** Start measuring DAU/WAU and time-to-draft against wizard baseline.

**Exit signal:** At least one officer using the agent for an actual published report, with a documented time-to-draft reduction.

---

## Phase 8 — AWS portability audit (deferred)

**Goal:** Verify nothing in v1 blocks the eventual move to AWS/Bedrock.

### Micro-phases
- **8.1 — LLM swap dry run.** Stand up a Bedrock backend locally; verify the abstraction holds.
- **8.2 — Storage swap dry run.** Prove Firestore/GCS code paths work behind an interface that could be backed by DynamoDB/S3.
- **8.3 — Auth portability.** Check that corporate-email login can move to Entra ID or AWS IAM Identity Center without restructuring user records.
- **8.4 — Container audit.** Confirm the Docker image runs on AWS infra with no GCP-specific SDK calls leaking out.

**Exit signal:** Written go/no-go note for an AWS migration, with any remaining code debt listed.

---

## Cross-Cutting Workstreams

These run alongside the phases, not as discrete phases:

- **Keep-alpha-alive.** The existing wizards remain available until Phase 2 exit, so no officer is stranded mid-transition.
- **Continuous tool-doc hygiene.** LLM-readable docstrings on every tool; reviewed whenever a tool is added or changed.
- **Officer feedback log.** Single document where every piece of officer feedback is captured, tagged, and routed into a phase.

## Open Questions / Assumptions Flagged

- **DataBridges coverage beyond prices.** Roadmap assumes we stay on price data for v1; if MFI survey access lands, Phase 2's tool list expands but the structure is unchanged.
- **Credit pricing model.** We build the ledger in Phase 4; the actual per-officer allowance and per-tool costs need to be set with stakeholders once we see real usage from Phase 7.
- **Web search provider.** To be picked in Phase 1; needs a provider with acceptable WFP data-handling terms.
