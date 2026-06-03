# Tech Stack

This document records the stack for the **agent version** of the app, what carries over from the current alpha, what changes, and the known gaps we need to close.

## What carries over from the alpha

| Layer | Technology | Notes |
|---|---|---|
| Frontend | **Streamlit** (WFP-branded theme) | Kept. The chat UI will use `st.chat_message` and streaming. |
| Backend API | FastAPI, Uvicorn | Kept as programmatic entry point; the Streamlit UI continues to use the in-process dispatcher. |
| Orchestration | **LangGraph** | Kept as the agent loop framework. The new agent is a LangGraph state machine with tool nodes. |
| LLM integration | LangChain (langchain-core) | Kept as the abstraction layer. |
| Data | Pandas, NumPy, OpenPyXL, chardet | Kept. |
| Visualisation | Matplotlib (Base64 PNG) | Kept. |
| Report export | python-docx | Kept, adapted to emit FR/ES in addition to EN. |
| Cloud (dev) | Google Cloud (Vertex AI, Firestore, GCS) | Kept for the development environment. |
| Container | Docker (Python 3.11-slim) | Kept. |
| Integrations | DataBridges, Seerist, ReliefWeb, Trading Economics | Kept; DataBridges access remains read-only and currently limited to price data. |

## What changes

### LLM provider

- **Now (GCP dev):** Claude (Anthropic) via **Vertex AI** — replacing Gemini 2.5 Pro.
- **Later (AWS prod):** Claude via **AWS Bedrock** — same model family, different hosting.

To keep the GCP→AWS move cheap, the LLM must be wrapped behind a thin provider abstraction from day one. Only the Vertex path is implemented in v1, but the interface is shaped so Bedrock is a drop-in.

### Interaction model

- **From:** four independent wizard pages, each a full linear pipeline.
- **To:** a single chat page backed by a LangGraph agent with access to a library of **fine-grained tools**. Existing pipelines are reconstituted as **pre-established tool sequences** the agent can invoke when the user asks for "a full MFI report" or "a Market Monitor" — the user does not step through each stage manually.

### Tool library

The current pipelines are decomposed into composable tools. Rough inventory (to be finalised in Phase 0 of the roadmap):

- **Data retrieval:** `fetch_prices_databridges`, `fetch_mfi_csv` (from upload), `fetch_news_seerist`, `fetch_news_reliefweb`, `fetch_fx_trading_economics`, `web_search`.
- **Validation:** `validate_mfi_csv`, `validate_price_xlsx` (layered checks from the current validators exposed as one tool each, or split further if needed).
- **Analysis:** `analyse_price_trend`, `score_mfi_dimensions`, `generate_radar_chart`, `generate_price_chart`.
- **Drafting:** `draft_section`, `draft_exec_summary`, `red_team_qa`.
- **Composition / export:** `render_docx_report`, `compose_mfi_report`, `compose_market_monitor` (the last two being the "pipeline shortcuts" that call the above in sequence).

### Authentication and credits

- **Corporate-email login** (Google Workspace OAuth during GCP development; portable to Entra ID / Bedrock IAM patterns later).
- **Per-user monthly credit ledger** — a usage accounting system we build ourselves. Credits are deducted per LLM call (token-weighted) and per heavy tool call (report generation). When a user hits zero, further requests are blocked until the next cycle or an admin top-up.
- Credits persist in **Firestore** during GCP dev; the storage layer is abstracted so it can move to DynamoDB/RDS when we move to AWS.

### Multilingual support

- Chat input/output and generated reports support **English, French, Spanish**.
- Language is detected per-session (with explicit user override). DOCX templates and section prompts carry a language parameter.

### Evals

- New **agentic eval suite** (built from scratch) covering:
  - **Tool selection accuracy** — does the agent pick the right tool for a prompt?
  - **Grounding** — are numeric/fact claims in drafts traceable to retrieved data?
  - **Report quality** — rubric-based LLM-judge scoring of drafted sections.
- The existing `evals/` folder is absorbed into this; old fixtures are reused where they still apply.

### Observability

- We extend the **current logging system** (see `8652c50 logging system implementation`) with:
  - Per-turn agent traces (messages, tool calls, tool results, token usage).
  - Structured events so we can slice by user, country, tool, language.
- No external tracing vendor (Langfuse/LangSmith) for v1 — data stays in our own logs. Revisit if debugging friction becomes severe.

## Known gaps to close

These are the concrete things missing today that the roadmap will address:

1. **Agent loop.** No chat agent exists yet — only linear pipelines.
2. **LLM abstraction + Claude/Vertex path.** The `llm.py` singleton today targets Gemini; needs an interface-level rewrite.
3. **Tool layer.** Pipelines are currently monolithic LangGraph graphs. Each stage needs to be extracted into a standalone callable with a typed schema and a docstring the LLM can reason over.
4. **Auth.** No end-user authentication exists. Needs to be added along with session identity.
5. **Credits.** No usage metering exists. Needs a ledger, enforcement middleware, and a minimal admin view.
6. **Multilingual drafting.** All current prompts and DOCX templates are English-only.
7. **Hallucination guardrails.** Current Red-Team QA runs at the end of a pipeline; in an agent loop we also need turn-level grounding checks, source citation in chat, and value-sanity checks on analytical outputs.
8. **Web search tool.** Not currently integrated; needs to be added with a vetted provider and safe-browsing controls.
9. **Provider-portable storage.** Firestore/GCS are fine for dev but the interfaces need to be abstracted so AWS migration is a swap, not a rewrite.
10. **Agentic evals.** No tool-selection or grounding eval exists yet.

## Deployment footprint

- **Development (current, 2026):** Google Cloud. Streamlit + FastAPI in a single container. Vertex AI for Claude. Firestore + GCS for state.
- **Production (future):** AWS. Same container layout. Bedrock for Claude. DynamoDB/RDS + S3 for state. Corporate SSO via whatever WFP standardises on for AWS.

The agent code must not hard-code GCP assumptions past Phase 0.
