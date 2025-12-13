### LQS — Product Requirements Document (PRD)

### Pitch (1–2 min)
**LQS (Language Query System)** is an enterprise “data copilot” that lets anyone ask questions in natural language and get back **fast, deterministic, auditable results** from live organizational data.  
Unlike typical NL→SQL tools that are brittle and slow, LQS treats the **Hypergraph as the spine** of the organization’s data: it stores schema, keys, join paths, policies, and business rules so the system can compile queries deterministically and execute them at engine speed.

**In one flow:**  
Natural language → **LLM produces an IntentSpec JSON** (schema-agnostic) → **Deterministic compiler** resolves tables/columns/joins via the Hypergraph → SQL → Engine execution → results table (SQL visible only on demand).

### Core USPs
- **Hypergraph as the Spine**: Central, evolving graph of tables/columns/keys/join paths, used for planning, governance, and query compilation.
- **Deterministic Compiler (not “LLM writes SQL”)**: LLM outputs an **IntentSpec**; the compiler resolves schema and emits stable SQL.
- **Fast Execution**: Planning happens during compilation; SQL execution is optimized and cached.
- **Governed by Design**: Business rules, RBAC, and approved join rules are first-class metadata.
- **Postman-style Data Ingestion**: Load JSON from hypothetical API calls, infer schema automatically, and query immediately.
- **Explainability & Auditability**: Every decision can be logged (tables chosen, join path, applied rules, SQL, timing).

---

### Problem Statement
Organizations have data spread across many tables and APIs. Asking even simple questions requires analysts, manual joins, and repeated tribal knowledge about “the right filters” and “the right join keys.” Existing NL→SQL approaches are fragile because the model is asked to guess schema details and joins.

### Goal
Deliver an MVP where a user can:
- Clear all cache/data (“reset the spine”)
- Ingest JSON (paste or fetch like Postman), auto-infer schema, load tables
- Ask natural language questions and get correct results for:
  - **single-table queries**
  - **single-join queries** (the MVP target)

### Non-Goals (MVP)
- Multi-hop joins beyond one join (supported later)
- Complex semantic layer (dimensions/facts) beyond what metadata provides
- Advanced BI features (dashboards, scheduled reports)
- Full self-serve join approval workflows (basic registry only)

---

### Primary Users
- **Business users**: want answers and a results table; do not want to see SQL by default
- **Data analysts**: want SQL visibility, query history, and explainability
- **Data platform/admin**: wants governance, audit logs, rule enforcement, and schema management

### Key Use Cases (MVP)
- “Show me all products”
- “How many customers do we have?”
- “Show me customers who have orders” (**single join**)
- “Show me products that cost more than 100”
- “What is the total price of all products?”

---

### Product Principles
- **LLM parses intent, not schema**: LLM produces an IntentSpec; it does not decide joins/columns definitively.
- **Determinism > cleverness**: The compiler and hypergraph should resolve ambiguity predictably.
- **Results-first UX**: Show results table; SQL behind a click.
- **Governance is a feature**: business rules + join approvals + RBAC should be metadata, not hard-coded.

---

### System Architecture (High Level)
### Components
- **UI (Web)**
  - Data ingestion panel (Postman-style)
  - Chat/query interface (results-first, SQL optional)
  - Schema viewer (tables/columns/joins)
  - Clear-all (reset)
- **Backend (Web server)**
  - Ingestion endpoints
  - Schema sync endpoint (WorldState ↔ execution schema)
  - Ask endpoint (NL query → pipeline)
- **LLM Layer**
  - Generates **IntentSpec JSON** from user text and a filtered MetadataPack
  - (Optional) scoring calls for table/column relevance (if enabled)
- **WorldState**
  - Authoritative metadata “spine”: schema registry, join rules, policies, business rules, stats
- **Hypergraph / Engine**
  - Execution schema and storage
  - Query execution + caching

### Data Flow (Query)
1. User submits a natural language query.
2. Backend builds a **MetadataPack** (RBAC-filtered subset of WorldState).
3. LLM outputs **IntentSpec** (schema-agnostic intent JSON).
4. Deterministic compiler:
   - Scores/selects tables (LLM scoring if available; deterministic fallback otherwise)
   - Finds shortest join path (MVP: **single join**) using hypergraph edges
   - Resolves best columns for projections/filters/metrics
   - Applies business rules unless explicitly overridden
   - Produces a **StructuredPlan**
5. SQL generator emits deterministic SQL.
6. Engine executes SQL and returns result batches + timing.
7. UI shows results table; SQL is available via an icon/toggle.

---

### Functional Requirements
### 1) Data Reset (Clear All)
- **User story**: As a user, I can reset all data/spine caches to start fresh.
- **Requirements**
  - Clears result cache, plan cache, operator cache, query patterns, WAL/spill where applicable
  - Drops all ingested tables (or resets execution schema)
  - Resets schema registry to match empty state
- **Acceptance**
  - After clear-all, schema endpoints show no tables and queries return “no tables loaded”

### 2) Postman-Style JSON Ingestion
- **User story**: As a user, I can paste JSON or fetch JSON from a URL and load it as tables.
- **Requirements**
  - Support basic request composition: method, URL, headers, body
  - Ingest JSON arrays of objects into tabular form
  - Auto-infer schema (types, nullability) and create tables
  - Store inferred schema in WorldState and execution schema
- **Acceptance**
  - After ingestion, schema viewer shows new tables/columns and queries can run

### 3) Schema Sync
- **User story**: As a user, I never get “schema out of sync” errors.
- **Requirements**
  - Provide `POST /api/schema/sync` (or equivalent) and call automatically when needed
  - On ingestion completion, auto-sync WorldState schema from execution schema
- **Acceptance**
  - Running ask/query after ingestion works without manual intervention

### 4) Natural Language Querying (MVP Target: Single Join)
- **User story**: As a user, I can ask questions in English and get results.
- **Requirements**
  - LLM produces IntentSpec JSON (task, metrics, grain, filters, table_hints, etc.)
  - Compiler resolves:
    - single-table queries
    - **single-join queries** between two tables via approved join edges
  - Show results as a table; show SQL only on demand
- **Acceptance**
  - Query “Show me customers who have orders” returns correct joined rows

### 5) Business Rules as Metadata (Default Filters)
- **User story**: As an org, we can enforce default filters (e.g., `writeoff='F'`) automatically.
- **Requirements**
  - Business rules stored as metadata (WorldState / node metadata)
  - Applied automatically unless explicitly overridden in user intent
  - Included in MetadataPack so LLM is aware of defaults (but compiler enforces)
- **Acceptance**
  - Queries apply business rules by default; explicit mention overrides

### 6) Explainability & Audit Log
- **User story**: As an analyst/admin, I can inspect why the system produced a query.
- **Requirements**
  - Store: original query, IntentSpec, StructuredPlan, SQL, selected tables, join path, applied rules, timing
  - Provide a lightweight endpoint or UI panel to view history
- **Acceptance**
  - For any query, show decisions + SQL + result summary

---

### UX Requirements (Results-first)
- **Chat panel**
  - Show: result grid + row count + execution time
  - Hide: SQL behind an icon/toggle
  - Keep assistant text minimal (no long explanations unless asked)
- **Schema panel**
  - Show tables, columns, and join relationships (approved vs unknown)
- **Ingestion panel**
  - “Postman-like” request builder + paste JSON option
  - Clear all button (prominent)

---

### API Surface (MVP)
- **`POST /api/clear_all`**
  - Clears caches and data; returns what was cleared/dropped
- **`POST /api/ingest`** (or existing ingestion routes)
  - Accepts JSON payload or fetch config; returns inferred schema + table names
- **`POST /api/schema/sync`**
  - Repairs schema mismatches between WorldState and execution schema
- **`POST /api/ask_llm`** (NL query)
  - Request: `{ "query": "..." }`
  - Response: `{ results_table, row_count, execution_time_ms, sql(optional), explanations(optional) }`

---

### Security & Governance (MVP)
- **RBAC filtering**: MetadataPack only includes tables/columns allowed by policy.
- **Join governance**: Only use joins that are Approved in JoinRuleRegistry (or hypergraph approved edges).
- **Audit**: Every query execution is logged with hashes for reproducibility.

---

### Performance Targets (MVP)
- **Cold query** (small demo dataset): < 50ms end-to-end
- **Warm query** (cache hit): < 10ms end-to-end
- **Execution** (engine only): typically < 5ms for demo datasets

---

### Quality Bar / Acceptance Criteria (MVP Definition of Done)
- Ingest sample data via UI and query successfully without manual schema sync
- Clear all resets the system to empty state
- NL queries work for:
  - single-table (list/filter/aggregate/count)
  - **single-join** between two tables
- Results table renders reliably; SQL is optional/hidden by default
- Audit log captures query + plan + SQL + timing

---

### Risks & Mitigations
- **LLM JSON reliability**
  - Mitigation: strict schema, low temperature, JSON-only prompting, repair/normalizer safety net, simulation mode for development
- **Schema drift / sync errors**
  - Mitigation: automatic sync post-ingestion and retry on sync-required errors
- **Join ambiguity**
  - Mitigation: only Approved joins in MVP; show warnings when ambiguous

---

### Roadmap (Post-MVP)
- Multi-hop join path planning (2+ joins)
- Stronger semantic layer (facts/dimensions, measures)
- Join approval UI + governance workflows
- Cost-based optimization & learned caching improvements
- Enterprise connectors (Snowflake/Postgres/BigQuery) + CDC ingestion


