## Benchmark Protocol for Hypergraph SQL Engine

This document describes how to **reproduce and verify** the performance results of the Hypergraph SQL Engine against PostgreSQL and DuckDB.

### 1. Environment

- **Hardware**: document at least:
  - CPU model, number of cores, clock speed.
  - RAM size.
  - Storage type (SSD/HDD).
- **Software**:
  - OS and version (e.g., macOS 15.x, Linux kernel version).
  - Rust toolchain (`rustc --version`).
  - PostgreSQL version (e.g., `psql --version`).
  - DuckDB version (embedded via the Rust crate, pinned in `Cargo.toml`).

Run on an otherwise idle machine where possible.

### 2. Dataset

Default dataset used in this repo:

- CSV: `annual-enterprise-survey-2024-financial-year-provisional.csv`
- Rows: ~55,620
- Columns: 10 (Year, industry aggregation columns, Units, Variable, Value, etc.)

You can substitute any CSV file with similar size/shape; just pass its path to the benchmark script.

### 3. Queries

The benchmark suite is implemented in `src/bin/compare_engines.rs`.  
If you run it with only the CSV argument:

```bash
cargo run --release --bin compare_engines -- /path/to/data.csv
```

it executes a **fixed list of queries**, covering:

- Simple scans with `LIMIT` (10, 100, 1000).
- Column projections (`SELECT Year, Value ...`).
- `WHERE` predicates (`=`, `>`, `>=`, AND).
- `ORDER BY` (ASC/DESC) with `LIMIT`.
- `GROUP BY` + aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`).
- Combined queries (`WHERE + GROUP BY + ORDER BY + LIMIT`).

The exact SQL strings are visible near the top of `compare_engines.rs` for inspection and modification.

### 4. Procedure

1. **Build**:

   ```bash
   cargo build --release --bin compare_engines
   ```

2. **Run full suite** (recommended):

   ```bash
   ./benchmarks/run_benchmarks.sh /path/to/data.csv
   ```

   This:
   - Builds the release binary.
   - Runs `target/release/compare_engines` with the given CSV.
   - Executes the full query list against:
     - Hypergraph SQL Engine.
     - PostgreSQL (local, default `postgres` DB).
     - DuckDB (embedded).

3. **PostgreSQL setup**:

   `compare_engines.rs` will:
   - Connect to PostgreSQL using several common connection strings, e.g.:
     - `host=localhost user=$USER dbname=postgres`
   - Create a table `enterprise_survey`.
   - Load the CSV into that table.

   Ensure:
   - PostgreSQL is running locally.
   - You have permission to connect and create tables.

### 5. Fairness & repetitions

For each query, the benchmark:

- Runs **N times** per engine (constant in the code, currently 5 runs).
- Prints:
  - Per-run timings.
  - Average latency in milliseconds.
  - Row counts.
  - Speedup vs PostgreSQL and DuckDB.

This makes the comparison fair:

- Same CSV source.
- Same machine.
- Same queries and execution order.

### 6. Interpreting results

Key lines to look at:

- **Per-query comparison**:

  ```text
  📊 Comparison:
    🚀 Hypergraph Engine is 18.84714x FASTER than PostgreSQL
    🚀 Hypergraph Engine is 15.82980x FASTER than DuckDB
  ```

- **Final summary table** (latency and speedup for all queries).
- **Average Speedup vs PostgreSQL / DuckDB** at the bottom.

### 7. Ablation studies (optional but recommended)

To show *why* the hypergraph engine is fast, you can re-run the benchmarks with individual features disabled (by temporarily changing code and rebuilding), for example:

- Disable adaptive fragmenting.
- Disable learned/tiered indexes.
- Disable trace-based specializations.

Record the new average speedups and compare to the baseline; this demonstrates the impact of each idea.


