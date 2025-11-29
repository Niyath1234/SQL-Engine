# Join Engine Architecture

## Overview

The join engine implements DuckDB/Presto-grade join execution with support for 50-100+ table joins. It uses a two-phase hash join algorithm with cost-based join ordering and stable column identity.

## Architecture Components

### 1. Stable Column Identity

**Location**: `src/metadata/column_id.rs`, `src/metadata/schema.rs`

- **ColumnId**: Stable identifier `{table_id, column_ordinal}` that never changes through joins
- **ColumnSchema**: Enhanced schema with:
  - `Vec<ColumnId>` - Ordered column IDs
  - `qualified_lookup` - Fast qualified name → ColumnId lookup
  - `unqualified_lookup` - Unqualified name → ColumnId lookup (handles ambiguity)
  - `physical_index` - ColumnId → physical array index mapping

**Key Invariant**: All column resolution is ColumnId-driven, not index-driven.

### 2. Join Graph (Logical Plan)

**Location**: `src/query/join_graph.rs`

- Represents joins as a graph: nodes (tables) and edges (join predicates)
- Unordered - planner determines join order by walking the graph
- Includes connectivity validation (DFS-based)

### 3. Cost-Based Join Ordering

**Location**: `src/query/join_order.rs`

- **Algorithm**: Greedy heuristic
  1. Start with table of smallest cardinality
  2. Iteratively pick next table that minimizes join cost
  3. Cost = left_rows × right_rows × selectivity

- **Rules**:
  - Inner joins: Can be reordered freely
  - Outer joins: Preserve textual order

### 4. Two-Phase Hash Join

**Location**: `src/execution/operators.rs` (JoinOperator)

**State Machine**:
- **Build**: Collect all right-side batches, build hash table
- **Probe**: Stream left batches, probe hash table, materialize results
- **Finished**: Always return None

**Key Features**:
- Strict state transitions (no fallthrough)
- Termination guards (max iterations, empty batch detection)
- Handles nested joins with retry logic

### 5. Hash Table Structure

**Location**: `src/execution/hash_table.rs`

- Unified `HashTable` API
- Fast hashing (FxHash)
- Bucket-based storage
- Partitioning hooks for future grace hash join
- Multi-key join support

### 6. Pipeline Model

**Location**: `src/execution/pipeline.rs`

- **PipelineOp**: Scan, Filter, Project, HashBuild, HashProbe, Sort, Aggregate
- **Pipeline Breakers**: HashBuild, Sort (must complete before next pipeline)
- **PipelineExecutor**: Coordinates build and probe pipelines

### 7. Runtime Filters (Bloom Filters)

**Location**: `src/execution/bloom_filter.rs`

- Constructed from join keys after hash table build
- Propagated to earlier scans to filter out non-matching rows
- Drastically improves performance for 20-100 table joins

### 8. Operator Contracts

**Location**: `src/execution/operator_contracts.rs`

**Core Contract**: `next()` must eventually return `None`

**Safety Counters**:
- MAX_BATCHES: 1M batches
- MAX_EMPTY_BATCHES: 100 consecutive empty batches
- MAX_EXECUTION_TIME: 60 seconds

## Execution Flow

```
1. Parse SQL → ParsedQuery
2. Build Join Graph from join edges
3. Determine Join Order (cost-based)
4. Build Physical Join Tree
5. Execute:
   a. Build Phase: Build hash tables for all joins (right sides)
   b. Probe Phase: Stream through probe pipeline
6. Return results
```

## Join Order Example

For query:
```sql
SELECT c.name, o.order_id, p.name
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON o.product_id = p.product_id;
```

**Join Graph**:
- Nodes: [c, o, p]
- Edges: [(c.customer_id = o.customer_id), (o.product_id = p.product_id)]

**Join Order** (cost-based):
1. Start with smallest table (e.g., `customers`)
2. Join `orders` (minimizes cost)
3. Join `products` (minimizes cost)

**Physical Tree**:
```
Join(customers, orders)
  left: Scan(customers)
  right: Scan(orders)
Join(Join(customers, orders), products)
  left: Join(customers, orders)
  right: Scan(products)
```

## Performance Optimizations

1. **Stable Column Identity**: Avoids re-resolution through joins
2. **Cost-Based Ordering**: Minimizes intermediate result sizes
3. **Hash Table Caching**: Reuses hash tables across batches
4. **Bloom Filters**: Filters non-matching rows early
5. **Vectorized Operations**: Works on Arrow arrays, not per-row

## Termination Guarantees

All operators must:
- Eventually return `None` (no infinite loops)
- Not produce infinite empty batches
- Respect timeout limits
- Log state transitions for debugging

## Future Enhancements

1. **Partitioned Hash Join**: Grace hash join with spill-to-disk
2. **Parallel Execution**: Build multiple hash tables concurrently
3. **Adaptive Join Ordering**: Learn from query execution statistics
4. **SIMD Hashing**: Use SIMD instructions for faster hashing

## Testing

See `tests/comprehensive_join_tests.rs` for:
- 5-table joins
- 10-table and 20-table joins
- Star schema tests
- Linear chain tests
- Repeated column name tests

## Benchmarks

See `benches/join_bench.rs` for performance measurements:
- Build-time latency
- Probe throughput
- Memory usage

