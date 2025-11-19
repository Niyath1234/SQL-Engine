# RAM & Graph Optimization Plan for >160x Speedup

## 🎯 Goal
Achieve >160x speedup vs DuckDB through aggressive RAM and graph optimizations.

## 📊 Current Performance
- **Current**: ~5.72x faster than DuckDB for `SELECT * LIMIT 10`
- **Target**: >160x faster
- **Gap**: Need ~28x more optimization

---

## 🚀 RAM Optimizations

### 1. Memory-Mapped Fragments (Zero-Copy)
**Impact**: 5-10x speedup for small LIMITs
- Use `mmap` to memory-map fragment data
- Avoid copying arrays from fragments to batches
- Direct pointer access to fragment memory
- **Implementation**:
  - Add `mmap` support to `ColumnFragment`
  - Use `Arc<Mmap>` instead of `Arc<dyn Array>` for fragments
  - Slice directly from mapped memory

### 2. Cache-Aligned Fragment Layout
**Impact**: 2-3x speedup for SIMD operations
- Align fragment data to 64-byte cache lines
- Ensure SIMD operations hit cache-aligned boundaries
- **Implementation**:
  - Pad fragment arrays to cache line boundaries
  - Use `#[repr(align(64))]` for fragment metadata
  - Reorganize fragment structure for cache locality

### 3. Lazy Fragment Loading
**Impact**: 10-20x speedup for small LIMITs
- Only load fragment metadata initially
- Defer loading actual array data until needed
- For `LIMIT 10`, only load first fragment
- **Implementation**:
  - Store fragment metadata (size, type, location) without arrays
  - Load arrays on-demand in `ScanOperator::next()`
  - Use lazy evaluation for fragment access

### 4. Hot Fragment Cache (L1 Cache)
**Impact**: 2-5x speedup for repeated queries
- Keep frequently accessed fragments in CPU cache
- Use LRU cache for hot fragments
- **Implementation**:
  - Track fragment access frequency
  - Maintain hot fragment list (top 10-20 fragments)
  - Pre-warm cache for common query patterns

### 5. Memory Pooling
**Impact**: 1.5-2x speedup by reducing allocations
- Reuse `ExecutionBatch` buffers
- Pool `ColumnarBatch` allocations
- **Implementation**:
  - Use `object-pool` crate for batch pooling
  - Reuse selection bitmaps
  - Pre-allocate common buffer sizes

### 6. Prefetching
**Impact**: 1.5-2x speedup by hiding memory latency
- Prefetch next fragment while processing current
- Use `std::hint::black_box` and `prefetch` intrinsics
- **Implementation**:
  - Prefetch fragment metadata in `ScanOperator::new()`
  - Prefetch next fragment array in `ScanOperator::next()`
  - Use `_mm_prefetch` for SIMD prefetching

---

## 🕸️ Graph Optimizations

### 1. Fragment-Level Min/Max Statistics
**Impact**: 10-50x speedup for filtered queries
- Store min/max values per fragment
- Skip entire fragments that cannot match WHERE clauses
- **Implementation**:
  - Add `min_value`, `max_value` to `FragmentMetadata`
  - Compute during fragment creation
  - Check in `ScanOperator` before loading fragment
  - Example: `WHERE col > 1000` → skip fragments with `max < 1000`

### 2. Hypergraph Path Cache
**Impact**: 100-1000x speedup for repeated queries
- Cache frequently used join paths as materialized fragments
- Store pre-computed join results in hypergraph
- **Implementation**:
  - Detect common join patterns (e.g., `fact JOIN dim1 JOIN dim2`)
  - Materialize join paths as new fragments
  - Reuse materialized paths for identical queries
  - Invalidate on data updates

### 3. Node/Edge Hash Indexes
**Impact**: 5-10x speedup for graph traversal
- O(1) lookups for table/column nodes
- Fast edge lookup by table/column pairs
- **Implementation**:
  - Add `HashMap<NodeId, HyperNode>` for nodes
  - Add `HashMap<(String, String), EdgeId>` for edges
  - Index by table name, column name
  - Replace `iter_nodes()` with direct hash lookups

### 4. Query Pattern Recognition
**Impact**: 50-200x speedup for repeated patterns
- Detect repeated query patterns
- Reuse cached execution plans and paths
- **Implementation**:
  - Hash query structure (tables, joins, filters)
  - Cache execution plans by query signature
  - Reuse materialized fragments for identical patterns
  - Learn common patterns over time

### 5. Fragment Bloom Filters
**Impact**: 5-20x speedup for filtered queries
- Use bloom filters to quickly skip fragments
- Check filter predicates against bloom filter
- **Implementation**:
  - Add bloom filter to `FragmentMetadata`
  - Build bloom filter during fragment creation
  - Check `WHERE col = value` against bloom filter
  - Skip fragment if bloom filter says "not present"

---

## 🎯 Implementation Priority

### Phase 1: Quick Wins (Target: 20-30x speedup)
1. ✅ **Lazy Fragment Loading** - Biggest impact for small LIMITs
2. ✅ **Fragment Min/Max Statistics** - Skip fragments early
3. ✅ **Node/Edge Hash Indexes** - Faster graph traversal

### Phase 2: Memory Optimizations (Target: 10-15x speedup)
4. ✅ **Memory-Mapped Fragments** - Zero-copy access
5. ✅ **Cache-Aligned Layout** - Better SIMD performance
6. ✅ **Memory Pooling** - Reduce allocations

### Phase 3: Advanced Optimizations (Target: 5-10x speedup)
7. ✅ **Hypergraph Path Cache** - Materialize common joins
8. ✅ **Query Pattern Recognition** - Reuse cached plans
9. ✅ **Fragment Bloom Filters** - Fast fragment skipping
10. ✅ **Hot Fragment Cache** - Keep hot data in cache
11. ✅ **Prefetching** - Hide memory latency

---

## 📈 Expected Combined Impact

- **Phase 1**: 20-30x speedup
- **Phase 2**: 10-15x additional speedup
- **Phase 3**: 5-10x additional speedup
- **Total**: **35-55x combined speedup** (multiplicative effects)

**Current**: 5.72x → **After optimizations**: **200-300x** (exceeds 160x target)

---

## 🔧 Technical Details

### Memory-Mapped Fragments
```rust
struct MappedFragment {
    mmap: Arc<Mmap>,
    offset: usize,
    length: usize,
    data_type: DataType,
}
```

### Fragment Statistics
```rust
struct FragmentMetadata {
    min_value: Option<Value>,
    max_value: Option<Value>,
    bloom_filter: Option<BloomFilter>,
    row_count: usize,
    // ... existing fields
}
```

### Graph Indexes
```rust
struct HyperGraph {
    nodes: DashMap<NodeId, HyperNode>,
    node_index: HashMap<String, NodeId>,  // table_name -> node_id
    edge_index: HashMap<(String, String), EdgeId>,  // (table, column) -> edge_id
    // ... existing fields
}
```

---

## ✅ Success Criteria

- [ ] `SELECT * LIMIT 10` achieves >160x speedup vs DuckDB
- [ ] `SELECT * WHERE col = value LIMIT 10` achieves >100x speedup
- [ ] Memory usage remains reasonable (<2x data size)
- [ ] Query latency < 0.01ms for simple queries

