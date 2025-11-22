# Performance Metadata Enhancement Plan

## Executive Summary
This document outlines metadata that can be stored in HyperNode to significantly improve query performance through better optimization decisions.

## Current State
- **NodeStatistics**: row_count, cardinality, size_bytes, last_updated
- **FragmentMetadata**: row_count, min_value, max_value, cardinality, compression, memory_size
- **Schema**: column_names, column_types, full_schema, constraints
- **Aliases**: table_aliases, alias_names

## High-Impact Metadata (Priority Order)

### 1. Column-Level Statistics ⚡ CRITICAL
**Impact**: 10-100x faster filter pushdown and selectivity estimation

**Metadata to Store**:
```json
{
  "column_stats": {
    "column_name": {
      "null_count": 0,
      "distinct_count": 1000,
      "histogram": {
        "buckets": [{"min": 0, "max": 100, "count": 50}, ...],
        "bucket_count": 10
      },
      "percentiles": {
        "p25": 25.0,
        "p50": 50.0,
        "p75": 75.0,
        "p95": 95.0,
        "p99": 99.0
      },
      "skew_indicator": 0.1  // 0 = uniform, 1 = highly skewed
    }
  }
}
```

**Benefits**:
- Accurate selectivity estimation for filters (WHERE col > 50)
- Better filter pushdown decisions
- Improved join cardinality estimation
- Enables histogram-based optimization

**Implementation**: Update during INSERT/UPDATE, compute during ANALYZE

---

### 2. Index Metadata ⚡ CRITICAL
**Impact**: 100-1000x faster indexed queries

**Metadata to Store**:
```json
{
  "indexes": {
    "column_name": [
      {
        "type": "bitmap|btree|vector|bloom",
        "selectivity": 0.05,
        "size_bytes": 1024,
        "usage_count": 1000,
        "last_used": 1234567890
      }
    ]
  }
}
```

**Benefits**:
- Automatic index selection in optimizer
- Index recommendation for slow queries
- Index maintenance decisions (drop unused indexes)
- Cost model uses index selectivity

**Implementation**: Track during index creation, update usage stats during query execution

---

### 3. Access Patterns 🔥 HIGH
**Impact**: 2-10x faster through adaptive optimization

**Metadata to Store**:
```json
{
  "access_patterns": {
    "access_frequency": 100.5,  // accesses per second
    "read_write_ratio": 0.9,    // 90% reads, 10% writes
    "frequently_accessed_columns": ["id", "name", "email"],
    "query_patterns": {
      "filter_patterns": ["WHERE id = ?", "WHERE name LIKE ?"],
      "join_patterns": ["JOIN customers ON ..."],
      "aggregation_patterns": ["GROUP BY status"]
    },
    "temporal_pattern": {
      "hot_hours": [9, 10, 11, 14, 15, 16],  // Peak hours
      "cold_hours": [0, 1, 2, 3, 4, 5]       // Off-peak hours
    }
  }
}
```

**Benefits**:
- Adaptive fragment sizing (smaller fragments for hot data)
- Optimal compression selection (fast decompression for hot data)
- Tier management (hot data in L1, cold in L2/L3)
- Pre-warming frequently accessed columns

**Implementation**: Track during query execution, update periodically

---

### 4. Join Statistics 🔥 HIGH
**Impact**: 5-50x faster join ordering

**Metadata to Store**:
```json
{
  "join_stats": {
    "join_partners": [
      {
        "table": "orders",
        "join_frequency": 1000,
        "join_selectivity": 0.1,
        "join_cardinality": 10000,
        "best_join_order": ["customers", "orders", "items"]
      }
    ],
    "best_join_order_cache": {
      "query_pattern": "customers JOIN orders JOIN items",
      "optimal_order": ["customers", "orders", "items"],
      "cost": 1234.5
    }
  }
}
```

**Benefits**:
- Optimal join order selection (smallest table first)
- Join order caching for repeated patterns
- Better join cardinality estimation
- Adaptive join algorithm selection (hash vs nested loop)

**Implementation**: Track during join execution, update join statistics

---

### 5. Query Optimization Hints ⚡ MEDIUM
**Impact**: 2-5x faster through plan caching and hints

**Metadata to Store**:
```json
{
  "optimization_hints": {
    "optimal_scan_strategy": "index",  // sequential|index|vector
    "filter_selectivity_cache": {
      "WHERE status = 'active'": 0.1,
      "WHERE created_at > '2024-01-01'": 0.3
    },
    "materialized_view_candidates": [
      {
        "expression": "COUNT(*) GROUP BY status",
        "benefit_score": 0.8,
        "access_frequency": 100
      }
    ],
    "cached_query_signatures": [
      "SELECT id, name FROM customers WHERE status = ?"
    ]
  }
}
```

**Benefits**:
- Plan caching for repeated queries
- Materialized view recommendations
- Filter selectivity caching
- Optimal scan strategy selection

**Implementation**: Track during query execution, analyze query patterns

---

### 6. Data Characteristics ⚡ MEDIUM
**Impact**: 2-5x faster through better storage decisions

**Metadata to Store**:
```json
{
  "data_characteristics": {
    "compression_ratio": 0.3,  // 70% compression
    "decompression_cost_ms": 0.5,
    "fragment_hotness": 0.9,   // 0 = cold, 1 = hot
    "sorted_columns": ["id", "created_at"],
    "data_freshness": 1234567890,  // Last update timestamp
    "update_frequency": 10.0  // Updates per second
  }
}
```

**Benefits**:
- Optimal compression selection
- Fragment hotness for tier management
- Sorted column detection (enables binary search)
- Cache invalidation decisions

**Implementation**: Compute during fragment creation, update during access

---

### 7. Cost Model Parameters ⚡ LOW (but improves accuracy)
**Impact**: Better cost estimation = better plans

**Metadata to Store**:
```json
{
  "cost_model": {
    "scan_cost_per_row_ns": 10.0,
    "filter_cost_per_row_ns": 5.0,
    "join_cost_per_row_ns": 50.0,
    "aggregate_cost_per_row_ns": 20.0,
    "last_calibrated": 1234567890
  }
}
```

**Benefits**:
- Accurate cost estimation
- Better plan selection
- Adaptive cost model calibration

**Implementation**: Measure during query execution, calibrate periodically

---

## Implementation Strategy

### Phase 1: Immediate High Impact (Week 1-2)
1. **Column-level statistics** (null_count, distinct_count, histogram)
2. **Index metadata** (indexes, index_selectivity, usage_stats)
3. **Access patterns** (access_frequency, frequently_accessed_columns)

**Expected Speedup**: 10-100x for filtered queries, 100-1000x for indexed queries

### Phase 2: Medium Impact (Week 3-4)
4. **Join statistics** (join_cardinality, join_selectivity, best_join_order)
5. **Query optimization hints** (optimal_scan_strategy, filter_selectivity_cache)

**Expected Speedup**: 5-50x for join queries, 2-5x for repeated queries

### Phase 3: Long-term (Month 2+)
6. **Data characteristics** (compression_ratio, fragment_hotness)
7. **Cost model parameters** (actual measured costs)
8. **Query result caching metadata** (cache_hit_rate, invalidation triggers)

**Expected Speedup**: 2-5x overall improvement

---

## Storage Format

All metadata stored in `HyperNode.metadata: HashMap<String, String>` as JSON:

```rust
// Example structure
node.metadata.insert("column_stats".to_string(), serde_json::to_string(&column_stats)?);
node.metadata.insert("indexes".to_string(), serde_json::to_string(&indexes)?);
node.metadata.insert("access_patterns".to_string(), serde_json::to_string(&access_patterns)?);
```

---

## Update Strategy

1. **Lazy Updates**: Update metadata during query execution (increment counters)
2. **Periodic Analysis**: Run ANALYZE command to recompute statistics
3. **Incremental Updates**: Update only changed columns/tables
4. **Background Jobs**: Heavy computations (histograms) in background

---

## Performance Impact Summary

| Metadata Type | Speedup | Use Case |
|--------------|---------|----------|
| Column Statistics | 10-100x | Filter pushdown, selectivity |
| Index Metadata | 100-1000x | Index selection |
| Access Patterns | 2-10x | Adaptive optimization |
| Join Statistics | 5-50x | Join ordering |
| Optimization Hints | 2-5x | Plan caching |
| Data Characteristics | 2-5x | Storage optimization |
| Cost Model | 1.5-2x | Better plans |

**Total Potential Speedup**: 100-1000x for optimized queries

