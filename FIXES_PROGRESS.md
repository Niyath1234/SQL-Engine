# SQL Engine Fixes - Progress Report

## Completed ✅

### 1. COMPONENT_1: Unified ColumnResolver Module
- ✅ Created `src/query/column_resolver.rs` with unified column resolution logic
- ✅ Added ColumnResolver struct with resolve() and try_resolve() methods
- ✅ Supports qualified names (table.column), unqualified names, table aliases, case-insensitive matching
- ✅ Added module to `src/query/mod.rs`
- ✅ Integrated ColumnResolver into WindowOperator

**Files Modified:**
- `src/query/column_resolver.rs` (NEW)
- `src/query/mod.rs` (added module)
- `src/execution/window.rs` (uses ColumnResolver)

### 2. WindowOperator Integration
- ✅ Updated WindowOperator to use ColumnResolver in `resolve_column_index()`
- ✅ Maintains backward compatibility with existing code

## In Progress 🔄

### 3. ProjectOperator Integration (Issue 4)
**Status**: Needs to replace complex column resolution logic (lines 2683-2774) with ColumnResolver
**Location**: `src/execution/operators.rs` around line 2680

**Current Issue**: ProjectOperator has 90+ lines of custom column resolution logic that should be replaced with ColumnResolver. This will fix Query 9.

**Action Needed**:
```rust
// Replace lines 2683-2774 with:
let resolver = ColumnResolver::new(batch.batch.schema.clone(), self.table_aliases.clone());
match resolver.try_resolve(col_name) {
    Some(col_idx) => {
        // Add column to output
    },
    None => {
        // Add to processing_errors and continue
    }
}
```

## Remaining Work 🚧

### 4. COMPONENT_2: Centralize Schema Propagation
**Status**: Not started
**Action**: Each operator should compute its output schema deterministically based on input schema
- Add `Operator::output_schema(input_schema: SchemaRef) -> SchemaRef` trait method
- Remove `self.schema` fields that freeze schema at initialization
- Update all operators: WindowOperator, ProjectOperator, JoinOperator, etc.

### 5. ISSUE_1: Fix WindowOperator Schema Problem (Query 2)
**Status**: ColumnResolver integrated, but schema propagation issue remains
**Root Cause**: WindowOperator output schema doesn't preserve all input columns when ProjectOperator tries to access them
**Fix Needed**: Ensure WindowOperator's `next()` method correctly preserves all input columns in output schema (already uses `batch.batch.schema`, but may need verification)

### 6. ISSUE_2: Implement Correlated Subquery Support (Query 4)
**Status**: Not started
**Files to Modify**:
- `src/query/parser_enhanced.rs`: Remove error on subquery detection (line 417)
- `src/execution/operators.rs`: Update FilterOperator to evaluate subqueries per row
- `src/execution/subquery_executor.rs`: Add outer_context support

**Action Needed**:
1. In parser: Change `extract_predicates()` to create `Expression::Subquery` instead of error
2. In FilterOperator: For each row, create single-row ExecutionBatch and pass to subquery executor
3. In SubqueryExecutor: Resolve columns from outer_context when not found in inner query

### 7. ISSUE_3: Fix CTE Wildcard Expansion (Query 7)
**Status**: Not started
**Root Cause**: Wildcard expansion happens at runtime, so materialized CTE loses columns
**Fix Needed**:
- Add planner pass to expand wildcards before execution
- Ensure CTE materialization uses fully expanded projection list
- Ensure WindowOperator output in CTE is preserved in materialized schema

**Files to Modify**:
- `src/query/planner.rs`: Add wildcard expansion pass
- `src/execution/cte_scan.rs`: Ensure materialized schema is complete

### 8. ISSUE_4: Fix ProjectOperator Dropping Columns (Query 9)
**Status**: Partially in progress (ColumnResolver ready, needs integration)
**Action**: Replace ProjectOperator column resolution with ColumnResolver (see section 3 above)

### 9. TEST_SUITE: Add Regression Tests
**Status**: Not started
**Action**: Add comprehensive tests for all fixes

## Test Results

**Current Status**: 6/10 queries passing (60%)

**Passing Queries**:
- ✅ Query 1: Nested CTEs with Multiple JOINs
- ✅ Query 3: Multiple Aggregations with GROUP BY and HAVING  
- ✅ Query 5: UNION with ORDER BY
- ✅ Query 6: Three-way JOIN
- ✅ Query 8: Multiple JOINs with EXISTS subquery
- ✅ Query 10: Complex Aggregation with CASE expressions

**Failing Queries**:
- ❌ Query 2: Window Functions - `All projection expressions failed: Column 'e.department_id' not found`
- ❌ Query 4: Correlated Subquery - `Correlated subqueries in WHERE clauses are not yet supported`
- ❌ Query 7: CTE with Window Functions - `All projection expressions failed: Column 'er.global_rank' not found`
- ❌ Query 9: Self JOIN - `Column 'e1.department_id' not found in schema. Available columns: ["salary_diff"]`

## Next Steps (Priority Order)

1. **Complete ProjectOperator ColumnResolver Integration** (Issue 4 - Query 9)
   - Replace complex column resolution logic with ColumnResolver
   - Test Query 9

2. **Fix WindowOperator Output Schema** (Issue 1 - Query 2)
   - Verify WindowOperator preserves all input columns correctly
   - Test Query 2

3. **Fix CTE Wildcard Expansion** (Issue 3 - Query 7)
   - Add planner pass for wildcard expansion
   - Ensure CTE materialization preserves all columns
   - Test Query 7

4. **Implement Correlated Subquery Support** (Issue 2 - Query 4)
   - Most complex, requires architectural changes
   - Test Query 4

5. **Add Comprehensive Test Suite**
   - Regression tests for all fixes
   - Edge case testing

## Files Modified (Summary)

**New Files:**
- `src/query/column_resolver.rs` - Unified column resolution

**Modified Files:**
- `src/query/mod.rs` - Added column_resolver module
- `src/execution/window.rs` - Integrated ColumnResolver

**Files That Need Modification:**
- `src/execution/operators.rs` - ProjectOperator column resolution
- `src/execution/operators.rs` - FilterOperator subquery evaluation  
- `src/query/parser_enhanced.rs` - Remove subquery error, expand wildcards
- `src/execution/cte_scan.rs` - CTE schema preservation
- `src/execution/subquery_executor.rs` - Outer context support
- `src/query/planner.rs` - Schema propagation, wildcard expansion

