# Edge Cases Analysis

This document details the 3 remaining edge cases found during comprehensive testing.

---

## Edge Case 1: Multiple JOINs - ORDER BY Column Name Mismatch

### Query
```sql
SELECT 
    e1.name AS employee,
    e2.name AS manager_candidate,
    e1.salary AS emp_salary,      -- Alias: emp_salary
    e2.salary AS mgr_salary,
    d.name AS department
FROM employees e1
INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
INNER JOIN departments d ON e1.department_id = d.id
ORDER BY e1.department_id, e1.salary  -- Using qualified name: e1.salary
```

### Error
```
Column 'emp_salary' not found in schema. 
Available columns: ["e1.salary", "e1.department_id"]
```

### Root Cause
The planner's ORDER BY column injection logic adds `emp_salary` (the SELECT alias) to the projection when it detects ORDER BY needs it. However:
- The ORDER BY clause uses `e1.salary` (qualified column name)
- The planner adds `emp_salary` (the alias) to projection
- SortOperator tries to resolve `e1.salary` but the projection may have changed the column name
- There's a mismatch between what ORDER BY expects vs what projection provides

### Code Location
- **Planner**: `src/query/planner.rs` - Lines ~764-803 (ORDER BY column injection)
- **SortOperator**: `src/execution/operators.rs` - Lines ~3366-3640 (Column resolution)

### Current Behavior
1. Planner sees ORDER BY `e1.salary`
2. Planner checks if `e1.salary` is in SELECT list → No (only `emp_salary` alias is there)
3. Planner adds `e1.salary` to projection
4. ProjectOperator projects columns including both `emp_salary` (aliased) and `e1.salary` (added for ORDER BY)
5. SortOperator tries to resolve `e1.salary` from output schema
6. Schema may have inconsistent column names after projection

### Expected Behavior
ORDER BY should be able to resolve columns by:
1. First trying the exact column name from ORDER BY (`e1.salary`)
2. Then trying the alias if available (`emp_salary`)
3. Using ColumnResolver with table aliases for robust matching

---

## Edge Case 2: Nested CTEs - CTE Context Not Propagated

### Query
```sql
WITH dept_stats AS (
    SELECT 
        department_id,
        AVG(salary) AS avg_salary,
        COUNT(*) AS emp_count
    FROM employees
    GROUP BY department_id
),
high_avg_depts AS (
    SELECT *
    FROM dept_stats           -- References first CTE
    WHERE avg_salary > 60000.0
)
SELECT * FROM high_avg_depts
```

### Error
```
Table 'dept_stats' not found in hypergraph. 
Available tables: ["employees", "departments"]
```

### Root Cause
When materializing nested CTEs:
1. First CTE (`dept_stats`) is materialized and stored in `cte_results`
2. Second CTE (`high_avg_depts`) needs to reference `dept_stats`
3. During materialization of `high_avg_depts`, the CTE context (`dept_stats`) is not available
4. The nested CTE executor doesn't have access to previously materialized CTEs

### Code Location
- **CTE Materialization**: `src/engine.rs` - `execute_query()` method
- **CTE Context**: `src/query/cte.rs` - CTE context management
- **Nested CTE Execution**: `src/execution/engine.rs` - `execute_subquery_ast()`

### Current Behavior
1. Materialize `dept_stats` CTE → stored in `cte_results` with key "dept_stats"
2. Start materializing `high_avg_depts` CTE
3. `high_avg_depts` queries `FROM dept_stats`
4. The query planner tries to resolve `dept_stats` as a physical table in hypergraph
5. `dept_stats` is not in hypergraph (it's a CTE), so it fails

### Expected Behavior
During nested CTE materialization:
1. CTE context should include all previously materialized CTEs
2. When planning a nested CTE query, CTE names should be resolved from `cte_results` first
3. CTE names should only fall back to hypergraph table lookup if not found in CTE context

---

## Edge Case 3: Complex JOIN + Window + CTE - Column Alias Lost in CTE Materialization

### Query
```sql
WITH dept_info AS (
    SELECT 
        e.*,
        d.name AS dept_name           -- Alias: dept_name
    FROM employees e
    INNER JOIN departments d ON e.department_id = d.id
)
SELECT 
    name,
    dept_name,                        -- Trying to use alias
    salary,
    RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC) AS rank_in_dept
FROM dept_info
ORDER BY dept_name, rank_in_dept
```

### Error
```
Column 'dept_name' not found. 
Available columns: ["e.id", "e.name", "e.salary", "e.department_id", "departments.name"]
```

### Root Cause
When materializing the CTE:
1. The CTE projection includes `d.name AS dept_name` (alias)
2. After JOIN, the column is qualified as `departments.name` in the schema
3. The alias `dept_name` is not preserved in the materialized CTE schema
4. When the main query references `dept_name`, it can't find it because the CTE schema has `departments.name`

### Code Location
- **CTE Materialization**: `src/engine.rs` - CTE materialization logic
- **ProjectOperator**: `src/execution/operators.rs` - Alias handling in projection
- **CTEScanOperator**: `src/execution/cte_scan.rs` - CTE schema return

### Current Behavior
1. Materialize CTE `dept_info`:
   - JOIN produces columns: `["e.id", "e.name", "e.salary", "e.department_id", "departments.name"]`
   - ProjectOperator projects with alias `dept_name` → but schema still has `departments.name`
   - CTE stored with schema: `["e.id", "e.name", "e.salary", "e.department_id", "departments.name"]`
2. Main query tries to SELECT `dept_name` from CTE
3. ColumnResolver looks for `dept_name` but schema only has `departments.name`
4. Resolution fails

### Expected Behavior
When materializing CTE:
1. ProjectOperator should preserve aliases in the output schema
2. If `d.name AS dept_name`, the materialized CTE schema should have a column named `dept_name`
3. The alias should be stored in the CTE schema, not just in the projection expressions
4. CTEScanOperator should return the schema with aliases preserved

---

## Summary

### Issue Categories

1. **Column Resolution Mismatch** (Edge Case 1 & 3)
   - Alias preservation through CTEs
   - ORDER BY column name vs alias mismatch
   - Qualified names vs aliases in different query stages

2. **CTE Context Propagation** (Edge Case 2)
   - Nested CTE materialization
   - CTE context not available during nested CTE planning
   - CTE name resolution fallback logic

### Impact

- **Severity**: Low-Medium (edge cases, not common queries)
- **Frequency**: Only affects specific query patterns (nested CTEs, complex aliasing)
- **Workaround**: Use qualified column names instead of aliases in ORDER BY; avoid nested CTEs

### Complexity to Fix

1. **Edge Case 1**: Medium - Need to improve ORDER BY column resolution strategy
2. **Edge Case 2**: High - Requires CTE context propagation infrastructure
3. **Edge Case 3**: Medium - Need to preserve aliases in CTE materialization schema

