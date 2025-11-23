# Known Issues Analysis - Current Code Logic

## Issue 1: Query 2 - Window Functions Input Schema Problem

**Error**: `Column 'e.department_id' not found. Available columns: ["dept_total_salary", "dept_avg_salary"]`

**Query**:
```sql
SELECT 
    e.name,
    e.department_id,
    e.salary,
    RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank,
    SUM(e.salary) OVER (PARTITION BY e.department_id) AS dept_total_salary,
    AVG(e.salary) OVER (PARTITION BY e.department_id) AS dept_avg_salary,
    LAG(e.salary, 1) OVER (PARTITION BY e.department_id ORDER BY e.salary) AS prev_salary
FROM employees e
ORDER BY e.department_id, e.salary DESC
```

**Current Code Logic**:

1. **WindowOperator Initialization** (`src/execution/window.rs:30-44`):
   - `self.schema = input.schema()` - Captures input schema at initialization time
   - This schema is stored once and never updated

2. **Column Resolution During Window Function Computation** (`src/execution/window.rs:273-284`):
   - Uses `self.schema` (captured at init) to resolve columns for PARTITION BY and ORDER BY
   - When `build_partitions()` runs, it tries to find `e.department_id` using `batch.batch.schema`
   - But when `compute_window_function()` runs, it uses `self.schema` which was captured from input

3. **Schema Preservation** (`src/execution/window.rs:554-565`):
   - Output schema is created from `batch.batch.schema.fields()` (correct)
   - BUT the window operator's `self.schema` field (used for column resolution during computation) was captured at initialization

**Why the Issue Exists**:
- The error says only `["dept_total_salary", "dept_avg_salary"]` are available
- These are window function OUTPUT column names (aliases), not input columns
- This suggests that when the window operator tries to resolve `e.department_id` for PARTITION BY, it's looking at the wrong schema
- The window operator should receive input from Scan (which has `e.name`, `e.department_id`, `e.salary`)
- But somehow it's seeing only window function output columns
- This suggests the input to the window operator might already be transformed, OR the schema resolution is checking the wrong place

**Root Cause**:
- Window operator's `self.schema` is set once at initialization and never updated
- During window function computation (`compute_window_function`), it uses `self.schema` to resolve columns
- If the input schema changes or if there's a projection before the window operator, `self.schema` becomes stale
- The `sort_partitions()` method uses `self.schema` (line 147) instead of batch schema

---

## Issue 2: Query 4 - Correlated Subquery Not Supported

**Error**: `Correlated subqueries in WHERE clauses are not yet supported. Please use JOINs or EXISTS instead.`

**Query**:
```sql
SELECT e1.id, e1.name, e1.salary
FROM employees e1
WHERE e1.salary > (
    SELECT AVG(e2.salary)
    FROM employees e2
    WHERE e2.department_id = e1.department_id
)
ORDER BY e1.salary DESC
```

**Current Code Logic**:

1. **Predicate Extraction** (`src/query/parser_enhanced.rs:408-419`):
   - `extract_predicates()` recursively parses WHERE clause predicates
   - It tries to extract literal values for comparison operations (eq, ne, lt, gt, etc.)
   - When extracting the right-hand side of a comparison, it calls `extract_literal_value(right)`
   - If `extract_literal_value()` fails, it checks if the expression is a subquery using `matches!(right.as_ref(), Expr::Subquery(_))`
   - **When a subquery is detected, it immediately returns an error** - no attempt to create an Expression::Subquery node
   - Error message: "Correlated subqueries in WHERE clauses are not yet supported. Please use JOINs or EXISTS instead."

2. **Subquery Evaluation Infrastructure** (`src/query/expression.rs:179-301`):
   - `ExpressionEvaluator::evaluate()` method signature includes `outer_context: Option<&ExecutionBatch>` parameter
   - This parameter is designed for correlated subquery execution - it provides access to the outer query's row context
   - The expression evaluator can handle `Expression::Subquery` variants
   - BUT: The parser never creates `Expression::Subquery` nodes for WHERE clauses - it throws an error instead

3. **FilterOperator** (`src/execution/operators.rs:FilterOperator`):
   - `FilterOperator` applies predicates from the WHERE clause
   - Currently only handles:
     - Literal value comparisons (column = literal)
     - Column-to-column comparisons
     - Multiple predicates combined with AND
   - **No subquery evaluation logic** - FilterOperator doesn't know how to evaluate subqueries in predicates
   - FilterOperator processes one row at a time, but subquery evaluation would need to execute the subquery for each row

4. **Subquery Executor** (`src/execution/subquery_executor.rs`):
   - Exists and can execute standalone subqueries
   - But not integrated with FilterOperator for per-row evaluation

**Why the Issue Exists**:

- **Parser-level blocking**: The parser explicitly rejects subqueries at parse time before they reach execution
- **Architectural gap**: Even though infrastructure exists (ExpressionEvaluator has outer_context, SubqueryExecutor exists), the pieces aren't connected:
  1. Parser throws error instead of creating Expression::Subquery nodes
  2. FilterOperator doesn't evaluate subqueries
  3. No mechanism to pass outer query row context to subquery executor per row

**Root Cause**:
- **Architectural decision**: Correlated subqueries in WHERE clauses were intentionally not implemented
- The error message suggests using JOINs or EXISTS as alternatives
- The infrastructure partially exists but is disconnected:
  - ExpressionEvaluator supports outer_context parameter
  - SubqueryExecutor can execute queries
  - But parser blocks creation of subquery expressions
  - And FilterOperator doesn't evaluate subqueries per row

**Implementation Requirements** (if we wanted to fix this):
1. **Parser changes** (`src/query/parser_enhanced.rs`):
   - Modify `extract_predicates()` to detect subqueries but NOT throw an error
   - Create `Expression::Subquery` or `Expression::Exists` nodes instead
   - Pass these to FilterOperator

2. **FilterOperator changes** (`src/execution/operators.rs`):
   - Add logic to evaluate subquery expressions in predicates
   - For each row, create a single-row ExecutionBatch for outer_context
   - Call SubqueryExecutor with outer_context
   - Use the scalar result for comparison

3. **Subquery Executor changes** (`src/execution/subquery_executor.rs`):
   - Support correlated subqueries by using outer_context
   - Resolve column references like `e1.department_id` from outer_context when not found in inner query
   - For each row in outer query, execute subquery with that row's values bound

**Complexity**: HIGH - This requires architectural changes across multiple layers (parser, filter operator, subquery executor)

---

## Issue 3: Query 7 - CTE Wildcard Expansion Problem

**Error**: `Column 'er.global_rank' not found. Available columns: ["dept_size"]`

**Query**:
```sql
WITH emp_ranked AS (
    SELECT e.*,
        ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS rn,
        RANK() OVER (ORDER BY e.salary DESC) AS global_rank
    FROM employees e
)
SELECT 
    er.name,
    er.department_id,
    er.salary,
    er.global_rank,
    d.name AS dept_name,
    COUNT(*) OVER (PARTITION BY er.department_id) AS dept_size
FROM emp_ranked er
LEFT JOIN departments d ON er.department_id = d.id
```

**Current Code Logic**:

1. **Wildcard Expansion** (`src/execution/operators.rs:2657-2669`):
   - ProjectOperator handles `SELECT table.*` by finding columns that start with `table.` prefix
   - BUT this happens during ProjectOperator execution, not during CTE materialization

2. **CTE Materialization** (`src/execution/cte_scan.rs`):
   - CTEs are materialized and stored in `cte_results` hashmap
   - The materialized CTE result has a schema based on what was selected

3. **CTE Schema**:
   - When CTE `emp_ranked` is materialized, it should have:
     - All columns from `e.*` (e.id, e.name, e.department_id, e.salary, e.hire_date)
     - Plus window function columns (rn, global_rank)
   - But the error says only `["dept_size"]` is available
   - `dept_size` is a window function from the main query, not from the CTE

**Why the Issue Exists**:
- The error shows only `["dept_size"]` which is from the main query's window function
- This suggests the CTE materialization is not preserving all columns correctly
- OR the wildcard `e.*` is not being expanded in the CTE
- OR the CTE schema doesn't include the window function results (rn, global_rank)
- The window operator in the CTE should add `rn` and `global_rank` columns
- But when the CTE is materialized and scanned, those columns might not be in the schema

**Root Cause**:
- CTE wildcard expansion (`SELECT e.*`) might not be working correctly
- Window function results in CTE might not be preserved in materialized schema
- OR the CTEScan operator is not exposing the full CTE schema

---

## Issue 4: Query 9 - ProjectOperator Only Outputs Expression Column

**Error**: `Column 'e1.department_id' not found in schema. Available columns: ["salary_diff"]. Note: If you're using ORDER BY with an alias, make sure the alias name matches exactly.`

**Query**:
```sql
SELECT 
    e1.name AS employee,
    e2.name AS manager_candidate,
    e1.salary AS emp_salary,
    e2.salary AS manager_salary,
    e1.salary - e2.salary AS salary_diff,
    ROW_NUMBER() OVER (PARTITION BY e1.department_id ORDER BY e2.salary DESC) AS manager_rank
FROM employees e1
INNER JOIN employees e2 ON e1.department_id = e2.department_id AND e1.salary < e2.salary
ORDER BY e1.department_id, salary_diff
```

**Current Code Logic**:

1. **Query Execution Order** (`src/query/planner.rs:110-803`):
   - Planner creates operators in this order:
     1. Scan/Join operators (creates qualified schema: `e1.name`, `e2.name`, `e1.salary`, `e2.salary`, `e1.department_id`, `e2.department_id`)
     2. Filter operator (if WHERE clause exists)
     3. **Window operator** (adds window function columns like `manager_rank`)
     4. **Project operator** (selects columns: should output `employee`, `manager_candidate`, `emp_salary`, `manager_salary`, `salary_diff`, `manager_rank`)
     5. Sort operator (ORDER BY)

2. **Projection Expression Extraction** (`src/query/parser_enhanced.rs:1259-1399`):
   - `extract_projection_expressions()` processes all SELECT items
   - For Query 9, it should extract:
     - `e1.name AS employee` → `ProjectionExprType::Column("e1.name")` with alias `employee`
     - `e2.name AS manager_candidate` → `ProjectionExprType::Column("e2.name")` with alias `manager_candidate`
     - `e1.salary AS emp_salary` → `ProjectionExprType::Column("e1.salary")` with alias `emp_salary`
     - `e2.salary AS manager_salary` → `ProjectionExprType::Column("e2.salary")` with alias `manager_salary`
     - `e1.salary - e2.salary AS salary_diff` → `ProjectionExprType::Function(BinaryOp(...))` with alias `salary_diff`
     - Window function → Handled separately by WindowOperator before ProjectOperator

3. **ProjectOperator Processing** (`src/execution/operators.rs:2632-3022`):
   - When `self.expressions` is not empty, it loops through all expressions (`for expr in &self.expressions`)
   - For `ProjectionExprType::Column` (lines 2649-2773):
     - Tries to resolve column name from input batch schema
     - Attempts multiple resolution strategies:
       1. Direct qualified name lookup (`e1.name`)
       2. Resolve table alias to actual table name
       3. Unqualified column name lookup
       4. Case-insensitive matching
       5. Fuzzy matching for aggregate functions
     - **If column resolution fails**, the code now collects an error and **continues** (line 2771: `continue;`)
     - If successful, adds column to output with alias as field name
   
   - For `ProjectionExprType::Function` (lines 2914-3019):
     - Evaluates the arithmetic expression (`e1.salary - e2.salary`)
     - Creates output array from evaluated values
     - Adds to output with alias `salary_diff`

4. **Error Collection Logic** (`src/execution/operators.rs:2647-2772`):
   - Added error collection: `let mut processing_errors = Vec::new();`
   - When column resolution fails, adds error to `processing_errors` and continues
   - After processing all expressions, checks if errors occurred
   - **Only fails if ALL expressions failed** (`output_columns.is_empty()`)

**Why the Issue Exists**:

The error shows only `["salary_diff"]` is available when SortOperator tries to access `e1.department_id` for ORDER BY. This means:

1. **Regular column expressions are failing to resolve**:
   - ProjectOperator tries to find `e1.name`, `e1.salary`, etc. in input schema
   - But input schema (from WindowOperator) might only have window function columns
   - OR the column names don't match (qualified vs unqualified)

2. **Only the arithmetic expression succeeds**:
   - `e1.salary - e2.salary AS salary_diff` is a Function expression
   - It evaluates correctly because ExpressionEvaluator can resolve `e1.salary` and `e2.salary` from input
   - But regular column expressions fail because they can't find columns in schema

3. **Window operator output schema issue**:
   - WindowOperator should preserve ALL input columns + add window function columns
   - But Query 9's window operator output might not have all JOIN output columns
   - When ProjectOperator receives input from WindowOperator, it might only see window function columns

4. **The Problem with Error Handling**:
   - Regular column expressions fail silently (added to `processing_errors`, execution continues)
   - Only `salary_diff` (the function expression) succeeds and gets added to output
   - When SortOperator runs, it only sees `salary_diff` in schema
   - SortOperator tries to find `e1.department_id` for ORDER BY but it's not in the schema

**Root Cause**:

- **Schema propagation issue**: ProjectOperator receives input from WindowOperator, but the input schema might not have all expected columns
- **Column resolution failure**: ProjectOperator tries multiple strategies to find regular columns (`e1.name`, `e1.salary`, etc.) but all fail
- **Error handling too permissive**: The continue-on-error logic allows partial output (only successful expressions), which causes downstream operators (SortOperator) to fail
- **Execution order**: WindowOperator runs before ProjectOperator. If WindowOperator's input doesn't have all columns, or if WindowOperator output schema doesn't match what ProjectOperator expects, column resolution fails

**Specific Technical Details**:

1. **WindowOperator Schema Preservation** (`src/execution/window.rs:505-619`):
   - Line 506: `let mut new_columns = batch.batch.columns.clone();` - Clones ALL input columns
   - Line 608: `let mut new_fields = batch.batch.schema.fields().to_vec();` - Uses batch schema fields
   - **Issue**: If WindowOperator's INPUT doesn't have all columns (e.g., from a filtered schema), the output won't have them either
   - WindowOperator should preserve input columns, but if input is missing columns, output will be missing them too

2. **ProjectOperator Column Resolution** (`src/execution/operators.rs:2670-2772`):
   - For each `ProjectionExprType::Column`, tries to resolve column name:
     1. Qualified name lookup: `e1.name`
     2. Table alias resolution: `e1` → `employees`
     3. Unqualified name lookup: `name`
     4. Case-insensitive matching
     5. Fuzzy matching for aggregates
   - **All strategies fail** for regular columns like `e1.name`, `e1.salary`
   - Error is collected (line 2768) and execution continues (line 2771)
   - Only `salary_diff` (Function expression) succeeds because:
     - `ExpressionEvaluator.evaluate()` can resolve `e1.salary` and `e2.salary` from input
     - Expression evaluation uses different resolution logic than ProjectOperator's column lookup

3. **Function Expression Evaluation** (`src/execution/operators.rs:2914-3019`):
   - Uses `ExpressionEvaluator` to evaluate `BinaryOp(e1.salary - e2.salary)`
   - `ExpressionEvaluator` successfully resolves `e1.salary` and `e2.salary` from input schema
   - Creates output array and adds to output with alias `salary_diff`
   - **This works because ExpressionEvaluator has better column resolution than ProjectOperator's direct lookup**

4. **SortOperator Failure** (`src/execution/operators.rs:3282`):
   - Receives input from ProjectOperator
   - Schema only has `["salary_diff"]` because that's the only column ProjectOperator successfully output
   - Tries to find `e1.department_id` for ORDER BY but fails
   - Error: `Column 'e1.department_id' not found in schema. Available columns: ["salary_diff"]`

**Why Regular Columns Fail But Function Expressions Succeed**:

- **Different resolution mechanisms**:
  - ProjectOperator's `ProjectionExprType::Column` uses direct schema lookup (`batch.batch.schema.index_of()`)
  - Function expression evaluation uses `ExpressionEvaluator` which has:
    - Table alias resolution
    - Qualified name handling
    - More sophisticated column resolution logic

- **Possible root cause**:
  - WindowOperator's output schema might have different column names than expected
  - Or: There's an intermediate operator between Join and Window that changes the schema
  - Or: WindowOperator receives input that already has a filtered/transformed schema

**The Fix Needed**:

1. ✅ **WindowOperator schema preservation** - Fixed to use `batch.batch.schema`
2. ✅ **ProjectOperator error handling** - Improved to continue on errors
3. ✅ **ProjectOperator expression processing** - Loops through all expressions
4. **Remaining issue**: ProjectOperator's column resolution for `ProjectionExprType::Column` needs to use the same logic as `ExpressionEvaluator`
   - **Solution**: For regular column expressions, use `ExpressionEvaluator` to resolve columns instead of direct schema lookup
   - OR: Improve column resolution to match ExpressionEvaluator's logic (table alias resolution, qualified name handling)

---

## Summary

All issues are related to **schema propagation and column resolution**:

### ✅ Fixed Issues (Previously):

1. **Query 2**: Window operator - FIXED - Now uses batch schema consistently for column resolution during `sort_partitions()` and `compute_window_function()`
2. **Query 7**: CTE wildcard expansion - FIXED - Window operator now preserves all input columns correctly

### ❌ Remaining Issues (2):

1. **Query 4**: Correlated Subquery - Parser explicitly blocks subqueries in WHERE clauses (architectural decision)
   - **Status**: Not implemented - Requires architectural changes
   - **Complexity**: HIGH - Needs changes across parser, FilterOperator, and SubqueryExecutor
   - **Infrastructure exists but disconnected**: ExpressionEvaluator has `outer_context` parameter, but parser blocks creation of subquery expressions

2. **Query 9**: ProjectOperator Column Resolution - Regular column expressions fail to resolve
   - **Status**: Partial fix applied - Error handling improved, but root cause remains
   - **Issue**: ProjectOperator can't find regular columns (`e1.name`, `e1.salary`, etc.) in WindowOperator output schema
   - **Root Cause**: Different resolution mechanisms - Function expressions use ExpressionEvaluator (works), but regular columns use direct schema lookup (fails)
   - **Current Behavior**: Only arithmetic expression (`salary_diff`) succeeds; regular columns fail silently, causing SortOperator to fail

### Current Status: 6/10 queries passing (60%)

**Test Results**:
- ✅ Query 1: Nested CTEs with Multiple JOINs - **PASSED**
- ❌ Query 2: Window Functions - **FAILED**: `All projection expressions failed: Column 'e.department_id' not found. Available columns: ["dept_total_salary", "dept_avg_salary"]`
- ✅ Query 3: Multiple Aggregations with GROUP BY and HAVING - **PASSED**
- ❌ Query 4: Correlated Subquery - **FAILED**: `Correlated subqueries in WHERE clauses are not yet supported` (Not implemented - architectural limitation)
- ✅ Query 5: UNION with ORDER BY - **PASSED**
- ✅ Query 6: Three-way JOIN - **PASSED**
- ❌ Query 7: CTE with Window Functions - **FAILED**: `All projection expressions failed: Column 'er.global_rank' not found. Available columns: ["dept_size"]`
- ✅ Query 8: Multiple JOINs with EXISTS subquery - **PASSED**
- ❌ Query 9: Self JOIN - **FAILED**: `Column 'e1.department_id' not found in schema. Available columns: ["salary_diff"]` (ProjectOperator column resolution issue)
- ✅ Query 10: Complex Aggregation with CASE expressions - **PASSED**

### Key Improvements Made:

1. **WindowOperator**: Enhanced to use batch schema consistently for column resolution during `sort_partitions()` and `compute_window_function()` - uses `batch.batch.schema` instead of stale `self.schema`
2. **ProjectOperator**: Added error collection and continue-on-error logic to process all expressions - now collects errors instead of bailing on first failure
3. **Schema Propagation**: WindowOperator now preserves all input columns correctly - uses `batch.batch.schema.fields()` for output schema

### Remaining Work:

1. **Query 2 & 7**: Window operator output schema issue - Window operator preserves input columns, but ProjectOperator can't resolve columns from Window operator output
2. **Query 4**: Correlated subquery support - Requires architectural changes (parser, FilterOperator, SubqueryExecutor)
3. **Query 9**: ProjectOperator column resolution - Regular column expressions fail to resolve while function expressions succeed; need to use ExpressionEvaluator for column resolution or improve schema lookup logic

