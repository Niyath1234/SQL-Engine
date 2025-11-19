# Missing Features in Hypergraph SQL Engine

## 🚨 Critical Missing Features (High Priority)

### 1. **DML Operations (Data Manipulation)**
- ❌ **INSERT INTO ... VALUES** - Cannot insert new data
- ❌ **INSERT INTO ... SELECT** - Cannot insert from queries
- ❌ **UPDATE ... SET ... WHERE** - Cannot update existing data
- ❌ **DELETE FROM ... WHERE** - Cannot delete data
- ❌ **TRUNCATE TABLE** - Cannot quickly clear tables

**Impact**: Engine is read-only. Cannot modify data after initial load.

### 2. **DDL Operations (Data Definition)**
- ❌ **CREATE TABLE** - Cannot create tables via SQL
- ❌ **ALTER TABLE** - Cannot modify table structure
- ❌ **DROP TABLE** - Cannot delete tables
- ❌ **CREATE INDEX** - Cannot create indexes via SQL
- ❌ **DROP INDEX** - Cannot drop indexes

**Impact**: All schema operations must be done programmatically.

### 3. **Set Operations**
- ❌ **UNION** - Cannot combine result sets (deduplicated)
- ❌ **UNION ALL** - Cannot combine result sets (all rows)
- ❌ **INTERSECT** - Cannot find common rows
- ❌ **EXCEPT/MINUS** - Cannot find difference between sets

**Status**: Code exists in `src/query/union.rs` but not integrated into parser/execution.

### 4. **HAVING Clause**
- ❌ **HAVING** - Cannot filter groups after aggregation

**Status**: Operator exists in `src/execution/having.rs` but not wired into planner.

### 5. **Subqueries**
- ❌ **Scalar subqueries** - Cannot use subquery in SELECT, WHERE
- ❌ **Correlated subqueries** - Cannot use correlated subqueries
- ❌ **Derived tables** - Cannot use subquery in FROM (partially supported in parser)
- ❌ **EXISTS / NOT EXISTS** - Cannot check existence

**Impact**: Many complex queries cannot be expressed.

### 6. **Common Table Expressions (CTEs)**
- ❌ **WITH ... AS** - Cannot use CTEs
- ❌ **Recursive CTEs** - Cannot use recursive CTEs

**Status**: Infrastructure exists in `src/query/cte.rs` but not fully integrated.

---

## ⚠️ Important Missing Features (Medium Priority)

### 7. **Advanced WHERE Predicates**
- ❌ **LIKE / NOT LIKE** - Cannot do pattern matching
- ❌ **IN / NOT IN** - Cannot check membership
- ❌ **BETWEEN / NOT BETWEEN** - Cannot check ranges
- ❌ **IS NULL / IS NOT NULL** - Cannot check NULL (partially supported)
- ❌ **ANY / SOME / ALL** - Cannot use quantifiers

**Status**: LIKE/IN parsing exists but not fully executed.

### 8. **CASE Expressions**
- ❌ **CASE ... WHEN ... THEN ... ELSE ... END** - Cannot use conditional logic

**Status**: Expression evaluator has CASE support but not integrated.

### 9. **Window Functions**
- ❌ **ROW_NUMBER()** - Cannot number rows
- ❌ **RANK() / DENSE_RANK()** - Cannot rank rows
- ❌ **LAG() / LEAD()** - Cannot access previous/next rows
- ❌ **Window aggregates** - Cannot use SUM/AVG/etc. as window functions
- ❌ **PARTITION BY** - Cannot partition window frames
- ❌ **Window frames** - Cannot specify ROWS/RANGE frames

**Impact**: Cannot do advanced analytical queries.

### 10. **Scalar Functions**
- ❌ **String functions**: CONCAT, SUBSTRING, UPPER, LOWER, TRIM, REPLACE, etc.
- ❌ **Numeric functions**: ABS, ROUND, FLOOR, CEIL, MOD, POWER, SQRT, etc.
- ❌ **Date/Time functions**: CURRENT_DATE, YEAR, MONTH, DAY, DATE_ADD, DATEDIFF, etc.
- ❌ **Type conversion**: CAST, CONVERT, TO_INTEGER, TO_STRING, etc.

**Status**: Some functions exist in `src/query/scalar_functions.rs` but not integrated.

### 11. **Advanced Aggregates**
- ❌ **COUNT(DISTINCT ...)** - Cannot count distinct values
- ❌ **SUM(DISTINCT ...)** - Cannot sum distinct values
- ❌ **Statistical functions**: STDDEV, VARIANCE, CORR, etc.
- ❌ **Array aggregates**: ARRAY_AGG, JSON_AGG, etc.

---

## 📋 Nice-to-Have Features (Lower Priority)

### 12. **Advanced JOIN Types**
- ⚠️ **LEFT JOIN** - Code exists but needs testing
- ⚠️ **RIGHT JOIN** - Code exists but needs testing
- ⚠️ **FULL OUTER JOIN** - Placeholder exists, not implemented
- ❌ **CROSS JOIN** - Cannot do cartesian products
- ❌ **NATURAL JOIN** - Cannot do natural joins

### 13. **Advanced GROUP BY**
- ❌ **GROUP BY with expressions** - Only columns supported
- ❌ **GROUP BY ROLLUP** - Cannot do hierarchical grouping
- ❌ **GROUP BY CUBE** - Cannot do multi-dimensional grouping
- ❌ **GROUP BY GROUPING SETS** - Cannot do multiple grouping sets

### 14. **Advanced ORDER BY**
- ❌ **ORDER BY with expressions** - Only columns supported
- ❌ **NULLS FIRST / NULLS LAST** - Cannot control NULL ordering
- ❌ **ORDER BY with positional references** - Cannot use column numbers

### 15. **Data Types**
- ❌ **DECIMAL/NUMERIC** - Only INT64/FLOAT64 supported
- ❌ **DATE/TIME/TIMESTAMP** - No date/time support
- ❌ **BOOLEAN** - No boolean type
- ❌ **JSON** - No JSON support
- ❌ **ARRAY** - No array support
- ❌ **UUID** - No UUID support

### 16. **Transactions**
- ❌ **BEGIN / COMMIT / ROLLBACK** - No transaction support
- ❌ **SAVEPOINT** - No savepoint support
- ❌ **Isolation levels** - No isolation level control

### 17. **Stored Procedures & Functions**
- ❌ **CREATE PROCEDURE** - No stored procedures
- ❌ **CREATE FUNCTION** - No user-defined functions
- ❌ **Variables** - No variable support
- ❌ **Control flow** - No IF/WHILE/LOOP

### 18. **Security & Permissions**
- ❌ **CREATE USER** - No user management
- ❌ **GRANT / REVOKE** - No permission system
- ❌ **Roles** - No role-based access control

### 19. **System Features**
- ❌ **EXPLAIN / EXPLAIN ANALYZE** - No query plan visualization
- ❌ **SHOW TABLES / SHOW COLUMNS** - No metadata queries
- ❌ **INFORMATION_SCHEMA** - No standard metadata access

### 20. **Advanced Features**
- ❌ **Materialized Views** - No materialized views
- ❌ **Partitioning** - No table partitioning
- ❌ **Full-Text Search** - No text search
- ❌ **Spatial Data** - No geographic data support

---

## ✅ What IS Implemented

### Core Query Features
- ✅ Basic SELECT (SELECT * FROM table)
- ✅ WHERE clause (basic comparisons: =, !=, <, >, <=, >=)
- ✅ LIMIT/OFFSET
- ✅ ORDER BY (basic sorting)
- ✅ GROUP BY
- ✅ Aggregate functions: COUNT, SUM, AVG, MIN, MAX
- ✅ INNER JOIN (hash join implementation)
- ✅ Column selection
- ✅ Basic data types: INT64, FLOAT64, STRING
- ✅ NULL handling

### Hypergraph-Specific Features
- ✅ Hypergraph-based join optimization
- ✅ Path caching
- ✅ Fragment-based storage
- ✅ Query plan reuse
- ✅ Hot-fragment tracking
- ✅ Multi-tier memory management
- ✅ Adaptive fragmenting
- ✅ Learned indexes
- ✅ Tiered indexes
- ✅ Cache-optimized layout

### Performance Features
- ✅ Query plan caching
- ✅ Result caching
- ✅ SIMD-optimized operations
- ✅ Columnar storage
- ✅ Vectorized execution

---

## 📊 Implementation Status Summary

| Category | Implemented | Missing | Total | % Complete |
|----------|-------------|---------|-------|------------|
| **Core DQL** | 10 | 15 | 25 | 40% |
| **DML** | 0 | 5 | 5 | 0% |
| **DDL** | 0 | 10 | 10 | 0% |
| **Functions** | 5 | 50+ | 55+ | ~9% |
| **Operators** | 6 | 20+ | 26+ | ~23% |
| **Advanced Features** | 0 | 15+ | 15+ | 0% |
| **Hypergraph Features** | 10 | 0 | 10 | 100% |

**Overall SQL Completeness: ~25-30%**

---

## 🎯 Recommended Implementation Order

### Phase 1: Make Engine Functional (Critical)
1. **INSERT INTO ... VALUES** - Enable data insertion
2. **UPDATE ... SET ... WHERE** - Enable data updates
3. **DELETE FROM ... WHERE** - Enable data deletion
4. **CREATE TABLE** - Enable table creation
5. **UNION / UNION ALL** - Wire up existing code
6. **HAVING clause** - Wire up existing operator

### Phase 2: Query Completeness (High Priority)
1. **LIKE / NOT LIKE** - Pattern matching
2. **IN / NOT IN** - Membership checks
3. **CASE expressions** - Conditional logic
4. **Scalar subqueries** - Basic subquery support
5. **CTEs (WITH clauses)** - Wire up existing infrastructure

### Phase 3: Advanced Queries (Medium Priority)
1. **Window functions** - Analytical queries
2. **Scalar functions** - String, numeric, date functions
3. **Advanced aggregates** - COUNT(DISTINCT), etc.
4. **Correlated subqueries** - Complex subqueries

### Phase 4: DDL & Schema (Medium Priority)
1. **ALTER TABLE** - Schema modifications
2. **DROP TABLE** - Table deletion
3. **CREATE INDEX** - Index management
4. **Data types** - DECIMAL, DATE, BOOLEAN, etc.

### Phase 5: Advanced Features (Lower Priority)
1. **Transactions** - ACID guarantees
2. **Stored procedures** - Programmable logic
3. **Security** - Users, permissions, roles
4. **System features** - EXPLAIN, SHOW, etc.

---

## 🔍 Quick Reference: What Works vs What Doesn't

### ✅ Works
```sql
SELECT * FROM table LIMIT 10;
SELECT col1, col2 FROM table WHERE col1 > 100;
SELECT COUNT(*), SUM(col1) FROM table GROUP BY col2;
SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
SELECT * FROM table ORDER BY col1 DESC LIMIT 100;
```

### ❌ Doesn't Work
```sql
-- DML
INSERT INTO table VALUES (1, 'test');
UPDATE table SET col1 = 100 WHERE col2 = 'x';
DELETE FROM table WHERE col1 < 0;

-- DDL
CREATE TABLE new_table (id INT, name VARCHAR);
ALTER TABLE table ADD COLUMN new_col INT;
DROP TABLE old_table;

-- Set operations
SELECT * FROM t1 UNION SELECT * FROM t2;
SELECT * FROM t1 INTERSECT SELECT * FROM t2;

-- Advanced WHERE
SELECT * FROM table WHERE name LIKE '%test%';
SELECT * FROM table WHERE id IN (1, 2, 3);
SELECT * FROM table WHERE col1 BETWEEN 10 AND 20;

-- Subqueries
SELECT * FROM t1 WHERE id IN (SELECT id FROM t2);
SELECT (SELECT COUNT(*) FROM t2) as count FROM t1;

-- CTEs
WITH cte AS (SELECT * FROM t1) SELECT * FROM cte;

-- Window functions
SELECT *, ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2) FROM table;

-- Scalar functions
SELECT UPPER(name), ROUND(price, 2) FROM table;
SELECT CONCAT(first, ' ', last) as full_name FROM table;

-- CASE
SELECT CASE WHEN col1 > 100 THEN 'high' ELSE 'low' END FROM table;
```

---

## 💡 Notes

- **Hypergraph features are 100% complete** - The unique value proposition is fully implemented
- **Core query features are ~40% complete** - Basic SELECT works well
- **DML/DDL are 0% complete** - Engine is currently read-only
- **Many features have infrastructure but need wiring** - UNION, HAVING, CTEs have code but aren't integrated
- **Focus on analytical workloads** - OLAP features are more important than OLTP features

