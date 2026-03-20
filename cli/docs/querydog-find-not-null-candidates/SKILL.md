---
name: querydog-find-not-null-candidates
description: Use this when the user wants to find candidates to remove nullable modifiers
version: 1.0.0
invocations:
  - find not null candidates
  - nullable columns
  - remove nullable
  - schema optimization nullable
  - clickhouse nullable analysis
  - optimize nullable columns
tags:
  - clickhouse
  - schema
  - optimization
  - nullable
  - querydog
---

# Nullable Column Analysis Skill

This skill guides you through identifying nullable columns in ClickHouse that contain no NULL values - candidates for schema optimization to `NOT NULL`.

## Why This Matters

Nullable columns in ClickHouse:
- Require additional storage for the null bitmap
- Have slightly slower query performance
- Prevent certain optimizations

If a nullable column never actually contains NULLs, converting it to `NOT NULL` improves storage efficiency and query performance.

## Step-by-Step Process

### Step 1: List All Nullable Columns

First, identify all nullable columns in your database using the system tables:

```sql
SELECT
  database,
  table,
  name AS column_name,
  type
FROM system.columns
WHERE type LIKE 'Nullable(%)'
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
ORDER BY database, table, name
```

**Filter by specific database:**
```sql
SELECT
  database,
  table,
  name AS column_name,
  type
FROM system.columns
WHERE type LIKE 'Nullable(%)'
  AND database = 'your_database_name'
ORDER BY table, name
```

### Step 2: Check Each Column for NULL Values

For each nullable column found, check if it actually contains any NULLs:

```sql
SELECT
  countIf(column_name IS NULL) AS null_count,
  count() AS total_rows
FROM database_name.table_name
```

**Example checking multiple columns at once:**
```sql
SELECT
  countIf(col1 IS NULL) AS col1_nulls,
  countIf(col2 IS NULL) AS col2_nulls,
  countIf(col3 IS NULL) AS col3_nulls,
  count() AS total_rows
FROM mydb.mytable
```

### Step 3: Identify Optimization Candidates

Columns with `null_count = 0` are candidates for conversion to `NOT NULL`.

**Combined query to find candidates in a specific table:**
```sql
SELECT
  name AS column_name,
  type
FROM system.columns
WHERE database = 'your_database'
  AND table = 'your_table'
  AND type LIKE 'Nullable(%)'
```

Then for each column, verify with:
```sql
SELECT count() FROM your_database.your_table WHERE column_name IS NULL
```

### Step 4: Generate ALTER Statements

For columns confirmed to have no NULLs, generate the ALTER statement:

```sql
ALTER TABLE database_name.table_name
  MODIFY COLUMN column_name Type_Without_Nullable
```

**Example:**
```sql
-- If column was Nullable(String), change to String
ALTER TABLE mydb.users
  MODIFY COLUMN middle_name String

-- If column was Nullable(Int32), change to Int32
ALTER TABLE mydb.orders
  MODIFY COLUMN discount_code Int32
```

## Automated Analysis Script

For comprehensive analysis across all tables, run this query to get a list of all nullable columns with row counts:

```sql
SELECT
  c.database,
  c.table,
  c.name AS column_name,
  c.type,
  t.total_rows
FROM system.columns c
JOIN system.tables t ON c.database = t.database AND c.table = t.name
WHERE c.type LIKE 'Nullable(%)'
  AND t.engine NOT IN ('View', 'MaterializedView', 'Merge', 'Distributed')
  AND t.total_rows > 0
  AND c.database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema')
ORDER BY t.total_rows DESC, c.database, c.table, c.name
```

Then iterate through results and check each column.

## Using QueryDog CLI

### List nullable columns:
```bash
querydog -e <env> schema-nullables
querydog -e <env> schema-nullables -d mydb
```

### Check a specific table's column statistics:
```bash
querydog -e <env> column-stats -d mydb -t mytable
```

## Sample Workflow

1. **Get nullable columns for a database:**
   ```bash
   querydog -e Production schema-nullables -d events
   ```

2. **For each table with nullable columns, run null check:**
   ```sql
   SELECT
     countIf(user_agent IS NULL) AS user_agent_nulls,
     countIf(referrer IS NULL) AS referrer_nulls,
     count() AS total
   FROM events.page_views
   ```

3. **For columns with 0 nulls, apply the fix:**
   ```sql
   ALTER TABLE events.page_views
     MODIFY COLUMN user_agent String
   ```

## Performance Considerations

- **Large Tables**: Use `SAMPLE` for initial checks on very large tables:
  ```sql
  SELECT countIf(column_name IS NULL) FROM db.table SAMPLE 0.1
  ```

- **Batch Processing**: Check multiple columns in a single query to reduce round trips

- **Off-Peak Hours**: Run comprehensive scans during low-traffic periods

## Caveats

1. **Future Data**: Just because current data has no NULLs doesn't mean future data won't. Consider the data source and business logic.

2. **Application Changes**: Ensure application code handles the schema change (won't try to insert NULLs).

3. **Default Values**: When removing Nullable, you may need to specify a default:
   ```sql
   ALTER TABLE db.table
     MODIFY COLUMN col String DEFAULT ''
   ```

4. **Materialized Views**: Check if any materialized views depend on the nullable column.
