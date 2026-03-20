# ClickHouse Performance Analysis Report

Use this guide to analyze a ClickHouse database and produce a structured performance report.

## Analysis Workflow

Run these commands in order, collecting outputs for each section of the report.

---

## Section 1: Query Performance Analysis

### 1.1 Slowest Queries

```bash
querydog -e <env> queries-slow --hours 24 --limit 20
```

**Document:**
- Top 5 slowest queries with duration and memory usage
- Patterns in slow queries (similar tables, similar operations)
- Any queries over 10 seconds

### 1.2 Memory-Intensive Queries

```bash
querydog -e <env> queries-memory --hours 24 --limit 20
```

**Document:**
- Queries using > 100MB memory
- Correlation between memory usage and read_rows
- Users running expensive queries

### 1.3 Query Patterns by Frequency

```bash
querydog -e <env> queries-frequent --hours 24 --limit 30
```

**Document:**
- Most frequent query patterns (top 10)
- Average vs max duration (consistency issues)
- High-frequency queries that are also slow (optimization priorities)

### 1.4 Query Load by Table

```bash
querydog -e <env> queries --mode bytable --hours 24
```

**Document:**
- Tables with highest query count
- Tables with highest total duration
- Tables with highest total memory consumption

---

## Section 2: Active Operations

### 2.1 Current Activity

```bash
querydog -e <env> processes
querydog -e <env> merges
querydog -e <env> mutations
```

**Document:**
- Long-running queries (> 60 seconds)
- Active merges and their progress
- Pending mutations

---

## Section 3: Error Analysis

### 3.1 Query Errors

```bash
querydog -e <env> queries-errors --hours 24 --limit 20
```

**Document:**
- Error patterns (memory limits, timeouts, syntax)
- Frequency of each error type
- Users experiencing errors

### 3.2 System Errors

```bash
querydog -e <env> system-errors
querydog -e <env> warnings
```

**Document:**
- Critical system errors
- Configuration warnings

---

## Section 4: Storage Analysis

### 4.1 Table Sizes

```bash
querydog -e <env> tables -w
```

**Document:**
- Largest tables by size
- Tables with high row counts
- Engine types in use

### 4.2 Partition Health

```bash
querydog -e <env> partitions --limit 100
```

**Document:**
- Tables with many small parts (part explosion risk)
- Unbalanced partition sizes
- Old partitions that could be dropped

### 4.3 Disk Usage

```bash
querydog -e <env> disks
```

**Document:**
- Available space per disk
- Usage percentage alerts (> 80%)

---

## Section 5: Schema Optimization

### 5.1 Nullable Column Analysis

```bash
querydog -e <env> schema-nullables
```

**Document:**
- Count of Nullable columns by table
- Candidates for removing Nullable (requires data check)

### 5.2 Oversized Types

```bash
querydog -e <env> schema-oversized
```

**Document:**
- Int64/UInt64 columns that may use smaller types
- Estimated storage savings

### 5.3 Column Statistics (for large tables)

```bash
querydog -e <env> column-stats -d <database> -t <large_table>
```

**Document:**
- Columns with poor compression ratio
- Largest columns by compressed size

---

## Section 6: Cluster Health (if applicable)

```bash
querydog -e <env> replicas
querydog -e <env> replication-queue
querydog -e <env> clusters
```

**Document:**
- Replica lag issues
- Queue backlogs
- Readonly replicas

---

## Report Template

```markdown
# ClickHouse Performance Report
**Environment:** [name]
**Date:** [date]
**Analysis Period:** Last 24 hours

## Executive Summary
- [1-2 sentence overall health assessment]
- [Top 3 issues requiring attention]

## Query Performance
### Slow Queries
[Top 5 slowest queries with recommendations]

### Memory Usage
[High memory queries and optimization suggestions]

### Hot Tables
[Tables with most query activity]

## Errors & Warnings
[Summary of errors found]

## Storage
[Disk usage, large tables, partition issues]

## Schema Optimization Opportunities
[Nullable columns, oversized types]

## Recommendations
1. [Priority 1 recommendation]
2. [Priority 2 recommendation]
3. [Priority 3 recommendation]

## Action Items
- [ ] [Specific action 1]
- [ ] [Specific action 2]
- [ ] [Specific action 3]
```

---

## Quick One-Liner Analysis

For a rapid assessment, run all key commands:

```bash
ENV=3  # Set your environment number

echo "=== SLOW QUERIES ===" && querydog -e $ENV queries-slow --limit 10
echo "=== MEMORY QUERIES ===" && querydog -e $ENV queries-memory --limit 10
echo "=== QUERY PATTERNS ===" && querydog -e $ENV queries-frequent --limit 10
echo "=== BY TABLE ===" && querydog -e $ENV queries --mode bytable
echo "=== ERRORS ===" && querydog -e $ENV queries-errors --limit 10
echo "=== CURRENT PROCESSES ===" && querydog -e $ENV processes
echo "=== TABLES ===" && querydog -e $ENV tables
echo "=== DISKS ===" && querydog -e $ENV disks
```

## Export for Further Analysis

```bash
# Export all data to JSON for processing
querydog -e $ENV queries-slow --hours 168 --limit 1000 -f json > slow_queries.json
querydog -e $ENV queries-frequent --hours 168 -f json > query_patterns.json
querydog -e $ENV queries --mode bytable -f json > table_stats.json
querydog -e $ENV tables -f json > tables.json
```
