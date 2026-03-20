---
name: querydog-general-usage
description: Use this when the user wants to interact with their ClickHouse database via QueryDog
version: 1.0.0
invocations:
  - querydog
  - querydog help
  - clickhouse cli
  - querydog commands
  - how to use querydog
  - querydog usage
tags:
  - clickhouse
  - cli
  - querydog
  - database
  - reference
---

# QueryDog CLI - Claude AI Skill Guide

This document describes how to use the QueryDog CLI tool for querying and managing ClickHouse databases.

## Important: Option Placement

**Global options MUST come BEFORE the command:**

```bash
# CORRECT
querydog --env 1 --limit 50 queries

# INCORRECT - options after command may not be parsed
querydog --env 1 queries --limit 50
```

## Environment Selection

All commands require an environment to be specified using `--env`:

```bash
# List available environments
querydog envs

# Use environment by number
querydog --env 1 tables

# Use environment by name
querydog --env "Production" tables
```

## Output Formats

```bash
# Default: Pretty-printed table
querydog --env 1 tables

# JSON format
querydog --env 1 --format json tables

# CSV format
querydog --env 1 --format csv tables
```

## Query Log Analysis

The main command for query analysis is `queries` with a `--mode` option:

```bash
# Recent queries (default mode: all)
querydog --env 1 queries

# With time/limit options (BEFORE the command)
querydog --env 1 --hours 4 --limit 20 queries

# Slowest queries
querydog --env 1 --hours 4 --limit 20 queries --mode slowest

# Highest memory usage
querydog --env 1 queries --mode highestmemory

# Most frequent queries (grouped)
querydog --env 1 queries --mode frequent

# Queries grouped by table
querydog --env 1 queries --mode bytable

# Query errors only
querydog --env 1 queries --mode errors
```

## Schema Commands

```bash
# List tables
querydog --env 1 tables
querydog --env 1 -d mydb tables

# List views
querydog --env 1 views

# List materialized views
querydog --env 1 materialized-views

# List databases with statistics
querydog --env 1 databases

# List indexes
querydog --env 1 indexes

# List projections
querydog --env 1 projections

# List dictionaries
querydog --env 1 dictionaries
```

## Schema Analysis

```bash
# Find Nullable columns (optimization candidates)
querydog --env 1 schema-nullables
querydog --env 1 -d mydb schema-nullables

# Find oversized integer columns
querydog --env 1 schema-oversized

# Column statistics for a specific table
querydog --env 1 -d mydb -t mytable column-stats
```

## Storage Commands

```bash
# List partitions
querydog --env 1 partitions
querydog --env 1 -d mydb -t mytable partitions

# List mutations
querydog --env 1 mutations
```

## Activity Commands

```bash
# Show running processes
querydog --env 1 processes

# Show active merges
querydog --env 1 merges
```

## Cluster Commands

```bash
# List clusters
querydog --env 1 clusters

# List replicas
querydog --env 1 replicas

# Show replication queue
querydog --env 1 replication-queue

# Browse ZooKeeper
querydog --env 1 zookeeper
querydog --env 1 zookeeper --path /clickhouse
```

## Storage & Metrics

```bash
# Disks and storage
querydog --env 1 disks
querydog --env 1 storage-policies

# System metrics
querydog --env 1 metrics
querydog --env 1 async-metrics
querydog --env 1 events
querydog --env 1 system-errors
querydog --env 1 warnings
```

## Users & Security

```bash
querydog --env 1 users
querydog --env 1 roles
querydog --env 1 grants
querydog --env 1 quotas
```

## Configuration

```bash
# List settings (searchable)
querydog --env 1 settings
querydog --env 1 settings --search max_memory
```

## Logs

```bash
# Show text log (last 1 hour by default)
querydog --env 1 text-log
querydog --env 1 --hours 2 text-log --level Error
```

## Background Operations

```bash
querydog --env 1 async-inserts
querydog --env 1 query-cache
querydog --env 1 view-refreshes
querydog --env 1 background-jobs
```

## Interactive TUI Mode

```bash
querydog tui
querydog --env 1 tui
```

## Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--env <name\|number>` | Environment name or number | (required) |
| `--format <fmt>` | Output format: table, json, csv | table |
| `--hours <n>` | Time range in hours | 24 |
| `--limit <n>` | Maximum results | 50 |
| `-d, --database <db>` | Filter by database | all |
| `-t, --table <tbl>` | Filter by table | all |
| `-w, --wide` | Wide output - don't truncate columns | false |

## Examples

```bash
# Check what's happening on a server
querydog --env 1 processes
querydog --env 1 merges
querydog --env 1 mutations

# Analyze query performance
querydog --env 1 --hours 4 --limit 20 queries --mode slowest
querydog --env 1 --hours 24 queries --mode frequent

# Check storage
querydog --env 1 disks
querydog --env 1 -d events partitions

# Export data as JSON
querydog --env 1 --format json tables > tables.json

# Check cluster health
querydog --env 1 replicas
querydog --env 1 replication-queue
```
