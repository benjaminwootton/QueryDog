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

## Overview

QueryDog CLI (`querydog`) is a command-line tool for interacting with ClickHouse databases. It provides commands for viewing tables, queries, partitions, cluster info, and more.

## Environment Selection

All commands require an environment to be specified using `--env`:

```bash
# List available environments
querydog envs

# Use a specific environment
querydog --env "Playground" tables
querydog --env Marlink queries
```

If `--env` is omitted, the CLI will display available environments from `querydog.yaml`.

## Output Formats

The CLI supports three output formats:

```bash
# Default: Pretty-printed table
querydog --env Playground tables

# JSON format
querydog --env Playground tables --format json

# CSV format
querydog --env Playground tables --format csv
```

## Schema Commands

```bash
# List tables
querydog --env <env> tables
querydog --env <env> tables -d mydb  # Filter by database

# List views
querydog --env <env> views

# List materialized views
querydog --env <env> materialized-views
querydog --env <env> mv  # Alias

# List databases with statistics
querydog --env <env> databases
querydog --env <env> dbs  # Alias

# List indexes
querydog --env <env> indexes

# List projections
querydog --env <env> projections

# List dictionaries
querydog -e <env> dictionaries
querydog -e <env> dicts  # Alias
```

## Schema Analysis

```bash
# Find Nullable columns (optimization candidates)
querydog -e <env> schema-nullables
querydog -e <env> schema-nullables -d mydb

# Find oversized integer columns (Int64 where Int32 may work)
querydog -e <env> schema-oversized
querydog -e <env> schema-oversized -d mydb

# Column statistics for a specific table
querydog -e <env> column-stats -d mydb -t mytable
```

## Storage Commands

```bash
# List partitions
querydog --env <env> partitions
querydog --env <env> partitions -d mydb -t mytable  # Filter

# List mutations
querydog --env <env> mutations
```

## Activity Commands

```bash
# Show running processes
querydog --env <env> processes
querydog --env <env> ps  # Alias

# Show active merges
querydog --env <env> merges
```

## Query Log Analysis

The query log commands support time filtering with `--hours` and result limiting with `--limit`:

```bash
# Recent queries (last 24h by default)
querydog --env <env> queries
querydog --env <env> queries --hours 1 --limit 100

# Available query modes
querydog --env <env> queries --mode all            # Recent queries (default)
querydog --env <env> queries --mode slowest        # Slowest queries
querydog --env <env> queries --mode highestmemory  # Highest memory usage
querydog --env <env> queries --mode frequent       # Most frequent (grouped)
querydog --env <env> queries --mode bytable        # Grouped by table
querydog --env <env> queries --mode errors         # Errors only
```

## Cluster Commands

```bash
# List clusters
querydog --env <env> clusters

# List replicas
querydog --env <env> replicas

# Show replication queue
querydog --env <env> replication-queue
querydog --env <env> repq  # Alias

# Browse ZooKeeper
querydog --env <env> zookeeper
querydog --env <env> zk --path /clickhouse  # Specific path
```

## Storage & Metrics

```bash
# Disks and storage
querydog --env <env> disks
querydog --env <env> storage-policies
querydog --env <env> policies  # Alias

# System metrics
querydog --env <env> metrics
querydog --env <env> async-metrics
querydog --env <env> events
querydog --env <env> system-errors
querydog --env <env> warnings
```

## Users & Security

```bash
querydog --env <env> users
querydog --env <env> roles
querydog --env <env> grants
querydog --env <env> quotas
```

## Configuration

```bash
# List settings (searchable)
querydog --env <env> settings
querydog --env <env> settings --search max_memory
```

## Logs

```bash
# Show text log (last 1 hour by default)
querydog --env <env> text-log
querydog --env <env> logs  # Alias
querydog --env <env> logs --hours 2 --level Error
```

## Background Operations

```bash
querydog --env <env> async-inserts
querydog --env <env> query-cache
querydog --env <env> view-refreshes
querydog --env <env> background-jobs
querydog --env <env> jobs  # Alias
```

## Interactive TUI Mode

Launch the interactive terminal UI:

```bash
querydog tui
querydog --env Playground tui  # Pre-select environment
```

### TUI Keyboard Shortcuts

- **Navigation**: Arrow keys or j/k to navigate menu
- **Selection**: Enter to select
- **Environment**: `e` to select environment
- **Refresh**: `r` to refresh current view
- **Quick Views**:
  - `t` - Tables
  - `v` - Views
  - `p` - Partitions
  - `m` - Mutations
  - `P` - Processes
  - `M` - Merges
- **Help**: `?` to show help
- **Quit**: `q` or Ctrl+C

## Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--env <name>` | Environment name | (required) |
| `--format <fmt>` | Output format: table, json, csv | table |
| `--hours <n>` | Time range for query log commands | 24 |
| `--limit <n>` | Maximum results | 50 |
| `-d, --database <db>` | Filter by database | all |
| `-t, --table <tbl>` | Filter by table | all |

## Examples

```bash
# Check what's happening on a server
querydog --env Prod processes
querydog --env Prod merges
querydog --env Prod mutations

# Analyze query performance
querydog --env Prod queries --mode slowest --hours 4 --limit 20
querydog --env Prod queries --mode frequent --hours 24

# Check storage
querydog --env Prod disks
querydog --env Prod partitions -d events

# Export data as JSON
querydog --env Prod tables --format json > tables.json

# Check cluster health
querydog --env Prod replicas
querydog --env Prod replication-queue
```

## Configuration File

The CLI reads environment configurations from `.querydog.yaml`:

```yaml
environments:
  - name: "Production"
    host: "clickhouse.example.com"
    port: 8443
    user: "admin"
    password: "secret"
    database: "default"
    secure: true
```

Column display settings are in `config/querydog-cli.yaml`.
