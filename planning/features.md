# QueryDog Feature Ideas (ClickHouse System Tables Focus)

This document consolidates ClickHouse‑specific feature ideas grounded in system tables and internals. The goal is to prioritize actionable dashboards, investigations, and operational workflows that are powered by real system tables rather than generic features.

## Currently Used System Tables
- `system.query_log`, `system.query_views_log`
- `system.parts`, `system.partitions`, `system.part_log`
- `system.processes`, `system.merges`, `system.mutations`
- `system.metrics`, `system.asynchronous_metrics`, `system.events`
- `system.tables`, `system.columns`, `system.databases`
- `system.users`, `system.settings`
- `system.view_refreshes`, `system.query_cache`
- `system.projections`, `system.data_skipping_indices`

## Query Analysis & Deep Dives
- **Query fingerprints with ClickHouse specifics** using `normalized_query_hash`, `query_kind`, `is_initial_query`, `read_rows`, `read_bytes`, `written_rows`, `written_bytes`, `memory_usage` from `system.query_log`.
- **Query plan diffs** by capturing `query_plan` (when available) and comparing across time or settings profiles.
- **Thread-level breakdowns** via `system.query_thread_log`: per‑thread CPU, memory, and I/O bottlenecks; parallel efficiency by `thread_id`.
- **Pipeline profiling** with `system.processors_profile_log`: slow processors, skewed stages, and input/output rows per processor.
- **Per‑second execution metrics** using `system.query_metric_log`: memory growth and read/write ramps during long queries.
- **Trace‑based investigation** with `system.trace_log`: flame graphs, hot functions, lock contention hints.
- **View usage analysis** via `system.query_views_log`: materialized view hit rate, view refresh impact.

## Replication & ZooKeeper/Keeper
- **Replica health and lag** from `system.replicas`: `is_readonly`, `is_session_expired`, `absolute_delay`, `relative_delay`.
- **Replication queue monitor** from `system.replication_queue`: queue size, operation types, failure reasons.
- **Active fetches** from `system.replicated_fetches`: progress, throughput, and source node hot spots.
- **Keeper/ZooKeeper browser** using `system.zookeeper` + `system.zookeeper_connection(_log)` for connection health and latency.

## Storage, Parts, and Disk Management
- **Disk usage dashboard** via `system.disks`: free vs total vs reserved, disk types.
- **Storage policy views** using `system.storage_policies`: volume layout and table mappings.
- **Active part moves** from `system.moves`: reasons and throughput.
- **Detached parts manager** with `system.detached_parts`: age, reason, disk usage.
- **Dropped table recovery window** using `system.dropped_tables` + `system.dropped_tables_parts`.
- **Column‑level part analysis** from `system.parts_columns`: compression ratios and growth over time.

## Errors, Crashes, and Warnings
- **Error dashboard** via `system.error_log`: error codes, peaks, affected users.
- **Crash analysis** with `system.crash_log`: signatures, frequency, related query IDs.
- **System warnings** via `system.warnings`: severity and remediation hints.
- **Server text logs** (if enabled) using `system.text_log`: correlate warnings/errors with query spikes.

## Security, Access Control, and Sessions
- **Grants and privileges** with `system.grants`: wildcard or overly broad grants.
- **Role hierarchy** via `system.roles` + `system.role_grants`.
- **Active role usage** from `system.current_roles` + `system.enabled_roles`.
- **Quota usage** with `system.quotas`, `system.quota_usage`, `system.quota_limits`.
- **Row‑level security** with `system.row_policies`: coverage and effective filters.
- **Session analytics** via `system.session_log`: login failures, session durations, client mix.

## Cluster & Distributed Operations
- **Cluster topology** from `system.clusters`: shard/replica layout and health.
- **Distributed DDL queue** via `system.distributed_ddl_queue`: stuck DDL detection.
- **Distributed insert backlog** using `system.distribution_queue`: data_files, bytes, error tracking.

## Metrics, Background Jobs, and Scheduling
- **Metric history explorer** with `system.metric_log`: baselines, correlations, and spikes.
- **Background job timeline** via `system.background_schedule_pool_log`: failures and duration outliers.
- **Async insert monitoring** using `system.asynchronous_inserts` + `system.asynchronous_insert_log`.

## Configuration, Engines, and Schema Intelligence
- **Server settings browser** via `system.server_settings`: defaults vs current, obsoletes, sources.
- **MergeTree settings diff** from `system.merge_tree_settings`: per‑table overrides.
- **Settings profiles** with `system.settings_profiles` + `system.settings_profile_elements`.
- **Engine usage and guidance** via `system.table_engines` + `system.database_engines`.
- **Codec coverage** with `system.codecs`: per‑column recommendations.
- **Function discovery** using `system.functions` + `system.user_defined_functions`.

## External Integrations
- **Dictionaries health** via `system.dictionaries`: errors, refresh lags, memory usage.
- **Kafka consumers** using `system.kafka_consumers`: lag and assignment view.
- **Object storage activity** via `system.blob_storage_log`.
- **Queue integrations** using `system.s3_queue_settings` / `system.azure_queue_settings`.

## Diagnostics & Profiling Tools
- **Live stack traces** via `system.stack_trace`: blocked threads and deadlocks.
- **DNS cache monitoring** with `system.dns_cache`.
- **Schema inference cache** via `system.schema_inference_cache`.
- **OpenTelemetry spans** with `system.opentelemetry_span_log` (if enabled).
- **Allocator stats** via `system.jemalloc_bins` (if available).

## Operational Workflows
- **Backups dashboard** using `system.backups` + `system.backup_log`: status, durations, integrity.
- **Projection storage insights** via `system.projection_parts` + `system.projection_parts_columns`.
- **Query cache efficiency** with `system.query_cache`: hit rates and eviction signals.

## Suggested Feature Priorities

**High Value, Easy**
1. `system.disks` disk usage dashboard
2. `system.replicas` replica health & lag
3. `system.error_log` error dashboard
4. `system.quotas` quota monitoring
5. `system.distributed_ddl_queue` DDL queue monitor

**High Value, Medium Effort**
1. `system.replication_queue` replication bottlenecks
2. `system.query_thread_log` thread‑level query analysis
3. `system.dictionaries` dictionary health
4. `system.session_log` session analytics
5. `system.metric_log` baseline & correlation views

**High Value, Complex**
1. `system.trace_log` flame graphs & hotspots
2. `system.processors_profile_log` pipeline analysis
3. `system.zookeeper` / `system.zookeeper_connection_log` browser
4. Full grants + roles visualization
5. Backups restore workflow UI

## Reference System Table Docs
- ClickHouse system tables overview: https://clickhouse.com/docs/operations/system-tables/overview
- Debugging with system tables: https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables
- Altinity monitoring guide: https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-monitoring/
