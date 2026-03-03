# QueryDog Feature Suggestions - ClickHouse Specific

Features based on ClickHouse system tables and internals not yet utilized.

---

## Currently Used System Tables
- `system.query_log`, `system.query_views_log`
- `system.parts`, `system.partitions`, `system.part_log`
- `system.processes`, `system.merges`, `system.mutations`
- `system.metrics`, `system.asynchronous_metrics`, `system.events`
- `system.tables`, `system.columns`, `system.databases`
- `system.users`, `system.settings`
- `system.view_refreshes`, `system.query_cache`
- `system.projections`, `system.data_skipping_indices`

---

## 🔄 Replication Monitoring (NEW)

### system.replicas
- **Replica Health Dashboard** - Show is_leader, is_readonly, is_session_expired, future_parts, parts_to_check, queue_size, inserts_in_queue, merges_in_queue
- **Replica Lag Visualization** - Compare absolute_delay and relative_delay across replicas
- **Replica Sync Status** - Show log_max_index vs log_pointer gap
- **Zookeeper Path Browser** - Display zookeeper_path for each replicated table
- **Replica Comparison** - Compare replica_name, total_replicas, active_replicas counts

### system.replication_queue
- **Replication Queue Monitor** - Show pending operations (GET_PART, MERGE_PARTS, MUTATE_PART, etc.)
- **Replication Bottleneck Detection** - Identify queued operations by type and age
- **Failed Replication Operations** - Filter by num_tries > 0 with last_exception
- **Replication Queue Timeline** - Show create_time distribution of pending operations
- **Source Replica Analysis** - Track which replicas are sources for fetches

### system.replicated_fetches
- **Active Fetch Monitor** - Real-time view of ongoing part fetches
- **Fetch Progress Tracking** - Show progress, elapsed, total_size_bytes_compressed
- **Source Node Analysis** - Which nodes are serving fetches (source_replica_hostname)
- **Fetch Throughput** - Calculate bytes_read_compressed per second

---

## 💾 Storage & Disk Management (NEW)

### system.disks
- **Disk Usage Dashboard** - Show free_space, total_space, keep_free_space per disk
- **Disk Type Breakdown** - Categorize by type (local, s3, hdfs, etc.)
- **Storage Capacity Alerts** - Warn when free_space falls below thresholds
- **Disk Path Browser** - Show path configurations
- **Cache Disk Monitoring** - Track cache disk usage separately

### system.storage_policies
- **Storage Policy Viewer** - Display configured policies and their volumes
- **Policy-to-Table Mapping** - Show which tables use which storage policies
- **Volume Configuration** - Show disk assignments per volume
- **Tiered Storage Analysis** - Visualize hot/cold data movement paths

### system.moves
- **Data Movement Tracker** - Monitor active part moves between disks/volumes
- **Move Queue Analysis** - Show pending moves and their reasons
- **Move History** - Track completed moves over time

### system.detached_parts
- **Detached Parts Manager** - List all detached parts with reason, disk, min_block_number
- **Orphaned Part Detection** - Find detached parts older than threshold
- **Detach/Attach UI** - Interface to manage detached parts
- **Disk Usage by Detached** - Calculate space used by detached parts

### system.dropped_tables / system.dropped_tables_parts
- **Recently Dropped Tables** - Show tables pending permanent deletion
- **Dropped Table Recovery Window** - Time until permanent deletion
- **Space Reclaimable** - Calculate space from dropped tables not yet purged

---

## 🔍 Query Deep Dive (NEW)

### system.query_thread_log
- **Thread-Level Analysis** - Break down query execution by thread
- **Thread Memory Profile** - Memory allocation per thread (peak_memory_usage)
- **Thread I/O Analysis** - read_rows, read_bytes per thread
- **Thread Timeline** - ProfileEvents per thread for bottleneck identification
- **Parallel Execution Efficiency** - Compare thread utilization across query execution

### system.trace_log
- **Stack Trace Browser** - View stack traces for slow queries
- **Hot Function Analysis** - Aggregate stack traces to find CPU hotspots
- **Memory Allocation Traces** - Filter trace_type = 'Memory' for memory profiling
- **Lock Contention Detection** - Find traces indicating lock waits
- **Query Flame Graphs** - Generate flame graphs from trace data

### system.query_metric_log
- **Per-Second Query Metrics** - Fine-grained metrics during query execution
- **Memory Growth Timeline** - Track memory_usage over query lifetime
- **I/O Pattern Analysis** - read_rows, written_rows progression
- **Query Execution Phases** - Identify which phase consumed most resources

### system.processors_profile_log
- **Query Pipeline Profiling** - View processor execution stats
- **Pipeline Bottlenecks** - Identify slow processors in execution pipeline
- **Processor Input/Output** - Track rows processed per processor
- **Elapsed Time Breakdown** - Time spent in each processor

---

## 🚨 Error & Crash Analysis (NEW)

### system.error_log
- **Error Dashboard** - Aggregate errors by code, name
- **Error Timeline** - Show error frequency over time
- **Error Details** - Display last_error_message, last_error_time
- **Error Correlation** - Link errors to queries via remote address/user

### system.crash_log
- **Crash History** - View server crash events
- **Crash Analysis** - Show signal, thread_id, query_id, stack_trace
- **Crash Patterns** - Identify recurring crash signatures
- **Crash Timeline** - Visualize crash frequency

### system.warnings
- **System Warnings Dashboard** - Display current server warnings
- **Warning Categories** - Group by warning type
- **Warning Severity** - Prioritize warnings by impact

---

## 🔐 Security & Access Control (NEW)

### system.grants
- **Permission Matrix** - Show all grants by user/role
- **Privilege Analysis** - View access_type, database, table, column grants
- **Grant Source Tracking** - Distinguish user grants vs role grants
- **Wildcard Permission Detection** - Find overly broad permissions

### system.roles / system.role_grants
- **Role Hierarchy Viewer** - Visualize role inheritance
- **Role Assignment Matrix** - Show user-to-role mappings
- **Role Usage Analysis** - Which roles are actively used

### system.current_roles / system.enabled_roles
- **Active Session Roles** - Show roles active in current sessions
- **Role Activation Patterns** - Track which roles are commonly enabled

### system.quotas / system.quota_usage / system.quota_limits
- **Quota Dashboard** - Display configured quotas and limits
- **Quota Consumption** - Show current usage vs limits
- **Quota Violation Tracking** - Alert on approaching limits
- **User Quota Breakdown** - Per-user quota consumption

### system.row_policies
- **Row-Level Security Viewer** - Display configured RLS policies
- **Policy Coverage** - Show which tables have RLS enabled
- **Policy Effectiveness** - Test RLS filter conditions

### system.session_log
- **Session Analytics** - Track login/logout events
- **Session Duration Analysis** - Average session lengths by user
- **Failed Login Detection** - Filter type='LoginFailure'
- **Client Distribution** - Analyze client_hostname, interface patterns
- **Authentication Methods** - Track auth_type usage

---

## 🌐 Cluster & Distributed (NEW)

### system.clusters
- **Cluster Topology View** - Visualize cluster/shard/replica configuration
- **Node Health Matrix** - Show is_local, errors_count per node
- **Shard Weight Analysis** - Display shard_weight distribution
- **Cluster Configuration Diff** - Compare cluster configs

### system.distributed_ddl_queue
- **DDL Queue Monitor** - Track pending ON CLUSTER operations
- **DDL Execution Status** - Show status across hosts
- **Stuck DDL Detection** - Find DDL operations not progressing
- **DDL History** - View completed distributed DDL operations

### system.distribution_queue
- **Distribution Queue Health** - Monitor pending distributed inserts
- **Queue Backlog Analysis** - Show data_files, data_compressed_bytes pending
- **Error Tracking** - Display last_exception for failed distributions
- **Queue Throughput** - Track is_blocked, data_files growth

### system.zookeeper
- **ZooKeeper Browser** - Navigate ZK paths used by ClickHouse
- **ZK Node Inspector** - View ctime, mtime, numChildren, dataLength
- **Replica Coordination Data** - Explore /clickhouse/tables paths

### system.zookeeper_connection
- **Keeper Connection Status** - Show connected session details
- **Connection Health** - Monitor xid, is_expired, op_num
- **Session Metrics** - Track session creation time

### system.zookeeper_log / system.zookeeper_connection_log
- **ZK Operation History** - View ZK operations performed
- **ZK Latency Analysis** - Track op_duration_ms
- **ZK Error Detection** - Filter by error responses

---

## 📊 Advanced Metrics (NEW)

### system.asynchronous_inserts / system.asynchronous_insert_log
- **Async Insert Monitor** - Track active async insert batches
- **Async Insert Latency** - Time from query to actual insert
- **Batch Size Analysis** - bytes, rows per async batch
- **Flush Triggers** - Track flush_time_microseconds, flush_query_id

### system.metric_log
- **Historical Metrics Browser** - Query any metric over time
- **Metric Correlation** - Compare multiple metrics on same timeline
- **Peak Detection** - Identify metric spikes
- **Metric Baselines** - Establish normal ranges

### system.background_schedule_pool_log
- **Background Job History** - View scheduled task execution
- **Job Duration Analysis** - Track execution times
- **Job Failure Detection** - Filter failed background tasks

---

## 🔧 Configuration & Schema (NEW)

### system.server_settings
- **Server Configuration Browser** - View all server settings
- **Setting Source Tracking** - Show changed, is_obsolete
- **Default vs Current** - Compare value to default
- **Configuration Validation** - Highlight unusual settings

### system.merge_tree_settings
- **MergeTree Configuration** - View engine-specific settings
- **Per-Table Settings** - Show settings applied to specific tables
- **Merge Policy Analysis** - Display merge-related thresholds

### system.settings_profiles / system.settings_profile_elements
- **Profile Browser** - View defined settings profiles
- **Profile Inheritance** - Show parent profiles
- **Profile Usage** - Track which users/roles use which profiles

### system.codecs
- **Available Codecs** - List compression codecs available
- **Codec Recommendations** - Based on data types in use

### system.data_type_families
- **Data Type Reference** - Browse all available data types
- **Type Compatibility** - Show aliases and case insensitivity

### system.table_engines / system.database_engines
- **Engine Reference** - Available engines and their features
- **Engine Usage Stats** - Which engines are used in your instance

### system.functions
- **Function Browser** - Browse all available functions
- **Function Categories** - Group by type (aggregate, arithmetic, etc.)
- **Function Search** - Search functions by name/description

### system.user_defined_functions
- **UDF Manager** - List and manage user-defined functions
- **UDF Source Viewer** - Show function definitions

---

## 📡 External Integrations (NEW)

### system.dictionaries
- **Dictionary Status Dashboard** - Show all external dictionaries
- **Dictionary Health** - last_exception, loading_duration
- **Dictionary Refresh Tracking** - lifetime, update intervals
- **Dictionary Memory Usage** - bytes_allocated
- **Stale Dictionary Detection** - last_successful_update_time analysis

### system.kafka_consumers
- **Kafka Consumer Monitor** - Track consumer status
- **Consumer Lag** - Show rdkafka statistics
- **Assignment Viewer** - Display topic/partition assignments
- **Consumer Errors** - Track last_exception

### system.s3_queue_settings / system.azure_queue_settings
- **Queue Settings Viewer** - Configuration for cloud queue integrations
- **Queue Processing Status** - Monitor cloud queue consumption

### system.blob_storage_log
- **Object Storage Operations** - Track S3/Azure/GCS operations
- **Operation Latency** - Duration of blob operations
- **Error Tracking** - Failed blob operations

### system.graphite_retentions
- **Graphite Retention Policies** - View rollup configurations
- **Retention Coverage** - Which metrics match which rules

---

## 🔬 Diagnostic Tools (NEW)

### system.stack_trace
- **Live Stack Traces** - Current stack traces of all threads
- **Thread State Analysis** - Identify blocked/waiting threads
- **Deadlock Detection** - Find potential deadlock patterns

### system.dns_cache
- **DNS Cache Viewer** - Current DNS cache entries
- **DNS Resolution Issues** - Identify failed lookups
- **Cache Efficiency** - Hit rate analysis

### system.schema_inference_cache
- **Schema Inference Cache** - Cached schema inferences
- **Cache Efficiency** - Track cache hits/misses
- **Inference Source** - Files/URLs with cached schemas

### system.opentelemetry_span_log
- **Distributed Tracing** - View OpenTelemetry spans
- **Trace Correlation** - Link ClickHouse operations to external traces
- **Span Duration Analysis** - Identify slow spans

### system.jemalloc_bins (if available)
- **Memory Allocator Stats** - Detailed jemalloc statistics
- **Memory Fragmentation** - Analyze bin utilization
- **Allocation Patterns** - Small vs large allocations

---

## 📋 Operational Features (NEW)

### system.backups / system.backup_log
- **Backup Status Dashboard** - Track backup operations
- **Backup History** - View completed backups with size, duration
- **Backup Verification** - Check backup integrity status
- **Restore Tracking** - Monitor restore operations

### system.projection_parts / system.projection_parts_columns
- **Projection Part Details** - Deeper dive into projection storage
- **Projection Efficiency** - Compare projection vs base table sizes
- **Projection Utilization** - Track which projections are being used

### system.parts_columns
- **Column-Level Part Analysis** - Storage per column per part
- **Column Compression Ratios** - Detailed compression analysis
- **Column Size Evolution** - Track column growth over time

---

## 🎯 Suggested Feature Priorities

### High Value, Easy to Implement
1. **system.disks** - Disk usage dashboard (simple SELECT)
2. **system.replicas** - Replica health view (if using replication)
3. **system.errors** - Error log dashboard
4. **system.quotas** - Quota monitoring
5. **system.distributed_ddl_queue** - DDL queue monitor

### High Value, Medium Effort
1. **system.trace_log** - Query flame graphs
2. **system.replication_queue** - Replication bottleneck detection
3. **system.dictionaries** - Dictionary health dashboard
4. **system.query_thread_log** - Thread-level query analysis
5. **system.session_log** - Session analytics

### High Value, More Complex
1. **system.zookeeper** - ZK browser with path navigation
2. **system.grants + roles** - Full permission visualization
3. **system.processors_profile_log** - Pipeline analysis
4. **Distributed tracing** - OpenTelemetry integration
5. **Backup management** - Full backup/restore UI

---

## 📚 References

- [ClickHouse System Tables Overview](https://clickhouse.com/docs/operations/system-tables/overview)
- [ClickHouse Debugging with System Tables](https://clickhouse.com/blog/clickhouse-debugging-issues-with-system-tables)
- [Altinity ClickHouse Monitoring Guide](https://kb.altinity.com/altinity-kb-setup-and-maintenance/altinity-kb-monitoring/)
