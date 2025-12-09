import type { TimeRange, BucketSize, HistogramData, ColumnMetadata, QueryLogEntry, PartLogEntry } from '../types/queryLog';
import type { RangeFilter } from '../stores/queryStore';

const API_BASE = '/api';

function formatDateTime(date: Date): string {
  return date.toISOString().slice(0, 19).replace('T', ' ');
}

export async function fetchQueryLog(
  timeRange: TimeRange,
  search: string,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {},
  limit = 1000,
  offset = 0
): Promise<QueryLogEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
    offset: offset.toString(),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch query log: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchTimeSeries(
  timeRange: TimeRange,
  bucketSize: BucketSize,
  search: string,
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {}
): Promise<{ time: string; count: number }[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    bucket: bucketSize,
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log/timeseries?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch time series: ${response.statusText}`);
  }
  return response.json();
}

export interface StackedTimeSeriesPoint {
  time: string;
  Select: number;
  Insert: number;
  Delete: number;
  Other: number;
}

export async function fetchStackedTimeSeries(
  timeRange: TimeRange,
  bucketSize: BucketSize,
  search: string,
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {}
): Promise<StackedTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    bucket: bucketSize,
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log/timeseries-stacked?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch stacked time series: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchHistogram(
  field: string,
  timeRange: TimeRange,
  search: string,
  filters: Record<string, string[]>,
  limit = 20
): Promise<HistogramData[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/query-log/histogram/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch histogram: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchDistinctValues(
  field: string,
  timeRange: TimeRange,
  limit = 100
): Promise<string[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
  });

  const response = await fetch(`${API_BASE}/query-log/distinct/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch distinct values: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchTotalCount(
  timeRange: TimeRange,
  search: string,
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {}
): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log/count?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch count: ${response.statusText}`);
  }
  const data = await response.json();
  return data.total;
}

export async function fetchColumnMetadata(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/query-log/columns`);
  if (!response.ok) {
    throw new Error(`Failed to fetch column metadata: ${response.statusText}`);
  }
  return response.json();
}

export interface GroupedQueryEntry {
  example_query: string;
  user: string;
  current_database: string;
  count: number;
  total_duration: number;
  avg_duration: number;
  max_duration: number;
  min_duration: number;
  total_memory: number;
  avg_memory: number;
  max_memory: number;
  total_read_rows: number;
  avg_read_rows: number;
  total_read_bytes: number;
  total_written_rows: number;
  avg_written_rows: number;
  total_result_rows: number;
  avg_result_rows: number;
  first_seen: string;
  last_seen: string;
}

export async function fetchGroupedQueryLog(
  timeRange: TimeRange,
  search: string,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {},
  limit = 1000
): Promise<GroupedQueryEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log/grouped?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch grouped query log: ${response.statusText}`);
  }
  return response.json();
}

// ==================== PART LOG API ====================

export async function fetchPartLog(
  timeRange: TimeRange,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]> = {},
  limit = 1000,
  offset = 0
): Promise<PartLogEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
    offset: offset.toString(),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/part-log?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part log: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartLogCount(
  timeRange: TimeRange,
  filters: Record<string, string[]> = {}
): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/part-log/count?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part log count: ${response.statusText}`);
  }
  const data = await response.json();
  return data.total;
}

export async function fetchPartLogColumnMetadata(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/part-log/columns`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part_log column metadata: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartLogDistinctValues(
  field: string,
  timeRange: TimeRange,
  limit = 100
): Promise<string[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
  });

  const response = await fetch(`${API_BASE}/part-log/distinct/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part_log distinct values: ${response.statusText}`);
  }
  return response.json();
}

export interface PartLogTimeSeriesPoint {
  time: string;
  count: number;
  new_rows: number;
  merged_rows: number;
  avg_duration: number;
  min_duration: number;
  max_duration: number;
  sum_duration: number;
}

export interface PartLogStackedTimeSeriesPoint {
  time: string;
  NewPart: number;
  MergeParts: number;
  DownloadPart: number;
  RemovePart: number;
  MutatePart: number;
  Other: number;
}

export async function fetchPartLogHistogram(
  field: string,
  timeRange: TimeRange,
  filters: Record<string, string[]> = {},
  limit = 20
): Promise<HistogramData[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/part-log/histogram/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part_log histogram: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartLogTimeSeries(
  timeRange: TimeRange,
  bucketSize: BucketSize,
  filters: Record<string, string[]> = {}
): Promise<PartLogTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    bucket: bucketSize,
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/part-log/timeseries?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part_log time series: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartLogStackedTimeSeries(
  timeRange: TimeRange,
  bucketSize: BucketSize,
  filters: Record<string, string[]> = {}
): Promise<PartLogStackedTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    bucket: bucketSize,
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/part-log/timeseries-stacked?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part_log stacked time series: ${response.statusText}`);
  }
  return response.json();
}

// ==================== SYSTEM PARTS API ====================

export async function fetchParts(
  sortField = 'modification_time',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  filters: Record<string, string[]> = {},
  limit = 1000,
  offset = 0
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString(), offset: offset.toString() });
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const response = await fetch(`${API_BASE}/parts?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch parts: ${response.statusText}`);
  return response.json();
}

export async function fetchPartsCount(
  filters: Record<string, string[]> = {}
): Promise<number> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const response = await fetch(`${API_BASE}/parts/count?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch parts count: ${response.statusText}`);
  const data = await response.json();
  return data.count;
}

export async function fetchPartsDistinctValues(
  field: string,
  limit = 100
): Promise<string[]> {
  const params = new URLSearchParams({ limit: limit.toString() });
  const response = await fetch(`${API_BASE}/parts/distinct/${field}?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch parts distinct values: ${response.statusText}`);
  return response.json();
}

export async function fetchPartsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/parts/columns`);
  if (!response.ok) throw new Error(`Failed to fetch parts columns: ${response.statusText}`);
  return response.json();
}

export async function fetchPartsHistogram(
  field: string,
  filters: Record<string, string[]> = {},
  limit = 20
): Promise<HistogramData[]> {
  const params = new URLSearchParams({
    limit: limit.toString(),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/parts/histogram/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch parts histogram: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartitions(
  sortField = 'modification_time',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  filters: Record<string, string[]> = {},
  limit = 1000,
  offset = 0
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString(), offset: offset.toString() });
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const response = await fetch(`${API_BASE}/partitions?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions: ${response.statusText}`);
  return response.json();
}

export async function fetchPartitionsCount(
  filters: Record<string, string[]> = {}
): Promise<number> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const response = await fetch(`${API_BASE}/partitions/count?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions count: ${response.statusText}`);
  const data = await response.json();
  return data.count;
}

export async function fetchPartitionsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/partitions/columns`);
  if (!response.ok) throw new Error(`Failed to fetch partitions columns: ${response.statusText}`);
  return response.json();
}

export interface GroupedPartsEntry {
  database: string;
  table: string;
  partition_count: number;
  part_count: number;
  total_rows: number;
  total_bytes: number;
  last_modification_time: string;
}

export interface TablePartitionEntry {
  partition_id: string;
  parts_count: number;
  total_rows: number;
  total_bytes: number;
  min_block: number;
  max_block: number;
  oldest_part: string;
  newest_part: string;
}

export async function fetchTablePartitions(
  database: string,
  table: string,
  activeOnly = true
): Promise<TablePartitionEntry[]> {
  const params = new URLSearchParams({ activeOnly: activeOnly ? '1' : '0' });
  const response = await fetch(`${API_BASE}/table-partitions/${encodeURIComponent(database)}/${encodeURIComponent(table)}?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch table partitions: ${response.statusText}`);
  return response.json();
}

export async function fetchGroupedParts(
  filters: Record<string, string[]> = {}
): Promise<GroupedPartsEntry[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/parts/grouped?${params}` : `${API_BASE}/parts/grouped`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch grouped parts: ${response.statusText}`);
  return response.json();
}

// ==================== ACTIVITY API ====================

export async function fetchProcesses(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/processes?${params}` : `${API_BASE}/processes`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch processes: ${response.statusText}`);
  return response.json();
}

export async function fetchProcessesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/processes/columns`);
  if (!response.ok) throw new Error(`Failed to fetch processes columns: ${response.statusText}`);
  return response.json();
}

export async function fetchProcessesDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/processes/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch processes distinct: ${response.statusText}`);
  return response.json();
}

export async function fetchMerges(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/merges?${params}` : `${API_BASE}/merges`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch merges: ${response.statusText}`);
  return response.json();
}

export async function fetchMergesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/merges/columns`);
  if (!response.ok) throw new Error(`Failed to fetch merges columns: ${response.statusText}`);
  return response.json();
}

export async function fetchMergesDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/merges/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch merges distinct: ${response.statusText}`);
  return response.json();
}

export async function fetchMutations(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/mutations?${params}` : `${API_BASE}/mutations`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch mutations: ${response.statusText}`);
  return response.json();
}

export async function fetchMutationsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/mutations/columns`);
  if (!response.ok) throw new Error(`Failed to fetch mutations columns: ${response.statusText}`);
  return response.json();
}

export async function fetchMutationsDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/mutations/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch mutations distinct: ${response.statusText}`);
  return response.json();
}

// ==================== VIEW REFRESHES API ====================

export async function fetchViewRefreshes(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/view-refreshes?${params}` : `${API_BASE}/view-refreshes`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch view refreshes: ${response.statusText}`);
  return response.json();
}

export async function fetchViewRefreshesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/view-refreshes/columns`);
  if (!response.ok) throw new Error(`Failed to fetch view refreshes columns: ${response.statusText}`);
  return response.json();
}

export async function fetchViewRefreshesDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/view-refreshes/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch view refreshes distinct: ${response.statusText}`);
  return response.json();
}

// ==================== METRICS API ====================

export async function fetchMetrics(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/metrics`);
  if (!response.ok) throw new Error(`Failed to fetch metrics: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncMetrics(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/async-metrics`);
  if (!response.ok) throw new Error(`Failed to fetch async metrics: ${response.statusText}`);
  return response.json();
}

export async function fetchEvents(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/events`);
  if (!response.ok) throw new Error(`Failed to fetch events: ${response.statusText}`);
  return response.json();
}

// ==================== DATABASE BROWSER API ====================

export interface BrowserDatabase {
  name: string;
  engine: string;
  data_path: string;
  metadata_path: string;
  uuid: string;
}

export interface BrowserTable {
  name: string;
  engine: string;
  total_rows: number;
  total_bytes: number;
  metadata_modification_time: string;
}

export interface BrowserPartition {
  partition_id: string;
  partition: string;
  part_count: number;
  total_rows: number;
  total_bytes: number;
  min_time: string;
  max_time: string;
}

export interface BrowserPart {
  name: string;
  partition_id: string;
  rows: number;
  bytes_on_disk: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  marks: number;
  modification_time: string;
  min_time: string;
  max_time: string;
  level: number;
  primary_key_bytes_in_memory: number;
}

export async function fetchBrowserDatabases(): Promise<BrowserDatabase[]> {
  const response = await fetch(`${API_BASE}/browser/databases`);
  if (!response.ok) throw new Error(`Failed to fetch databases: ${response.statusText}`);
  return response.json();
}

export async function fetchBrowserTables(database: string): Promise<BrowserTable[]> {
  const response = await fetch(`${API_BASE}/browser/tables/${encodeURIComponent(database)}`);
  if (!response.ok) throw new Error(`Failed to fetch tables: ${response.statusText}`);
  return response.json();
}

export async function fetchBrowserPartitions(database: string, table: string): Promise<BrowserPartition[]> {
  const response = await fetch(`${API_BASE}/browser/partitions/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions: ${response.statusText}`);
  return response.json();
}

export async function fetchBrowserParts(database: string, table: string, partition: string): Promise<BrowserPart[]> {
  const response = await fetch(`${API_BASE}/browser/parts/${encodeURIComponent(database)}/${encodeURIComponent(table)}/${encodeURIComponent(partition)}`);
  if (!response.ok) throw new Error(`Failed to fetch parts: ${response.statusText}`);
  return response.json();
}

export interface BrowserColumn {
  name: string;
  type: string;
  default_kind: string;
  default_expression: string;
  comment: string;
  is_in_partition_key: number;
  is_in_sorting_key: number;
  is_in_primary_key: number;
  compression_codec: string;
}

export async function fetchBrowserColumns(database: string, table: string): Promise<BrowserColumn[]> {
  const response = await fetch(`${API_BASE}/browser/columns/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch columns: ${response.statusText}`);
  return response.json();
}

// ==================== TEXT LOG API ====================

export interface TextLogEntry {
  event_time: string;
  event_time_microseconds: string;
  event_date: string;
  level: string;
  query_id: string;
  logger_name: string;
  message: string;
  revision: number;
  source_file: string;
  source_line: number;
  thread_name: string;
  thread_id: number;
}

export interface TextLogTimeSeriesPoint {
  time: string;
  count: number;
  errors: number;
  warnings: number;
}

export async function fetchTextLog(
  timeRange: TimeRange,
  search: string,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]>,
  limit: number,
  offset: number
): Promise<TextLogEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
    offset: offset.toString(),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/text-log?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch text log: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchTextLogCount(
  timeRange: TimeRange,
  search: string,
  filters: Record<string, string[]>
): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/text-log/count?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch text log count: ${response.statusText}`);
  }
  const data = await response.json();
  return data.total;
}

export async function fetchTextLogTimeSeries(
  timeRange: TimeRange,
  bucket: BucketSize,
  filters: Record<string, string[]>
): Promise<TextLogTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    bucket,
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  const response = await fetch(`${API_BASE}/text-log/timeseries?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch text log time series: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchTextLogDistinct(
  field: string,
  timeRange: TimeRange
): Promise<string[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  const response = await fetch(`${API_BASE}/text-log/distinct/${field}?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch text log distinct values: ${response.statusText}`);
  }
  return response.json();
}

// ==================== INSTANCE API ====================

export async function fetchUsers(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/users`);
  if (!response.ok) throw new Error(`Failed to fetch users: ${response.statusText}`);
  return response.json();
}

export async function fetchUsersColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/users/columns`);
  if (!response.ok) throw new Error(`Failed to fetch users columns: ${response.statusText}`);
  return response.json();
}

export async function fetchSettings(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/settings`);
  if (!response.ok) throw new Error(`Failed to fetch settings: ${response.statusText}`);
  return response.json();
}

export async function fetchSettingsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/settings/columns`);
  if (!response.ok) throw new Error(`Failed to fetch settings columns: ${response.statusText}`);
  return response.json();
}

// ==================== EXPLAIN API ====================

export async function fetchExplainPlan(query: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/explain`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!response.ok) throw new Error(`Failed to run explain: ${response.statusText}`);
  return response.json();
}

export type ExplainType = 'plan' | 'indexes' | 'actions' | 'pipeline' | 'ast' | 'syntax' | 'estimate';

export async function fetchExplainByType(query: string, type: ExplainType): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/explain/${type}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(error.error || `Failed to run explain ${type}`);
  }
  return response.json();
}

export interface QueryResult {
  data: Record<string, unknown>[];
  rowCount: number;
  duration: number;
}

export async function executeQuery(query: string, limit = 1000): Promise<QueryResult> {
  const response = await fetch(`${API_BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, limit }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: response.statusText }));
    throw new Error(error.error || 'Failed to execute query');
  }
  return response.json();
}

// ==================== PROFILE EVENTS API ====================

export async function fetchProfileEvents(
  timeRange: TimeRange,
  filters: Record<string, string[]>,
  eventColumns: string[],
  search?: string,
  limit = 1000
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
    eventColumns: eventColumns.join(','),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (search) {
    params.set('search', search);
  }

  const response = await fetch(`${API_BASE}/query-log/profile-events?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch profile events: ${response.statusText}`);
  return response.json();
}
