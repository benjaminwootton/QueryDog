import type { TimeRange, BucketSize, HistogramData, ColumnMetadata, QueryLogEntry, PartLogEntry } from '../types/queryLog';
import type { RangeFilter } from '../stores/queryStore';

const API_BASE = '/api';

// Common connection error patterns - errors that indicate the server/database is unreachable
const CONNECTION_ERROR_PATTERNS = [
  'ENOTFOUND', 'ECONNREFUSED', 'ETIMEDOUT', 'ECONNRESET',
  'EHOSTUNREACH', 'EAI_AGAIN', 'socket hang up', 'DEPTH_ZERO_SELF_SIGNED_CERT',
  'UNABLE_TO_VERIFY_LEAF_SIGNATURE', 'CERT_HAS_EXPIRED',
  'Failed to fetch', 'NetworkError', 'net::ERR_',
];

// Proxy/server error patterns - generic errors from Vite proxy when backend is down
const SERVER_ERROR_PATTERNS = [
  'Internal Server Error',
  '502 Bad Gateway',
  '503 Service Unavailable',
  '504 Gateway Timeout',
];

export function isConnectionError(message: string): boolean {
  return CONNECTION_ERROR_PATTERNS.some(pattern => message.includes(pattern));
}

export function isServerError(message: string): boolean {
  return SERVER_ERROR_PATTERNS.some(pattern => message.includes(pattern));
}

export function extractConnectionError(message: string): string {
  // Strip prefixes like "Failed to fetch query log: ", "Entries: " etc.
  const cleaned = message
    .replace(/^Failed to (fetch|load) [^:]+:\s*/i, '')
    .replace(/^(Entries|Time series|Stacked series|Count):\s*/i, '');
  return cleaned;
}

// Transform error messages into user-friendly versions
export function getUserFriendlyError(message: string): string {
  const cleaned = extractConnectionError(message);

  // Server/proxy errors indicate backend is down
  if (isServerError(cleaned)) {
    return 'Cannot connect to backend server. Please ensure the server is running.';
  }

  // Network/connection errors
  if (cleaned.includes('ECONNREFUSED')) {
    return 'Connection refused. The backend server is not running or is unreachable.';
  }
  if (cleaned.includes('ETIMEDOUT')) {
    return 'Connection timed out. The server may be overloaded or unreachable.';
  }
  if (cleaned.includes('ENOTFOUND')) {
    return 'Server not found. Please check your network connection and server address.';
  }
  if (cleaned.includes('Failed to fetch') || cleaned.includes('NetworkError')) {
    return 'Network error. Please check your connection and ensure the server is running.';
  }
  if (isConnectionError(cleaned)) {
    return `Connection error: ${cleaned}`;
  }

  return cleaned;
}

export async function fetchHealth(): Promise<{ status: string; error?: string }> {
  try {
    const response = await fetch(`${API_BASE}/health`);
    if (!response.ok) {
      // Server returned an error status
      const statusText = response.statusText || `HTTP ${response.status}`;
      if (isServerError(statusText) || response.status >= 500) {
        return { status: 'unhealthy', error: 'Backend server error' };
      }
      return { status: 'unhealthy', error: statusText };
    }
    return response.json();
  } catch (err) {
    // Network error - couldn't reach the server at all
    const message = err instanceof Error ? err.message : 'Unknown error';
    return { status: 'unhealthy', error: getUserFriendlyError(message) };
  }
}

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
  offset = 0,
  bucketSize?: BucketSize
): Promise<QueryLogEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
    offset: offset.toString(),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

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
  limit = 20,
  bucketSize?: BucketSize
): Promise<HistogramData[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    limit: limit.toString(),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

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
  rangeFilters: Record<string, RangeFilter> = {},
  bucketSize?: BucketSize
): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

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
  normalized_query_hash?: string;  // Only present when normalize=true
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
  limit = 1000,
  normalize = false,
  bucketSize?: BucketSize
): Promise<GroupedQueryEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  if (normalize) {
    params.set('normalize', 'true');
  }

  const response = await fetch(`${API_BASE}/query-log/grouped?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch grouped query log: ${response.statusText}`);
  }
  return response.json();
}

// ==================== BY TABLE STATS API ====================

export interface ByTableEntry {
  table_name: string;
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
  error_count: number;
  error_rate: number;
  first_seen: string;
  last_seen: string;
}

export async function fetchByTableStats(
  timeRange: TimeRange,
  search: string,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]>,
  rangeFilters: Record<string, RangeFilter> = {},
  limit = 500,
  bucketSize?: BucketSize
): Promise<ByTableEntry[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

  if (search) {
    params.set('search', search);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  const response = await fetch(`${API_BASE}/query-log/by-table?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch by-table stats: ${response.statusText}`);
  }
  return response.json();
}

// ==================== QUERY VIEWS LOG API ====================

export interface QueryViewsLogEntry {
  event_time: string;
  view_name: string;
  view_type: string;
  view_query: string;
  view_target: string;
  read_rows: number;
  read_bytes: number;
  written_rows: number;
  written_bytes: number;
  peak_memory_usage: number;
  view_duration_ms: number;
  status: string;
  exception: string;
  initial_query_id: string;
}

export async function fetchQueryViewsLog(
  timeRange: TimeRange,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
  filters: Record<string, string[]> = {},
  search = '',
  limit = 1000,
  offset = 0
): Promise<QueryViewsLogEntry[]> {
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

  if (search) {
    params.set('search', search);
  }

  const response = await fetch(`${API_BASE}/query-views-log?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch query views log: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchQueryViewsLogCount(
  timeRange: TimeRange,
  filters: Record<string, string[]> = {},
  search = ''
): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (search) {
    params.set('search', search);
  }

  const response = await fetch(`${API_BASE}/query-views-log/count?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch query views log count: ${response.statusText}`);
  }
  const data = await response.json();
  return data.total;
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
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = errorData.error || response.statusText;
    const error: any = new Error(errorMessage);
    error.status = response.status;
    error.type = errorData.type;
    error.details = errorData.details;
    throw error;
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
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = errorData.error || response.statusText;
    const error: any = new Error(errorMessage);
    error.status = response.status;
    error.type = errorData.type;
    error.details = errorData.details;
    throw error;
  }
  const data = await response.json();
  return data.total;
}

export async function fetchPartLogColumnMetadata(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/part-log/columns`);
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = errorData.error || response.statusText;
    const error: any = new Error(errorMessage);
    error.status = response.status;
    error.type = errorData.type;
    error.details = errorData.details;
    throw error;
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
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = errorData.error || response.statusText;
    const error: any = new Error(errorMessage);
    error.status = response.status;
    error.type = errorData.type;
    error.details = errorData.details;
    throw error;
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
    const errorData = await response.json().catch(() => ({}));
    const errorMessage = errorData.error || response.statusText;
    const error: any = new Error(errorMessage);
    error.status = response.status;
    error.type = errorData.type;
    error.details = errorData.details;
    throw error;
  }
  return response.json();
}

// ==================== SYSTEM PARTS API ====================

export async function fetchParts(
  sortField = 'modification_time',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  filters: Record<string, string[]> = {},
  limit = 1000,
  offset = 0,
  search = ''
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString(), offset: offset.toString() });
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const response = await fetch(`${API_BASE}/parts?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch parts: ${response.statusText}`);
  return response.json();
}

export async function fetchPartsCount(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<number> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
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
  offset = 0,
  search = ''
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString(), offset: offset.toString() });
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const response = await fetch(`${API_BASE}/partitions?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions: ${response.statusText}`);
  return response.json();
}

export async function fetchPartitionsCount(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<number> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
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

// Aggregated partitions (grouped by partition_id)
export async function fetchPartitionsSummary(
  sortField = 'latest_modification',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  filters: Record<string, string[]> = {},
  limit = 1000,
  offset = 0,
  search = ''
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString(), offset: offset.toString() });
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const response = await fetch(`${API_BASE}/partitions-summary?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions summary: ${response.statusText}`);
  return response.json();
}

export async function fetchPartitionsSummaryCount(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<number> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const response = await fetch(`${API_BASE}/partitions-summary/count?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partitions summary count: ${response.statusText}`);
  const data = await response.json();
  return data.count;
}

export async function fetchPartitionsSummaryColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/partitions-summary/columns`);
  if (!response.ok) throw new Error(`Failed to fetch partitions summary columns: ${response.statusText}`);
  return response.json();
}

export interface GroupedPartsEntry {
  database: string;
  table: string;
  engine_full: string;
  partition_count: number;
  part_count: number;
  total_rows: number;
  total_bytes: number;
  compressed_bytes: number;
  uncompressed_bytes: number;
  savings_pct: number;
  last_modification_time: string;
}

export interface ColumnCompressionEntry {
  name: string;
  type: string;
  compressed_bytes: number;
  uncompressed_bytes: number;
  savings_pct: number;
}

export async function fetchTableCompression(
  database: string,
  table: string
): Promise<ColumnCompressionEntry[]> {
  const response = await fetch(`${API_BASE}/table-compression/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch table compression: ${response.statusText}`);
  return response.json();
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

export type MergeTreeIndexEntry = Record<string, unknown>;

export async function fetchMergeTreeIndex(
  database: string,
  table: string
): Promise<MergeTreeIndexEntry[]> {
  const response = await fetch(`${API_BASE}/table-mergetree-index/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch MergeTree index: ${response.statusText}`);
  return response.json();
}

export async function fetchTableDefinition(
  database: string,
  table: string
): Promise<string> {
  const response = await fetch(`${API_BASE}/table-definition/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch table definition: ${response.statusText}`);
  const data = await response.json();
  return data.definition;
}

export interface PartitionPartEntry {
  name: string;
  rows: number;
  bytes_on_disk: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  marks: number;
  modification_time: string;
  min_block_number: number;
  max_block_number: number;
  level: number;
  primary_key_bytes_in_memory: number;
  active: number;
}

export async function fetchPartitionParts(
  database: string,
  table: string,
  partitionId: string,
  activeOnly = true
): Promise<PartitionPartEntry[]> {
  const params = new URLSearchParams({ activeOnly: activeOnly ? '1' : '0' });
  const response = await fetch(`${API_BASE}/partition-parts/${encodeURIComponent(database)}/${encodeURIComponent(table)}/${encodeURIComponent(partitionId)}?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch partition parts: ${response.statusText}`);
  return response.json();
}

export async function fetchGroupedParts(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<GroupedPartsEntry[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/parts/grouped?${params}` : `${API_BASE}/parts/grouped`;
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

// ==================== QUERY CACHE API ====================

export async function fetchQueryCache(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/query-cache?${params}` : `${API_BASE}/query-cache`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch query cache: ${response.statusText}`);
  return response.json();
}

export async function fetchQueryCacheColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/query-cache/columns`);
  if (!response.ok) throw new Error(`Failed to fetch query cache columns: ${response.statusText}`);
  return response.json();
}

// ==================== BACKGROUND JOBS API ====================

export async function fetchBackgroundJobs(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/background-jobs?${params}` : `${API_BASE}/background-jobs`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch background jobs: ${response.statusText}`);
  return response.json();
}

export async function fetchBackgroundJobsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/background-jobs/columns`);
  if (!response.ok) throw new Error(`Failed to fetch background jobs columns: ${response.statusText}`);
  return response.json();
}

export async function fetchBackgroundJobsDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/background-jobs/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch background jobs distinct: ${response.statusText}`);
  return response.json();
}

// ==================== ASYNC INSERTS API ====================

export async function fetchAsyncInserts(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/async-inserts?${params}` : `${API_BASE}/async-inserts`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch async inserts: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncInsertsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/async-inserts/columns`);
  if (!response.ok) throw new Error(`Failed to fetch async inserts columns: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncInsertsDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/async-inserts/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch async inserts distinct: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncInsertLog(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/async-insert-log?${params}` : `${API_BASE}/async-insert-log`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch async insert log: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncInsertLogColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/async-insert-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch async insert log columns: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncInsertLogDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/async-insert-log/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch async insert log distinct: ${response.statusText}`);
  return response.json();
}

// ==================== DISTRIBUTED DDL API ====================

export async function fetchDistributedDDL(filters: Record<string, string[]> = {}): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  const url = Object.keys(filters).length > 0 ? `${API_BASE}/distributed-ddl?${params}` : `${API_BASE}/distributed-ddl`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch distributed DDL: ${response.statusText}`);
  return response.json();
}

export async function fetchDistributedDDLColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/distributed-ddl/columns`);
  if (!response.ok) throw new Error(`Failed to fetch distributed DDL columns: ${response.statusText}`);
  return response.json();
}

export async function fetchDistributedDDLDistinct(field: string): Promise<string[]> {
  const response = await fetch(`${API_BASE}/distributed-ddl/distinct/${field}`);
  if (!response.ok) throw new Error(`Failed to fetch distributed DDL distinct: ${response.statusText}`);
  return response.json();
}

// ==================== DISKS API ====================

export async function fetchDisks(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/disks`);
  if (!response.ok) throw new Error(`Failed to fetch disks: ${response.statusText}`);
  return response.json();
}

export async function fetchDisksColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/disks/columns`);
  if (!response.ok) throw new Error(`Failed to fetch disks columns: ${response.statusText}`);
  return response.json();
}

// ==================== STORAGE POLICIES API ====================

export async function fetchStoragePolicies(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/storage-policies`);
  if (!response.ok) throw new Error(`Failed to fetch storage policies: ${response.statusText}`);
  return response.json();
}

export async function fetchStoragePoliciesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/storage-policies/columns`);
  if (!response.ok) throw new Error(`Failed to fetch storage policies columns: ${response.statusText}`);
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

export async function fetchErrors(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/errors`);
  if (!response.ok) throw new Error(`Failed to fetch errors: ${response.statusText}`);
  return response.json();
}

export async function fetchErrorsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/errors/columns`);
  if (!response.ok) throw new Error(`Failed to fetch errors columns: ${response.statusText}`);
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
  partition_count: number;
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

export interface DatabaseSummary {
  database: string;
  engine: string;
  table_count: number;
  total_rows: number;
  total_bytes: number;
  partition_count: number;
  part_count: number;
  part_rows: number;
  bytes_on_disk: number;
  compressed_bytes: number;
  uncompressed_bytes: number;
  compression_ratio: number;
  latest_modification: string;
}

export async function fetchDatabasesSummary(): Promise<DatabaseSummary[]> {
  const response = await fetch(`${API_BASE}/databases/summary`);
  if (!response.ok) throw new Error(`Failed to fetch databases summary: ${response.statusText}`);
  return response.json();
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

export async function fetchBrowserSampleData(database: string, table: string): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/browser/sample/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch sample data: ${response.statusText}`);
  return response.json();
}

export interface BrowserProjection {
  name: string;
  type: string;
  sorting_key: string;
  query: string;
  storage_policy: string;
  partition_key: string;
  primary_key: string;
}

export interface BrowserProjectionPart {
  name: string;
  part_name: string;
  partition_id: string;
  rows: number;
  bytes_on_disk: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  marks: number;
  modification_time: string;
  parent_part_name: string;
  is_broken: number;
}

export async function fetchBrowserProjections(database: string, table: string): Promise<BrowserProjection[]> {
  const response = await fetch(`${API_BASE}/browser/projections/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch projections: ${response.statusText}`);
  return response.json();
}

export async function fetchBrowserProjectionParts(database: string, table: string, projection: string): Promise<BrowserProjectionPart[]> {
  const response = await fetch(`${API_BASE}/browser/projection-parts/${encodeURIComponent(database)}/${encodeURIComponent(table)}/${encodeURIComponent(projection)}`);
  if (!response.ok) throw new Error(`Failed to fetch projection parts: ${response.statusText}`);
  return response.json();
}

// System-wide projections API
export interface SystemProjection {
  database: string;
  table: string;
  name: string;
  type: string;
  sorting_key: string;
  query: string;
}

export async function fetchSystemProjections(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<SystemProjection[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/projections?${params}` : `${API_BASE}/projections`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch projections: ${response.statusText}`);
  return response.json();
}

export async function fetchProjectionParts(
  database: string,
  table: string,
  projection: string
): Promise<BrowserProjectionPart[]> {
  const response = await fetch(`${API_BASE}/projection-parts/${encodeURIComponent(database)}/${encodeURIComponent(table)}/${encodeURIComponent(projection)}`);
  if (!response.ok) throw new Error(`Failed to fetch projection parts: ${response.statusText}`);
  return response.json();
}

// System-wide views API
export interface SystemView {
  database: string;
  name: string;
  engine: string;
  as_select: string;
  metadata_modification_time: string;
  create_table_query: string;
}

export async function fetchSystemViews(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<SystemView[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/views?${params}` : `${API_BASE}/views`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch views: ${response.statusText}`);
  return response.json();
}

export async function fetchViewDefinition(
  database: string,
  view: string
): Promise<string> {
  const response = await fetch(`${API_BASE}/view-definition/${encodeURIComponent(database)}/${encodeURIComponent(view)}`);
  if (!response.ok) throw new Error(`Failed to fetch view definition: ${response.statusText}`);
  const data = await response.json();
  return data.definition;
}

// System-wide dictionaries API
export interface SystemDictionary {
  database: string;
  name: string;
  uuid: string;
  status: string;
  origin: string;
  type: string;
  key: string;
  attribute_names: string[];
  attribute_types: string[];
  bytes_allocated: number;
  hierarchical_index_bytes_allocated: number;
  query_count: number;
  hit_rate: number;
  found_rate: number;
  element_count: number;
  load_factor: number;
  source: string;
  lifetime_min: number;
  lifetime_max: number;
  loading_start_time: string;
  last_successful_update_time: string;
  loading_duration: number;
  last_exception: string;
}

export async function fetchDictionaries(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<SystemDictionary[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/dictionaries?${params}` : `${API_BASE}/dictionaries`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch dictionaries: ${response.statusText}`);
  return response.json();
}

// System-wide data skipping indexes API
export interface SystemIndex {
  database: string;
  table: string;
  name: string;
  type: string;
  type_full: string;
  expr: string;
  granularity: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  marks: number;
}

export async function fetchSystemIndexes(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<SystemIndex[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/indexes?${params}` : `${API_BASE}/indexes`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch indexes: ${response.statusText}`);
  return response.json();
}

// Browser endpoint for indexes per table
export interface BrowserIndex {
  name: string;
  type: string;
  type_full: string;
  expr: string;
  granularity: number;
  data_compressed_bytes: number;
  data_uncompressed_bytes: number;
  marks: number;
}

export async function fetchBrowserIndexes(database: string, table: string): Promise<BrowserIndex[]> {
  const response = await fetch(`${API_BASE}/browser/indexes/${encodeURIComponent(database)}/${encodeURIComponent(table)}`);
  if (!response.ok) throw new Error(`Failed to fetch indexes: ${response.statusText}`);
  return response.json();
}

// Data skipping indexes (formatted size)
export interface DataSkippingIndex {
  database: string;
  table: string;
  name: string;
  type_full: string;
  size: string;
}

export async function fetchDataSkippingIndexes(
  filters: Record<string, string[]> = {},
  search = ''
): Promise<DataSkippingIndex[]> {
  const params = new URLSearchParams();
  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }
  if (search) {
    params.set('search', search);
  }
  const url = params.toString() ? `${API_BASE}/data-skipping-indexes?${params}` : `${API_BASE}/data-skipping-indexes`;
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch data skipping indexes: ${response.statusText}`);
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

// ==================== USERS & SECURITY API ====================

export async function fetchGrants(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/grants`);
  if (!response.ok) throw new Error(`Failed to fetch grants: ${response.statusText}`);
  return response.json();
}

export async function fetchGrantsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/grants/columns`);
  if (!response.ok) throw new Error(`Failed to fetch grants columns: ${response.statusText}`);
  return response.json();
}

export async function fetchRoles(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/roles`);
  if (!response.ok) throw new Error(`Failed to fetch roles: ${response.statusText}`);
  return response.json();
}

export async function fetchRolesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/roles/columns`);
  if (!response.ok) throw new Error(`Failed to fetch roles columns: ${response.statusText}`);
  return response.json();
}

export async function fetchRoleGrants(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/role-grants`);
  if (!response.ok) throw new Error(`Failed to fetch role grants: ${response.statusText}`);
  return response.json();
}

export async function fetchRoleGrantsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/role-grants/columns`);
  if (!response.ok) throw new Error(`Failed to fetch role grants columns: ${response.statusText}`);
  return response.json();
}

export async function fetchCurrentRoles(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/current-roles`);
  if (!response.ok) throw new Error(`Failed to fetch current roles: ${response.statusText}`);
  return response.json();
}

export async function fetchEnabledRoles(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/enabled-roles`);
  if (!response.ok) throw new Error(`Failed to fetch enabled roles: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotas(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/quotas`);
  if (!response.ok) throw new Error(`Failed to fetch quotas: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotasColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/quotas/columns`);
  if (!response.ok) throw new Error(`Failed to fetch quotas columns: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotaUsage(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/quota-usage`);
  if (!response.ok) throw new Error(`Failed to fetch quota usage: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotaUsageColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/quota-usage/columns`);
  if (!response.ok) throw new Error(`Failed to fetch quota usage columns: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotaLimits(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/quota-limits`);
  if (!response.ok) throw new Error(`Failed to fetch quota limits: ${response.statusText}`);
  return response.json();
}

export async function fetchQuotaLimitsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/quota-limits/columns`);
  if (!response.ok) throw new Error(`Failed to fetch quota limits columns: ${response.statusText}`);
  return response.json();
}

export async function fetchRowPolicies(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/row-policies`);
  if (!response.ok) throw new Error(`Failed to fetch row policies: ${response.statusText}`);
  return response.json();
}

export async function fetchRowPoliciesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/row-policies/columns`);
  if (!response.ok) throw new Error(`Failed to fetch row policies columns: ${response.statusText}`);
  return response.json();
}

export async function fetchSessionLog(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/session-log`);
  if (!response.ok) throw new Error(`Failed to fetch session log: ${response.statusText}`);
  return response.json();
}

export async function fetchSessionLogColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/session-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch session log columns: ${response.statusText}`);
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
  sortField = 'event_time',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  rangeFilters: Record<string, RangeFilter> = {},
  limit = 1000,
  offset = 0,
  bucketSize?: BucketSize
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
    sortField,
    sortOrder,
    limit: limit.toString(),
    offset: offset.toString(),
    eventColumns: eventColumns.join(','),
  });

  if (bucketSize) {
    params.set('bucket', bucketSize);
  }

  if (Object.keys(filters).length > 0) {
    params.set('filters', JSON.stringify(filters));
  }

  if (Object.keys(rangeFilters).length > 0) {
    params.set('rangeFilters', JSON.stringify(rangeFilters));
  }

  if (search) {
    params.set('search', search);
  }

  const response = await fetch(`${API_BASE}/query-log/profile-events?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch profile events: ${response.statusText}`);
  return response.json();
}

// ==================== CLUSTER API ====================

export async function fetchReplicationQueue(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/replication-queue`);
  if (!response.ok) throw new Error(`Failed to fetch replication queue: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicationQueueColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/replication-queue/columns`);
  if (!response.ok) throw new Error(`Failed to fetch replication queue columns: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicationQueueGrouped(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/replication-queue/grouped`);
  if (!response.ok) throw new Error(`Failed to fetch grouped replication queue: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicas(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/replicas`);
  if (!response.ok) throw new Error(`Failed to fetch replicas: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicasColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/replicas/columns`);
  if (!response.ok) throw new Error(`Failed to fetch replicas columns: ${response.statusText}`);
  return response.json();
}

export async function fetchClusters(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/clusters`);
  if (!response.ok) throw new Error(`Failed to fetch clusters: ${response.statusText}`);
  return response.json();
}

export async function fetchClustersColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/clusters/columns`);
  if (!response.ok) throw new Error(`Failed to fetch clusters columns: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicatedFetches(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/fetches`);
  if (!response.ok) throw new Error(`Failed to fetch replicated fetches: ${response.statusText}`);
  return response.json();
}

export async function fetchReplicatedFetchesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/fetches/columns`);
  if (!response.ok) throw new Error(`Failed to fetch replicated fetches columns: ${response.statusText}`);
  return response.json();
}

export async function fetchDistributedDdlQueue(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/distributed-ddl`);
  if (!response.ok) throw new Error(`Failed to fetch distributed DDL queue: ${response.statusText}`);
  return response.json();
}

export async function fetchDistributedDdlQueueColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/distributed-ddl/columns`);
  if (!response.ok) throw new Error(`Failed to fetch distributed DDL queue columns: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeper(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper/columns`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper columns: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperConnection(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-connection`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_connection: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperConnectionColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-connection/columns`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_connection columns: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperConnectionLog(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-connection-log`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_connection_log: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperConnectionLogColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-connection-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_connection_log columns: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperLog(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-log`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_log: ${response.statusText}`);
  return response.json();
}

export async function fetchZookeeperLogColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/cluster/zookeeper-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch zookeeper_log columns: ${response.statusText}`);
  return response.json();
}

// ==================== METRIC LOG API (Dashboard) ====================

export interface MetricLogColumn {
  name: string;
  type: string;
}

export interface MetricLogTimeSeriesPoint {
  time: string;
  [key: string]: string | number;
}

export async function fetchMetricLogColumns(): Promise<MetricLogColumn[]> {
  const response = await fetch(`${API_BASE}/metric-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch metric_log columns: ${response.statusText}`);
  return response.json();
}

export async function fetchMetricLogTimeSeries(
  start: Date,
  end: Date,
  bucket: 'second' | 'minute' | 'hour' = 'minute',
  metrics?: string[]
): Promise<MetricLogTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: start.toISOString().slice(0, 19).replace('T', ' '),
    end: end.toISOString().slice(0, 19).replace('T', ' '),
    bucket,
  });

  if (metrics && metrics.length > 0) {
    params.set('metrics', metrics.join(','));
  }

  const response = await fetch(`${API_BASE}/metric-log/timeseries?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch metric_log timeseries: ${response.statusText}`);
  return response.json();
}

export async function fetchMetricLogDefaults(): Promise<string[]> {
  const response = await fetch(`${API_BASE}/metric-log/defaults`);
  if (!response.ok) throw new Error(`Failed to fetch metric_log defaults: ${response.statusText}`);
  return response.json();
}

// ==================== ASYNC METRIC LOG API (Dashboard) ====================

export interface AsyncMetricLogColumn {
  name: string;
}

export async function fetchAsyncMetricLogColumns(): Promise<AsyncMetricLogColumn[]> {
  const response = await fetch(`${API_BASE}/async-metric-log/columns`);
  if (!response.ok) throw new Error(`Failed to fetch async metric_log columns: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncMetricLogTimeSeries(
  start: Date,
  end: Date,
  bucket: 'second' | 'minute' | 'hour' = 'minute',
  metrics?: string[]
): Promise<MetricLogTimeSeriesPoint[]> {
  const params = new URLSearchParams({
    start: start.toISOString().slice(0, 19).replace('T', ' '),
    end: end.toISOString().slice(0, 19).replace('T', ' '),
    bucket,
  });

  if (metrics && metrics.length > 0) {
    params.set('metrics', metrics.join(','));
  }

  const response = await fetch(`${API_BASE}/async-metric-log/timeseries?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch async metric_log timeseries: ${response.statusText}`);
  return response.json();
}

export async function fetchAsyncMetricLogDefaults(): Promise<string[]> {
  const response = await fetch(`${API_BASE}/async-metric-log/defaults`);
  if (!response.ok) throw new Error(`Failed to fetch async metric_log defaults: ${response.statusText}`);
  return response.json();
}
