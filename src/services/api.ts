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

// ==================== PART LOG API ====================

export async function fetchPartLog(
  timeRange: TimeRange,
  sortField: string,
  sortOrder: 'ASC' | 'DESC',
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

  const response = await fetch(`${API_BASE}/part-log?${params}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch part log: ${response.statusText}`);
  }
  return response.json();
}

export async function fetchPartLogCount(timeRange: TimeRange): Promise<number> {
  const params = new URLSearchParams({
    start: formatDateTime(timeRange.start),
    end: formatDateTime(timeRange.end),
  });

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

// ==================== SYSTEM PARTS API ====================

export async function fetchParts(
  sortField = 'modification_time',
  sortOrder: 'ASC' | 'DESC' = 'DESC',
  limit = 1000
): Promise<Record<string, unknown>[]> {
  const params = new URLSearchParams({ sortField, sortOrder, limit: limit.toString() });
  const response = await fetch(`${API_BASE}/parts?${params}`);
  if (!response.ok) throw new Error(`Failed to fetch parts: ${response.statusText}`);
  return response.json();
}

export async function fetchPartsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/parts/columns`);
  if (!response.ok) throw new Error(`Failed to fetch parts columns: ${response.statusText}`);
  return response.json();
}

// ==================== ACTIVITY API ====================

export async function fetchProcesses(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/processes`);
  if (!response.ok) throw new Error(`Failed to fetch processes: ${response.statusText}`);
  return response.json();
}

export async function fetchProcessesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/processes/columns`);
  if (!response.ok) throw new Error(`Failed to fetch processes columns: ${response.statusText}`);
  return response.json();
}

export async function fetchMerges(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/merges`);
  if (!response.ok) throw new Error(`Failed to fetch merges: ${response.statusText}`);
  return response.json();
}

export async function fetchMergesColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/merges/columns`);
  if (!response.ok) throw new Error(`Failed to fetch merges columns: ${response.statusText}`);
  return response.json();
}

export async function fetchMutations(): Promise<Record<string, unknown>[]> {
  const response = await fetch(`${API_BASE}/mutations`);
  if (!response.ok) throw new Error(`Failed to fetch mutations: ${response.statusText}`);
  return response.json();
}

export async function fetchMutationsColumns(): Promise<ColumnMetadata[]> {
  const response = await fetch(`${API_BASE}/mutations/columns`);
  if (!response.ok) throw new Error(`Failed to fetch mutations columns: ${response.statusText}`);
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
