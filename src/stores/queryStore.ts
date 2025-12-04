import { create } from 'zustand';
import type { QueryLogEntry, PartLogEntry, TimeRange, BucketSize, ColumnConfig } from '../types/queryLog';
import { FALLBACK_COLUMNS, FALLBACK_PART_LOG_COLUMNS } from '../types/queryLog';

export interface RangeFilter {
  min?: number;
  max?: number;
}

export type ChartMetric = 'count' | 'duration' | 'memory';

export interface TimeSeriesPoint {
  time: string;
  count: number;
  avg_duration?: number;
  max_duration?: number;
  avg_memory?: number;
  max_memory?: number;
}

interface QueryState {
  // Data
  entries: QueryLogEntry[];
  timeSeries: TimeSeriesPoint[];
  totalCount: number;
  loading: boolean;
  error: string | null;

  // Part Log Data
  partLogEntries: PartLogEntry[];
  partLogTotalCount: number;
  partLogLoading: boolean;

  // Chart
  chartMetric: ChartMetric;

  // Filters
  timeRange: TimeRange;
  bucketSize: BucketSize;
  search: string;
  fieldFilters: Record<string, string[]>;
  rangeFilters: Record<string, RangeFilter>;

  // Table config
  columns: ColumnConfig[];
  columnsLoaded: boolean;
  sortField: string;
  sortOrder: 'ASC' | 'DESC';

  // Part Log Table config
  partLogColumns: ColumnConfig[];
  partLogColumnsLoaded: boolean;
  partLogSortField: string;
  partLogSortOrder: 'ASC' | 'DESC';

  // UI state
  selectedEntry: QueryLogEntry | null;
  activeTab: 'queries' | 'histograms';

  // Actions
  setEntries: (entries: QueryLogEntry[]) => void;
  setTimeSeries: (data: TimeSeriesPoint[]) => void;
  setChartMetric: (metric: ChartMetric) => void;
  setTotalCount: (count: number) => void;
  setLoading: (loading: boolean) => void;
  setError: (error: string | null) => void;
  setTimeRange: (range: TimeRange) => void;
  setBucketSize: (size: BucketSize) => void;
  setSearch: (search: string) => void;
  setFieldFilter: (field: string, values: string[]) => void;
  setRangeFilter: (field: string, filter: RangeFilter) => void;
  clearFieldFilter: (field: string) => void;
  clearRangeFilter: (field: string) => void;
  clearAllFilters: () => void;
  setColumns: (columns: ColumnConfig[]) => void;
  toggleColumnVisibility: (field: string) => void;
  setSortField: (field: string) => void;
  setSortOrder: (order: 'ASC' | 'DESC') => void;
  setSelectedEntry: (entry: QueryLogEntry | null) => void;
  setActiveTab: (tab: 'queries' | 'histograms') => void;
  // Part Log Actions
  setPartLogEntries: (entries: PartLogEntry[]) => void;
  setPartLogTotalCount: (count: number) => void;
  setPartLogLoading: (loading: boolean) => void;
  setPartLogColumns: (columns: ColumnConfig[]) => void;
  togglePartLogColumnVisibility: (field: string) => void;
  setPartLogSortField: (field: string) => void;
  setPartLogSortOrder: (order: 'ASC' | 'DESC') => void;
}

const now = new Date();
const oneHourAgo = new Date(now.getTime() - 60 * 60 * 1000);

export const useQueryStore = create<QueryState>((set) => ({
  entries: [],
  timeSeries: [],
  totalCount: 0,
  loading: false,
  error: null,

  // Part Log Data
  partLogEntries: [],
  partLogTotalCount: 0,
  partLogLoading: false,

  chartMetric: 'count',

  timeRange: { start: oneHourAgo, end: now },
  bucketSize: 'minute',
  search: '',
  fieldFilters: {},
  rangeFilters: {},

  columns: FALLBACK_COLUMNS,
  columnsLoaded: false,
  sortField: 'event_time',
  sortOrder: 'DESC',

  // Part Log Table config
  partLogColumns: FALLBACK_PART_LOG_COLUMNS,
  partLogColumnsLoaded: false,
  partLogSortField: 'event_time',
  partLogSortOrder: 'DESC',

  selectedEntry: null,
  activeTab: 'queries',

  setEntries: (entries) => set({ entries }),
  setTimeSeries: (timeSeries) => set({ timeSeries }),
  setChartMetric: (chartMetric) => set({ chartMetric }),
  setTotalCount: (totalCount) => set({ totalCount }),
  setLoading: (loading) => set({ loading }),
  setError: (error) => set({ error }),
  setTimeRange: (timeRange) => set({ timeRange }),
  setBucketSize: (bucketSize) => set({ bucketSize }),
  setSearch: (search) => set({ search }),
  setFieldFilter: (field, values) =>
    set((state) => ({
      fieldFilters: { ...state.fieldFilters, [field]: values },
    })),
  setRangeFilter: (field, filter) =>
    set((state) => ({
      rangeFilters: { ...state.rangeFilters, [field]: filter },
    })),
  clearFieldFilter: (field) =>
    set((state) => {
      const { [field]: _, ...rest } = state.fieldFilters;
      return { fieldFilters: rest };
    }),
  clearRangeFilter: (field) =>
    set((state) => {
      const { [field]: _, ...rest } = state.rangeFilters;
      return { rangeFilters: rest };
    }),
  clearAllFilters: () => set({ fieldFilters: {}, rangeFilters: {}, search: '' }),
  setColumns: (columns) => {
    console.log('Store setColumns called with', columns.length, 'columns');
    set({ columns, columnsLoaded: true });
  },
  toggleColumnVisibility: (field) =>
    set((state) => ({
      columns: state.columns.map((col) =>
        col.field === field ? { ...col, visible: !col.visible } : col
      ),
    })),
  setSortField: (sortField) => set({ sortField }),
  setSortOrder: (sortOrder) => set({ sortOrder }),
  setSelectedEntry: (selectedEntry) => set({ selectedEntry }),
  setActiveTab: (activeTab) => set({ activeTab }),
  // Part Log Actions
  setPartLogEntries: (partLogEntries) => set({ partLogEntries }),
  setPartLogTotalCount: (partLogTotalCount) => set({ partLogTotalCount }),
  setPartLogLoading: (partLogLoading) => set({ partLogLoading }),
  setPartLogColumns: (partLogColumns) => set({ partLogColumns, partLogColumnsLoaded: true }),
  togglePartLogColumnVisibility: (field) =>
    set((state) => ({
      partLogColumns: state.partLogColumns.map((col) =>
        col.field === field ? { ...col, visible: !col.visible } : col
      ),
    })),
  setPartLogSortField: (partLogSortField) => set({ partLogSortField }),
  setPartLogSortOrder: (partLogSortOrder) => set({ partLogSortOrder }),
}));
