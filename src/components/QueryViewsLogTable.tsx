import { useState, useEffect, useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, SortChangedEvent } from 'ag-grid-community';
import { Eye, X } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import { fetchQueryViewsLog, fetchQueryViewsLogCount, type QueryViewsLogEntry } from '../services/api';

ModuleRegistry.registerModules([AllCommunityModule]);

const darkTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#9ca3af',
  headerTextColor: '#f3f4f6',
  fontFamily: '"JetBrains Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
  fontSize: 9,
  headerFontSize: 11,
  headerFontWeight: 600,
  cellTextColor: '#9ca3af',
  rowHeight: 26,
  headerHeight: 30,
});

function formatBytes(bytes: number): string {
  if (bytes === null || bytes === undefined || bytes === 0) return '-';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num === null || num === undefined) return '-';
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

function formatDuration(ms: number): string {
  if (ms === null || ms === undefined) return '-';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's';
  return ms.toFixed(0) + 'ms';
}

function formatDateTime(date: string): string {
  if (!date) return '-';
  const d = new Date(date);
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

function numericComparator(valueA: unknown, valueB: unknown): number {
  const a = valueA === null || valueA === undefined ? null : Number(valueA);
  const b = valueB === null || valueB === undefined ? null : Number(valueB);
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  if (isNaN(a) && isNaN(b)) return 0;
  if (isNaN(a)) return 1;
  if (isNaN(b)) return -1;
  return a - b;
}

export function QueryViewsLogTable() {
  const { timeRange, search, fieldFilters, rangeFilters, globalRefreshTrigger } = useQueryStore();
  const [data, setData] = useState<QueryViewsLogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [sortField, setSortField] = useState('event_time');
  const [sortOrder, setSortOrder] = useState<'ASC' | 'DESC'>('DESC');

  // Modal state
  const [selectedEntry, setSelectedEntry] = useState<QueryViewsLogEntry | null>(null);

  // Get latest time range from store
  const getLatestTimeRange = useCallback(() => {
    return useQueryStore.getState().timeRange;
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const currentTimeRange = getLatestTimeRange();
      const [entries, count] = await Promise.all([
        fetchQueryViewsLog(currentTimeRange, sortField, sortOrder, fieldFilters, search),
        fetchQueryViewsLogCount(currentTimeRange, fieldFilters, search),
      ]);
      setData(entries);
      setTotalCount(count);
    } catch (error) {
      console.error('Failed to fetch query views log:', error);
      setData([]);
      setTotalCount(0);
    } finally {
      setLoading(false);
    }
  }, [sortField, sortOrder, fieldFilters, search, getLatestTimeRange]);

  useEffect(() => {
    loadData();
  }, [loadData, globalRefreshTrigger]);

  const handleSortChanged = useCallback((event: SortChangedEvent) => {
    const columnState = event.api.getColumnState();
    const sortedColumn = columnState.find(col => col.sort);
    if (sortedColumn) {
      setSortField(sortedColumn.colId);
      setSortOrder(sortedColumn.sort === 'asc' ? 'ASC' : 'DESC');
    }
  }, []);

  const handleCloseModal = useCallback(() => {
    setSelectedEntry(null);
  }, []);

  // Escape key handler
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && selectedEntry) {
        handleCloseModal();
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [selectedEntry, handleCloseModal]);

  const columnDefs = useMemo((): ColDef<QueryViewsLogEntry>[] => [
    {
      headerName: '',
      field: 'view_name',
      width: 40,
      sortable: false,
      cellRenderer: (params: { data: QueryViewsLogEntry }) => (
        <button
          onClick={() => setSelectedEntry(params.data)}
          className="flex items-center justify-center w-full h-full hover:bg-gray-700/50 transition-colors"
          title="View details"
        >
          <Eye className="w-3.5 h-3.5 text-gray-400 hover:text-blue-400" />
        </button>
      ),
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 0 },
    },
    {
      headerName: 'Time',
      field: 'event_time',
      width: 140,
      sortable: true,
      sort: 'desc',
      valueFormatter: (params) => formatDateTime(params.value),
      cellStyle: { color: '#fca5a5' },
    },
    {
      headerName: 'View Name',
      field: 'view_name',
      minWidth: 200,
      flex: 1,
      sortable: true,
      cellStyle: { color: '#93c5fd' },
    },
    {
      headerName: 'Type',
      field: 'view_type',
      width: 120,
      sortable: true,
      cellStyle: { color: '#fde047' },
    },
    {
      headerName: 'Status',
      field: 'status',
      width: 100,
      sortable: true,
      cellStyle: (params) => ({
        color: params.value === 'QueryFinish' ? '#86efac' : '#f87171',
      }),
    },
    {
      headerName: 'Duration',
      field: 'view_duration_ms',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatDuration(params.value),
      cellStyle: { textAlign: 'right', color: '#c4b5fd' },
      comparator: numericComparator,
    },
    {
      headerName: 'Read Rows',
      field: 'read_rows',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatNumber(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
      comparator: numericComparator,
    },
    {
      headerName: 'Written Rows',
      field: 'written_rows',
      width: 110,
      sortable: true,
      valueFormatter: (params) => formatNumber(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
      comparator: numericComparator,
    },
    {
      headerName: 'Read Bytes',
      field: 'read_bytes',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
      comparator: numericComparator,
    },
    {
      headerName: 'Written Bytes',
      field: 'written_bytes',
      width: 110,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
      comparator: numericComparator,
    },
    {
      headerName: 'Memory',
      field: 'peak_memory_usage',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#fbbf24' },
      comparator: numericComparator,
    },
  ], []);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
    sortingOrder: ['desc', 'asc'] as const,
  }), []);

  return (
    <div className="h-full flex flex-col">
      {/* Stats bar */}
      <div className="flex items-center gap-4 mb-2 text-xs">
        <span className="text-gray-400">
          Total: <span className="text-white font-medium">{totalCount.toLocaleString()}</span> view executions
        </span>
      </div>

      {/* Table */}
      <div className="flex-1 bg-gray-900 border border-gray-700 rounded overflow-hidden">
        <AgGridReact<QueryViewsLogEntry>
          theme={darkTheme}
          rowData={data}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          onSortChanged={handleSortChanged}
          getRowId={(params) => `${params.data.event_time}-${params.data.view_name}-${params.data.initial_query_id}`}
        />
      </div>

      {/* Detail Modal */}
      {selectedEntry && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[900px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">View Execution Details</h2>
                <p className="text-xs text-gray-400 font-mono">{selectedEntry.view_name}</p>
              </div>
              <button onClick={handleCloseModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {/* Stats Grid */}
              <div className="grid grid-cols-4 gap-2 mb-4">
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Type</div>
                  <div className="text-sm font-semibold text-yellow-300">{selectedEntry.view_type}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Status</div>
                  <div className={`text-sm font-semibold ${selectedEntry.status === 'QueryFinish' ? 'text-green-300' : 'text-red-300'}`}>
                    {selectedEntry.status}
                  </div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Duration</div>
                  <div className="text-sm font-semibold text-purple-300">{formatDuration(selectedEntry.view_duration_ms)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Peak Memory</div>
                  <div className="text-sm font-semibold text-yellow-300">{formatBytes(selectedEntry.peak_memory_usage)}</div>
                </div>
              </div>

              <div className="grid grid-cols-4 gap-2 mb-4">
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Read Rows</div>
                  <div className="text-sm font-semibold text-green-300">{formatNumber(selectedEntry.read_rows)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Written Rows</div>
                  <div className="text-sm font-semibold text-green-300">{formatNumber(selectedEntry.written_rows)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Read Bytes</div>
                  <div className="text-sm font-semibold text-green-300">{formatBytes(selectedEntry.read_bytes)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Written Bytes</div>
                  <div className="text-sm font-semibold text-green-300">{formatBytes(selectedEntry.written_bytes)}</div>
                </div>
              </div>

              {/* View Target */}
              {selectedEntry.view_target && (
                <div className="bg-gray-800 p-2 rounded mb-4">
                  <div className="text-xs text-gray-400 mb-1">Target Table</div>
                  <div className="text-sm text-blue-300 font-mono">{selectedEntry.view_target}</div>
                </div>
              )}

              {/* View Query */}
              {selectedEntry.view_query && (
                <div className="bg-gray-800 p-2 rounded mb-4">
                  <div className="text-xs text-gray-400 mb-1">View Query</div>
                  <pre className="text-xs text-gray-300 font-mono overflow-x-auto whitespace-pre-wrap max-h-40 overflow-y-auto">
                    {selectedEntry.view_query}
                  </pre>
                </div>
              )}

              {/* Exception */}
              {selectedEntry.exception && (
                <div className="bg-gray-800 p-2 rounded mb-4 border border-red-700/50">
                  <div className="text-xs text-red-400 mb-1">Exception</div>
                  <pre className="text-xs text-red-300 font-mono overflow-x-auto whitespace-pre-wrap max-h-32 overflow-y-auto">
                    {selectedEntry.exception}
                  </pre>
                </div>
              )}

              {/* Query ID */}
              <div className="bg-gray-800 p-2 rounded">
                <div className="text-xs text-gray-400 mb-1">Initial Query ID</div>
                <div className="text-xs text-gray-300 font-mono">{selectedEntry.initial_query_id || '-'}</div>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
