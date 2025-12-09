import { useEffect, useState, useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, SortChangedEvent, ICellRendererParams, CellStyle } from 'ag-grid-community';
import { Loader2, Copy, Check, X, Eye, ExternalLink, List } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import { fetchGroupedQueryLog, type GroupedQueryEntry } from '../services/api';

// Register AG Grid Community modules
ModuleRegistry.registerModules([AllCommunityModule]);

// Create dark theme with JetBrains Mono for cells, lighter weight
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

function formatDuration(ms: number): string {
  if (ms == null) return 'N/A';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}m`;
}

function formatBytes(bytes: number): string {
  if (bytes == null) return 'N/A';
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num == null) return 'N/A';
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

function formatDateTime(dateStr: string): string {
  if (!dateStr) return 'N/A';
  const date = new Date(dateStr);
  if (isNaN(date.getTime())) return 'N/A';
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation();
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      onClick={handleCopy}
      className="p-1 hover:bg-gray-600 rounded text-gray-400 hover:text-white"
      title="Copy query"
    >
      {copied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
    </button>
  );
}

export function GroupedQueriesTable() {
  const {
    timeRange,
    search,
    fieldFilters,
    rangeFilters,
    groupedEntries,
    groupedLoading,
    groupedSortField,
    groupedSortOrder,
    setGroupedEntries,
    setGroupedLoading,
    setGroupedSortField,
    setGroupedSortOrder,
    setSearch,
    setActiveTab,
  } = useQueryStore();

  const [selectedEntry, setSelectedEntry] = useState<GroupedQueryEntry | null>(null);

  const ActionCellRenderer = useCallback((params: ICellRendererParams<GroupedQueryEntry>) => {
    return (
      <button
        onClick={(e) => {
          e.stopPropagation();
          setSelectedEntry(params.data!);
        }}
        className="p-1 hover:bg-gray-600 rounded"
        title="View Details"
      >
        <Eye className="w-3.5 h-3.5 text-gray-400 hover:text-white" />
      </button>
    );
  }, []);

  // Fetch grouped data
  useEffect(() => {
    setGroupedLoading(true);
    fetchGroupedQueryLog(
      timeRange,
      search,
      groupedSortField,
      groupedSortOrder,
      fieldFilters,
      rangeFilters,
      1000
    )
      .then(setGroupedEntries)
      .catch(console.error)
      .finally(() => setGroupedLoading(false));
  }, [timeRange, search, fieldFilters, rangeFilters, groupedSortField, groupedSortOrder, setGroupedEntries, setGroupedLoading]);

  // Column definitions for AG Grid
  const columnDefs: ColDef<GroupedQueryEntry>[] = useMemo(() => [
    {
      headerName: '',
      field: 'example_query' as const,
      width: 44,
      sortable: false,
      cellRenderer: ActionCellRenderer,
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center', padding: 0 } as CellStyle,
    },
    {
      headerName: 'Last Seen',
      field: 'last_seen',
      width: 150,
      sortable: true,
      valueFormatter: (params) => formatDateTime(params.value),
    },
    {
      headerName: 'Query',
      field: 'example_query',
      minWidth: 300,
      flex: 1,
      sortable: false,
      tooltipField: 'example_query',
      valueFormatter: (params) => {
        const q = params.value as string;
        return q?.length > 100 ? q.substring(0, 100) + '...' : q;
      },
      cellStyle: { color: '#60a5fa' } as CellStyle,
    },
    {
      headerName: 'Count',
      field: 'count',
      width: 80,
      sortable: true,
      valueFormatter: (params) => formatNumber(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Avg Duration',
      field: 'avg_duration',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatDuration(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Max Duration',
      field: 'max_duration',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatDuration(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Total Duration',
      field: 'total_duration',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatDuration(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Avg Memory',
      field: 'avg_memory',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Max Memory',
      field: 'max_memory',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Avg Rows Read',
      field: 'avg_read_rows',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatNumber(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Avg Rows Written',
      field: 'avg_written_rows',
      width: 110,
      sortable: true,
      valueFormatter: (params) => formatNumber(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'Total Read',
      field: 'total_read_bytes',
      width: 100,
      sortable: true,
      valueFormatter: (params) => formatBytes(params.value),
      cellStyle: { textAlign: 'right', color: '#86efac' },
    },
    {
      headerName: 'User',
      field: 'user',
      width: 100,
      sortable: false,
    },
    {
      headerName: 'First Seen',
      field: 'first_seen',
      width: 130,
      sortable: true,
      valueFormatter: (params) => formatDateTime(params.value),
    },
  ], [ActionCellRenderer]);

  const defaultColDef = useMemo(() => ({
    resizable: true,
  }), []);

  const onSortChanged = useCallback((event: SortChangedEvent) => {
    const sortModel = event.api.getColumnState().find(c => c.sort);
    if (sortModel) {
      setGroupedSortField(sortModel.colId);
      setGroupedSortOrder(sortModel.sort === 'asc' ? 'ASC' : 'DESC');
    }
  }, [setGroupedSortField, setGroupedSortOrder]);


  if (groupedLoading) {
    return (
      <div className="h-full flex items-center justify-center">
        <Loader2 className="w-8 h-8 text-blue-500 animate-spin" />
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col">
      <div className="flex-1 min-h-0">
        <AgGridReact
          theme={darkTheme}
          rowData={groupedEntries}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          onSortChanged={onSortChanged}
          getRowId={(params) => params.data.example_query}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
        />
      </div>

      {/* Modal popup for grouped query details */}
      {selectedEntry && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={() => setSelectedEntry(null)}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[900px] max-h-[85vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Grouped Query Details</h2>
                <p className="text-xs text-gray-400">
                  {formatNumber(selectedEntry.count)} executions
                </p>
              </div>
              <button onClick={() => setSelectedEntry(null)} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {/* Query */}
              <div className="mb-4">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    <h3 className="text-xs font-semibold text-gray-400">Example Query</h3>
                    <CopyButton text={selectedEntry.example_query} />
                  </div>
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => {
                        // Extract first 50 chars of query for search (enough to be unique)
                        const searchTerm = selectedEntry.example_query.substring(0, 80).trim();
                        setSearch(searchTerm);
                        setActiveTab('queries');
                        setSelectedEntry(null);
                      }}
                      className="flex items-center gap-1 px-2 py-1 text-xs bg-gray-600 hover:bg-gray-500 rounded text-white"
                    >
                      <List className="w-3 h-3" />
                      List All
                    </button>
                    <button
                      onClick={() => {
                        const openQueryEditor = (window as unknown as { openQueryEditor?: (query: string) => void }).openQueryEditor;
                        if (openQueryEditor) {
                          openQueryEditor(selectedEntry.example_query);
                          setSelectedEntry(null);
                        }
                      }}
                      className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-500 rounded text-white"
                    >
                      <ExternalLink className="w-3 h-3" />
                      Analyse Query
                    </button>
                  </div>
                </div>
                <pre className="bg-gray-800 p-3 rounded text-xs text-gray-300 max-h-48 overflow-y-auto whitespace-pre-wrap break-words font-mono">
                  {selectedEntry.example_query}
                </pre>
              </div>

              {/* Summary Stats */}
              <div className="grid grid-cols-5 gap-2 mb-4">
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Count</div>
                  <div className="text-sm font-semibold text-blue-400">{formatNumber(selectedEntry.count)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">User</div>
                  <div className="text-sm font-semibold text-white">{selectedEntry.user || 'N/A'}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Database</div>
                  <div className="text-sm font-semibold text-white">{selectedEntry.current_database || 'N/A'}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">First Seen</div>
                  <div className="text-sm font-semibold text-white">{formatDateTime(selectedEntry.first_seen)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Last Seen</div>
                  <div className="text-sm font-semibold text-white">{formatDateTime(selectedEntry.last_seen)}</div>
                </div>
              </div>

              {/* Metrics Table with Min/Avg/Max/Total columns */}
              <div className="bg-gray-800 rounded mb-4 overflow-hidden">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-700 bg-gray-800/50">
                      <th className="text-left p-2 text-gray-400 font-semibold">Metric</th>
                      <th className="text-right p-2 text-green-400 font-semibold">Min</th>
                      <th className="text-right p-2 text-blue-400 font-semibold">Avg</th>
                      <th className="text-right p-2 text-orange-400 font-semibold">Max</th>
                      <th className="text-right p-2 text-purple-400 font-semibold">Total</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr className="border-b border-gray-700/50">
                      <td className="p-2 text-gray-300 font-medium">Duration</td>
                      <td className="p-2 text-right text-green-400">{formatDuration(selectedEntry.min_duration)}</td>
                      <td className="p-2 text-right text-blue-400">{formatDuration(selectedEntry.avg_duration)}</td>
                      <td className="p-2 text-right text-orange-400">{formatDuration(selectedEntry.max_duration)}</td>
                      <td className="p-2 text-right text-purple-400">{formatDuration(selectedEntry.total_duration)}</td>
                    </tr>
                    <tr className="border-b border-gray-700/50">
                      <td className="p-2 text-gray-300 font-medium">Memory</td>
                      <td className="p-2 text-right text-green-400">-</td>
                      <td className="p-2 text-right text-blue-400">{formatBytes(selectedEntry.avg_memory)}</td>
                      <td className="p-2 text-right text-orange-400">{formatBytes(selectedEntry.max_memory)}</td>
                      <td className="p-2 text-right text-purple-400">{formatBytes(selectedEntry.total_memory)}</td>
                    </tr>
                    <tr className="border-b border-gray-700/50">
                      <td className="p-2 text-gray-300 font-medium">Read Rows</td>
                      <td className="p-2 text-right text-green-400">-</td>
                      <td className="p-2 text-right text-blue-400">{formatNumber(selectedEntry.avg_read_rows)}</td>
                      <td className="p-2 text-right text-orange-400">-</td>
                      <td className="p-2 text-right text-purple-400">{formatNumber(selectedEntry.total_read_rows)}</td>
                    </tr>
                    <tr className="border-b border-gray-700/50">
                      <td className="p-2 text-gray-300 font-medium">Read Bytes</td>
                      <td className="p-2 text-right text-green-400">-</td>
                      <td className="p-2 text-right text-blue-400">-</td>
                      <td className="p-2 text-right text-orange-400">-</td>
                      <td className="p-2 text-right text-purple-400">{formatBytes(selectedEntry.total_read_bytes)}</td>
                    </tr>
                    <tr>
                      <td className="p-2 text-gray-300 font-medium">Result Rows</td>
                      <td className="p-2 text-right text-green-400">-</td>
                      <td className="p-2 text-right text-blue-400">{formatNumber(selectedEntry.avg_result_rows)}</td>
                      <td className="p-2 text-right text-orange-400">-</td>
                      <td className="p-2 text-right text-purple-400">{formatNumber(selectedEntry.total_result_rows)}</td>
                    </tr>
                  </tbody>
                </table>
              </div>


              {/* Note about profile events */}
              <div className="bg-gray-800/50 border border-gray-700 rounded p-3 text-xs text-gray-400">
                <span className="font-semibold">Note:</span> ProfileEvents and Settings are not available for grouped queries as they are aggregated data.
                Click on a specific query in the Queries tab to see detailed ProfileEvents.
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
