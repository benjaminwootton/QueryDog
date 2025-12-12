import { useState, useEffect, useCallback, useMemo } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams } from 'ag-grid-community';
import { Play, RotateCcw, Loader2, Eye, X, Copy, Check, Search, PlayCircle, ExternalLink } from 'lucide-react';

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
  rowHeight: 32,
  headerHeight: 30,
});

interface MyQuery {
  filename: string;
  query: string;
  lastRunTime: string | null;
  lastDuration: number | null;
  lastRowCount: number | null;
  avgRunTime: number | null;
  slowestRunTime: number | null;
  fastestRunTime: number | null;
  runCount: number;
}

function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '-';
  if (ms >= 1000) return (ms / 1000).toFixed(2) + 's';
  return ms.toFixed(0) + 'ms';
}

function formatDateTime(isoString: string | null): string {
  if (!isoString) return '-';
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

export function MyQueriesPage() {
  const [queries, setQueries] = useState<MyQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningQuery, setRunningQuery] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<{ filename: string; rowCount: number; duration: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedQuery, setSelectedQuery] = useState<MyQuery | null>(null);
  const [queryCopied, setQueryCopied] = useState(false);
  const [localSearch, setLocalSearch] = useState('');
  const [search, setSearch] = useState('');
  const [loadMessage, setLoadMessage] = useState<string | null>(null);
  const [runningAll, setRunningAll] = useState(false);
  const [runAllProgress, setRunAllProgress] = useState<{ current: number; total: number } | null>(null);

  // Close modal on Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && selectedQuery) {
        setSelectedQuery(null);
        setQueryCopied(false);
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [selectedQuery]);

  const fetchQueries = useCallback(async (showLoadMessage = false) => {
    setLoading(true);
    try {
      const res = await fetch('/api/my-queries');
      if (!res.ok) throw new Error('Failed to fetch queries');
      const data = await res.json();
      setQueries(data);
      setError(null);
      if (showLoadMessage) {
        setLoadMessage(`${data.length} ${data.length === 1 ? 'query' : 'queries'} loaded from ./queries folder`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
      setLoadMessage(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchQueries(true); // Show load message on initial load
  }, [fetchQueries]);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      setSearch(localSearch);
    }, 300);
    return () => clearTimeout(timer);
  }, [localSearch]);

  // Filter queries based on search
  const filteredQueries = useMemo(() => {
    if (!search) return queries;
    const searchLower = search.toLowerCase();
    return queries.filter(q =>
      q.filename.toLowerCase().includes(searchLower) ||
      q.query.toLowerCase().includes(searchLower)
    );
  }, [queries, search]);

  const runQuery = useCallback(async (query: MyQuery, skipRefresh = false) => {
    setRunningQuery(query.filename);
    if (!skipRefresh) {
      setLastResult(null);
      setError(null);
    }
    try {
      const res = await fetch('/api/my-queries/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: query.filename, query: query.query }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to run query');

      // Update just the affected row instead of refetching all data
      if (data.stats) {
        setQueries(prev => prev.map(q =>
          q.filename === query.filename
            ? { ...q, ...data.stats }
            : q
        ));
      }

      if (!skipRefresh) {
        setLastResult({
          filename: query.filename,
          rowCount: data.rowCount,
          duration: data.duration,
        });
      }
      return { success: true, duration: data.duration, rowCount: data.rowCount };
    } catch (err) {
      if (!skipRefresh) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      }
      return { success: false, error: err instanceof Error ? err.message : 'Unknown error' };
    } finally {
      setRunningQuery(null);
    }
  }, []);

  const resetStats = useCallback(async () => {
    try {
      const res = await fetch('/api/my-queries/stats', { method: 'DELETE' });
      if (!res.ok) throw new Error('Failed to reset stats');
      setLastResult(null);
      setLoadMessage('All run statistics have been reset');
      await fetchQueries();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    }
  }, [fetchQueries]);

  const runAllQueries = useCallback(async () => {
    if (runningAll || filteredQueries.length === 0) return;
    setRunningAll(true);
    setError(null);
    setLastResult(null);
    setLoadMessage(null);
    const total = filteredQueries.length;
    let successCount = 0;
    let failCount = 0;

    for (let i = 0; i < filteredQueries.length; i++) {
      setRunAllProgress({ current: i + 1, total });
      const result = await runQuery(filteredQueries[i], true);
      if (result.success) {
        successCount++;
      } else {
        failCount++;
      }
    }

    setRunAllProgress(null);
    setRunningAll(false);
    // No need to fetchQueries - each runQuery already updates its row
    setLoadMessage(`Completed: ${successCount} succeeded, ${failCount} failed out of ${total} queries`);
  }, [runningAll, filteredQueries, runQuery]);

  const PlayButtonRenderer = useCallback((params: ICellRendererParams<MyQuery>) => {
    const isRunning = runningQuery === params.data?.filename;
    return (
      <button
        onClick={() => params.data && runQuery(params.data)}
        disabled={isRunning || runningQuery !== null}
        className="p-1.5 hover:bg-blue-600 bg-blue-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
        title="Run query"
      >
        {isRunning ? (
          <Loader2 className="w-3.5 h-3.5 text-white animate-spin" />
        ) : (
          <Play className="w-3.5 h-3.5 text-white" />
        )}
      </button>
    );
  }, [runningQuery, runQuery]);

  const EyeButtonRenderer = useCallback((params: ICellRendererParams<MyQuery>) => {
    return (
      <button
        onClick={() => params.data && setSelectedQuery(params.data)}
        className="p-1.5 hover:bg-gray-700 rounded text-gray-300 hover:text-blue-400 transition-colors"
        title="View query details"
      >
        <Eye className="w-4 h-4" />
      </button>
    );
  }, []);

  const handleCopyQuery = useCallback(async () => {
    if (!selectedQuery) return;
    await navigator.clipboard.writeText(selectedQuery.query);
    setQueryCopied(true);
    setTimeout(() => setQueryCopied(false), 2000);
  }, [selectedQuery]);

  const handleCloseModal = useCallback(() => {
    setSelectedQuery(null);
    setQueryCopied(false);
  }, []);

  const handleAnalyseQuery = useCallback(() => {
    if (!selectedQuery) return;
    // Use the global function exposed by App.tsx to open the query editor
    const openQueryEditor = (window as unknown as { openQueryEditor?: (query: string) => void }).openQueryEditor;
    if (openQueryEditor) {
      openQueryEditor(selectedQuery.query);
      handleCloseModal();
    }
  }, [selectedQuery, handleCloseModal]);

  const columnDefs = useMemo((): ColDef<MyQuery>[] => {
    const defs: ColDef<MyQuery>[] = [
      {
        headerName: '',
        field: 'filename',
        width: 50,
        sortable: false,
        cellRenderer: EyeButtonRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
      {
        headerName: 'Filename',
        field: 'filename',
        width: 350,
        sortable: true,
        cellStyle: { color: '#93c5fd' },
      },
      {
        headerName: 'Query',
        field: 'query',
        flex: 1,
        minWidth: 200,
        sortable: false,
        cellStyle: { color: '#93c5fd' },
        tooltipValueGetter: (params) => {
          const q = params.value as string;
          if (!q) return '';
          // Format SQL with newlines after keywords and limit to 1000 chars
          const formatted = q
            .replace(/\s+/g, ' ')
            .replace(/(SELECT|FROM|WHERE|AND|OR|JOIN|LEFT|RIGHT|INNER|OUTER|GROUP BY|ORDER BY|LIMIT|HAVING|UNION|INSERT|UPDATE|DELETE|SET|INTO|VALUES)/gi, '\n$1')
            .trim();
          const maxLen = 1000;
          return formatted.length > maxLen ? formatted.substring(0, maxLen) + '\n...(truncated)' : formatted;
        },
        valueFormatter: (params) => {
          const q = params.value as string;
          return q?.length > 110 ? q.substring(0, 110) + '...' : q;
        },
      },
      {
        headerName: '',
        field: 'filename',
        width: 50,
        sortable: false,
        cellRenderer: PlayButtonRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
      {
        headerName: 'Last Run',
        field: 'lastRunTime',
        width: 140,
        sortable: true,
        valueFormatter: (params) => formatDateTime(params.value),
        cellStyle: { color: '#fca5a5' },
      },
      {
        headerName: 'Last Time',
        field: 'lastDuration',
        width: 90,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Last Rows',
        field: 'lastRowCount',
        width: 90,
        sortable: true,
        valueFormatter: (params) => params.value != null ? params.value.toLocaleString() : '-',
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Avg Time',
        field: 'avgRunTime',
        width: 90,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Fastest',
        field: 'fastestRunTime',
        width: 80,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Slowest',
        field: 'slowestRunTime',
        width: 80,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Runs',
        field: 'runCount',
        width: 60,
        sortable: true,
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
    ];
    return defs;
  }, [PlayButtonRenderer, EyeButtonRenderer]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
    suppressAutoSize: true,
  }), []);

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          {/* Search bar */}
          <div className="relative flex items-center">
            <Search className="absolute left-2 w-3 h-3 text-gray-400" />
            <input
              type="text"
              value={localSearch}
              onChange={(e) => setLocalSearch(e.target.value)}
              placeholder="Search filename or query..."
              className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
            />
            {localSearch && (
              <button
                onClick={() => { setLocalSearch(''); setSearch(''); }}
                className="absolute right-2 text-gray-400 hover:text-white"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
        </div>
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400">
            {filteredQueries.length} {filteredQueries.length === 1 ? 'query' : 'queries'}
            {search && ` (filtered from ${queries.length})`}
          </span>
          <button
            onClick={runAllQueries}
            disabled={loading || runningAll || runningQuery !== null}
            className="flex items-center gap-1.5 px-2 py-1 bg-blue-700 hover:bg-blue-600 rounded text-white text-xs disabled:opacity-50"
          >
            {runningAll ? (
              <>
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
                {runAllProgress ? `${runAllProgress.current}/${runAllProgress.total}` : 'Running...'}
              </>
            ) : (
              <>
                <PlayCircle className="w-3.5 h-3.5" />
                Run All
              </>
            )}
          </button>
          <button
            onClick={resetStats}
            disabled={loading || runningAll || runningQuery !== null}
            className="flex items-center gap-1.5 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs disabled:opacity-50"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset
          </button>
        </div>
      </div>

      {/* Status Bar - always reserve space to prevent layout shift */}
      <div className={`mx-4 mt-3 px-3 py-2 rounded text-xs h-8 ${
        error ? 'bg-red-900/50 text-red-300' :
        lastResult ? 'bg-green-900/50 text-green-300' :
        loadMessage ? 'bg-green-900/50 text-green-300' :
        'bg-transparent'
      }`}>
        {error ? (
          <span>Error: {error}</span>
        ) : lastResult ? (
          <span>
            Ran <strong>{lastResult.filename}</strong>: {lastResult.rowCount} rows in {formatDuration(lastResult.duration)}
          </span>
        ) : loadMessage ? (
          <span>{loadMessage}</span>
        ) : null}
      </div>

      {/* Grid */}
      <div className="flex-1 mx-4 mb-4 mt-3 bg-gray-900 border border-gray-700 rounded overflow-hidden">
        <AgGridReact<MyQuery>
          theme={darkTheme}
          rowData={filteredQueries}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          tooltipShowDelay={300}
          tooltipInteraction={true}
          suppressColumnVirtualisation={true}
          getRowId={(params) => params.data.filename}
        />
      </div>

      {/* Empty State */}
      {!loading && filteredQueries.length === 0 && (
        <div className="mx-4 mt-4 text-center text-gray-500">
          {queries.length === 0 ? (
            <>
              <p className="text-sm">No queries found in ./queries folder</p>
              <p className="text-xs mt-1">Add .sql files to the queries folder to get started</p>
            </>
          ) : (
            <p className="text-sm">No queries match your search</p>
          )}
        </div>
      )}

      {/* Query Details Modal */}
      {selectedQuery && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[900px] max-h-[85vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Query Details</h2>
                <p className="text-xs text-gray-400 font-mono">{selectedQuery.filename}</p>
              </div>
              <button onClick={handleCloseModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {/* Query */}
              <div className="mb-4">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    <h3 className="text-xs font-semibold text-gray-400">Query</h3>
                    <button
                      onClick={handleCopyQuery}
                      className="flex items-center gap-1 px-1.5 py-0.5 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                      title="Copy query"
                    >
                      {queryCopied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
                      {queryCopied ? 'Copied' : 'Copy'}
                    </button>
                  </div>
                  <button
                    onClick={handleAnalyseQuery}
                    className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-500 rounded text-white"
                  >
                    <ExternalLink className="w-3 h-3" />
                    Analyse Query
                  </button>
                </div>
                <pre className="bg-gray-800 p-3 rounded text-xs text-blue-400 max-h-64 overflow-y-auto whitespace-pre-wrap break-words font-mono">
                  {selectedQuery.query}
                </pre>
              </div>

              {/* Run Statistics */}
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-gray-400 mb-2">Run Statistics</h3>
                <div className="grid grid-cols-4 gap-2 mb-2">
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Total Runs</div>
                    <div className="text-sm font-semibold text-white">{selectedQuery.runCount}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Run</div>
                    <div className="text-sm font-semibold text-white">{formatDateTime(selectedQuery.lastRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Duration</div>
                    <div className="text-sm font-semibold text-blue-400">{formatDuration(selectedQuery.lastDuration)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Rows</div>
                    <div className="text-sm font-semibold text-blue-400">{selectedQuery.lastRowCount?.toLocaleString() ?? '-'}</div>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Average Time</div>
                    <div className="text-sm font-semibold text-green-400">{formatDuration(selectedQuery.avgRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Fastest Time</div>
                    <div className="text-sm font-semibold text-green-400">{formatDuration(selectedQuery.fastestRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Slowest Time</div>
                    <div className="text-sm font-semibold text-red-400">{formatDuration(selectedQuery.slowestRunTime)}</div>
                  </div>
                </div>
              </div>

              {/* Run button in modal */}
              <div className="flex justify-end">
                <button
                  onClick={() => {
                    runQuery(selectedQuery);
                    handleCloseModal();
                  }}
                  disabled={runningQuery !== null}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-white text-xs disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <Play className="w-3.5 h-3.5" />
                  Run Query
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
