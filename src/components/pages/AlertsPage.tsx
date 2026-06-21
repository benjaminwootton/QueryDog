import { useState, useEffect, useCallback, useMemo } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams, SelectionChangedEvent } from 'ag-grid-community';
import { Play, RotateCcw, Loader2, Eye, X, Copy, Check, Search, PlayCircle, ExternalLink, BarChart2, GitCompare, Pencil } from 'lucide-react';
import { QueryCompareModal } from '../QueryCompareModal';
import { QueryResultsModal } from '../QueryResultsModal';
import type { QueryLogEntry } from '../../types/queryLog';

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

interface Alert {
  filename: string;
  query: string;
  description?: string;
  lastRunTime: string | null;
  lastDuration: number | null;
  lastRowCount: number | null;
  avgRunTime: number | null;
  slowestRunTime: number | null;
  fastestRunTime: number | null;
  runCount: number;
}

interface QueryLogInfo {
  query_id: string;
  type: string;
  query_duration_ms: number;
  read_rows: number;
  read_bytes: number;
  result_rows: number;
  result_bytes: number;
  memory_usage: number;
  ProfileEvents: Record<string, number>;
}

interface RunLogEntry {
  queryId: string;
  runTime: string;
  duration: number;
  rowCount: number;
  readRows: number | null;
  readBytes: number | null;
  queryLog: QueryLogInfo | null;
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

function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined) return '-';
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
  if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
  return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
}

export function AlertsPage() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [loading, setLoading] = useState(true);
  const [runningAlert, setRunningAlert] = useState<string | null>(null);
  const [lastResult, setLastResult] = useState<{ filename: string; rowCount: number; duration: number } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedAlert, setSelectedAlert] = useState<Alert | null>(null);
  const [queryCopied, setQueryCopied] = useState(false);
  const [localSearch, setLocalSearch] = useState('');
  const [search, setSearch] = useState('');
  const [loadMessage, setLoadMessage] = useState<string | null>(null);
  const [runningAll, setRunningAll] = useState(false);
  const [runAllProgress, setRunAllProgress] = useState<{ current: number; total: number } | null>(null);
  const [runLog, setRunLog] = useState<RunLogEntry[]>([]);
  const [runLogLoading, setRunLogLoading] = useState(false);
  const [selectedAlerts, setSelectedAlerts] = useState<Alert[]>([]);
  const [compareModalOpen, setCompareModalOpen] = useState(false);
  const [compareEntries, setCompareEntries] = useState<QueryLogEntry[]>([]);
  const [isEditMode, setIsEditMode] = useState(false);
  const [editedQuery, setEditedQuery] = useState('');
  const [editedDescription, setEditedDescription] = useState('');
  const [resultsAlert, setResultsAlert] = useState<Alert | null>(null);

  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && selectedAlert) {
        setSelectedAlert(null);
        setQueryCopied(false);
        setRunLog([]);
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [selectedAlert]);

  useEffect(() => {
    if (!selectedAlert) {
      setRunLog([]);
      return;
    }
    setRunLogLoading(true);
    fetch(`/api/alerts/run-log/${encodeURIComponent(selectedAlert.filename)}`)
      .then(res => res.json())
      .then(data => setRunLog(data.runLog || []))
      .catch(() => setRunLog([]))
      .finally(() => setRunLogLoading(false));
  }, [selectedAlert]);

  const fetchAlerts = useCallback(async (showLoadMessage = false) => {
    setLoading(true);
    try {
      const res = await fetch('/api/alerts');
      if (!res.ok) throw new Error('Failed to fetch alerts');
      const data = await res.json();
      const list = data.alerts || [];
      const alertsPath = data.path || './alerts';
      setAlerts(list);
      setError(null);
      if (showLoadMessage) {
        setLoadMessage(`${list.length} ${list.length === 1 ? 'alert' : 'alerts'} loaded from ${alertsPath}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
      setLoadMessage(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchAlerts(true);
  }, [fetchAlerts]);

  useEffect(() => {
    const timer = setTimeout(() => setSearch(localSearch), 300);
    return () => clearTimeout(timer);
  }, [localSearch]);

  const filteredAlerts = useMemo(() => {
    if (!search) return alerts;
    const searchLower = search.toLowerCase();
    return alerts.filter(a =>
      a.filename.toLowerCase().includes(searchLower) ||
      a.query.toLowerCase().includes(searchLower) ||
      (a.description ?? '').toLowerCase().includes(searchLower)
    );
  }, [alerts, search]);

  const runAlert = useCallback(async (alert: Alert, skipRefresh = false) => {
    setRunningAlert(alert.filename);
    if (!skipRefresh) {
      setLastResult(null);
      setError(null);
    }
    try {
      const res = await fetch('/api/alerts/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ filename: alert.filename, query: alert.query }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed to run alert');

      if (data.stats) {
        setAlerts(prev => prev.map(a =>
          a.filename === alert.filename ? { ...a, ...data.stats } : a
        ));
      }

      if (!skipRefresh) {
        setLastResult({ filename: alert.filename, rowCount: data.rowCount, duration: data.duration });
      }
      return { success: true, duration: data.duration, rowCount: data.rowCount };
    } catch (err) {
      const errorMsg = err instanceof Error ? err.message : 'Unknown error';
      if (!skipRefresh) setError(errorMsg);
      return { success: false, error: errorMsg };
    } finally {
      setRunningAlert(null);
    }
  }, []);

  const resetStats = useCallback(async () => {
    try {
      const res = await fetch('/api/alerts/stats', { method: 'DELETE' });
      if (!res.ok) throw new Error('Failed to reset stats');
      setLastResult(null);
      setLoadMessage('All run statistics have been reset');
      await fetchAlerts();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    }
  }, [fetchAlerts]);

  const runAllAlerts = useCallback(async () => {
    if (runningAll || filteredAlerts.length === 0) return;
    setRunningAll(true);
    setError(null);
    setLastResult(null);
    setLoadMessage(null);
    const total = filteredAlerts.length;
    let firingCount = 0;
    let failCount = 0;

    for (let i = 0; i < filteredAlerts.length; i++) {
      setRunAllProgress({ current: i + 1, total });
      const result = await runAlert(filteredAlerts[i], true);
      if (!result.success) {
        failCount++;
      } else if ((result.rowCount ?? 0) > 0) {
        firingCount++;
      }
    }

    setRunAllProgress(null);
    setRunningAll(false);
    setLoadMessage(`Completed: ${firingCount} firing, ${failCount} failed out of ${total} alerts`);
  }, [runningAll, filteredAlerts, runAlert]);

  const PlayButtonRenderer = useCallback((params: ICellRendererParams<Alert>) => {
    const isRunning = runningAlert === params.data?.filename;
    return (
      <button
        onClick={() => params.data && runAlert(params.data)}
        disabled={isRunning || runningAlert !== null}
        className="p-1.5 hover:bg-blue-600 bg-blue-700 rounded disabled:opacity-50 disabled:cursor-not-allowed"
        title="Run alert"
      >
        {isRunning ? (
          <Loader2 className="w-3.5 h-3.5 text-white animate-spin" />
        ) : (
          <Play className="w-3.5 h-3.5 text-white" />
        )}
      </button>
    );
  }, [runningAlert, runAlert]);

  const ActionCellRenderer = useCallback((params: ICellRendererParams<Alert>) => {
    const handleView = (e: React.MouseEvent) => {
      e.stopPropagation();
      if (params.data) {
        setSelectedAlert(params.data);
        setIsEditMode(false);
      }
    };

    const handleEdit = (e: React.MouseEvent) => {
      e.stopPropagation();
      if (params.data) {
        setSelectedAlert(params.data);
        setEditedQuery(params.data.query);
        setEditedDescription(params.data.description ?? '');
        setIsEditMode(true);
      }
    };

    return (
      <div className="flex items-center gap-0.5">
        <button onClick={handleView} className="p-1 hover:bg-gray-600 rounded" title="View alert (read-only)">
          <Eye className="w-3.5 h-3.5 text-gray-400 hover:text-white" />
        </button>
        <button onClick={handleEdit} className="p-1 hover:bg-gray-600 rounded" title="Edit alert">
          <Pencil className="w-3.5 h-3.5 text-gray-400 hover:text-white" />
        </button>
      </div>
    );
  }, []);

  const ViewResultsRenderer = useCallback((params: ICellRendererParams<Alert>) => {
    return (
      <button
        onClick={(e) => {
          e.stopPropagation();
          if (params.data) setResultsAlert(params.data);
        }}
        className="p-1 hover:bg-blue-700/50 rounded text-blue-400 hover:text-blue-300"
        title="Run and view results"
      >
        <Eye className="w-4 h-4" />
      </button>
    );
  }, []);

  const StatusRenderer = useCallback((params: ICellRendererParams<Alert>) => {
    const a = params.data;
    if (!a || a.runCount === 0) return null;
    const rows = a.lastRowCount ?? 0;
    if (rows > 0) {
      return (
        <span
          className="inline-flex items-center justify-center w-5 h-5 bg-orange-500 text-white text-[10px] font-bold rounded-full cursor-help"
          title={`Firing (${rows} row${rows === 1 ? '' : 's'} on last run)`}
        >
          🔔
        </span>
      );
    }
    return (
      <span className="text-green-400 text-lg font-bold" title="OK – no rows on last run">
        ✓
      </span>
    );
  }, []);

  const handleCopyQuery = useCallback(async () => {
    if (!selectedAlert) return;
    await navigator.clipboard.writeText(selectedAlert.query);
    setQueryCopied(true);
    setTimeout(() => setQueryCopied(false), 2000);
  }, [selectedAlert]);

  const handleCloseModal = useCallback(() => {
    setSelectedAlert(null);
    setQueryCopied(false);
    setRunLog([]);
    setIsEditMode(false);
    setEditedQuery('');
    setEditedDescription('');
  }, []);

  const handleAnalyseQuery = useCallback(() => {
    if (!selectedAlert) return;
    const openQueryEditor = (window as unknown as { openQueryEditor?: (query: string) => void }).openQueryEditor;
    if (openQueryEditor) {
      openQueryEditor(selectedAlert.query);
      handleCloseModal();
    }
  }, [selectedAlert, handleCloseModal]);

  const handleSaveQuery = useCallback(async () => {
    if (!selectedAlert) return;
    try {
      const res = await fetch('/api/alerts/update', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          filename: selectedAlert.filename,
          query: editedQuery,
          description: editedDescription,
        }),
      });
      if (!res.ok) throw new Error('Failed to save alert');
      setAlerts(prev => prev.map(a =>
        a.filename === selectedAlert.filename
          ? { ...a, query: editedQuery, description: editedDescription }
          : a
      ));
      setSelectedAlert({ ...selectedAlert, query: editedQuery, description: editedDescription });
      setIsEditMode(false);
      setLoadMessage(`Alert "${selectedAlert.filename}" updated successfully`);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save alert');
    }
  }, [selectedAlert, editedQuery, editedDescription]);

  const handleCancelEdit = useCallback(() => {
    setIsEditMode(false);
    setEditedQuery('');
    setEditedDescription('');
  }, []);

  const handleCloneAlert = useCallback(async () => {
    if (selectedAlerts.length !== 1) return;
    const alertToClone = selectedAlerts[0];
    const baseName = alertToClone.filename.replace(/\.sql$/, '');
    const newFilename = `${baseName}_copy.sql`;

    try {
      const res = await fetch('/api/alerts/clone', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sourceFilename: alertToClone.filename,
          newFilename,
          query: alertToClone.query,
        }),
      });
      if (!res.ok) throw new Error('Failed to clone alert');
      const data = await res.json();
      setLoadMessage(`Alert cloned as "${data.filename}"`);
      await fetchAlerts();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to clone alert');
    }
  }, [selectedAlerts, fetchAlerts]);

  const onSelectionChanged = useCallback((event: SelectionChangedEvent<Alert>) => {
    const selectedRows = event.api.getSelectedRows();
    setSelectedAlerts(selectedRows.slice(0, 10));
  }, []);

  const handleCompareAlerts = useCallback(async () => {
    if (selectedAlerts.length < 2) return;

    try {
      setLoading(true);
      const runLogPromises = selectedAlerts.map(a =>
        fetch(`/api/alerts/run-log/${encodeURIComponent(a.filename)}`)
          .then(res => res.json())
          .then(data => ({ alert: a, runLog: data.runLog || [] }))
      );

      const results = await Promise.all(runLogPromises);
      const entries: QueryLogEntry[] = results
        .filter(r => r.runLog.length > 0 && r.runLog[0].queryLog)
        .map(r => {
          const latestRun = r.runLog[0];
          const queryLog = latestRun.queryLog!;
          return {
            query_id: queryLog.query_id,
            query: r.alert.query,
            query_duration_ms: queryLog.query_duration_ms,
            read_rows: queryLog.read_rows,
            read_bytes: queryLog.read_bytes,
            result_rows: queryLog.result_rows,
            result_bytes: queryLog.result_bytes,
            memory_usage: queryLog.memory_usage,
            ProfileEvents: queryLog.ProfileEvents || {},
            Settings: {},
          };
        });

      if (entries.length < 2) {
        setError('Not enough alerts with run history to compare. Please run them first.');
        return;
      }

      setCompareEntries(entries);
      setCompareModalOpen(true);
    } catch {
      setError('Failed to fetch run logs for comparison');
    } finally {
      setLoading(false);
    }
  }, [selectedAlerts]);

  const timeComparator = useCallback((valueA: number | null, valueB: number | null) => {
    if (valueA === null && valueB === null) return 0;
    if (valueA === null) return -1;
    if (valueB === null) return 1;
    return valueA - valueB;
  }, []);

  const columnDefs = useMemo((): ColDef<Alert>[] => {
    return [
      {
        headerName: '',
        field: 'filename',
        width: 70,
        sortable: false,
        cellRenderer: ActionCellRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
      {
        headerName: 'Filename',
        field: 'filename',
        width: 280,
        sortable: true,
        cellStyle: { color: '#93c5fd' },
      },
      {
        headerName: 'Description',
        field: 'description',
        flex: 1,
        minWidth: 250,
        sortable: false,
        cellStyle: { color: '#d1d5db' },
        tooltipValueGetter: (params) => (params.value as string) || '',
        valueFormatter: (params) => {
          const d = (params.value as string) ?? '';
          if (!d) return '';
          const oneLine = d.replace(/\s+/g, ' ').trim();
          return oneLine.length > 140 ? oneLine.substring(0, 140) + '...' : oneLine;
        },
      },
      {
        headerName: 'Query',
        field: 'query',
        width: 380,
        sortable: false,
        cellStyle: { color: '#93c5fd' },
        tooltipValueGetter: (params) => {
          const q = params.value as string;
          if (!q) return '';
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
        comparator: timeComparator,
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
        comparator: timeComparator,
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Fastest',
        field: 'fastestRunTime',
        width: 80,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        comparator: timeComparator,
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Slowest',
        field: 'slowestRunTime',
        width: 80,
        sortable: true,
        valueFormatter: (params) => formatDuration(params.value),
        comparator: timeComparator,
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Runs',
        field: 'runCount',
        width: 60,
        sortable: true,
        cellStyle: { textAlign: 'right', color: '#86efac' },
      },
      {
        headerName: 'Status',
        field: 'filename',
        width: 60,
        sortable: false,
        cellRenderer: StatusRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
      {
        headerName: '',
        field: 'filename',
        width: 50,
        sortable: false,
        cellRenderer: ViewResultsRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
    ];
  }, [PlayButtonRenderer, ActionCellRenderer, StatusRenderer, ViewResultsRenderer, timeComparator]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
    suppressAutoSize: true,
    sortingOrder: ['desc', 'asc'],
  }), []);

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-3 sm:px-4 h-9 flex items-center justify-between shrink-0">
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
            {filteredAlerts.length} {filteredAlerts.length === 1 ? 'alert' : 'alerts'}
            {search && ` (filtered from ${alerts.length})`}
          </span>
          {selectedAlerts.length >= 2 ? (
            <button
              onClick={handleCompareAlerts}
              className="flex items-center gap-1.5 px-2 py-1 bg-blue-600 hover:bg-blue-500 rounded text-white text-xs"
            >
              <GitCompare className="w-3.5 h-3.5" />
              Compare ({selectedAlerts.length})
            </button>
          ) : selectedAlerts.length === 1 ? (
            <>
              <button
                onClick={handleCloneAlert}
                className="flex items-center gap-1.5 px-2 py-1 bg-blue-600 hover:bg-blue-500 rounded text-white text-xs"
              >
                <Copy className="w-3.5 h-3.5" />
                Clone
              </button>
              <span className="text-xs text-gray-400">
                or select 1 more to compare
              </span>
            </>
          ) : null}
          <button
            onClick={runAllAlerts}
            disabled={loading || runningAll || runningAlert !== null}
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
            disabled={loading || runningAll || runningAlert !== null}
            className="flex items-center gap-1.5 px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 text-xs disabled:opacity-50"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset
          </button>
        </div>
      </div>

      {/* Status Bar */}
      <div className={`mx-4 mt-3 px-3 py-2 rounded text-xs ${
        error ? 'bg-red-900/50 text-red-300' :
        lastResult ? 'bg-green-900/50 text-green-300 h-8' :
        loadMessage ? 'bg-green-900/50 text-green-300 h-8' :
        'bg-transparent h-8'
      }`}>
        {error ? (
          <div className="break-words whitespace-pre-wrap">Error: {error}</div>
        ) : lastResult ? (
          <span>
            Ran <strong>{lastResult.filename}</strong>: {lastResult.rowCount} rows in {formatDuration(lastResult.duration)}
            {lastResult.rowCount > 0 && <span className="ml-2 text-orange-300">— FIRING</span>}
          </span>
        ) : loadMessage ? (
          <span>{loadMessage}</span>
        ) : null}
      </div>

      {/* Grid */}
      <div className="flex-1 mx-4 mb-4 mt-3 bg-gray-900 border border-gray-700 rounded overflow-hidden">
        <AgGridReact<Alert>
          theme={darkTheme}
          rowData={filteredAlerts}
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
          rowSelection="multiple"
          rowMultiSelectWithClick={true}
          onSelectionChanged={onSelectionChanged}
          initialState={{
            sort: {
              sortModel: [{ colId: 'lastRunTime', sort: 'desc' }]
            }
          }}
        />
      </div>

      {/* Empty State */}
      {!loading && filteredAlerts.length === 0 && (
        <div className="mx-4 mt-4 text-center text-gray-500">
          {alerts.length === 0 ? (
            <>
              <p className="text-sm">No alerts found in ./alerts folder</p>
              <p className="text-xs mt-1">Add .sql files to the alerts folder to get started</p>
            </>
          ) : (
            <p className="text-sm">No alerts match your search</p>
          )}
        </div>
      )}

      {/* Alert Details Modal */}
      {selectedAlert && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[1350px] max-h-[85vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Alert Details</h2>
                <p className="text-xs text-gray-400 font-mono">{selectedAlert.filename}</p>
              </div>
              <button onClick={handleCloseModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {/* Description */}
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-gray-400 mb-1">Description</h3>
                {isEditMode ? (
                  <textarea
                    value={editedDescription}
                    onChange={(e) => setEditedDescription(e.target.value)}
                    className="w-full bg-gray-800 p-3 rounded text-xs text-gray-200 font-mono border border-gray-700 focus:outline-none focus:border-blue-500"
                    rows={6}
                    placeholder="What does this alert detect? When should someone care?"
                    spellCheck={true}
                  />
                ) : selectedAlert.description ? (
                  <pre className="bg-gray-800 p-3 rounded text-xs text-gray-200 whitespace-pre-wrap break-words font-mono max-h-64 overflow-y-auto">
                    {selectedAlert.description}
                  </pre>
                ) : (
                  <div className="text-xs text-gray-500 italic px-3 py-2">No description. Click Edit to add one.</div>
                )}
              </div>

              <div className="mb-4">
                <div className="flex items-center justify-between mb-1">
                  <div className="flex items-center gap-2">
                    <h3 className="text-xs font-semibold text-gray-400">Query</h3>
                    {!isEditMode && (
                      <button
                        onClick={handleCopyQuery}
                        className="flex items-center gap-1 px-1.5 py-0.5 text-xs text-gray-400 hover:text-white hover:bg-gray-700 rounded transition-colors"
                        title="Copy query"
                      >
                        {queryCopied ? <Check className="w-3 h-3 text-green-400" /> : <Copy className="w-3 h-3" />}
                        {queryCopied ? 'Copied' : 'Copy'}
                      </button>
                    )}
                  </div>
                  <div className="flex items-center gap-2">
                    {isEditMode ? (
                      <>
                        <button
                          onClick={handleCancelEdit}
                          className="flex items-center gap-1 px-2 py-1 text-xs bg-gray-600 hover:bg-gray-500 rounded text-white"
                        >
                          <X className="w-3 h-3" />
                          Cancel
                        </button>
                        <button
                          onClick={handleSaveQuery}
                          className="flex items-center gap-1 px-2 py-1 text-xs bg-green-600 hover:bg-green-500 rounded text-white"
                        >
                          <Check className="w-3 h-3" />
                          Save
                        </button>
                      </>
                    ) : (
                      <button
                        onClick={handleAnalyseQuery}
                        className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-600 hover:bg-blue-500 rounded text-white"
                      >
                        <ExternalLink className="w-3 h-3" />
                        Analyse Query
                      </button>
                    )}
                  </div>
                </div>
                {isEditMode ? (
                  <textarea
                    value={editedQuery}
                    onChange={(e) => setEditedQuery(e.target.value)}
                    className="w-full bg-gray-800 p-3 rounded text-xs text-blue-400 font-mono border border-blue-500 focus:outline-none focus:border-blue-400"
                    rows={10}
                    spellCheck={false}
                  />
                ) : (
                  <pre className="bg-gray-800 p-3 rounded text-xs text-blue-400 max-h-64 overflow-y-auto whitespace-pre-wrap break-words font-mono">
                    {selectedAlert.query}
                  </pre>
                )}
              </div>

              {/* Run Statistics */}
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-gray-400 mb-2">Run Statistics</h3>
                <div className="grid grid-cols-4 gap-2 mb-2">
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Total Runs</div>
                    <div className="text-sm font-semibold text-white">{selectedAlert.runCount}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Run</div>
                    <div className="text-sm font-semibold text-white">{formatDateTime(selectedAlert.lastRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Duration</div>
                    <div className="text-sm font-semibold text-blue-400">{formatDuration(selectedAlert.lastDuration)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Last Rows</div>
                    <div className="text-sm font-semibold text-blue-400">{selectedAlert.lastRowCount?.toLocaleString() ?? '-'}</div>
                  </div>
                </div>
                <div className="grid grid-cols-4 gap-2">
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Average Time</div>
                    <div className="text-sm font-semibold text-green-400">{formatDuration(selectedAlert.avgRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Fastest Time</div>
                    <div className="text-sm font-semibold text-green-400">{formatDuration(selectedAlert.fastestRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Slowest Time</div>
                    <div className="text-sm font-semibold text-red-400">{formatDuration(selectedAlert.slowestRunTime)}</div>
                  </div>
                  <div className="bg-gray-800 p-2 rounded">
                    <div className="text-xs text-gray-400">Avg Row Count</div>
                    <div className="text-sm font-semibold text-cyan-400">-</div>
                  </div>
                </div>
              </div>

              {/* Run Log */}
              <div className="mb-4">
                <h3 className="text-xs font-semibold text-gray-400 mb-2">Run Log</h3>
                {runLogLoading ? (
                  <div className="text-xs text-gray-500">Loading run log...</div>
                ) : runLog.length === 0 ? (
                  <div className="text-xs text-gray-500">No runs recorded yet. Run the alert to see execution history.</div>
                ) : (
                  <div className="bg-gray-800 rounded overflow-hidden">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="bg-gray-700 text-gray-300">
                          <th className="px-2 py-1.5 text-left font-medium">Query ID</th>
                          <th className="px-2 py-1.5 text-left font-medium">Run Time</th>
                          <th className="px-2 py-1.5 text-right font-medium">Duration</th>
                          <th className="px-2 py-1.5 text-right font-medium">Rows</th>
                          <th className="px-2 py-1.5 text-right font-medium">Read Rows</th>
                          <th className="px-2 py-1.5 text-right font-medium">Read Bytes</th>
                          <th className="px-2 py-1.5 text-right font-medium">Memory</th>
                        </tr>
                      </thead>
                      <tbody>
                        {runLog.map((entry, idx) => (
                          <tr key={entry.queryId || idx} className="border-t border-gray-700 hover:bg-gray-750">
                            <td className="px-2 py-1.5 font-mono text-[10px]" title={entry.queryId}>
                              {entry.queryId ? (
                                <div className="flex items-center gap-1">
                                  <button
                                    onClick={() => {
                                      const navigateToQueryId = (window as unknown as { navigateToQueryId?: (queryId: string) => void }).navigateToQueryId;
                                      if (navigateToQueryId) {
                                        handleCloseModal();
                                        navigateToQueryId(entry.queryId);
                                      }
                                    }}
                                    className="text-blue-400 hover:text-blue-300 hover:underline"
                                  >
                                    {entry.queryId.substring(0, 16) + '...'}
                                  </button>
                                  <button
                                    onClick={() => navigator.clipboard.writeText(entry.queryId)}
                                    className="text-gray-500 hover:text-gray-300 p-0.5"
                                    title="Copy query ID"
                                  >
                                    <Copy className="w-3 h-3" />
                                  </button>
                                </div>
                              ) : '-'}
                            </td>
                            <td className="px-2 py-1.5 text-gray-300">{formatDateTime(entry.runTime)}</td>
                            <td className="px-2 py-1.5 text-right text-green-400">{formatDuration(entry.duration)}</td>
                            <td className="px-2 py-1.5 text-right text-gray-300">{entry.rowCount?.toLocaleString() ?? '-'}</td>
                            <td className="px-2 py-1.5 text-right text-gray-300">
                              {(entry.readRows ?? entry.queryLog?.read_rows)?.toLocaleString() ?? '-'}
                            </td>
                            <td className="px-2 py-1.5 text-right text-gray-300">
                              {formatBytes(entry.readBytes ?? entry.queryLog?.read_bytes)}
                            </td>
                            <td className="px-2 py-1.5 text-right text-gray-300">
                              {formatBytes(entry.queryLog?.memory_usage)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              <div className="flex justify-end gap-2">
                {runLog.filter(e => e.queryId).length > 0 && (
                  <button
                    onClick={() => {
                      const queryIds = runLog.filter(e => e.queryId).map(e => e.queryId);
                      const navigateToQueryIds = (window as unknown as { navigateToQueryIds?: (queryIds: string[]) => void }).navigateToQueryIds;
                      if (navigateToQueryIds) {
                        handleCloseModal();
                        navigateToQueryIds(queryIds);
                      }
                    }}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-600 hover:bg-gray-500 rounded text-white text-xs"
                  >
                    <BarChart2 className="w-3.5 h-3.5" />
                    Analyse Results ({runLog.filter(e => e.queryId).length})
                  </button>
                )}
                <button
                  onClick={() => {
                    runAlert(selectedAlert);
                    handleCloseModal();
                  }}
                  disabled={runningAlert !== null}
                  className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-500 rounded text-white text-xs disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <Play className="w-3.5 h-3.5" />
                  Run Alert
                </button>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Query Compare Modal */}
      {compareModalOpen && compareEntries.length > 0 && (
        <QueryCompareModal
          entries={compareEntries}
          onClose={() => {
            setCompareModalOpen(false);
            setCompareEntries([]);
          }}
        />
      )}

      {/* Run + Results Modal */}
      {resultsAlert && (
        <QueryResultsModal
          filename={resultsAlert.filename}
          query={resultsAlert.query}
          description={resultsAlert.description}
          runEndpoint="/api/alerts/run"
          onClose={() => setResultsAlert(null)}
        />
      )}
    </div>
  );
}
