import { useState, useEffect, useMemo, useCallback, forwardRef, useImperativeHandle } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
import { X } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer, CartesianGrid } from 'recharts';
import { useQueryStore } from '../stores/queryStore';
import { fetchProfileEvents } from '../services/api';

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

// All available profile events
const ALL_PROFILE_EVENTS = [
  'IOBufferAllocBytes',
  'NetworkReceiveBytes',
  'InterfaceHTTPReceiveBytes',
  'MergeTreeDataWriterUncompressedBytes',
  'InsertedBytes',
  'SelectedBytes',
  'LoggerElapsedNanoseconds',
  'OSWriteBytes',
  'RealTimeMicroseconds',
  'OSWriteChars',
  'WriteBufferFromFileDescriptorWriteBytes',
  'MergeTreeDataWriterCompressedBytes',
  'OSCPUVirtualTimeMicroseconds',
  'UserTimeMicroseconds',
  'OSReadChars',
  'InsertedRows',
  'SelectedRows',
  'MergeTreeDataWriterRows',
  'MergeTreeDataWriterMergingBlocksMicroseconds',
  'NetworkReceiveElapsedMicroseconds',
  'SystemTimeMicroseconds',
  'PartsLockHoldMicroseconds',
  'PartsLockWaitMicroseconds',
  'NetworkSendBytes',
  'InterfaceHTTPSendBytes',
  'DiskWriteElapsedMicroseconds',
  'SoftPageFaults',
  'NetworkSendElapsedMicroseconds',
  'LocalThreadPoolThreadCreationMicroseconds',
  'IOBufferAllocs',
  'OSCPUWaitMicroseconds',
  'MergeTreeDataWriterSortingBlocksMicroseconds',
  'ContextLock',
  'GlobalThreadPoolLockWaitMicroseconds',
  'FileOpen',
  'WriteBufferFromFileDescriptorWrite',
  'FunctionExecute',
  'AsyncLoggingFileLogTotalMessages',
  'AsyncLoggingTextLogTotalMessages',
  'LogTrace',
  'GlobalThreadPoolJobs',
  'LocalThreadPoolExpansions',
  'LocalThreadPoolJobs',
  'LocalThreadPoolShrinks',
  'LogDebug',
  'ConcurrencyControlSlotsAcquired',
  'MergeTreeDataWriterBlocks',
  'MergeTreeDataWriterBlocksAlreadySorted',
  'InsertedCompactParts',
  'RWLockAcquiredReadLocks',
  'Query',
  'InsertQuery',
  'InitialQuery',
  'ConcurrencyControlSlotsGranted',
  'ConcurrencyControlSlotsAcquiredNonCompeting',
];

// Most important profile events to show by default
const DEFAULT_VISIBLE_EVENTS = [
  'RealTimeMicroseconds',
  'UserTimeMicroseconds',
  'SystemTimeMicroseconds',
  'SelectedRows',
  'SelectedBytes',
  'InsertedRows',
  'InsertedBytes',
  'OSWriteBytes',
  'OSReadChars',
  'NetworkReceiveBytes',
  'NetworkSendBytes',
];

// Colors for chart lines
const CHART_COLORS = [
  '#3b82f6', // blue
  '#10b981', // green
  '#f59e0b', // amber
  '#ef4444', // red
  '#8b5cf6', // violet
  '#ec4899', // pink
  '#06b6d4', // cyan
  '#84cc16', // lime
  '#f97316', // orange
  '#6366f1', // indigo
];

function formatValue(value: number, columnName: string): string {
  if (value === 0 || value === null || value === undefined) return '-';

  // Format bytes
  if (columnName.includes('Bytes')) {
    if (value >= 1073741824) return (value / 1073741824).toFixed(2) + ' GB';
    if (value >= 1048576) return (value / 1048576).toFixed(2) + ' MB';
    if (value >= 1024) return (value / 1024).toFixed(2) + ' KB';
    return value + ' B';
  }

  // Format microseconds
  if (columnName.includes('Microseconds')) {
    if (value >= 1000000) return (value / 1000000).toFixed(2) + 's';
    if (value >= 1000) return (value / 1000).toFixed(2) + 'ms';
    return value.toLocaleString() + 'us';
  }

  // Format nanoseconds
  if (columnName.includes('Nanoseconds')) {
    if (value >= 1000000000) return (value / 1000000000).toFixed(2) + 's';
    if (value >= 1000000) return (value / 1000000).toFixed(2) + 'ms';
    if (value >= 1000) return (value / 1000).toFixed(2) + 'us';
    return value.toLocaleString() + 'ns';
  }

  // Default number format
  return value.toLocaleString();
}

// Format value for chart tooltip
function formatChartValue(value: number, columnName: string): string {
  return formatValue(value, columnName);
}

interface ProfileEventRow {
  event_time: string;
  query_id: string;
  [key: string]: string | number;
}

// Export constants for use in parent
export { ALL_PROFILE_EVENTS, DEFAULT_VISIBLE_EVENTS };

// Exported ref interface for parent to control
export interface ProfileEventsTableRef {
  openChart: () => void;
  openColumnSelector: () => void;
  closeColumnSelector: () => void;
  toggleEventVisibility: (eventName: string) => void;
  visibleEvents: Set<string>;
  allEvents: string[];
}

export const ProfileEventsTable = forwardRef<ProfileEventsTableRef, object>(function ProfileEventsTable(_props, ref) {
  const { timeRange, fieldFilters, search } = useQueryStore();
  const [data, setData] = useState<ProfileEventRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [visibleEvents, setVisibleEvents] = useState<Set<string>>(new Set(DEFAULT_VISIBLE_EVENTS));
  const [columnSelectorOpen, setColumnSelectorOpen] = useState(false);
  const [chartModalOpen, setChartModalOpen] = useState(false);

  const toggleEventVisibility = useCallback((eventName: string) => {
    setVisibleEvents(prev => {
      const next = new Set(prev);
      if (next.has(eventName)) {
        next.delete(eventName);
      } else {
        next.add(eventName);
      }
      return next;
    });
  }, []);

  // Expose controls to parent via ref
  useImperativeHandle(ref, () => ({
    openChart: () => setChartModalOpen(true),
    openColumnSelector: () => setColumnSelectorOpen(true),
    closeColumnSelector: () => setColumnSelectorOpen(false),
    toggleEventVisibility,
    visibleEvents,
    allEvents: ALL_PROFILE_EVENTS,
  }), [visibleEvents, toggleEventVisibility]);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      // Always fetch all columns so we have data for charting
      const result = await fetchProfileEvents(timeRange, fieldFilters, ALL_PROFILE_EVENTS, search);
      setData(result as ProfileEventRow[]);
    } catch (err) {
      console.error('Failed to load profile events:', err);
    } finally {
      setLoading(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timeRange.start.getTime(), timeRange.end.getTime(), fieldFilters, search]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const columnDefs = useMemo((): ColDef[] => {
    const cols: ColDef[] = [
      {
        headerName: 'Event Time',
        field: 'event_time',
        width: 150,
        sortable: true,
        pinned: 'left',
        cellStyle: { color: '#fca5a5' },
        valueFormatter: (params) => {
          if (!params.value) return '-';
          const date = new Date(params.value);
          return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            hour12: false,
          });
        },
      },
      {
        headerName: 'Query ID',
        field: 'query_id',
        width: 280,
        sortable: true,
        pinned: 'left',
        cellStyle: { color: '#93c5fd' },
      },
    ];

    // Add only visible profile event columns
    ALL_PROFILE_EVENTS.filter(e => visibleEvents.has(e)).forEach((eventName) => {
      cols.push({
        headerName: eventName.replace(/([A-Z])/g, ' $1').trim(),
        field: eventName,
        width: 120,
        sortable: true,
        cellStyle: { textAlign: 'right', color: '#86efac' },
        valueFormatter: (params) => formatValue(params.value, eventName),
        headerTooltip: eventName,
      });
    });

    return cols;
  }, [visibleEvents]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  // Prepare chart data - sorted by time
  const chartData = useMemo(() => {
    return [...data].sort((a, b) =>
      new Date(a.event_time).getTime() - new Date(b.event_time).getTime()
    ).map(row => ({
      ...row,
      time: new Date(row.event_time).toLocaleString('en-US', {
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }),
    }));
  }, [data]);

  const visibleEventsList = useMemo(() =>
    ALL_PROFILE_EVENTS.filter(e => visibleEvents.has(e)),
  [visibleEvents]);

  return (
    <div className="h-full flex flex-col bg-gray-900 border border-gray-700 rounded overflow-hidden">
      {/* AG Grid */}
      <div className="flex-1">
        <AgGridReact
          theme={darkTheme}
          rowData={data}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          getRowId={(params) => params.data.query_id}
        />
      </div>

      {/* Column Selector Modal */}
      {columnSelectorOpen && (
        <div className="fixed inset-0 bg-black/50 flex items-start justify-end z-50 pt-16 pr-4" onClick={() => setColumnSelectorOpen(false)}>
          <div
            className="bg-gray-800 border border-gray-700 rounded-lg shadow-xl w-80 max-h-[70vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
              <h3 className="text-sm font-semibold text-white">Profile Event Columns</h3>
              <button onClick={() => setColumnSelectorOpen(false)} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>
            <div className="overflow-y-auto p-2 flex-1">
              {[...ALL_PROFILE_EVENTS].sort((a, b) => a.localeCompare(b)).map((eventName) => (
                <label
                  key={eventName}
                  className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-700 rounded cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={visibleEvents.has(eventName)}
                    onChange={() => toggleEventVisibility(eventName)}
                    className="rounded border-gray-600 bg-gray-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
                  />
                  <span className="text-xs text-gray-300">{eventName.replace(/([A-Z])/g, ' $1').trim()}</span>
                </label>
              ))}
            </div>
          </div>
        </div>
      )}

      {/* Chart Modal */}
      {chartModalOpen && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={() => setChartModalOpen(false)}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[90vw] h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700 shrink-0">
              <div>
                <h2 className="text-sm font-semibold text-white">Profile Events Chart</h2>
                <p className="text-xs text-gray-400">{visibleEventsList.length} series, {chartData.length} data points</p>
              </div>
              <button onClick={() => setChartModalOpen(false)} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 p-4 overflow-auto">
              {chartData.length === 0 ? (
                <div className="flex items-center justify-center h-full text-gray-500">
                  No data to chart
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 15, right: 15, left: 5, bottom: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
                    <XAxis
                      dataKey="time"
                      tick={{ fontSize: 9, fill: '#9ca3af' }}
                      axisLine={{ stroke: '#374151' }}
                      tickLine={{ stroke: '#374151' }}
                    />
                    <YAxis
                      tick={{ fontSize: 9, fill: '#9ca3af' }}
                      axisLine={{ stroke: '#374151' }}
                      tickLine={{ stroke: '#374151' }}
                      width={60}
                      tickFormatter={(value) => {
                        if (value >= 1000000000) return (value / 1000000000).toFixed(0) + 'B';
                        if (value >= 1000000) return (value / 1000000).toFixed(0) + 'M';
                        if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
                        return value;
                      }}
                    />
                    <Tooltip
                      contentStyle={{
                        backgroundColor: '#1f2937',
                        border: '1px solid #374151',
                        borderRadius: '4px',
                        fontSize: '11px',
                      }}
                      labelStyle={{ color: '#9ca3af' }}
                      formatter={(value: number, name: string) => [
                        formatChartValue(value, name),
                        name.replace(/([A-Z])/g, ' $1').trim()
                      ]}
                    />
                    <Legend
                      wrapperStyle={{ fontSize: '10px', paddingTop: '10px' }}
                      formatter={(value) => value.replace(/([A-Z])/g, ' $1').trim()}
                    />
                    {visibleEventsList.map((eventName, idx) => (
                      <Line
                        key={eventName}
                        type="monotone"
                        dataKey={eventName}
                        stroke={CHART_COLORS[idx % CHART_COLORS.length]}
                        strokeWidth={2}
                        dot={chartData.length < 50}
                        name={eventName}
                      />
                    ))}
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
});
