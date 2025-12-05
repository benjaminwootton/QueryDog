import { useState, useEffect, useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
import { RefreshCw } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import { fetchProfileEvents } from '../services/api';

ModuleRegistry.registerModules([AllCommunityModule]);

const darkTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#d1d5db',
  headerTextColor: '#f3f4f6',
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  fontSize: 9,
  headerFontSize: 11,
  headerFontWeight: 600,
  cellTextColor: '#d1d5db',
  rowHeight: 28,
  headerHeight: 30,
});

// Hardcoded list of profile events to display as columns
const PROFILE_EVENT_COLUMNS = [
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

interface ProfileEventRow {
  event_time: string;
  query_id: string;
  [key: string]: string | number;
}

export function ProfileEventsTable() {
  const { timeRange, fieldFilters } = useQueryStore();
  const [data, setData] = useState<ProfileEventRow[]>([]);
  const [loading, setLoading] = useState(true);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchProfileEvents(timeRange, fieldFilters, PROFILE_EVENT_COLUMNS);
      setData(result as ProfileEventRow[]);
    } catch (err) {
      console.error('Failed to load profile events:', err);
    } finally {
      setLoading(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [timeRange.start.getTime(), timeRange.end.getTime(), fieldFilters]);

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
        valueFormatter: (params) => {
          if (!params.value) return '-';
          const date = new Date(params.value);
          return date.toLocaleString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
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
      },
    ];

    // Add profile event columns
    PROFILE_EVENT_COLUMNS.forEach((eventName) => {
      cols.push({
        headerName: eventName.replace(/([A-Z])/g, ' $1').trim(),
        field: eventName,
        width: 120,
        sortable: true,
        cellStyle: { textAlign: 'right' },
        valueFormatter: (params) => formatValue(params.value, eventName),
        headerTooltip: eventName,
      });
    });

    return cols;
  }, []);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full flex flex-col bg-gray-900 border border-gray-700 rounded overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 bg-gray-900/50 border-b border-gray-700">
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400">{data.length.toLocaleString()} rows</span>
        </div>
        <button
          onClick={loadData}
          disabled={loading}
          className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
          title="Refresh"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
        </button>
      </div>
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
    </div>
  );
}
