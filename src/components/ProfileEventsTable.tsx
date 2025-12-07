import { useState, useEffect, useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef } from 'ag-grid-community';
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
  const { timeRange, fieldFilters, search } = useQueryStore();
  const [data, setData] = useState<ProfileEventRow[]>([]);
  const [loading, setLoading] = useState(true);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const result = await fetchProfileEvents(timeRange, fieldFilters, PROFILE_EVENT_COLUMNS, search);
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
    <div className="h-full bg-gray-900 border border-gray-700 rounded overflow-hidden">
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
  );
}
