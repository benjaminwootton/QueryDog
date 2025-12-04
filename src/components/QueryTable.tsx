import { useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, SortChangedEvent, ICellRendererParams } from 'ag-grid-community';
import { Eye } from 'lucide-react';
import { useQueryStore } from '../stores/queryStore';
import type { QueryLogEntry } from '../types/queryLog';

// Register AG Grid Community modules
ModuleRegistry.registerModules([AllCommunityModule]);

// Create dark theme
const darkTheme = themeAlpine.withParams({
  backgroundColor: '#111827',
  headerBackgroundColor: '#1f2937',
  oddRowBackgroundColor: '#111827',
  rowHoverColor: '#1f2937',
  borderColor: '#374151',
  foregroundColor: '#f3f4f6',
  headerTextColor: '#f3f4f6',
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
  fontSize: 10,
  cellTextColor: '#f3f4f6',
  rowHeight: 28,
  headerHeight: 30,
});

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
}

function ArrayCellRenderer({ value }: { value: string[] }) {
  if (!value || value.length === 0) return <span className="text-gray-500">-</span>;
  if (value.length <= 2) {
    return <span className="text-xs">{value.join(', ')}</span>;
  }
  return (
    <span className="text-xs" title={value.join(', ')}>
      {value.slice(0, 2).join(', ')} +{value.length - 2}
    </span>
  );
}

export function QueryTable() {
  const { entries, columns, setSortField, setSortOrder, setSelectedEntry, loading } = useQueryStore();

  const ActionCellRenderer = useCallback((params: ICellRendererParams<QueryLogEntry>) => {
    return (
      <button
        onClick={() => setSelectedEntry(params.data!)}
        className="p-1 hover:bg-gray-600 rounded"
        title="View ProfileEvents & Settings"
      >
        <Eye className="w-3.5 h-3.5 text-gray-400 hover:text-white" />
      </button>
    );
  }, [setSelectedEntry]);

  const columnDefs: ColDef<QueryLogEntry>[] = useMemo(() => {
    const visibleCols = columns.filter((c) => c.visible);

    const defs: ColDef<QueryLogEntry>[] = [
      {
        headerName: '',
        field: 'query_id' as keyof QueryLogEntry,
        width: 40,
        sortable: false,
        cellRenderer: ActionCellRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      },
    ];

    visibleCols.forEach((col) => {
      const def: ColDef<QueryLogEntry> = {
        headerName: col.headerName,
        field: col.field as keyof QueryLogEntry,
        width: col.width,
        sortable: col.sortable,
        resizable: true,
        headerTooltip: col.comment || col.headerName,
      };

      // Format based on type for array/map fields
      if (col.type.startsWith('Array(')) {
        def.cellRenderer = (params: ICellRendererParams) => <ArrayCellRenderer value={params.value} />;
      }

      // Custom formatters for specific fields
      switch (col.field) {
        case 'event_time':
          def.valueFormatter = (params) => {
            if (!params.value) return '';
            const date = new Date(params.value);
            return date.toLocaleString('en-US', {
              month: 'short',
              day: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
              second: '2-digit',
              hour12: false,
            });
          };
          break;
        case 'query':
          def.tooltipField = 'query';
          def.valueFormatter = (params) => {
            const q = params.value as string;
            return q?.length > 80 ? q.substring(0, 80) + '...' : q;
          };
          break;
        case 'memory_usage':
        case 'read_bytes':
        case 'written_bytes':
        case 'result_bytes':
          def.valueFormatter = (params) => formatBytes(params.value as number);
          def.cellStyle = { textAlign: 'right' };
          break;
        case 'read_rows':
        case 'written_rows':
        case 'result_rows':
          def.valueFormatter = (params) => formatNumber(params.value as number);
          def.cellStyle = { textAlign: 'right' };
          break;
        case 'query_duration_ms':
          def.valueFormatter = (params) => {
            const ms = params.value as number;
            if (ms >= 1000) return (ms / 1000).toFixed(2) + 's';
            return ms + 'ms';
          };
          def.cellStyle = { textAlign: 'right' };
          break;
      }

      // Format bytes for any _bytes field
      if (col.field.endsWith('_bytes') && !def.valueFormatter) {
        def.valueFormatter = (params) => formatBytes(params.value as number);
        def.cellStyle = { textAlign: 'right' };
      }

      // Format rows for any _rows field
      if (col.field.endsWith('_rows') && !def.valueFormatter) {
        def.valueFormatter = (params) => formatNumber(params.value as number);
        def.cellStyle = { textAlign: 'right' };
      }

      defs.push(def);
    });

    return defs;
  }, [columns, ActionCellRenderer]);

  const onSortChanged = useCallback(
    (event: SortChangedEvent) => {
      const sortModel = event.api.getColumnState().find((col) => col.sort);
      if (sortModel) {
        setSortField(sortModel.colId);
        setSortOrder(sortModel.sort === 'asc' ? 'ASC' : 'DESC');
      }
    },
    [setSortField, setSortOrder]
  );

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full w-full">
      <AgGridReact<QueryLogEntry>
        theme={darkTheme}
        rowData={entries}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        onSortChanged={onSortChanged}
        loading={loading}
        animateRows={false}
        suppressCellFocus={true}
        enableCellTextSelection={true}
        tooltipShowDelay={300}
        tooltipInteraction={true}
        getRowId={(params) => String(params.data.query_id) + String(params.data.event_time)}
      />
    </div>
  );
}
