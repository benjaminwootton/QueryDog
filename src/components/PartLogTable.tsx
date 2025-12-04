import { useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, SortChangedEvent, ICellRendererParams } from 'ag-grid-community';
import { useQueryStore } from '../stores/queryStore';
import type { PartLogEntry } from '../types/queryLog';

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

export function PartLogTable() {
  const { partLogEntries, partLogColumns, setPartLogSortField, setPartLogSortOrder, partLogLoading } = useQueryStore();

  const columnDefs: ColDef<PartLogEntry>[] = useMemo(() => {
    const visibleCols = partLogColumns.filter((c) => c.visible);

    const defs: ColDef<PartLogEntry>[] = [];

    visibleCols.forEach((col) => {
      const def: ColDef<PartLogEntry> = {
        headerName: col.headerName,
        field: col.field as keyof PartLogEntry,
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
        case 'size_in_bytes':
        case 'bytes_uncompressed':
        case 'bytes_on_disk':
        case 'peak_memory_usage':
          def.valueFormatter = (params) => formatBytes(params.value as number);
          def.cellStyle = { textAlign: 'right' };
          break;
        case 'rows':
        case 'rows_where_condition':
          def.valueFormatter = (params) => formatNumber(params.value as number);
          def.cellStyle = { textAlign: 'right' };
          break;
        case 'duration_ms':
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

      defs.push(def);
    });

    return defs;
  }, [partLogColumns]);

  const onSortChanged = useCallback(
    (event: SortChangedEvent) => {
      const sortModel = event.api.getColumnState().find((col) => col.sort);
      if (sortModel) {
        setPartLogSortField(sortModel.colId);
        setPartLogSortOrder(sortModel.sort === 'asc' ? 'ASC' : 'DESC');
      }
    },
    [setPartLogSortField, setPartLogSortOrder]
  );

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full w-full">
      <AgGridReact<PartLogEntry>
        theme={darkTheme}
        rowData={partLogEntries}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        onSortChanged={onSortChanged}
        loading={partLogLoading}
        animateRows={false}
        suppressCellFocus={true}
        enableCellTextSelection={true}
        tooltipShowDelay={300}
        tooltipInteraction={true}
        getRowId={(params) => String(params.data.event_time) + String(params.data.part_name)}
      />
    </div>
  );
}
