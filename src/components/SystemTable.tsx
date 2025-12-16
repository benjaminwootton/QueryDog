import { useMemo, useCallback, useEffect, useState, useRef, useImperativeHandle, forwardRef } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, SortChangedEvent, ICellRendererParams, FirstDataRenderedEvent } from 'ag-grid-community';
import { Settings, X, Eye, RefreshCw } from 'lucide-react';
import type { ColumnMetadata } from '../types/queryLog';
import { createColumnsFromMetadata, type ColumnConfig } from '../types/queryLog';
import { useQueryStore } from '../stores/queryStore';

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

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toLocaleString();
}

function ArrayCellRenderer({ value }: { value: unknown[] }) {
  if (!value || value.length === 0) return <span className="text-gray-500">-</span>;
  const strValue = value.map(v => String(v));
  if (strValue.length <= 2) {
    return <span className="text-xs">{strValue.join(', ')}</span>;
  }
  return (
    <span className="text-xs" title={strValue.join(', ')}>
      {strValue.slice(0, 2).join(', ')} +{strValue.length - 2}
    </span>
  );
}

export interface SystemTableRef {
  columns: ColumnConfig[];
  toggleColumnVisibility: (field: string) => void;
  columnSelectorOpen: boolean;
  setColumnSelectorOpen: (open: boolean) => void;
}

interface SystemTableProps {
  title?: string;
  fetchData: (filters?: Record<string, string[]>) => Promise<Record<string, unknown>[]>;
  fetchColumns?: () => Promise<ColumnMetadata[]>;
  defaultColumns?: ColumnConfig[];
  defaultVisibleFields?: string[];
  filters?: Record<string, string[]>;
  getRowId?: (data: Record<string, unknown>) => string;
  onSortChange?: (field: string, order: 'ASC' | 'DESC') => void;
  onColumnsChange?: (columns: ColumnConfig[]) => void;
  hideTitle?: boolean;
  hideHeader?: boolean;
  showActionColumn?: boolean;
  onRowAction?: (data: Record<string, unknown>) => void;
  columnWidthOverrides?: Record<string, number>;
}

function SystemTableInner({
  title,
  fetchData,
  fetchColumns,
  defaultColumns,
  defaultVisibleFields,
  filters,
  getRowId,
  onSortChange,
  onColumnsChange,
  hideTitle = false,
  hideHeader = true,
  showActionColumn = false,
  onRowAction,
  columnWidthOverrides,
}: SystemTableProps, ref: React.ForwardedRef<SystemTableRef>) {
  const [data, setData] = useState<Record<string, unknown>[]>([]);
  const [columns, setColumns] = useState<ColumnConfig[]>(defaultColumns || []);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [columnSelectorOpen, setColumnSelectorOpen] = useState(false);
  const columnsLoadedRef = useRef(false);

  // Load columns
  useEffect(() => {
    if (columnsLoadedRef.current || !fetchColumns) return;
    columnsLoadedRef.current = true;

    fetchColumns()
      .then((metadata) => {
        const cols = createColumnsFromMetadata(metadata, 'query_log');
        // If defaultVisibleFields is provided, only show those and sort by that order
        if (defaultVisibleFields && defaultVisibleFields.length > 0) {
          const visibleSet = new Set(defaultVisibleFields);
          const sortedCols = cols
            .map(c => ({ ...c, visible: visibleSet.has(c.field) }))
            .sort((a, b) => {
              const aIndex = defaultVisibleFields.indexOf(a.field);
              const bIndex = defaultVisibleFields.indexOf(b.field);
              if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
              if (aIndex !== -1) return -1;
              if (bIndex !== -1) return 1;
              return 0;
            });
          setColumns(sortedCols);
        } else {
          setColumns(cols.map(c => ({ ...c, visible: true })));
        }
      })
      .catch((err) => console.error('Failed to load columns:', err));
  }, [fetchColumns, defaultVisibleFields]);

  // Notify parent when columns change
  useEffect(() => {
    if (onColumnsChange && columns.length > 0) {
      onColumnsChange(columns);
    }
  }, [columns, onColumnsChange]);

  // Load data
  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await fetchData(filters);
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load data');
    } finally {
      setLoading(false);
    }
  }, [fetchData, filters]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Respond to global refresh trigger
  const globalRefreshTrigger = useQueryStore((state) => state.globalRefreshTrigger);
  const initialRenderRef = useRef(true);
  useEffect(() => {
    // Skip the initial render (data is already loaded above)
    if (initialRenderRef.current) {
      initialRenderRef.current = false;
      return;
    }
    loadData();
  }, [globalRefreshTrigger, loadData]);

  const toggleColumnVisibility = useCallback((field: string) => {
    setColumns(cols => cols.map(c => c.field === field ? { ...c, visible: !c.visible } : c));
  }, []);

  // Expose column selector functionality via ref
  useImperativeHandle(ref, () => ({
    columns,
    toggleColumnVisibility,
    columnSelectorOpen,
    setColumnSelectorOpen,
  }), [columns, toggleColumnVisibility, columnSelectorOpen]);

  const ActionCellRenderer = useCallback((params: ICellRendererParams) => {
    if (!onRowAction) return null;
    return (
      <button
        onClick={() => onRowAction(params.data)}
        className="p-1 hover:bg-gray-600 rounded"
        title="View Details"
      >
        <Eye className="w-3.5 h-3.5 text-gray-400 hover:text-white" />
      </button>
    );
  }, [onRowAction]);

  const columnDefs: ColDef[] = useMemo(() => {
    const visibleCols = columns.filter(c => c.visible);
    const defs: ColDef[] = [];

    // Add action column if enabled
    if (showActionColumn && onRowAction) {
      defs.push({
        headerName: '',
        field: '_action',
        width: 40,
        sortable: false,
        cellRenderer: ActionCellRenderer,
        cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      });
    }

    visibleCols.forEach((col) => {
      const def: ColDef = {
        headerName: col.headerName,
        field: col.field,
        width: columnWidthOverrides?.[col.field] ?? col.width,
        sortable: col.sortable,
        resizable: true,
        headerTooltip: col.comment || col.headerName,
      };

      if (col.type.startsWith('Array(')) {
        def.cellRenderer = (params: ICellRendererParams) => <ArrayCellRenderer value={params.value} />;
      }

      // Table name styling - light blue
      if (col.field === 'table') {
        def.cellStyle = { color: '#60a5fa' };
      }

      // Format bytes fields - light green
      if (col.field.includes('bytes') || col.field.includes('size') || col.field.includes('memory')) {
        def.valueFormatter = (params) => params.value != null ? formatBytes(Number(params.value)) : '-';
        def.cellStyle = { textAlign: 'right', color: '#86efac' };
      }

      // Format number fields - light green
      if (col.field.includes('rows') || col.field.includes('count') || col.field.includes('num_')) {
        def.valueFormatter = (params) => params.value != null ? formatNumber(Number(params.value)) : '-';
        def.cellStyle = { textAlign: 'right', color: '#86efac' };
      }

      // Format percentage fields - yellow
      if (col.field.includes('pct') || col.field.includes('percent') || col.field.includes('ratio')) {
        def.valueFormatter = (params) => {
          if (params.value == null || Number(params.value) < 0) return '-';
          return `${Number(params.value).toFixed(0)}%`;
        };
        def.cellStyle = { textAlign: 'right', color: '#fde047' };
      }

      // Format time fields
      if (col.field.includes('time') && col.type.includes('DateTime')) {
        def.valueFormatter = (params) => {
          if (!params.value) return '-';
          const date = new Date(params.value);
          return date.toLocaleString('en-US', {
            month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
          });
        };
      }

      defs.push(def);
    });

    return defs;
  }, [columns, showActionColumn, onRowAction, ActionCellRenderer, columnWidthOverrides]);

  const onSortChanged = useCallback((event: SortChangedEvent) => {
    if (!onSortChange) return;
    const sortModel = event.api.getColumnState().find(col => col.sort);
    if (sortModel) {
      onSortChange(sortModel.colId, sortModel.sort === 'asc' ? 'ASC' : 'DESC');
    }
  }, [onSortChange]);

  // Auto-size columns on first data render, excluding query columns
  const onFirstDataRendered = useCallback((event: FirstDataRenderedEvent) => {
    const columnsToSkip = ['query', 'example_query', 'normalized_query_hash'];
    const allColumns = event.api.getColumns();
    if (allColumns) {
      const columnsToSize = allColumns
        .filter(col => !columnsToSkip.includes(col.getColId()))
        .map(col => col.getColId());
      event.api.autoSizeColumns(columnsToSize);
    }
  }, []);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  return (
    <div className="h-full flex flex-col bg-gray-900 border border-gray-700 rounded overflow-hidden">
      {!hideHeader && (
        <div className="flex items-center justify-between px-3 py-2 bg-gray-900/50 border-b border-gray-700">
          <div className="flex items-center gap-3">
            {!hideTitle && title && <h2 className="text-sm font-semibold text-white">{title}</h2>}
            <span className="text-xs text-gray-400">{data.length.toLocaleString()} rows</span>
            {error && <span className="text-xs text-red-400">{error}</span>}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={loadData}
              disabled={loading}
              className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50"
              title="Refresh"
            >
              <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
            {columns.length > 0 && (
              <div className="relative">
                <button
                  onClick={() => setColumnSelectorOpen(!columnSelectorOpen)}
                  className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
                  title="Configure columns"
                >
                  <Settings className="w-3.5 h-3.5" />
                </button>
                {columnSelectorOpen && (
                  <>
                    <div className="fixed inset-0 z-40" onClick={() => setColumnSelectorOpen(false)} />
                    <div className="absolute right-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[200px]">
                      <div className="flex items-center justify-between p-2 border-b border-gray-700">
                        <span className="text-xs font-semibold text-gray-300">Columns</span>
                        <button onClick={() => setColumnSelectorOpen(false)} className="text-gray-400 hover:text-white">
                          <X className="w-3 h-3" />
                        </button>
                      </div>
                      <div className="max-h-64 overflow-y-auto p-1">
                        {columns.map((col) => (
                          <label key={col.field} className="flex items-center gap-2 p-1.5 hover:bg-gray-700 rounded cursor-pointer">
                            <input
                              type="checkbox"
                              checked={col.visible}
                              onChange={() => toggleColumnVisibility(col.field)}
                              className="w-3 h-3 rounded border-gray-500 bg-gray-700 text-blue-500"
                            />
                            <span className="text-xs text-gray-300">{col.headerName}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      )}
      <div className="flex-1">
        <AgGridReact
          theme={darkTheme}
          rowData={data}
          columnDefs={columnDefs}
          defaultColDef={defaultColDef}
          onSortChanged={onSortChanged}
          onFirstDataRendered={onFirstDataRendered}
          loading={loading}
          animateRows={false}
          suppressCellFocus={true}
          enableCellTextSelection={true}
          tooltipShowDelay={300}
          tooltipInteraction={true}
          getRowId={getRowId ? (params) => getRowId(params.data) : undefined}
        />
      </div>
    </div>
  );
}

export const SystemTable = forwardRef(SystemTableInner);
