import { useState, useEffect, useMemo, useCallback } from 'react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams, FirstDataRenderedEvent } from 'ag-grid-community';
import { Eye, X, Loader2 } from 'lucide-react';
import { fetchSystemIndexes, type SystemIndex } from '../services/api';

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

interface IndexesTabProps {
  filters: Record<string, string[]>;
  search: string;
}

export function IndexesTab({ filters, search }: IndexesTabProps) {
  const [indexes, setIndexes] = useState<SystemIndex[]>([]);
  const [loading, setLoading] = useState(false);

  // Modal state
  const [selectedIndex, setSelectedIndex] = useState<SystemIndex | null>(null);

  // Fetch indexes
  useEffect(() => {
    setLoading(true);
    fetchSystemIndexes(filters, search)
      .then(setIndexes)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [filters, search]);

  // Handle viewing index details
  const handleViewDetails = useCallback((idx: SystemIndex) => {
    setSelectedIndex(idx);
  }, []);

  const handleCloseModal = useCallback(() => {
    setSelectedIndex(null);
  }, []);

  // Close modal on Escape
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && selectedIndex) {
        handleCloseModal();
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [selectedIndex, handleCloseModal]);

  const columnDefs = useMemo((): ColDef<SystemIndex>[] => [
    {
      headerName: '',
      field: 'database' as const,
      width: 50,
      sortable: false,
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      cellRenderer: (params: ICellRendererParams<SystemIndex>) => {
        if (!params.data) return null;
        return (
          <button
            onClick={() => handleViewDetails(params.data!)}
            className="p-1.5 hover:bg-gray-700 rounded text-gray-300 hover:text-blue-400 transition-colors"
            title="View index details"
          >
            <Eye className="w-4 h-4" />
          </button>
        );
      },
    },
    {
      headerName: 'Database',
      field: 'database',
      width: 150,
      sortable: true,
      cellStyle: { color: '#93c5fd' },
    },
    {
      headerName: 'Table',
      field: 'table',
      width: 250,
      sortable: true,
      cellStyle: { color: '#93c5fd' },
    },
    {
      headerName: 'Index',
      field: 'name',
      width: 180,
      sortable: true,
      cellStyle: { color: '#fbbf24' },
    },
    {
      headerName: 'Type',
      field: 'type',
      width: 120,
      sortable: true,
      cellStyle: { color: '#a78bfa' },
    },
    {
      headerName: 'Expression',
      field: 'expr',
      width: 250,
      sortable: true,
      cellStyle: { color: '#86efac' },
    },
    {
      headerName: 'Granularity',
      field: 'granularity',
      width: 100,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatNumber(params.value),
    },
    {
      headerName: 'Compressed',
      field: 'data_compressed_bytes',
      width: 110,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatBytes(params.value),
    },
    {
      headerName: 'Uncompressed',
      field: 'data_uncompressed_bytes',
      width: 120,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatBytes(params.value),
    },
    {
      headerName: 'Marks',
      field: 'marks',
      flex: 1,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatNumber(params.value),
    },
  ], [handleViewDetails]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  const onFirstDataRendered = useCallback((event: FirstDataRenderedEvent) => {
    const allColumns = event.api.getColumns();
    if (allColumns) {
      const columnsToSize = allColumns.map(col => col.getColId());
      event.api.autoSizeColumns(columnsToSize);
    }
  }, []);

  return (
    <div className="h-full bg-gray-900 border border-gray-700 rounded overflow-hidden">
      <AgGridReact<SystemIndex>
        theme={darkTheme}
        rowData={indexes}
        columnDefs={columnDefs}
        defaultColDef={defaultColDef}
        loading={loading}
        animateRows={false}
        suppressCellFocus={true}
        enableCellTextSelection={true}
        onFirstDataRendered={onFirstDataRendered}
        getRowId={(params) => `${params.data.database}-${params.data.table}-${params.data.name}`}
      />

      {/* Index Details Modal */}
      {selectedIndex && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[700px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Index Details</h2>
                <p className="text-xs text-gray-400 font-mono">
                  {selectedIndex.database}.{selectedIndex.table} / {selectedIndex.name}
                </p>
              </div>
              <button onClick={handleCloseModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {/* Index Info Grid */}
              <div className="grid grid-cols-3 gap-2 mb-4">
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Type</div>
                  <div className="text-sm font-semibold text-purple-300">{selectedIndex.type}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Granularity</div>
                  <div className="text-sm font-semibold text-green-300">{formatNumber(selectedIndex.granularity)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Marks</div>
                  <div className="text-sm font-semibold text-green-300">{formatNumber(selectedIndex.marks)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Compressed</div>
                  <div className="text-sm font-semibold text-green-300">{formatBytes(selectedIndex.data_compressed_bytes)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Uncompressed</div>
                  <div className="text-sm font-semibold text-green-300">{formatBytes(selectedIndex.data_uncompressed_bytes)}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Savings</div>
                  <div className="text-sm font-semibold text-yellow-300">
                    {selectedIndex.data_uncompressed_bytes > 0
                      ? `${((selectedIndex.data_uncompressed_bytes - selectedIndex.data_compressed_bytes) / selectedIndex.data_uncompressed_bytes * 100).toFixed(0)}%`
                      : '-'}
                  </div>
                </div>
              </div>

              {/* Full Type */}
              {selectedIndex.type_full && selectedIndex.type_full !== selectedIndex.type && (
                <div className="bg-gray-800 p-2 rounded mb-4">
                  <div className="text-xs text-gray-400 mb-1">Full Type</div>
                  <pre className="text-xs text-purple-300 font-mono overflow-x-auto whitespace-pre-wrap">{selectedIndex.type_full}</pre>
                </div>
              )}

              {/* Expression */}
              <div className="bg-gray-800 p-2 rounded">
                <div className="text-xs text-gray-400 mb-1">Index Expression</div>
                <pre className="text-xs text-green-300 font-mono overflow-x-auto whitespace-pre-wrap">{selectedIndex.expr}</pre>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
