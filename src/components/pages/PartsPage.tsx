import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { HardDrive, ChevronLeft, ChevronRight, Layers, Grid3X3, BarChart2, Settings, X, Eye, Search, Sparkles, Zap } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams, FirstDataRenderedEvent } from 'ag-grid-community';
import { SystemTable, type SystemTableRef } from '../SystemTable';
import { PartsFilterPanel } from '../PartsFilterPanel';
import { PartsHistogramsTab } from '../PartsHistogramsTab';
import { ProjectionsTab } from '../ProjectionsTab';
import { IndexesTab } from '../IndexesTab';
import { fetchParts, fetchPartsColumns, fetchPartsCount, fetchPartitionsSummary, fetchPartitionsSummaryColumns, fetchPartitionsSummaryCount, fetchGroupedParts, fetchTablePartitions, fetchPartitionParts, fetchTableCompression, fetchBrowserColumns, type GroupedPartsEntry, type TablePartitionEntry, type PartitionPartEntry, type ColumnCompressionEntry, type BrowserColumn } from '../../services/api';
import { useQueryStore } from '../../stores/queryStore';

ModuleRegistry.registerModules([AllCommunityModule]);

// Create dark theme
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

type PartsTab = 'parts' | 'partitions' | 'grouped' | 'projections' | 'indexes' | 'histograms';

const PARTS_DEFAULT_VISIBLE_FIELDS = [
  'database',
  'table',
  'partition_id',
  'name',
  'rows',
  'bytes_on_disk',
  'data_compressed_bytes',
  'modification_time',
  'active',
  'marks',
];

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

export function PartsPage() {
  const {
    partsTotalCount,
    partsPageSize,
    partsCurrentPage,
    setPartsTotalCount,
    setPartsCurrentPage,
    partitionsTotalCount,
    partitionsPageSize,
    partitionsCurrentPage,
    setPartitionsTotalCount,
    setPartitionsCurrentPage,
  } = useQueryStore();
  const [activeTab, setActiveTab] = useState<PartsTab>('grouped');
  const [groupedData, setGroupedData] = useState<GroupedPartsEntry[]>([]);
  const [groupedLoading, setGroupedLoading] = useState(false);
  const partsTableRef = useRef<SystemTableRef>(null);
  const partitionsTableRef = useRef<SystemTableRef>(null);
  const [partsColumnSelectorOpen, setPartsColumnSelectorOpen] = useState(false);
  const [partitionsColumnSelectorOpen, setPartitionsColumnSelectorOpen] = useState(false);

  // Search state
  const [partsSearch, setPartsSearch] = useState('');
  const [localSearch, setLocalSearch] = useState('');

  // Modal state for partition details
  const [selectedTable, setSelectedTable] = useState<{ database: string; table: string } | null>(null);
  const [partitionDetails, setPartitionDetails] = useState<TablePartitionEntry[]>([]);
  const [partitionDetailsLoading, setPartitionDetailsLoading] = useState(false);

  // Schema state for table details modal
  const [schemaColumns, setSchemaColumns] = useState<BrowserColumn[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(false);

  // Modal state for partition parts (parts within a partition)
  const [selectedPartition, setSelectedPartition] = useState<{ database: string; table: string; partitionId: string } | null>(null);
  const [partitionParts, setPartitionParts] = useState<PartitionPartEntry[]>([]);
  const [partitionPartsLoading, setPartitionPartsLoading] = useState(false);

  // Modal state for single part details
  const [selectedPart, setSelectedPart] = useState<Record<string, unknown> | null>(null);

  // Modal state for compression details
  const [compressionTable, setCompressionTable] = useState<{ database: string; table: string } | null>(null);
  const [compressionDetails, setCompressionDetails] = useState<ColumnCompressionEntry[]>([]);
  const [compressionLoading, setCompressionLoading] = useState(false);

  // Close modals on Escape key
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (compressionTable) setCompressionTable(null);
        else if (selectedPart) setSelectedPart(null);
        else if (selectedPartition) setSelectedPartition(null);
        else if (selectedTable) setSelectedTable(null);
      }
    };
    document.addEventListener('keydown', handleEscape);
    return () => document.removeEventListener('keydown', handleEscape);
  }, [selectedTable, selectedPartition, selectedPart, compressionTable]);

  // Parts pagination
  const partsTotalPages = Math.ceil(partsTotalCount / partsPageSize);
  const partsStartRow = partsCurrentPage * partsPageSize + 1;
  const partsEndRow = Math.min((partsCurrentPage + 1) * partsPageSize, partsTotalCount);

  // Partitions pagination
  const partitionsTotalPages = Math.ceil(partitionsTotalCount / partitionsPageSize);
  const partitionsStartRow = partitionsCurrentPage * partitionsPageSize + 1;
  const partitionsEndRow = Math.min((partitionsCurrentPage + 1) * partitionsPageSize, partitionsTotalCount);

  const [partsFilters, setPartsFilters] = useState<Record<string, string[]>>({ active: ['1'] });

  const handlePartsFilterChange = useCallback((field: string, values: string[]) => {
    setPartsFilters((prev) => ({ ...prev, [field]: values }));
  }, []);

  const handleClearPartsFilter = useCallback((field: string) => {
    setPartsFilters((prev) => {
      const { [field]: _, ...rest } = prev;
      return rest;
    });
  }, []);

  const handleClearAllPartsFilters = useCallback(() => {
    setPartsFilters({ active: ['1'] });
    setPartsSearch('');
    setLocalSearch('');
  }, []);

  // Debounce search input
  useEffect(() => {
    const timer = setTimeout(() => {
      setPartsSearch(localSearch);
    }, 300);
    return () => clearTimeout(timer);
  }, [localSearch]);

  // Navigate to partitions tab with search for a specific partition
  const handlePartitionClick = useCallback((database: string, table: string, partitionId: string) => {
    // Set filters for database, table, and partition
    setPartsFilters({ database: [database], table: [table], partition_id: [partitionId] });
    // Clear search
    setLocalSearch('');
    setPartsSearch('');
    // Switch to partitions tab
    setActiveTab('partitions');
    // Close modal
    setSelectedTable(null);
    setPartitionDetails([]);
  }, []);

  const fetchPartsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchParts(
      'modification_time',
      'DESC',
      filters || partsFilters,
      partsPageSize,
      partsCurrentPage * partsPageSize,
      partsSearch
    ),
    [partsFilters, partsPageSize, partsCurrentPage, partsSearch]
  );

  const partsFilterCount = Object.values(partsFilters).filter((v) => v.length > 0).length;

  // Partitions uses aggregated endpoint (grouped by partition_id)
  const fetchPartitionsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchPartitionsSummary(
      'latest_modification',
      'DESC',
      filters || partsFilters,
      partitionsPageSize,
      partitionsCurrentPage * partitionsPageSize,
      partsSearch
    ),
    [partsFilters, partitionsPageSize, partitionsCurrentPage, partsSearch]
  );

  // Fetch counts when filters or search change
  useEffect(() => {
    fetchPartsCount(partsFilters, partsSearch).then(setPartsTotalCount);
  }, [partsFilters, partsSearch, setPartsTotalCount]);

  useEffect(() => {
    fetchPartitionsSummaryCount(partsFilters, partsSearch).then(setPartitionsTotalCount);
  }, [partsFilters, partsSearch, setPartitionsTotalCount]);

  // Fetch grouped data
  useEffect(() => {
    if (activeTab === 'grouped') {
      setGroupedLoading(true);
      fetchGroupedParts(partsFilters, partsSearch)
        .then(setGroupedData)
        .finally(() => setGroupedLoading(false));
    }
  }, [activeTab, partsFilters, partsSearch]);

  // Reset page when filters or search change
  useEffect(() => {
    setPartsCurrentPage(0);
    setPartitionsCurrentPage(0);
  }, [partsFilters, partsSearch, setPartsCurrentPage, setPartitionsCurrentPage]);

  // Handle viewing partition details
  const handleViewPartitions = useCallback(async (database: string, table: string) => {
    setSelectedTable({ database, table });
    setPartitionDetailsLoading(true);
    setSchemaLoading(true);
    try {
      const [partitionData, schemaData] = await Promise.all([
        fetchTablePartitions(database, table),
        fetchBrowserColumns(database, table),
      ]);
      setPartitionDetails(partitionData);
      setSchemaColumns(schemaData);
    } catch (error) {
      console.error('Error fetching table details:', error);
      setPartitionDetails([]);
      setSchemaColumns([]);
    } finally {
      setPartitionDetailsLoading(false);
      setSchemaLoading(false);
    }
  }, []);

  const handleCloseModal = useCallback(() => {
    setSelectedTable(null);
    setPartitionDetails([]);
    setSchemaColumns([]);
  }, []);

  // Handle viewing parts for a specific partition
  const handleViewPartitionParts = useCallback(async (database: string, table: string, partitionId: string) => {
    setSelectedPartition({ database, table, partitionId });
    setPartitionPartsLoading(true);
    try {
      const data = await fetchPartitionParts(database, table, partitionId);
      setPartitionParts(data);
    } catch (error) {
      console.error('Error fetching partition parts:', error);
      setPartitionParts([]);
    } finally {
      setPartitionPartsLoading(false);
    }
  }, []);

  const handleClosePartsModal = useCallback(() => {
    setSelectedPartition(null);
    setPartitionParts([]);
  }, []);

  // Handle viewing single part details
  const handleViewPartDetails = useCallback((data: Record<string, unknown>) => {
    setSelectedPart(data);
  }, []);

  const handleClosePartDetails = useCallback(() => {
    setSelectedPart(null);
  }, []);

  // Handle viewing compression details for a table
  const handleViewCompression = useCallback(async (database: string, table: string) => {
    setCompressionTable({ database, table });
    setCompressionLoading(true);
    try {
      const data = await fetchTableCompression(database, table);
      setCompressionDetails(data);
    } catch (error) {
      console.error('Error fetching compression details:', error);
      setCompressionDetails([]);
    } finally {
      setCompressionLoading(false);
    }
  }, []);

  const handleCloseCompressionModal = useCallback(() => {
    setCompressionTable(null);
    setCompressionDetails([]);
  }, []);

  // Drill down from partition parts modal to Parts tab
  const handleDrillDownToPart = useCallback((database: string, table: string, partitionId: string, partName: string) => {
    // Close the modal
    setSelectedPartition(null);
    setPartitionParts([]);
    // Set filters including name
    setPartsFilters({ database: [database], table: [table], partition_id: [partitionId], name: [partName] });
    // Clear search
    setPartsSearch('');
    setLocalSearch('');
    // Navigate to Parts tab
    setActiveTab('parts');
  }, []);

  // Column definitions for grouped table
  const groupedColumnDefs = useMemo((): ColDef<GroupedPartsEntry>[] => [
    {
      headerName: '',
      field: 'database' as const,
      width: 50,
      sortable: false,
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
      cellRenderer: (params: ICellRendererParams<GroupedPartsEntry>) => {
        if (!params.data) return null;
        return (
          <button
            onClick={() => handleViewPartitions(params.data!.database, params.data!.table)}
            className="p-1.5 hover:bg-gray-700 rounded text-gray-300 hover:text-blue-400 transition-colors"
            title="View partition details"
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
      width: 400,
      sortable: true,
      cellStyle: { color: '#93c5fd' },
    },
    {
      headerName: 'Partitions',
      field: 'partition_count',
      width: 100,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatNumber(params.value),
    },
    {
      headerName: 'Parts',
      field: 'part_count',
      width: 100,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatNumber(params.value),
    },
    {
      headerName: 'Total Rows',
      field: 'total_rows',
      width: 120,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatNumber(params.value),
    },
    {
      headerName: 'Total Size',
      field: 'total_bytes',
      width: 120,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatBytes(params.value),
    },
    {
      headerName: 'Uncompressed',
      field: 'uncompressed_bytes',
      width: 120,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatBytes(params.value),
    },
    {
      headerName: 'Compressed',
      field: 'compressed_bytes',
      width: 110,
      sortable: true,
      cellStyle: { textAlign: 'right', color: '#86efac' },
      valueFormatter: (params) => formatBytes(params.value),
    },
    {
      headerName: 'Savings',
      field: 'savings_pct',
      width: 90,
      sortable: true,
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: '6px' },
      cellRenderer: (params: ICellRendererParams<GroupedPartsEntry>) => {
        if (!params.data) return null;
        const pct = params.value as number;
        return (
          <div className="flex items-center gap-1.5">
            <span className="text-yellow-300">{pct != null && pct >= 0 ? `${pct.toFixed(0)}%` : '-'}</span>
            <button
              onClick={(e) => {
                e.stopPropagation();
                handleViewCompression(params.data!.database, params.data!.table);
              }}
              className="p-0.5 hover:bg-gray-700 rounded text-gray-400 hover:text-blue-400 transition-colors"
              title="View column compression details"
            >
              <Search className="w-3 h-3" />
            </button>
          </div>
        );
      },
    },
    {
      headerName: 'Last Modified',
      field: 'last_modification_time',
      flex: 1,
      sortable: true,
      cellStyle: { color: '#fca5a5' },
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
  ], [handleViewPartitions, handleViewCompression]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  // Auto-size columns on first data render
  const onFirstDataRendered = useCallback((event: FirstDataRenderedEvent) => {
    const allColumns = event.api.getColumns();
    if (allColumns) {
      const columnsToSize = allColumns.map(col => col.getColId());
      event.api.autoSizeColumns(columnsToSize);
    }
  }, []);

  const tabs: { id: PartsTab; label: string; icon: typeof HardDrive }[] = [
    { id: 'grouped', label: 'Tables', icon: Grid3X3 },
    { id: 'partitions', label: 'Partitions', icon: Layers },
    { id: 'parts', label: 'Parts', icon: HardDrive },
    { id: 'projections', label: 'Projections', icon: Sparkles },
    { id: 'indexes', label: 'Indexes', icon: Zap },
    { id: 'histograms', label: 'Histograms', icon: BarChart2 },
  ];

  // Column selector component for Parts/Partitions
  const renderColumnSelector = (
    isOpen: boolean,
    setIsOpen: (open: boolean) => void,
    tableRef: React.RefObject<SystemTableRef | null>
  ) => {
    const columns = tableRef.current?.columns || [];
    const toggleVisibility = tableRef.current?.toggleColumnVisibility;

    if (columns.length === 0) return null;

    return (
      <div className="relative">
        <button
          onClick={() => setIsOpen(!isOpen)}
          className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
          title="Configure columns"
        >
          <Settings className="w-3.5 h-3.5" />
        </button>
        {isOpen && (
          <>
            <div className="fixed inset-0 z-40" onClick={() => setIsOpen(false)} />
            <div className="absolute right-0 top-full mt-1 bg-gray-800 border border-gray-600 rounded shadow-lg z-50 min-w-[200px]">
              <div className="flex items-center justify-between p-2 border-b border-gray-700">
                <span className="text-xs font-semibold text-gray-300">Columns</span>
                <button onClick={() => setIsOpen(false)} className="text-gray-400 hover:text-white">
                  <X className="w-3 h-3" />
                </button>
              </div>
              <div className="max-h-64 overflow-y-auto p-1">
                {columns.map((col) => (
                  <label key={col.field} className="flex items-center gap-2 p-1.5 hover:bg-gray-700 rounded cursor-pointer">
                    <input
                      type="checkbox"
                      checked={col.visible}
                      onChange={() => toggleVisibility?.(col.field)}
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
    );
  };

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <PartsFilterPanel
            filters={partsFilters}
            onFilterChange={handlePartsFilterChange}
            onClearFilter={handleClearPartsFilter}
            onClearAll={handleClearAllPartsFilters}
          />
          {/* Search bar */}
          <div className="relative flex items-center">
            <Search className="absolute left-2 w-3 h-3 text-gray-400" />
            <input
              type="text"
              value={localSearch}
              onChange={(e) => setLocalSearch(e.target.value)}
              placeholder="Search table, database, partition..."
              className="bg-gray-800 border border-gray-600 rounded pl-6 pr-6 py-0.5 text-white text-xs w-64"
            />
            {localSearch && (
              <button
                onClick={() => { setLocalSearch(''); setPartsSearch(''); }}
                className="absolute right-2 text-gray-400 hover:text-white"
              >
                <X className="w-3 h-3" />
              </button>
            )}
          </div>
        </div>
        <div className="flex items-center gap-4 text-xs">
          {(partsFilterCount > 0 || partsSearch) && (
            <span className="text-blue-400">
              {partsFilterCount > 0 && `${partsFilterCount} filter${partsFilterCount > 1 ? 's' : ''}`}
              {partsFilterCount > 0 && partsSearch && ', '}
              {partsSearch && 'search active'}
            </span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => {
              setActiveTab(id);
              setPartsColumnSelectorOpen(false);
              setPartitionsColumnSelectorOpen(false);
              // Set active=1 filter when switching to Parts tab
              if (id === 'parts' && !partsFilters.active?.includes('1')) {
                setPartsFilters({ ...partsFilters, active: ['1'] });
              }
            }}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === id
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Icon className="w-3 h-3" />
            {label}
          </button>
        ))}
        {/* Column selector and pagination on the right */}
        <div className="ml-auto flex items-center gap-3">
          {activeTab === 'grouped' && (
            <span className="text-gray-400 text-xs">
              {groupedData.length.toLocaleString()} tables
            </span>
          )}
          {activeTab === 'parts' && (
            <>
              {partsTotalPages > 1 && (
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-gray-400">
                    {partsStartRow.toLocaleString()}-{partsEndRow.toLocaleString()} of {partsTotalCount.toLocaleString()}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPartsCurrentPage(partsCurrentPage - 1)}
                      disabled={partsCurrentPage === 0}
                      className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Previous page"
                    >
                      <ChevronLeft className="w-4 h-4" />
                    </button>
                    <span className="text-gray-300 px-2">
                      {partsCurrentPage + 1} / {partsTotalPages}
                    </span>
                    <button
                      onClick={() => setPartsCurrentPage(partsCurrentPage + 1)}
                      disabled={partsCurrentPage >= partsTotalPages - 1}
                      className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Next page"
                    >
                      <ChevronRight className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              )}
              {renderColumnSelector(partsColumnSelectorOpen, setPartsColumnSelectorOpen, partsTableRef)}
            </>
          )}
          {activeTab === 'partitions' && (
            <>
              {partitionsTotalPages > 1 && (
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-gray-400">
                    {partitionsStartRow.toLocaleString()}-{partitionsEndRow.toLocaleString()} of {partitionsTotalCount.toLocaleString()}
                  </span>
                  <div className="flex items-center gap-1">
                    <button
                      onClick={() => setPartitionsCurrentPage(partitionsCurrentPage - 1)}
                      disabled={partitionsCurrentPage === 0}
                      className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Previous page"
                    >
                      <ChevronLeft className="w-4 h-4" />
                    </button>
                    <span className="text-gray-300 px-2">
                      {partitionsCurrentPage + 1} / {partitionsTotalPages}
                    </span>
                    <button
                      onClick={() => setPartitionsCurrentPage(partitionsCurrentPage + 1)}
                      disabled={partitionsCurrentPage >= partitionsTotalPages - 1}
                      className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                      title="Next page"
                    >
                      <ChevronRight className="w-4 h-4" />
                    </button>
                  </div>
                </div>
              )}
              {renderColumnSelector(partitionsColumnSelectorOpen, setPartitionsColumnSelectorOpen, partitionsTableRef)}
            </>
          )}
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'grouped' && (
          <div className="h-full bg-gray-900 border border-gray-700 rounded overflow-hidden">
            <AgGridReact<GroupedPartsEntry>
              theme={darkTheme}
              rowData={groupedData}
              columnDefs={groupedColumnDefs}
              defaultColDef={defaultColDef}
              loading={groupedLoading}
              animateRows={false}
              suppressCellFocus={true}
              enableCellTextSelection={true}
              onFirstDataRendered={onFirstDataRendered}
              getRowId={(params) => `${params.data.database}-${params.data.table}`}
            />
          </div>
        )}
        {activeTab === 'parts' && (
          <SystemTable
            ref={partsTableRef}
            fetchData={fetchPartsWithFilters}
            fetchColumns={fetchPartsColumns}
            defaultVisibleFields={PARTS_DEFAULT_VISIBLE_FIELDS}
            filters={partsFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.name}`}
            hideTitle
            showActionColumn
            onRowAction={handleViewPartDetails}
          />
        )}
        {activeTab === 'partitions' && (
          <SystemTable
            ref={partitionsTableRef}
            fetchData={fetchPartitionsWithFilters}
            fetchColumns={fetchPartitionsSummaryColumns}
            defaultVisibleFields={['database', 'table', 'partition', 'parts_count', 'total_rows', 'total_bytes', 'total_compressed', 'total_uncompressed', 'savings_pct', 'latest_modification', 'min_block', 'max_block']}
            filters={partsFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.partition_id}`}
            hideTitle
            showActionColumn
            onRowAction={(data) => handleViewPartitionParts(String(data.database), String(data.table), String(data.partition_id))}
          />
        )}
        {activeTab === 'projections' && (
          <ProjectionsTab
            filters={partsFilters}
            search={partsSearch}
          />
        )}
        {activeTab === 'indexes' && (
          <IndexesTab
            filters={partsFilters}
            search={partsSearch}
          />
        )}
        {activeTab === 'histograms' && (
          <PartsHistogramsTab
            filters={partsFilters}
            onFilterChange={handlePartsFilterChange}
          />
        )}
      </div>

      {/* Partition Details Modal */}
      {selectedTable && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[1100px] max-h-[90vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Table Details</h2>
                <p className="text-xs text-gray-400 font-mono">{selectedTable.database}.{selectedTable.table}</p>
              </div>
              <button onClick={handleCloseModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {partitionDetailsLoading ? (
                <div className="flex items-center justify-center h-32 text-gray-400">Loading...</div>
              ) : partitionDetails.length === 0 ? (
                <div className="flex items-center justify-center h-32 text-gray-400">No partitions found</div>
              ) : (
                <>
                  <div className="grid grid-cols-6 gap-2 mb-4">
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Rows</div>
                      <div className="text-sm font-semibold text-green-400">
                        {partitionDetails.reduce((sum, p) => sum + Number(p.total_rows), 0).toLocaleString()}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Size on Disk</div>
                      <div className="text-sm font-semibold text-green-400">
                        {formatBytes(partitionDetails.reduce((sum, p) => sum + Number(p.total_bytes), 0))}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="flex items-center justify-between">
                        <div className="text-xs text-gray-400">Partitions</div>
                        <button
                          onClick={() => {
                            setPartsFilters({ database: [selectedTable.database], table: [selectedTable.table] });
                            setActiveTab('partitions');
                            handleCloseModal();
                          }}
                          className="text-gray-400 hover:text-blue-400 transition-colors"
                          title="View all partitions for this table"
                        >
                          <Search className="w-3 h-3" />
                        </button>
                      </div>
                      <div className="text-sm font-semibold text-white">{partitionDetails.length.toLocaleString()}</div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="flex items-center justify-between">
                        <div className="text-xs text-gray-400">Parts</div>
                        <button
                          onClick={() => {
                            setPartsFilters({ database: [selectedTable.database], table: [selectedTable.table], active: ['1'] });
                            setActiveTab('parts');
                            handleCloseModal();
                          }}
                          className="text-gray-400 hover:text-blue-400 transition-colors"
                          title="View all parts for this table"
                        >
                          <Search className="w-3 h-3" />
                        </button>
                      </div>
                      <div className="text-sm font-semibold text-white">
                        {partitionDetails.reduce((sum, p) => sum + p.parts_count, 0).toLocaleString()}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Block Range</div>
                      <div className="text-sm font-semibold text-white">
                        {Math.min(...partitionDetails.map(p => p.min_block))} - {Math.max(...partitionDetails.map(p => p.max_block))}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Last Modified</div>
                      <div className="text-sm font-semibold text-white">
                        {new Date(Math.max(...partitionDetails.map(p => new Date(p.newest_part).getTime()))).toLocaleDateString()}
                      </div>
                    </div>
                  </div>

                  {/* Schema Section */}
                  <div className="mb-4">
                    <h3 className="text-xs font-semibold text-gray-300 mb-2">Schema ({schemaColumns.length} columns)</h3>
                    {schemaLoading ? (
                      <div className="flex items-center justify-center h-20 text-gray-400">Loading schema...</div>
                    ) : schemaColumns.length === 0 ? (
                      <div className="flex items-center justify-center h-20 text-gray-400">No schema found</div>
                    ) : (
                      <div className="bg-gray-800 rounded max-h-[280px] overflow-y-auto">
                        <table className="w-full text-xs">
                          <thead className="sticky top-0 bg-gray-800">
                            <tr className="border-b border-gray-700">
                              <th className="text-left p-2 text-gray-400">Column</th>
                              <th className="text-left p-2 text-gray-400">Type</th>
                              <th className="text-center p-2 text-gray-400">Keys</th>
                              <th className="text-left p-2 text-gray-400">Codec</th>
                              <th className="text-left p-2 text-gray-400">Comment</th>
                            </tr>
                          </thead>
                          <tbody>
                            {schemaColumns.map((col) => (
                              <tr key={col.name} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                                <td className="p-2 font-mono text-blue-300">{col.name}</td>
                                <td className="p-2 text-gray-300">{col.type}</td>
                                <td className="p-2 text-center">
                                  {col.is_in_primary_key === 1 && <span className="inline-block px-1 bg-yellow-600/30 text-yellow-400 rounded text-[10px] mr-1">PK</span>}
                                  {col.is_in_partition_key === 1 && <span className="inline-block px-1 bg-purple-600/30 text-purple-400 rounded text-[10px] mr-1">PART</span>}
                                  {col.is_in_sorting_key === 1 && <span className="inline-block px-1 bg-green-600/30 text-green-400 rounded text-[10px]">SORT</span>}
                                </td>
                                <td className="p-2 text-gray-400">{col.compression_codec || '-'}</td>
                                <td className="p-2 text-gray-500 truncate max-w-[150px]" title={col.comment}>{col.comment || '-'}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>

                  {/* Partitions Section */}
                  <div>
                    <h3 className="text-xs font-semibold text-gray-300 mb-2">Partitions ({partitionDetails.length})</h3>
                    <div className="bg-gray-800 rounded max-h-[250px] overflow-y-auto">
                      <table className="w-full text-xs">
                        <thead className="sticky top-0 bg-gray-800">
                          <tr className="border-b border-gray-700">
                            <th className="text-left p-2 text-gray-400">Partition ID</th>
                            <th className="text-right p-2 text-gray-400">Parts</th>
                            <th className="text-right p-2 text-gray-400">Rows</th>
                            <th className="text-right p-2 text-gray-400">Size</th>
                            <th className="text-right p-2 text-gray-400">Block Range</th>
                            <th className="text-right p-2 text-gray-400">Newest Part</th>
                          </tr>
                        </thead>
                        <tbody>
                          {partitionDetails.map((partition) => (
                            <tr key={partition.partition_id} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                              <td className="p-2 font-mono">
                                <button
                                  onClick={() => selectedTable && handlePartitionClick(selectedTable.database, selectedTable.table, partition.partition_id || '')}
                                  className="text-blue-400 hover:text-blue-300 hover:underline cursor-pointer"
                                  title="View in Partitions tab"
                                >
                                  {partition.partition_id || '(all)'}
                                </button>
                              </td>
                              <td className="p-2 text-right text-green-400">{partition.parts_count.toLocaleString()}</td>
                              <td className="p-2 text-right text-green-400">{Number(partition.total_rows).toLocaleString()}</td>
                              <td className="p-2 text-right text-green-400">{formatBytes(Number(partition.total_bytes))}</td>
                              <td className="p-2 text-right text-gray-300">{partition.min_block} - {partition.max_block}</td>
                              <td className="p-2 text-right text-red-300">
                                {partition.newest_part
                                  ? new Date(partition.newest_part).toLocaleString('en-US', {
                                      month: 'short',
                                      day: 'numeric',
                                      hour: '2-digit',
                                      minute: '2-digit',
                                      second: '2-digit',
                                      hour12: false,
                                    })
                                  : '-'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Partition Parts Modal */}
      {selectedPartition && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleClosePartsModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[1000px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Active Parts</h2>
                <p className="text-xs text-gray-400 font-mono">
                  {selectedPartition.database}.{selectedPartition.table} / {selectedPartition.partitionId || '(all)'}
                </p>
              </div>
              <button onClick={handleClosePartsModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {partitionPartsLoading ? (
                <div className="flex items-center justify-center h-32 text-gray-400">Loading...</div>
              ) : partitionParts.length === 0 ? (
                <div className="flex items-center justify-center h-32 text-gray-400">No parts found</div>
              ) : (
                <>
                  <div className="grid grid-cols-4 gap-2 mb-4">
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="flex items-center justify-between">
                        <div className="text-xs text-gray-400">Total Parts</div>
                        <button
                          onClick={() => {
                            setPartsFilters({ database: [selectedPartition.database], table: [selectedPartition.table], partition_id: [selectedPartition.partitionId], active: ['1'] });
                            setActiveTab('parts');
                            handleClosePartsModal();
                          }}
                          className="text-gray-400 hover:text-blue-400 transition-colors"
                          title="View all parts for this partition"
                        >
                          <Search className="w-3 h-3" />
                        </button>
                      </div>
                      <div className="text-sm font-semibold text-white">{partitionParts.length.toLocaleString()}</div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Rows</div>
                      <div className="text-sm font-semibold text-green-400">
                        {partitionParts.reduce((sum, p) => sum + Number(p.rows), 0).toLocaleString()}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Size</div>
                      <div className="text-sm font-semibold text-green-400">
                        {formatBytes(partitionParts.reduce((sum, p) => sum + Number(p.bytes_on_disk), 0))}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Marks</div>
                      <div className="text-sm font-semibold text-white">
                        {partitionParts.reduce((sum, p) => sum + Number(p.marks), 0).toLocaleString()}
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-800 rounded max-h-[calc(80vh-220px)] overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="sticky top-0 bg-gray-800">
                        <tr className="border-b border-gray-700">
                          <th className="text-left p-2 text-gray-400">Name</th>
                          <th className="text-right p-2 text-gray-400">Rows</th>
                          <th className="text-right p-2 text-gray-400">Size</th>
                          <th className="text-right p-2 text-gray-400">Marks</th>
                          <th className="text-right p-2 text-gray-400">Level</th>
                          <th className="text-right p-2 text-gray-400">Block Range</th>
                          <th className="text-right p-2 text-gray-400">Modified</th>
                        </tr>
                      </thead>
                      <tbody>
                        {partitionParts.map((part) => (
                          <tr key={part.name} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                            <td className="p-2 font-mono">
                              <button
                                onClick={() => selectedPartition && handleDrillDownToPart(
                                  selectedPartition.database,
                                  selectedPartition.table,
                                  selectedPartition.partitionId,
                                  part.name
                                )}
                                className="text-blue-400 hover:text-blue-300 hover:underline text-left"
                                title="View in Parts tab"
                              >
                                {part.name}
                              </button>
                            </td>
                            <td className="p-2 text-right text-green-400">{Number(part.rows).toLocaleString()}</td>
                            <td className="p-2 text-right text-green-400">{formatBytes(Number(part.bytes_on_disk))}</td>
                            <td className="p-2 text-right text-green-400">{Number(part.marks).toLocaleString()}</td>
                            <td className="p-2 text-right text-gray-300">{part.level}</td>
                            <td className="p-2 text-right text-gray-300">{part.min_block_number} - {part.max_block_number}</td>
                            <td className="p-2 text-right text-red-300">
                              {part.modification_time
                                ? new Date(part.modification_time).toLocaleString('en-US', {
                                    month: 'short',
                                    day: 'numeric',
                                    hour: '2-digit',
                                    minute: '2-digit',
                                    second: '2-digit',
                                    hour12: false,
                                  })
                                : '-'}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Single Part Details Modal */}
      {selectedPart && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleClosePartDetails}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[500px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Part Details</h2>
                <p className="text-xs text-gray-400 font-mono">{String(selectedPart.name || '')}</p>
              </div>
              <button onClick={handleClosePartDetails} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              <div className="grid grid-cols-2 gap-2">
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Database</div>
                  <div className="text-sm font-semibold text-blue-300">{String(selectedPart.database || '-')}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Table</div>
                  <div className="text-sm font-semibold text-blue-300">{String(selectedPart.table || '-')}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Partition ID</div>
                  <div className="text-sm font-semibold text-blue-300">{String(selectedPart.partition_id || '-')}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Active</div>
                  <div className="text-sm font-semibold text-white">{selectedPart.active ? 'Yes' : 'No'}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Rows</div>
                  <div className="text-sm font-semibold text-green-400">{Number(selectedPart.rows || 0).toLocaleString()}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Marks</div>
                  <div className="text-sm font-semibold text-green-400">{Number(selectedPart.marks || 0).toLocaleString()}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Size on Disk</div>
                  <div className="text-sm font-semibold text-green-400">{formatBytes(Number(selectedPart.bytes_on_disk || 0))}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Compressed</div>
                  <div className="text-sm font-semibold text-green-400">{formatBytes(Number(selectedPart.data_compressed_bytes || 0))}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Uncompressed</div>
                  <div className="text-sm font-semibold text-green-400">{formatBytes(Number(selectedPart.data_uncompressed_bytes || 0))}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded">
                  <div className="text-xs text-gray-400">Primary Key Memory</div>
                  <div className="text-sm font-semibold text-green-400">{formatBytes(Number(selectedPart.primary_key_bytes_in_memory || 0))}</div>
                </div>
                <div className="bg-gray-800 p-2 rounded col-span-2">
                  <div className="text-xs text-gray-400">Modification Time</div>
                  <div className="text-sm font-semibold text-red-300">
                    {selectedPart.modification_time
                      ? new Date(String(selectedPart.modification_time)).toLocaleString()
                      : '-'}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Compression Details Modal */}
      {compressionTable && (
        <div className="fixed inset-0 bg-black/70 flex items-center justify-center z-50" onClick={handleCloseCompressionModal}>
          <div
            className="bg-gray-900 border border-gray-700 rounded-lg w-[700px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Compression Details</h2>
                <p className="text-xs text-gray-400 font-mono">{compressionTable.database}.{compressionTable.table}</p>
              </div>
              <button onClick={handleCloseCompressionModal} className="text-gray-400 hover:text-white">
                <X className="w-4 h-4" />
              </button>
            </div>

            <div className="flex-1 overflow-auto p-3">
              {compressionLoading ? (
                <div className="flex items-center justify-center h-32 text-gray-400">Loading...</div>
              ) : compressionDetails.length === 0 ? (
                <div className="flex items-center justify-center h-32 text-gray-400">No compression data found</div>
              ) : (
                <>
                  <div className="grid grid-cols-4 gap-2 mb-4">
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Columns</div>
                      <div className="text-sm font-semibold text-green-300">{compressionDetails.length}</div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Uncompressed</div>
                      <div className="text-sm font-semibold text-green-300">
                        {formatBytes(compressionDetails.reduce((sum, c) => sum + Number(c.uncompressed_bytes), 0))}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Compressed</div>
                      <div className="text-sm font-semibold text-green-300">
                        {formatBytes(compressionDetails.reduce((sum, c) => sum + Number(c.compressed_bytes), 0))}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Savings</div>
                      <div className="text-sm font-semibold text-yellow-300">
                        {(() => {
                          const totalCompressed = compressionDetails.reduce((sum, c) => sum + Number(c.compressed_bytes), 0);
                          const totalUncompressed = compressionDetails.reduce((sum, c) => sum + Number(c.uncompressed_bytes), 0);
                          return totalUncompressed > 0 ? `${((totalUncompressed - totalCompressed) / totalUncompressed * 100).toFixed(0)}%` : '-';
                        })()}
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-800 rounded max-h-[calc(80vh-200px)] overflow-y-auto">
                    <table className="w-full text-xs">
                      <thead className="sticky top-0 bg-gray-800">
                        <tr className="border-b border-gray-700">
                          <th className="text-left p-2 text-gray-400">Column</th>
                          <th className="text-left p-2 text-gray-400">Type</th>
                          <th className="text-right p-2 text-gray-400">Uncompressed</th>
                          <th className="text-right p-2 text-gray-400">Compressed</th>
                          <th className="text-right p-2 text-gray-400">Savings</th>
                        </tr>
                      </thead>
                      <tbody>
                        {compressionDetails.map((column) => (
                          <tr key={column.name} className="border-b border-gray-700/50 hover:bg-gray-700/30">
                            <td className="p-2 font-mono text-blue-300">{column.name}</td>
                            <td className="p-2 text-gray-400">{column.type}</td>
                            <td className="p-2 text-right text-green-300">{formatBytes(Number(column.uncompressed_bytes))}</td>
                            <td className="p-2 text-right text-green-300">{formatBytes(Number(column.compressed_bytes))}</td>
                            <td className="p-2 text-right text-yellow-300">{column.savings_pct != null && Number(column.savings_pct) >= 0 ? `${Number(column.savings_pct).toFixed(0)}%` : '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
