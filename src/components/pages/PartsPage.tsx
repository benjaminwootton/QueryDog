import { useState, useCallback, useEffect, useMemo, useRef } from 'react';
import { HardDrive, ChevronLeft, ChevronRight, Layers, Grid3X3, BarChart2, Settings, X, Eye } from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeAlpine } from 'ag-grid-community';
import type { ColDef, ICellRendererParams } from 'ag-grid-community';
import { SystemTable, type SystemTableRef } from '../SystemTable';
import { PartsFilterPanel } from '../PartsFilterPanel';
import { PartsHistogramsTab } from '../PartsHistogramsTab';
import { fetchParts, fetchPartsColumns, fetchPartsCount, fetchPartitions, fetchPartitionsColumns, fetchPartitionsCount, fetchGroupedParts, fetchTablePartitions, type GroupedPartsEntry, type TablePartitionEntry } from '../../services/api';
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

type PartsTab = 'parts' | 'partitions' | 'grouped' | 'histograms';

const PARTS_DEFAULT_VISIBLE_FIELDS = [
  'table',
  'partition_id',
  'name',
  'database',
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

  // Modal state for partition details
  const [selectedTable, setSelectedTable] = useState<{ database: string; table: string } | null>(null);
  const [partitionDetails, setPartitionDetails] = useState<TablePartitionEntry[]>([]);
  const [partitionDetailsLoading, setPartitionDetailsLoading] = useState(false);

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
  }, []);

  const fetchPartsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchParts(
      'modification_time',
      'DESC',
      filters || partsFilters,
      partsPageSize,
      partsCurrentPage * partsPageSize
    ),
    [partsFilters, partsPageSize, partsCurrentPage]
  );

  const partsFilterCount = Object.values(partsFilters).filter((v) => v.length > 0).length;

  // Partitions uses the same filters as parts
  const fetchPartitionsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchPartitions(
      'modification_time',
      'DESC',
      filters || partsFilters,
      partitionsPageSize,
      partitionsCurrentPage * partitionsPageSize
    ),
    [partsFilters, partitionsPageSize, partitionsCurrentPage]
  );

  // Fetch counts when filters change
  useEffect(() => {
    fetchPartsCount(partsFilters).then(setPartsTotalCount);
  }, [partsFilters, setPartsTotalCount]);

  useEffect(() => {
    fetchPartitionsCount(partsFilters).then(setPartitionsTotalCount);
  }, [partsFilters, setPartitionsTotalCount]);

  // Fetch grouped data
  useEffect(() => {
    if (activeTab === 'grouped') {
      setGroupedLoading(true);
      fetchGroupedParts(partsFilters)
        .then(setGroupedData)
        .finally(() => setGroupedLoading(false));
    }
  }, [activeTab, partsFilters]);

  // Reset page when filters change
  useEffect(() => {
    setPartsCurrentPage(0);
    setPartitionsCurrentPage(0);
  }, [partsFilters, setPartsCurrentPage, setPartitionsCurrentPage]);

  // Handle viewing partition details
  const handleViewPartitions = useCallback(async (database: string, table: string) => {
    setSelectedTable({ database, table });
    setPartitionDetailsLoading(true);
    try {
      const data = await fetchTablePartitions(database, table);
      setPartitionDetails(data);
    } catch (error) {
      console.error('Error fetching partition details:', error);
      setPartitionDetails([]);
    } finally {
      setPartitionDetailsLoading(false);
    }
  }, []);

  const handleCloseModal = useCallback(() => {
    setSelectedTable(null);
    setPartitionDetails([]);
  }, []);

  // Column definitions for grouped table
  const groupedColumnDefs = useMemo((): ColDef<GroupedPartsEntry>[] => [
    {
      headerName: '',
      field: 'action',
      width: 40,
      sortable: false,
      cellRenderer: (params: ICellRendererParams<GroupedPartsEntry>) => {
        if (!params.data) return null;
        return (
          <button
            onClick={() => handleViewPartitions(params.data!.database, params.data!.table)}
            className="p-1 hover:bg-gray-700 rounded text-gray-400 hover:text-blue-400 transition-colors"
            title="View partition details"
          >
            <Eye className="w-3.5 h-3.5" />
          </button>
        );
      },
    },
    {
      headerName: 'Database',
      field: 'database',
      width: 150,
      sortable: true,
    },
    {
      headerName: 'Table',
      field: 'table',
      width: 200,
      sortable: true,
      cellStyle: { color: '#60a5fa' },
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
      headerName: 'Last Modified',
      field: 'last_modification_time',
      flex: 1,
      sortable: true,
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
  ], [handleViewPartitions]);

  const defaultColDef = useMemo<ColDef>(() => ({
    resizable: true,
    suppressMovable: true,
  }), []);

  const tabs: { id: PartsTab; label: string; icon: typeof HardDrive }[] = [
    { id: 'grouped', label: 'Grouped', icon: Grid3X3 },
    { id: 'partitions', label: 'Partitions', icon: Layers },
    { id: 'parts', label: 'Parts', icon: HardDrive },
    { id: 'histograms', label: 'Histograms', icon: BarChart2 },
  ];

  // Column selector component for Parts/Partitions
  const renderColumnSelector = (
    isOpen: boolean,
    setIsOpen: (open: boolean) => void,
    tableRef: React.RefObject<SystemTableRef>
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
        </div>
        <div className="flex items-center gap-4 text-xs">
          {partsFilterCount > 0 && (
            <span className="text-blue-400">
              {partsFilterCount} filter{partsFilterCount > 1 ? 's' : ''} active
            </span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => { setActiveTab(id); setPartsColumnSelectorOpen(false); setPartitionsColumnSelectorOpen(false); }}
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
          />
        )}
        {activeTab === 'partitions' && (
          <SystemTable
            ref={partitionsTableRef}
            fetchData={fetchPartitionsWithFilters}
            fetchColumns={fetchPartitionsColumns}
            defaultVisibleFields={PARTS_DEFAULT_VISIBLE_FIELDS}
            filters={partsFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.partition_id}`}
            hideTitle
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
            className="bg-gray-900 border border-gray-700 rounded-lg w-[900px] max-h-[80vh] overflow-hidden flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between p-3 border-b border-gray-700">
              <div>
                <h2 className="text-sm font-semibold text-white">Partition Details</h2>
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
                  <div className="grid grid-cols-3 gap-2 mb-4">
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Partitions</div>
                      <div className="text-sm font-semibold text-white">{partitionDetails.length.toLocaleString()}</div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Parts</div>
                      <div className="text-sm font-semibold text-white">
                        {partitionDetails.reduce((sum, p) => sum + p.parts_count, 0).toLocaleString()}
                      </div>
                    </div>
                    <div className="bg-gray-800 p-2 rounded">
                      <div className="text-xs text-gray-400">Total Rows</div>
                      <div className="text-sm font-semibold text-white">
                        {partitionDetails.reduce((sum, p) => sum + Number(p.total_rows), 0).toLocaleString()}
                      </div>
                    </div>
                  </div>

                  <div className="bg-gray-800 rounded max-h-[calc(80vh-200px)] overflow-y-auto">
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
                            <td className="p-2 text-blue-400 font-mono">{partition.partition_id || '(all)'}</td>
                            <td className="p-2 text-right text-green-400">{partition.parts_count.toLocaleString()}</td>
                            <td className="p-2 text-right text-green-400">{Number(partition.total_rows).toLocaleString()}</td>
                            <td className="p-2 text-right text-green-400">{formatBytes(Number(partition.total_bytes))}</td>
                            <td className="p-2 text-right text-gray-300">{partition.min_block} - {partition.max_block}</td>
                            <td className="p-2 text-right text-gray-300">
                              {partition.newest_part
                                ? new Date(partition.newest_part).toLocaleString('en-US', {
                                    month: 'short',
                                    day: 'numeric',
                                    hour: '2-digit',
                                    minute: '2-digit',
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
    </div>
  );
}
