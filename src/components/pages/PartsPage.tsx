import { useState, useCallback } from 'react';
import { HardDrive, FileText, ChevronLeft, ChevronRight, Layers } from 'lucide-react';
import { SystemTable } from '../SystemTable';
import { PartLogTable } from '../PartLogTable';
import { PartLogColumnSelector } from '../PartLogColumnSelector';
import { PartLogFilterPanel } from '../PartLogFilterPanel';
import { PartsFilterPanel } from '../PartsFilterPanel';
import { TimeRangeSelector } from '../TimeRangeSelector';
import { PartLogTimelineChart } from '../PartLogTimelineChart';
import { fetchParts, fetchPartsColumns, fetchPartitions, fetchPartitionsColumns } from '../../services/api';
import { useQueryStore } from '../../stores/queryStore';

type PartsTab = 'parts' | 'partlog' | 'partitions';

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

export function PartsPage() {
  const {
    partLogTotalCount,
    partLogFieldFilters,
    partLogPageSize,
    partLogCurrentPage,
    setPartLogCurrentPage
  } = useQueryStore();
  const [activeTab, setActiveTab] = useState<PartsTab>('partlog');

  const totalPages = Math.ceil(partLogTotalCount / partLogPageSize);
  const startRow = partLogCurrentPage * partLogPageSize + 1;
  const endRow = Math.min((partLogCurrentPage + 1) * partLogPageSize, partLogTotalCount);
  const activeFilterCount = Object.values(partLogFieldFilters).filter((v) => v.length > 0).length;

  const [partsFilters, setPartsFilters] = useState<Record<string, string[]>>({});

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
    setPartsFilters({});
  }, []);

  const fetchPartsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchParts('modification_time', 'DESC', filters || partsFilters),
    [partsFilters]
  );

  const partsFilterCount = Object.values(partsFilters).filter((v) => v.length > 0).length;

  // Partitions uses the same filters as parts
  const fetchPartitionsWithFilters = useCallback(
    (filters?: Record<string, string[]>) => fetchPartitions('modification_time', 'DESC', filters || partsFilters),
    [partsFilters]
  );

  const tabs: { id: PartsTab; label: string; icon: typeof HardDrive }[] = [
    { id: 'partlog', label: 'Part Log', icon: FileText },
    { id: 'parts', label: 'Parts', icon: HardDrive },
    { id: 'partitions', label: 'Partitions', icon: Layers },
  ];

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          {activeTab === 'partlog' && <PartLogFilterPanel />}
          {(activeTab === 'parts' || activeTab === 'partitions') && (
            <PartsFilterPanel
              filters={partsFilters}
              onFilterChange={handlePartsFilterChange}
              onClearFilter={handleClearPartsFilter}
              onClearAll={handleClearAllPartsFilters}
            />
          )}
          <TimeRangeSelector />
        </div>
        <div className="flex items-center gap-4 text-xs">
          {activeTab === 'partlog' && (
            <>
              <span className="text-gray-400">
                Total: <span className="text-white font-medium">{partLogTotalCount.toLocaleString()}</span> events
              </span>
              {activeFilterCount > 0 && (
                <span className="text-blue-400">
                  {activeFilterCount} filter{activeFilterCount > 1 ? 's' : ''} active
                </span>
              )}
            </>
          )}
          {(activeTab === 'parts' || activeTab === 'partitions') && partsFilterCount > 0 && (
            <span className="text-blue-400">
              {partsFilterCount} filter{partsFilterCount > 1 ? 's' : ''} active
            </span>
          )}
        </div>
      </div>

      {/* Timeline Chart - visible on all tabs */}
      <div className="px-4 pt-4 pb-3 shrink-0">
        <PartLogTimelineChart />
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
        {tabs.map(({ id, label, icon: Icon }) => (
          <button
            key={id}
            onClick={() => setActiveTab(id)}
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
        {activeTab === 'partlog' && (
          <div className="ml-auto flex items-center gap-4">
            {totalPages > 1 && (
              <div className="flex items-center gap-2 text-xs">
                <span className="text-gray-400">
                  {startRow.toLocaleString()}-{endRow.toLocaleString()} of {partLogTotalCount.toLocaleString()}
                </span>
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => setPartLogCurrentPage(partLogCurrentPage - 1)}
                    disabled={partLogCurrentPage === 0}
                    className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                    title="Previous page"
                  >
                    <ChevronLeft className="w-4 h-4" />
                  </button>
                  <span className="text-gray-300 px-2">
                    {partLogCurrentPage + 1} / {totalPages}
                  </span>
                  <button
                    onClick={() => setPartLogCurrentPage(partLogCurrentPage + 1)}
                    disabled={partLogCurrentPage >= totalPages - 1}
                    className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                    title="Next page"
                  >
                    <ChevronRight className="w-4 h-4" />
                  </button>
                </div>
              </div>
            )}
            <PartLogColumnSelector />
          </div>
        )}
      </div>

      {/* Content */}
      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'parts' && (
          <SystemTable
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
            fetchData={fetchPartitionsWithFilters}
            fetchColumns={fetchPartitionsColumns}
            defaultVisibleFields={PARTS_DEFAULT_VISIBLE_FIELDS}
            filters={partsFilters}
            getRowId={(data) => `${data.database}-${data.table}-${data.partition_id}`}
            hideTitle
          />
        )}
        {activeTab === 'partlog' && <PartLogTable />}
      </div>
    </div>
  );
}
