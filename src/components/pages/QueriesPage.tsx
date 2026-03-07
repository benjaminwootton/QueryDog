import { useRef, useState } from 'react';
import { Database, BarChart3, ChevronLeft, ChevronRight, Activity, Layers, Settings, BarChart2, GitCompare, Pin, X, Table, Eye } from 'lucide-react';
import { TimeRangeSelector } from '../TimeRangeSelector';
import { SearchBar } from '../SearchBar';
import { TimelineChart } from '../TimelineChart';
import { QueryTable } from '../QueryTable';
import { GroupedQueriesTable } from '../GroupedQueriesTable';
import { HistogramsTab } from '../HistogramsTab';
import { ProfileEventsTable, type ProfileEventsTableRef } from '../ProfileEventsTable';
import { ByTableTable } from '../ByTableTable';
import { QueryViewsLogTable } from '../QueryViewsLogTable';
import { ColumnSelector } from '../ColumnSelector';
import { FilterPanel } from '../FilterPanel';
import { QueryCompareModal } from '../QueryCompareModal';
import { useQueryStore } from '../../stores/queryStore';

export function QueriesPage() {
  const { activeTab, setActiveTab, totalCount, fieldFilters, pageSize, currentPage, setCurrentPage, normalizeQueries, setNormalizeQueries, selectedEntries, pinnedEntries, clearPinnedEntries } = useQueryStore();
  const profileEventsRef = useRef<ProfileEventsTableRef>(null);
  const [compareModalOpen, setCompareModalOpen] = useState(false);

  const totalPages = Math.ceil(totalCount / pageSize);
  const startRow = currentPage * pageSize + 1;
  const endRow = Math.min((currentPage + 1) * pageSize, totalCount);

  const activeFilterCount = Object.values(fieldFilters).filter((v) => v.length > 0).length;

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-1.5 h-9 flex items-center justify-between shrink-0">
        <div className="flex items-center gap-3">
          <FilterPanel />
          <TimeRangeSelector />
          <SearchBar />
        </div>
        <div className="flex items-center gap-4 text-xs">
          <span className="text-gray-400">
            Total: <span className="text-white font-medium">{totalCount.toLocaleString()}</span> queries
          </span>
          {activeFilterCount > 0 && (
            <span className="text-blue-400">
              {activeFilterCount} filter{activeFilterCount > 1 ? 's' : ''} active
            </span>
          )}
          {pinnedEntries.length > 0 && (
            <span className="flex items-center gap-1.5 text-blue-400">
              <Pin className="w-3 h-3 fill-current" />
              {pinnedEntries.length} pinned
              <button
                onClick={clearPinnedEntries}
                className="ml-1 p-0.5 hover:bg-gray-700 rounded text-gray-400 hover:text-white"
                title="Clear all pinned"
              >
                <X className="w-3 h-3" />
              </button>
            </span>
          )}
        </div>
      </div>

      {/* Timeline Chart */}
      <div className="px-1.5 pt-3 pb-2 shrink-0">
        <TimelineChart />
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center justify-between shrink-0">
        <div className="flex flex-wrap items-center gap-1">
          <button
            onClick={() => setActiveTab('queries')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'queries'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Database className="w-3 h-3" />
            Queries
          </button>
          <button
            onClick={() => setActiveTab('grouped')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'grouped'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Layers className="w-3 h-3" />
            Grouped
          </button>
          <button
            onClick={() => setActiveTab('profileEvents')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'profileEvents'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Activity className="w-3 h-3" />
            Profile Events
          </button>
          <button
            onClick={() => setActiveTab('byTable')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'byTable'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Table className="w-3 h-3" />
            By Table
          </button>
          <button
            onClick={() => setActiveTab('queryViewsLog')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'queryViewsLog'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <Eye className="w-3 h-3" />
            Views Log
          </button>
          <button
            onClick={() => setActiveTab('histograms')}
            className={`flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium border-b-2 -mb-px transition-colors ${
              activeTab === 'histograms'
                ? 'border-blue-500 text-blue-400'
                : 'border-transparent text-gray-400 hover:text-gray-300'
            }`}
          >
            <BarChart3 className="w-3 h-3" />
            Histograms
          </button>
        </div>
        {(activeTab === 'queries' || activeTab === 'grouped' || activeTab === 'profileEvents') && (
          <div className="flex items-center gap-4">
            {activeTab === 'queries' && (
              <>
                {selectedEntries.length >= 2 && (
                  <button
                    onClick={() => setCompareModalOpen(true)}
                    className="flex items-center gap-1.5 px-2 py-1 bg-blue-600 hover:bg-blue-500 rounded text-white text-xs"
                  >
                    <GitCompare className="w-3.5 h-3.5" />
                    Compare ({selectedEntries.length})
                  </button>
                )}
                {selectedEntries.length === 1 && (
                  <span className="text-xs text-gray-400">Select 1 more to compare</span>
                )}
                {totalPages > 1 && (
                  <div className="flex items-center gap-2 text-xs">
                    <span className="text-gray-400">
                      {startRow.toLocaleString()}-{endRow.toLocaleString()} of {totalCount.toLocaleString()}
                    </span>
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => setCurrentPage(currentPage - 1)}
                        disabled={currentPage === 0}
                        className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Previous page"
                      >
                        <ChevronLeft className="w-4 h-4" />
                      </button>
                      <span className="text-gray-300 px-2">
                        {currentPage + 1} / {totalPages}
                      </span>
                      <button
                        onClick={() => setCurrentPage(currentPage + 1)}
                        disabled={currentPage >= totalPages - 1}
                        className="p-1 bg-gray-700 hover:bg-gray-600 rounded text-gray-300 disabled:opacity-50 disabled:cursor-not-allowed"
                        title="Next page"
                      >
                        <ChevronRight className="w-4 h-4" />
                      </button>
                    </div>
                  </div>
                )}
                <ColumnSelector />
              </>
            )}
            {activeTab === 'grouped' && (
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="checkbox"
                  checked={normalizeQueries}
                  onChange={(e) => setNormalizeQueries(e.target.checked)}
                  className="rounded border-gray-600 bg-gray-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
                />
                <span className="text-xs text-gray-300">Normalise queries</span>
              </label>
            )}
            {activeTab === 'profileEvents' && (
              <>
                <button
                  onClick={() => profileEventsRef.current?.openChart()}
                  className="p-1.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
                  title="Chart profile events"
                >
                  <BarChart2 className="w-4 h-4" />
                </button>
                <button
                  onClick={() => profileEventsRef.current?.openColumnSelector()}
                  className="p-1.5 bg-gray-700 hover:bg-gray-600 rounded text-gray-300"
                  title="Select columns"
                >
                  <Settings className="w-4 h-4" />
                </button>
              </>
            )}
          </div>
        )}
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden px-1.5 pt-3 pb-4">
        {activeTab === 'queries' && <QueryTable />}
        {activeTab === 'grouped' && <GroupedQueriesTable />}
        {activeTab === 'profileEvents' && <ProfileEventsTable ref={profileEventsRef} />}
        {activeTab === 'byTable' && <ByTableTable />}
        {activeTab === 'queryViewsLog' && <QueryViewsLogTable />}
        {activeTab === 'histograms' && <HistogramsTab />}
      </div>

      {/* Compare Modal */}
      {compareModalOpen && (
        <QueryCompareModal
          entries={selectedEntries}
          onClose={() => setCompareModalOpen(false)}
        />
      )}
    </div>
  );
}
