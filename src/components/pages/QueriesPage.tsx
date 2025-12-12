import { useRef } from 'react';
import { Database, BarChart3, ChevronLeft, ChevronRight, Activity, Layers, Settings, BarChart2 } from 'lucide-react';
import { TimeRangeSelector } from '../TimeRangeSelector';
import { SearchBar } from '../SearchBar';
import { TimelineChart } from '../TimelineChart';
import { QueryTable } from '../QueryTable';
import { GroupedQueriesTable } from '../GroupedQueriesTable';
import { HistogramsTab } from '../HistogramsTab';
import { ProfileEventsTable, type ProfileEventsTableRef } from '../ProfileEventsTable';
import { ColumnSelector } from '../ColumnSelector';
import { FilterPanel } from '../FilterPanel';
import { useQueryStore } from '../../stores/queryStore';

export function QueriesPage() {
  const { activeTab, setActiveTab, totalCount, fieldFilters, pageSize, currentPage, setCurrentPage, normalizeQueries, setNormalizeQueries } = useQueryStore();
  const profileEventsRef = useRef<ProfileEventsTableRef>(null);

  const totalPages = Math.ceil(totalCount / pageSize);
  const startRow = currentPage * pageSize + 1;
  const endRow = Math.min((currentPage + 1) * pageSize, totalCount);

  const activeFilterCount = Object.values(fieldFilters).filter((v) => v.length > 0).length;

  return (
    <div className="h-full flex flex-col">
      {/* Filter bar */}
      <div className="bg-gray-900/50 border-b border-gray-700 px-4 py-2 flex items-center justify-between shrink-0">
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
        </div>
      </div>

      {/* Timeline Chart */}
      <div className="px-4 pt-4 pb-3 shrink-0">
        <TimelineChart />
      </div>

      {/* Tabs */}
      <div className="border-b border-gray-700 mx-4 flex items-center gap-1 shrink-0">
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
        {activeTab === 'queries' && (
          <div className="ml-auto flex items-center gap-4">
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
          </div>
        )}
        {activeTab === 'grouped' && (
          <div className="ml-auto flex items-center gap-4">
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={normalizeQueries}
                onChange={(e) => setNormalizeQueries(e.target.checked)}
                className="rounded border-gray-600 bg-gray-700 text-blue-500 focus:ring-blue-500 focus:ring-offset-0"
              />
              <span className="text-xs text-gray-300">Normalise queries</span>
            </label>
          </div>
        )}
        {activeTab === 'profileEvents' && (
          <div className="ml-auto flex items-center gap-2">
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
          </div>
        )}
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-hidden p-4">
        {activeTab === 'queries' && <QueryTable />}
        {activeTab === 'grouped' && <GroupedQueriesTable />}
        {activeTab === 'profileEvents' && <ProfileEventsTable ref={profileEventsRef} />}
        {activeTab === 'histograms' && <HistogramsTab />}
      </div>
    </div>
  );
}
